import * as crypto from 'crypto';
import * as net from 'net';
import * as os from 'os';
import * as path from 'path';

interface ProxyHeaders {
  host?: string | string[];
  'proxy-authorization'?: string | string[];
}

export interface ProxyRequestLike {
  url?: string | undefined;
  headers: ProxyHeaders;
}

export interface LocalProxyTunnelConfigLike {
  user?: string;
  identityFile?: string;
  sshQueryConfigPath?: string;
}

function uniqueList(values: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const value of values) {
    if (!seen.has(value)) {
      seen.add(value);
      result.push(value);
    }
  }
  return result;
}

export function normalizeProxyPort(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  const port = Math.floor(value);
  if (port < 0 || port > 65535) {
    return 0;
  }
  return port;
}

export function buildLocalProxyRemotePortCandidates(
  basePort: number,
  attempts: number,
  random: () => number = Math.random
): number[] {
  const total = Math.max(1, Math.floor(attempts || 1));
  const ports = new Set<number>();
  const base = normalizeProxyPort(basePort);
  if (base > 0) {
    ports.add(base);
  }

  const min = 49152;
  const max = 65535;
  while (ports.size < total) {
    const port = Math.floor(random() * (max - min + 1)) + min;
    if (port > 0) {
      ports.add(port);
    }
  }
  return Array.from(ports);
}

export function normalizeHostForMatch(host: string): string {
  const trimmed = (host || '').trim().toLowerCase();
  if (!trimmed) {
    return '';
  }
  if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
    return trimmed.slice(1, -1);
  }
  return trimmed.endsWith('.') ? trimmed.slice(0, -1) : trimmed;
}

export function extractIpv4FromMappedIpv6(host: string): string | undefined {
  const match = /^(?:0:0:0:0:0:ffff:|::ffff:)(.+)$/.exec(host);
  if (!match) {
    return undefined;
  }

  const tail = match[1];
  if (tail.includes('.')) {
    return net.isIP(tail) === 4 ? tail : undefined;
  }

  const parts = tail.split(':');
  const isHex = (value: string): boolean => /^[0-9a-f]{1,4}$/.test(value);
  let num: number | undefined;
  if (parts.length === 2 && parts.every(isHex)) {
    num = (parseInt(parts[0], 16) << 16) | parseInt(parts[1], 16);
  } else if (parts.length === 1 && /^[0-9a-f]{1,8}$/.test(parts[0])) {
    num = parseInt(parts[0], 16);
  }
  if (num === undefined || !Number.isFinite(num)) {
    return undefined;
  }

  const bytes = [
    (num >>> 24) & 0xff,
    (num >>> 16) & 0xff,
    (num >>> 8) & 0xff,
    num & 0xff
  ];
  return bytes.join('.');
}

export function isLoopbackHost(host: string): boolean {
  const normalized = normalizeHostForMatch(host);
  if (!normalized) {
    return false;
  }
  if (normalized === 'localhost' || normalized === '0.0.0.0' || normalized === '::' || normalized === '::1') {
    return true;
  }
  if (normalized === 'ip6-localhost' || normalized === 'ip6-loopback') {
    return true;
  }

  const ipType = net.isIP(normalized);
  if (ipType === 4) {
    return normalized.startsWith('127.') || normalized === '0.0.0.0';
  }
  if (ipType === 6) {
    if (normalized === '0:0:0:0:0:0:0:1') {
      return true;
    }
    const mapped = extractIpv4FromMappedIpv6(normalized);
    if (mapped) {
      return mapped.startsWith('127.') || mapped === '0.0.0.0';
    }
  }
  return false;
}

export function splitHostPort(input: string): { host: string; port?: number } {
  const trimmed = (input || '').trim();
  if (!trimmed) {
    return { host: '' };
  }
  if (trimmed.startsWith('[')) {
    const closeIndex = trimmed.indexOf(']');
    if (closeIndex > 0) {
      const host = trimmed.slice(1, closeIndex);
      const rest = trimmed.slice(closeIndex + 1);
      if (rest.startsWith(':')) {
        const port = Number(rest.slice(1));
        return { host, port: Number.isFinite(port) ? port : undefined };
      }
      return { host };
    }
  }
  const lastColon = trimmed.lastIndexOf(':');
  if (lastColon > 0 && trimmed.indexOf(':') === lastColon) {
    const host = trimmed.slice(0, lastColon);
    const port = Number(trimmed.slice(lastColon + 1));
    return { host, port: Number.isFinite(port) ? port : undefined };
  }
  return { host: trimmed };
}

export function isHostAllowed(host: string): boolean {
  if (!host) {
    return false;
  }
  return !isLoopbackHost(host);
}

export function buildNoProxyValue(values: string[]): string {
  const cleaned = values.map((entry) => entry.trim()).filter(Boolean);
  if (cleaned.length === 0) {
    return '';
  }
  return uniqueList(cleaned).join(',');
}

export function buildLocalProxyTunnelConfigKey(
  cfg: LocalProxyTunnelConfigLike,
  loginHost: string,
  remoteBind: string,
  localPort: number
): string {
  return JSON.stringify({
    loginHost,
    user: cfg.user || '',
    identityFile: cfg.identityFile || '',
    sshQueryConfigPath: cfg.sshQueryConfigPath || '',
    remoteBind,
    localPort
  });
}

export function buildLocalProxyTunnelTarget(
  cfg: LocalProxyTunnelConfigLike,
  loginHost: string
): string {
  return cfg.user ? `${cfg.user}@${loginHost}` : loginHost;
}

export function buildLocalProxyControlPath(key: string): string {
  const hash = crypto.createHash('sha256').update(key).digest('hex').slice(0, 12);
  const baseDir = process.platform === 'win32' ? os.tmpdir() : '/tmp';
  return path.join(baseDir, `slurm-connect-tunnel-${hash}.sock`);
}

export function resolveRemoteBindConnectHost(bindHost: string): string {
  const trimmed = (bindHost || '').trim().toLowerCase();
  if (!trimmed || trimmed === '0.0.0.0' || trimmed === '::' || trimmed === '[::]') {
    return '127.0.0.1';
  }
  return trimmed;
}

export function parseProxyTarget(req: ProxyRequestLike): {
  hostname: string;
  port: number;
  path: string;
  isHttps: boolean;
  hostHeader: string;
} | null {
  const rawUrl = String(req.url || '');
  let parsed: URL;
  if (/^https?:\/\//i.test(rawUrl)) {
    try {
      parsed = new URL(rawUrl);
    } catch {
      return null;
    }
  } else {
    const rawHost = req.headers.host;
    const hostHeader = Array.isArray(rawHost) ? rawHost[0] : String(rawHost || '').trim();
    if (!hostHeader) {
      return null;
    }
    try {
      parsed = new URL(`http://${hostHeader}${rawUrl}`);
    } catch {
      return null;
    }
  }

  const hostnameRaw = parsed.hostname;
  const hostname =
    hostnameRaw.startsWith('[') && hostnameRaw.endsWith(']')
      ? hostnameRaw.slice(1, -1)
      : hostnameRaw;
  const isHttps = parsed.protocol === 'https:';
  const port = parsed.port ? Number(parsed.port) : isHttps ? 443 : 80;
  if (!hostname || !Number.isFinite(port)) {
    return null;
  }

  const pathValue = `${parsed.pathname || '/'}${parsed.search || ''}`;
  const hostHeader = (() => {
    const isIpv6 = net.isIP(hostname) === 6;
    if (parsed.port) {
      return isIpv6 ? `[${hostname}]:${parsed.port}` : `${hostname}:${parsed.port}`;
    }
    return isIpv6 ? `[${hostname}]` : hostname;
  })();

  return {
    hostname,
    port,
    path: pathValue,
    isHttps,
    hostHeader
  };
}

export function isProxyAuthValid(
  req: ProxyRequestLike,
  authUser: string,
  authToken: string
): boolean {
  const header = req.headers['proxy-authorization'];
  if (!header) {
    return false;
  }
  const value = Array.isArray(header) ? header[0] : header;
  const match = /^Basic\s+(.+)$/i.exec(value.trim());
  if (!match) {
    return false;
  }

  let decoded = '';
  try {
    decoded = Buffer.from(match[1], 'base64').toString('utf8');
  } catch {
    return false;
  }

  const separatorIndex = decoded.indexOf(':');
  if (separatorIndex < 0) {
    return false;
  }

  const user = decoded.slice(0, separatorIndex);
  const pass = decoded.slice(separatorIndex + 1);
  return user === authUser && pass === authToken;
}
