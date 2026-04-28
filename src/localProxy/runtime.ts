import * as crypto from 'crypto';
import * as http from 'http';
import * as https from 'https';
import * as net from 'net';
import * as fs from 'fs/promises';
import * as vscode from 'vscode';
import { execFile } from 'child_process';
import { promisify } from 'util';

import {
  buildLocalProxyControlPath,
  buildLocalProxyTunnelConfigKey,
  buildLocalProxyTunnelTarget,
  buildNoProxyValue,
  isHostAllowed,
  isProxyAuthValid,
  normalizeProxyPort,
  parseProxyTarget,
  resolveRemoteBindConnectHost,
  splitHostPort
} from '../utils/localProxy';
import {
  isShellOperatorToken,
  joinShellCommand,
  quoteShellArg,
  splitShellArgs
} from '../utils/shellArgs';
import { expandHome } from '../utils/sshConfig';
import type { ConnectionState } from '../state/connectionState';
import type { LocalProxyPlan } from '../connect/types';
import type { SlurmConnectConfig } from '../config/types';
import type { StoredConnectionState } from './state';

const execFileAsync = promisify(execFile);

const LOCAL_PROXY_TUNNEL_STATE_KEY = 'slurmConnect.localProxyTunnelState';
const LOCAL_PROXY_RUNTIME_STATE_KEY = 'slurmConnect.localProxyRuntimeState';
const DEFAULT_PROXY_DEBUG_LOG_FILE_TEMPLATE = '~/.slurm-connect/vscode-proxy-[PID].log';
const DEFAULT_SESSION_STATE_DIR = '~/.slurm-connect';
const LOCAL_PROXY_AUTH_USER = 'slurm-connect';
const SETTINGS_SECTION = 'slurmConnect';

interface LocalProxyState {
  server: http.Server;
  port: number;
  authUser: string;
  authToken: string;
}

interface LocalProxyRuntimeState {
  port: number;
  authUser: string;
  authToken: string;
  loginHost: string;
  remoteBind: string;
}

interface LocalProxyTunnelState {
  remotePort: number;
  configKey: string;
  controlPath: string;
  target: string;
}

interface LocalProxyEnv {
  proxyUrl: string;
  noProxy?: string;
}

export interface LocalProxyDependencies {
  getOutputChannel(): vscode.OutputChannel;
  formatError(error: unknown): string;
  getGlobalState(): vscode.Memento | undefined;
  getConfig(): SlurmConnectConfig;
  getConnectionState(): ConnectionState;
  getStoredConnectionState(): StoredConnectionState | undefined;
  resolveRemoteSshAlias(): string | undefined;
  hasSessionE2EContext(): boolean;
  resolveSshToolPath(tool: 'ssh'): Promise<string>;
  normalizeSshErrorText(error: unknown): string;
  pickSshErrorSummary(text: string): string;
  runSshCommand(host: string, cfg: SlurmConnectConfig, command: string): Promise<string>;
  runPreSshCommandInTerminal(
    command: string,
    options?: {
      terminalName?: string;
      promptMessage?: string;
      failureMessage?: string;
      wrapInBash?: boolean;
    }
  ): Promise<void>;
}

export interface LocalProxyRuntime {
  cancelPersistentSessionJob(
    loginHost: string,
    sessionKey: string | undefined,
    cfg: SlurmConnectConfig,
    options?: { useTerminal?: boolean }
  ): Promise<boolean>;
  ensureLocalProxyPlan(
    cfg: SlurmConnectConfig,
    loginHost: string,
    options?: { remotePortOverride?: number }
  ): Promise<LocalProxyPlan>;
  buildRemoteCommand(
    cfg: SlurmConnectConfig,
    sallocArgs: string[],
    sessionKey?: string,
    clientId?: string,
    installSnippet?: string,
    localProxyEnv?: { proxyUrl: string; noProxy?: string },
    localProxyPlan?: LocalProxyPlan
  ): string;
  stopLocalProxyServer(options?: { stopTunnel?: boolean; clearRuntimeState?: boolean }): void;
  resumeLocalProxyForRemoteSession(): Promise<void>;
  getCurrentLocalProxyPort(): number;
  resetState(): void;
}

function stripModuleTagSuffix(token: string): string {
  return token.replace(/\(.+\)$/, '');
}

function escapeModuleLoadCommand(command: string): string {
  const trimmed = command.trim();
  if (!trimmed) {
    return trimmed;
  }
  const tokens = splitShellArgs(trimmed);
  if (tokens.length === 0) {
    return trimmed;
  }
  const moduleSubcommands = new Set(['load', 'add', 'switch', 'swap', 'try-load', 'unload']);
  const result: string[] = [];
  let changed = false;
  let i = 0;
  while (i < tokens.length) {
    const token = tokens[i];
    if (isShellOperatorToken(token)) {
      result.push(token);
      i += 1;
      continue;
    }
    const lower = token.toLowerCase();
    if (lower === 'module' && i + 1 < tokens.length) {
      const sub = tokens[i + 1];
      const lowerSub = sub.toLowerCase();
      if (moduleSubcommands.has(lowerSub)) {
        result.push(token, sub);
        i += 2;
        while (i < tokens.length && !isShellOperatorToken(tokens[i])) {
          const cleaned = stripModuleTagSuffix(tokens[i]);
          if (!cleaned) {
            i += 1;
            continue;
          }
          const escaped = quoteShellArg(cleaned);
          if (cleaned !== tokens[i] || escaped !== cleaned) {
            changed = true;
          }
          result.push(escaped);
          i += 1;
        }
        continue;
      }
    }
    if (lower === 'ml') {
      result.push(token);
      i += 1;
      while (i < tokens.length && !isShellOperatorToken(tokens[i])) {
        const cleaned = stripModuleTagSuffix(tokens[i]);
        if (!cleaned) {
          i += 1;
          continue;
        }
        const escaped = quoteShellArg(cleaned);
        if (cleaned !== tokens[i] || escaped !== cleaned) {
          changed = true;
        }
        result.push(escaped);
        i += 1;
      }
      continue;
    }
    result.push(token);
    i += 1;
  }
  if (!changed) {
    return trimmed;
  }
  return result.join(' ');
}

function buildSessionJobLookupScript(stateDir: string, sessionKey: string): string {
  const dirLiteral = JSON.stringify(stateDir);
  const keyLiteral = JSON.stringify(sessionKey);
  return [
    'import json, os, subprocess, sys, re',
    `state_dir = os.path.expanduser(${dirLiteral})`,
    `raw_key = ${keyLiteral}`,
    'def sanitize(value):',
    '    trimmed = (value or "").strip()',
    '    if not trimmed:',
    '        return "default"',
    '    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "-", trimmed).strip(".-")',
    '    sanitized = sanitized or "default"',
    '    return sanitized[:64]',
    'sessions_dir = os.path.join(state_dir, "sessions")',
    'safe_key = sanitize(raw_key)',
    'try:',
    '    user = os.environ.get("USER") or subprocess.check_output(["id", "-un"], text=True).strip()',
    'except Exception:',
    '    user = None',
    'safe_user = sanitize(user or "unknown")',
    'candidates = []',
    'user_root = os.path.join(sessions_dir, safe_user)',
    'if os.path.isdir(user_root):',
    '    candidates.append(os.path.join(user_root, safe_key, "job.json"))',
    'candidates.append(os.path.join(sessions_dir, safe_key, "job.json"))',
    'job_id = ""',
    'for path in candidates:',
    '    if not os.path.isfile(path):',
    '        continue',
    '    try:',
    '        with open(path, "r") as handle:',
    '            data = json.load(handle)',
    '        job_id = str(data.get("job_id") or "").strip()',
    '        if job_id:',
    '            break',
    '    except Exception:',
    '        pass',
    'if not job_id:',
    '    search_roots = []',
    '    if os.path.isdir(user_root):',
    '        search_roots.append(user_root)',
    '    if os.path.isdir(sessions_dir):',
    '        search_roots.append(sessions_dir)',
    '    for root in search_roots:',
    '        for entry in sorted(os.listdir(root)):',
    '            path = os.path.join(root, entry, "job.json")',
    '            if not os.path.isfile(path):',
    '                continue',
    '            try:',
    '                with open(path, "r") as handle:',
    '                    data = json.load(handle)',
    '            except Exception:',
    '                continue',
    '            entry_key = str(data.get("session_key") or entry).strip()',
    '            if entry_key == raw_key or sanitize(entry_key) == safe_key:',
    '                job_id = str(data.get("job_id") or "").strip()',
    '                if job_id:',
    '                    break',
    '        if job_id:',
    '            break',
    'print(job_id)'
  ].join('\n');
}

function buildLocalCancelPersistentSessionCommand(stateDir: string, sessionKey?: string): string {
  const key = (sessionKey || '').trim();
  const lines = [
    'set +e',
    'JOB_ID="${SLURM_JOB_ID:-${SLURM_JOBID:-}}"',
    'if [ -n "$JOB_ID" ]; then',
    '  echo "Slurm Connect: cancelling job $JOB_ID (from SLURM_JOB_ID)"',
    '  scancel "$JOB_ID"',
    '  exit $?',
    'fi'
  ];
  if (!key) {
    lines.push('echo "Slurm Connect: no session key available to look up job id."');
    lines.push('exit 1');
    return lines.join('\n');
  }
  const script = buildSessionJobLookupScript(stateDir, key);
  return lines
    .concat([
      'PYTHON=$(command -v python3 || command -v python || true)',
      'if [ -z "$PYTHON" ]; then',
      '  echo "Slurm Connect: python not available to resolve session job."',
      '  exit 1',
      'fi',
      'JOB_ID=$("$PYTHON" - <<\'PY\'',
      script,
      'PY',
      ')',
      'JOB_ID=$(printf "%s" "$JOB_ID" | tr -d "\\r" | tail -n 1)',
      'if [ -z "$JOB_ID" ]; then',
      '  echo "Slurm Connect: no session job found."',
      '  exit 1',
      'fi',
      'echo "Slurm Connect: cancelling job $JOB_ID"',
      'scancel "$JOB_ID"'
    ])
    .join('\n');
}

function buildProxyArgs(
  cfg: SlurmConnectConfig,
  sessionKey?: string,
  clientId?: string,
  localProxyPlan?: LocalProxyPlan
): string[] {
  const args = cfg.proxyArgs.filter(Boolean);
  if (cfg.proxyDebugLogging) {
    const hasVerbose = args.some((arg) => /^-v+$/.test(arg) || arg === '--verbose');
    const hasQuiet = args.some((arg) => /^-q+$/.test(arg) || arg === '--quiet');
    if (!hasVerbose && !hasQuiet) {
      args.push('-vv');
    }
    const hasLogFile = args.some((arg) => arg === '--log-file' || arg.startsWith('--log-file='));
    if (!hasLogFile) {
      args.push(`--log-file=${DEFAULT_PROXY_DEBUG_LOG_FILE_TEMPLATE}`);
    }
  }
  if (localProxyPlan?.computeTunnel) {
    const tunnel = localProxyPlan.computeTunnel;
    args.push('--local-proxy-tunnel');
    args.push(`--local-proxy-tunnel-host=${tunnel.loginHost}`);
    args.push(`--local-proxy-tunnel-port=${tunnel.port}`);
    if (tunnel.loginUser) {
      args.push(`--local-proxy-tunnel-user=${tunnel.loginUser}`);
    }
    args.push(`--local-proxy-auth-user=${tunnel.authUser}`);
    args.push(`--local-proxy-auth-token=${tunnel.authToken}`);
    const tunnelTimeout = resolveLocalProxyTunnelTimeoutSeconds();
    if (tunnelTimeout) {
      args.push(`--local-proxy-tunnel-timeout=${tunnelTimeout}`);
    }
    if (tunnel.noProxy) {
      args.push(`--local-proxy-no-proxy=${tunnel.noProxy}`);
    }
    const probeUrl = (process.env.SLURM_CONNECT_SESSION_E2E_PROXY_PROBE_TARGET_URL || '').trim();
    if (probeUrl) {
      args.push(`--local-proxy-probe-url=${probeUrl}`);
      const probeToken = (process.env.SLURM_CONNECT_SESSION_E2E_PROXY_PROBE_EXPECTED_TOKEN || '').trim();
      if (probeToken) {
        args.push(`--local-proxy-probe-token=${probeToken}`);
      }
    }
  }
  const key = (sessionKey || cfg.sessionKey || '').trim();
  if (cfg.sessionMode !== 'persistent') {
    if (key) {
      args.push(`--session-key=${key}`);
    }
    return args;
  }
  const sessionArgs = ['--session-mode=persistent'];
  if (key) {
    sessionArgs.push(`--session-key=${key}`);
  }
  if (clientId && clientId.trim().length > 0) {
    sessionArgs.push(`--session-client-id=${clientId.trim()}`);
  }
  if (cfg.sessionIdleTimeoutSeconds > 0) {
    sessionArgs.push(`--session-idle-timeout=${Math.floor(cfg.sessionIdleTimeoutSeconds)}`);
  }
  if (cfg.sessionStateDir && cfg.sessionStateDir.trim().length > 0) {
    sessionArgs.push(`--session-state-dir=${cfg.sessionStateDir.trim()}`);
  }
  return args.concat(sessionArgs);
}

function resolveLocalProxyTunnelTimeoutSeconds(): number | undefined {
  const cfg = vscode.workspace.getConfiguration(SETTINGS_SECTION);
  const configured = Number(cfg.get<number>('sshConnectTimeoutSeconds', 15));
  if (Number.isFinite(configured) && configured > 0) {
    return Math.floor(configured);
  }
  const remoteTimeout = vscode.workspace.getConfiguration('remote.SSH').get<number>('connectTimeout');
  if (Number.isFinite(remoteTimeout) && Number(remoteTimeout) > 0) {
    return Math.floor(Number(remoteTimeout));
  }
  return undefined;
}

function buildProxyEnvTokens(env?: LocalProxyEnv): string[] {
  if (!env || !env.proxyUrl) {
    return [];
  }
  const tokens = [
    'env',
    `HTTP_PROXY=${env.proxyUrl}`,
    `HTTPS_PROXY=${env.proxyUrl}`,
    `http_proxy=${env.proxyUrl}`,
    `https_proxy=${env.proxyUrl}`,
    `ALL_PROXY=${env.proxyUrl}`,
    `all_proxy=${env.proxyUrl}`
  ];
  if (env.noProxy) {
    tokens.push(`NO_PROXY=${env.noProxy}`, `no_proxy=${env.noProxy}`);
  }
  return tokens;
}

function appendSshHostKeyCheckingArgs(args: string[], cfg: SlurmConnectConfig): void {
  args.push('-o', `StrictHostKeyChecking=${cfg.sshHostKeyChecking}`);
}

export function createLocalProxyRuntime(deps: LocalProxyDependencies): LocalProxyRuntime {
  let localProxyState: LocalProxyState | undefined;
  let localProxyTunnelState: LocalProxyTunnelState | undefined;
  let localProxyConfigKey: string | undefined;

  function loadStoredLocalProxyTunnelState(): LocalProxyTunnelState | undefined {
    const globalState = deps.getGlobalState();
    if (!globalState) {
      return undefined;
    }
    const stored = globalState.get<LocalProxyTunnelState>(LOCAL_PROXY_TUNNEL_STATE_KEY);
    if (!stored || typeof stored !== 'object') {
      return undefined;
    }
    if (!stored.controlPath || !stored.target || !stored.configKey || !stored.remotePort) {
      persistLocalProxyTunnelState(undefined);
      return undefined;
    }
    return stored;
  }

  function persistLocalProxyTunnelState(state?: LocalProxyTunnelState): void {
    const globalState = deps.getGlobalState();
    if (!globalState) {
      return;
    }
    void globalState.update(LOCAL_PROXY_TUNNEL_STATE_KEY, state);
  }

  function loadStoredLocalProxyRuntimeState(): LocalProxyRuntimeState | undefined {
    const globalState = deps.getGlobalState();
    if (!globalState) {
      return undefined;
    }
    const stored = globalState.get<LocalProxyRuntimeState>(LOCAL_PROXY_RUNTIME_STATE_KEY);
    if (!stored || typeof stored !== 'object') {
      return undefined;
    }
    if (!stored.port || !stored.authUser || !stored.authToken || !stored.loginHost || !stored.remoteBind) {
      persistLocalProxyRuntimeState(undefined);
      return undefined;
    }
    return stored;
  }

  function persistLocalProxyRuntimeState(state?: LocalProxyRuntimeState): void {
    const globalState = deps.getGlobalState();
    if (!globalState) {
      return;
    }
    void globalState.update(LOCAL_PROXY_RUNTIME_STATE_KEY, state);
  }

  async function cleanupControlPath(controlPath: string): Promise<void> {
    if (!controlPath) {
      return;
    }
    try {
      await fs.unlink(controlPath);
    } catch {
      // Ignore cleanup failures.
    }
  }

  async function pickRemoteForwardPort(
    loginHost: string,
    cfg: SlurmConnectConfig,
    bindHost: string
  ): Promise<number> {
    const host = (bindHost || '').trim() || '127.0.0.1';
    const script = [
      'import socket',
      `host=${JSON.stringify(host)}`,
      's=socket.socket()',
      's.bind((host,0))',
      'print(s.getsockname()[1])',
      's.close()'
    ].join('\n');
    const command = [
      'PYTHON=$(command -v python3 || command -v python || true)',
      'if [ -z "$PYTHON" ]; then exit 1; fi',
      '"$PYTHON" - <<\'PY\'',
      script,
      'PY'
    ].join('\n');
    const output = await deps.runSshCommand(loginHost, cfg, command);
    const raw = output
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .pop();
    const port = raw ? Number(raw) : NaN;
    if (!Number.isFinite(port) || port <= 0 || port > 65535) {
      throw new Error(`Unable to parse remote port from: ${output}`);
    }
    return Math.floor(port);
  }

  async function verifyRemoteForwardListener(
    cfg: SlurmConnectConfig,
    loginHost: string,
    bindHost: string,
    remotePort: number
  ): Promise<boolean> {
    const host = resolveRemoteBindConnectHost(bindHost);
    const script = [
      'import socket,sys',
      `host=${JSON.stringify(host)}`,
      `port=${Math.floor(remotePort)}`,
      's=socket.socket()',
      's.settimeout(1.5)',
      'err=s.connect_ex((host,port))',
      's.close()',
      'sys.exit(0 if err==0 else 1)'
    ].join('\n');
    const command = [
      'PYTHON=$(command -v python3 || command -v python || true)',
      'if [ -z "$PYTHON" ]; then exit 1; fi',
      '"$PYTHON" - <<\'PY\'',
      script,
      'PY'
    ].join('\n');
    try {
      await deps.runSshCommand(loginHost, cfg, command);
      return true;
    } catch {
      return false;
    }
  }

  function buildLocalProxyRuntimeState(
    loginHost: string,
    remoteBind: string,
    state: LocalProxyState
  ): LocalProxyRuntimeState {
    return {
      port: state.port,
      authUser: state.authUser,
      authToken: state.authToken,
      loginHost,
      remoteBind
    };
  }

  async function checkLocalProxyTunnel(state: LocalProxyTunnelState, cfg: SlurmConnectConfig): Promise<boolean> {
    const sshPath = await deps.resolveSshToolPath('ssh');
    const args: string[] = [];
    if (cfg.sshQueryConfigPath) {
      args.push('-F', expandHome(cfg.sshQueryConfigPath));
    }
    args.push('-S', state.controlPath, '-O', 'check');
    args.push('-o', 'BatchMode=yes', '-o', `ConnectTimeout=${cfg.sshConnectTimeoutSeconds}`);
    appendSshHostKeyCheckingArgs(args, cfg);
    if (cfg.identityFile) {
      args.push('-i', expandHome(cfg.identityFile));
    }
    args.push(state.target);
    try {
      await execFileAsync(sshPath, args, { timeout: cfg.sshConnectTimeoutSeconds * 1000 });
      return true;
    } catch {
      await cleanupControlPath(state.controlPath);
      return false;
    }
  }

  function stopLocalProxyTunnel(): void {
    const state = localProxyTunnelState ?? loadStoredLocalProxyTunnelState();
    if (!state) {
      return;
    }
    const args = ['-S', state.controlPath, '-O', 'exit', state.target];
    void deps.resolveSshToolPath('ssh')
      .then(async (sshPath) => await execFileAsync(sshPath, args, { timeout: 5000 }))
      .catch(() => undefined);
    localProxyTunnelState = undefined;
    persistLocalProxyTunnelState(undefined);
  }

  function buildLocalProxyTunnelArgs(
    cfg: SlurmConnectConfig,
    loginHost: string,
    remoteBind: string,
    remotePort: number,
    localPort: number,
    controlPath: string
  ): string[] {
    const args: string[] = [];
    if (cfg.sshQueryConfigPath) {
      args.push('-F', expandHome(cfg.sshQueryConfigPath));
    }
    args.push('-M', '-S', controlPath);
    args.push('-N', '-f');
    args.push('-o', 'BatchMode=yes');
    args.push('-o', `ConnectTimeout=${cfg.sshConnectTimeoutSeconds}`);
    appendSshHostKeyCheckingArgs(args, cfg);
    args.push('-o', 'ExitOnForwardFailure=yes');
    args.push('-o', 'ServerAliveInterval=30');
    args.push('-o', 'ServerAliveCountMax=3');
    args.push('-R', `${remoteBind}:${remotePort}:127.0.0.1:${localPort}`);
    if (cfg.identityFile) {
      args.push('-i', expandHome(cfg.identityFile));
    }
    args.push(buildLocalProxyTunnelTarget(cfg, loginHost));
    return args;
  }

  async function startLocalProxyTunnel(
    cfg: SlurmConnectConfig,
    loginHost: string,
    remoteBind: string,
    localPort: number,
    configKey: string
  ): Promise<LocalProxyTunnelState> {
    const log = deps.getOutputChannel();
    const sshPath = await deps.resolveSshToolPath('ssh');
    const maxAttempts = 3;
    let lastError: Error | undefined;
    const target = buildLocalProxyTunnelTarget(cfg, loginHost);

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      let remotePort: number;
      try {
        remotePort = await pickRemoteForwardPort(loginHost, cfg, remoteBind);
      } catch (error) {
        lastError = error as Error;
        break;
      }
      const controlPath = buildLocalProxyControlPath(`${configKey}:${remotePort}`);
      await cleanupControlPath(controlPath);
      log.appendLine(
        `Starting local proxy tunnel (${attempt}/${maxAttempts}): ${remoteBind}:${remotePort} -> 127.0.0.1:${localPort}`
      );
      const args = buildLocalProxyTunnelArgs(cfg, loginHost, remoteBind, remotePort, localPort, controlPath);
      try {
        const { stderr } = await execFileAsync(sshPath, args, { timeout: cfg.sshConnectTimeoutSeconds * 1000 });
        if (stderr && /remote port forwarding failed/i.test(stderr)) {
          log.appendLine(`Local proxy tunnel SSH warning: ${stderr.trim()}`);
          lastError = new Error('Remote port forwarding failed');
          await cleanupControlPath(controlPath);
          continue;
        }
      } catch (error) {
        const errorText = deps.normalizeSshErrorText(error);
        const summary = deps.pickSshErrorSummary(errorText);
        log.appendLine(`Local proxy tunnel SSH failed: ${summary || errorText}`);
        lastError = new Error(summary || 'SSH tunnel failed');
        continue;
      }
      const state: LocalProxyTunnelState = {
        remotePort,
        configKey,
        controlPath,
        target
      };
      if (!(await checkLocalProxyTunnel(state, cfg))) {
        log.appendLine('Local proxy tunnel did not respond to control check.');
        await cleanupControlPath(controlPath);
        lastError = new Error('SSH tunnel failed to start.');
        continue;
      }
      if (!(await verifyRemoteForwardListener(cfg, loginHost, remoteBind, remotePort))) {
        log.appendLine(`Local proxy tunnel listener not detected on ${remoteBind}:${remotePort}.`);
        await cleanupControlPath(controlPath);
        lastError = new Error('Remote proxy listener not available.');
        continue;
      }
      persistLocalProxyTunnelState(state);
      log.appendLine(`Local proxy tunnel ready on ${remoteBind}:${remotePort}`);
      return state;
    }

    throw lastError ?? new Error('Failed to start local proxy tunnel.');
  }

  async function ensureLocalProxyTunnel(
    cfg: SlurmConnectConfig,
    loginHost: string,
    remoteBind: string,
    localPort: number
  ): Promise<number> {
    const configKey = buildLocalProxyTunnelConfigKey(cfg, loginHost, remoteBind, localPort);
    const inMemory = localProxyTunnelState;
    if (inMemory && inMemory.configKey === configKey) {
      if (await checkLocalProxyTunnel(inMemory, cfg)) {
        return inMemory.remotePort;
      }
      stopLocalProxyTunnel();
    }
    const stored = loadStoredLocalProxyTunnelState();
    if (stored && stored.configKey === configKey) {
      if (await checkLocalProxyTunnel(stored, cfg)) {
        localProxyTunnelState = stored;
        return stored.remotePort;
      }
      stopLocalProxyTunnel();
    }
    stopLocalProxyTunnel();
    const state = await startLocalProxyTunnel(cfg, loginHost, remoteBind, localPort, configKey);
    localProxyTunnelState = state;
    return state.remotePort;
  }

  async function startLocalProxyServer(
    port: number,
    authOverride?: { authUser: string; authToken: string }
  ): Promise<LocalProxyState> {
    const authUser = authOverride?.authUser || LOCAL_PROXY_AUTH_USER;
    const authToken = authOverride?.authToken || crypto.randomBytes(18).toString('hex');
    const server = http.createServer((req, res) => {
      if (!isProxyAuthValid(req, authUser, authToken)) {
        res.writeHead(407, {
          'Proxy-Authenticate': 'Basic realm="Slurm Connect"',
          Connection: 'close'
        });
        res.end('Proxy authentication required.');
        return;
      }
      const target = parseProxyTarget(req);
      if (!target) {
        res.writeHead(400, { Connection: 'close' });
        res.end('Unable to parse proxy target.');
        return;
      }
      if (!isHostAllowed(target.hostname)) {
        res.writeHead(403, { Connection: 'close' });
        res.end('Proxy target not allowed.');
        return;
      }
      const headers = { ...req.headers };
      delete headers['proxy-authorization'];
      delete headers['proxy-connection'];
      delete headers.connection;
      headers.host = target.hostHeader;
      const requestOptions = {
        hostname: target.hostname,
        port: target.port,
        method: req.method,
        path: target.path,
        headers
      };
      const proxyReq = (target.isHttps ? https : http).request(requestOptions, (proxyRes) => {
        res.writeHead(proxyRes.statusCode || 502, proxyRes.headers);
        proxyRes.pipe(res);
      });
      proxyReq.on('error', () => {
        res.writeHead(502, { Connection: 'close' });
        res.end('Proxy request failed.');
      });
      req.pipe(proxyReq);
    });

    server.on('connect', (req: http.IncomingMessage, clientSocket: net.Socket, head: Buffer) => {
      if (!isProxyAuthValid(req, authUser, authToken)) {
        clientSocket.write('HTTP/1.1 407 Proxy Authentication Required\r\n');
        clientSocket.write('Proxy-Authenticate: Basic realm="Slurm Connect"\r\n');
        clientSocket.write('Connection: close\r\n\r\n');
        clientSocket.destroy();
        return;
      }
      const targetValue = String(req.url || '').trim();
      const { host, port: targetPort } = splitHostPort(targetValue);
      if (!host || !targetPort) {
        clientSocket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
        clientSocket.destroy();
        return;
      }
      if (!isHostAllowed(host)) {
        clientSocket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
        clientSocket.destroy();
        return;
      }
      const upstream = net.connect(targetPort, host, () => {
        clientSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
        if (head && head.length > 0) {
          upstream.write(head);
        }
        clientSocket.pipe(upstream);
        upstream.pipe(clientSocket);
      });
      upstream.on('error', () => {
        clientSocket.write('HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n');
        clientSocket.destroy();
      });
    });

    server.on('clientError', (_err, socket) => {
      socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    });

    return await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, '127.0.0.1', () => {
        server.removeListener('error', reject);
        const address = server.address() as net.AddressInfo | null;
        if (!address) {
          reject(new Error('Failed to resolve local proxy server address.'));
          return;
        }
        resolve({
          server,
          port: address.port,
          authUser,
          authToken
        });
      });
    });
  }

  function stopLocalProxyServer(options?: { stopTunnel?: boolean; clearRuntimeState?: boolean }): void {
    const stopTunnel = options?.stopTunnel !== false;
    const clearRuntimeState = options?.clearRuntimeState !== false;
    if (stopTunnel) {
      stopLocalProxyTunnel();
    }
    if (!localProxyState) {
      if (clearRuntimeState) {
        persistLocalProxyRuntimeState(undefined);
      }
      return;
    }
    try {
      localProxyState.server.close();
    } catch {
      // Ignore shutdown errors.
    }
    localProxyState = undefined;
    localProxyConfigKey = undefined;
    if (clearRuntimeState) {
      persistLocalProxyRuntimeState(undefined);
    }
  }

  async function resumeLocalProxyForRemoteSession(): Promise<void> {
    if (vscode.env.remoteName !== 'ssh-remote') {
      return;
    }
    if (!deps.hasSessionE2EContext()) {
      const remoteAlias = (deps.resolveRemoteSshAlias() || '').trim();
      const storedAlias = (deps.getStoredConnectionState()?.alias || '').trim();
      if (!remoteAlias || !storedAlias || remoteAlias !== storedAlias) {
        return;
      }
    }
    const cfg = deps.getConfig();
    if (!cfg.localProxyEnabled) {
      stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: true });
      return;
    }
    const storedRuntime = loadStoredLocalProxyRuntimeState();
    if (!storedRuntime) {
      return;
    }
    const log = deps.getOutputChannel();
    const configKey = JSON.stringify({ port: normalizeProxyPort(cfg.localProxyPort) });
    const tunnelMode = cfg.localProxyTunnelMode;
    if (!localProxyState) {
      try {
        localProxyState = await startLocalProxyServer(storedRuntime.port, {
          authUser: storedRuntime.authUser,
          authToken: storedRuntime.authToken
        });
        localProxyConfigKey = configKey;
        log.appendLine(`Local proxy resumed on 127.0.0.1:${localProxyState.port}`);
      } catch (error) {
        log.appendLine(`Failed to resume local proxy: ${deps.formatError(error)}`);
        persistLocalProxyRuntimeState(undefined);
        return;
      }
    }
    if (tunnelMode === 'dedicated') {
      try {
        await ensureLocalProxyTunnel(cfg, storedRuntime.loginHost, storedRuntime.remoteBind, storedRuntime.port);
      } catch (error) {
        log.appendLine(`Failed to resume local proxy tunnel: ${deps.formatError(error)}`);
      }
    } else {
      stopLocalProxyTunnel();
    }
  }

  async function ensureLocalProxyPlan(
    cfg: SlurmConnectConfig,
    loginHost: string,
    options?: { remotePortOverride?: number }
  ): Promise<LocalProxyPlan> {
    if (!cfg.localProxyEnabled) {
      if (localProxyState && deps.getConnectionState() !== 'connected') {
        stopLocalProxyServer();
      }
      return { enabled: false };
    }
    const log = deps.getOutputChannel();
    const remoteBindInput = (cfg.localProxyRemoteBind || '').trim() || '0.0.0.0';
    const remoteBind = cfg.localProxyComputeTunnel ? '127.0.0.1' : remoteBindInput;
    const tunnelMode = cfg.localProxyTunnelMode;
    const localPort = normalizeProxyPort(cfg.localProxyPort);
    const configKey = JSON.stringify({ port: localPort });
    if (!localProxyState || localProxyConfigKey !== configKey) {
      stopLocalProxyServer({ stopTunnel: false, clearRuntimeState: false });
      const storedRuntime = loadStoredLocalProxyRuntimeState();
      const canResume = Boolean(
        storedRuntime &&
          storedRuntime.loginHost === loginHost &&
          storedRuntime.remoteBind === remoteBind &&
          (localPort === 0 || storedRuntime.port === localPort)
      );
      if (canResume && storedRuntime) {
        try {
          localProxyState = await startLocalProxyServer(storedRuntime.port, {
            authUser: storedRuntime.authUser,
            authToken: storedRuntime.authToken
          });
          localProxyConfigKey = configKey;
          log.appendLine(`Local proxy resumed on 127.0.0.1:${localProxyState.port}`);
        } catch (error) {
          log.appendLine(`Failed to resume local proxy: ${deps.formatError(error)}`);
          persistLocalProxyRuntimeState(undefined);
        }
      }
      if (!localProxyState) {
        try {
          localProxyState = await startLocalProxyServer(localPort);
          localProxyConfigKey = configKey;
          log.appendLine(`Local proxy listening on 127.0.0.1:${localProxyState.port}`);
        } catch (error) {
          log.appendLine(`Failed to start local proxy: ${deps.formatError(error)}`);
          throw error;
        }
      }
    }
    const remoteHost = (cfg.localProxyRemoteHost || '').trim() || loginHost;
    const proxyPort = localProxyState.port;
    let remotePort: number;
    let sshOptions: string[] | undefined;
    if (tunnelMode === 'dedicated') {
      try {
        remotePort = await ensureLocalProxyTunnel(cfg, loginHost, remoteBind, proxyPort);
      } catch (error) {
        deps.getOutputChannel().appendLine(`Failed to start local proxy tunnel: ${deps.formatError(error)}`);
        throw error;
      }
    } else {
      stopLocalProxyTunnel();
      const overridePort = normalizeProxyPort(options?.remotePortOverride ?? proxyPort);
      remotePort = overridePort || proxyPort;
      sshOptions = [
        `RemoteForward ${remoteBind}:${remotePort} 127.0.0.1:${proxyPort}`,
        'ExitOnForwardFailure yes'
      ];
    }
    persistLocalProxyRuntimeState(buildLocalProxyRuntimeState(loginHost, remoteBind, localProxyState));
    const noProxy = buildNoProxyValue(cfg.localProxyNoProxy);
    const computeTunnel = cfg.localProxyComputeTunnel
      ? {
          loginHost: remoteHost || loginHost,
          loginUser: cfg.user || undefined,
          port: remotePort,
          authUser: localProxyState.authUser,
          authToken: localProxyState.authToken,
          noProxy: noProxy || undefined
        }
      : undefined;
    const proxyHostForEnv = computeTunnel ? '127.0.0.1' : remoteHost;
    const proxyUrl = `http://${localProxyState.authUser}:${localProxyState.authToken}@${proxyHostForEnv}:${remotePort}`;
    return {
      enabled: true,
      proxyUrl,
      noProxy: noProxy || undefined,
      sshOptions,
      computeTunnel
    };
  }

  function buildRemoteCommand(
    cfg: SlurmConnectConfig,
    sallocArgs: string[],
    sessionKey?: string,
    clientId?: string,
    installSnippet?: string,
    localProxyEnv?: LocalProxyEnv,
    localProxyPlan?: LocalProxyPlan
  ): string {
    const proxyTokens = splitShellArgs(cfg.proxyCommand);
    if (proxyTokens.length === 0) {
      return '';
    }
    const envTokens = buildProxyEnvTokens(localProxyEnv);
    const proxyArgs = buildProxyArgs(cfg, sessionKey, clientId, localProxyPlan);
    const sallocFlags = sallocArgs.map((arg) => `--salloc-arg=${arg}`);
    const fullProxyCommand = joinShellCommand([...envTokens, ...proxyTokens, ...proxyArgs, ...sallocFlags]);
    const startupCommand = (cfg.startupCommand || '').trim();
    const moduleLoad = cfg.moduleLoad ? escapeModuleLoadCommand(cfg.moduleLoad) : '';
    const baseCommand = [startupCommand, moduleLoad, fullProxyCommand].filter((entry) => entry).join(' && ').trim();
    if (!baseCommand) {
      return '';
    }
    if (!installSnippet) {
      return baseCommand;
    }
    const encoded = Buffer.from(installSnippet, 'utf8').toString('base64');
    const installScript = `echo ${encoded} | base64 -d | bash`;
    const wrapped = `${installScript}; ${baseCommand}`;
    return joinShellCommand(['bash', '-lc', wrapped]);
  }

  async function cancelPersistentSessionJob(
    loginHost: string,
    sessionKey: string | undefined,
    cfg: SlurmConnectConfig,
    _options?: { useTerminal?: boolean }
  ): Promise<boolean> {
    const log = deps.getOutputChannel();
    try {
      const stateDir = cfg.sessionStateDir.trim() || DEFAULT_SESSION_STATE_DIR;
      if (vscode.env.remoteName === 'ssh-remote') {
        const localCommand = buildLocalCancelPersistentSessionCommand(stateDir, sessionKey);
        const label = sessionKey || 'current session';
        log.appendLine(`Cancelling session "${label}" in remote terminal (loginHost=${loginHost}).`);
        await vscode.window.withProgress(
          {
            location: vscode.ProgressLocation.Notification,
            title: 'Cancelling Slurm allocation',
            cancellable: false
          },
          async () =>
            await deps.runPreSshCommandInTerminal(localCommand, {
              terminalName: 'Slurm Connect Cancel',
              promptMessage: '',
              failureMessage: 'Cancel command failed in the terminal. Check terminal output for details.',
              wrapInBash: true
            })
        );
        log.appendLine(`Session cancel completed for "${label}".`);
        void vscode.window.showInformationMessage(`Cancelled Slurm allocation for "${label}".`);
        return true;
      }
      void vscode.window.showWarningMessage(
        'Cancel job is only supported from an active remote session. Connect to the session first.'
      );
      return false;
    } catch (error) {
      const message = `Failed to cancel session ${sessionKey}: ${deps.formatError(error)}`;
      log.appendLine(message);
      void vscode.window.showErrorMessage(message);
      return false;
    }
  }

  function getCurrentLocalProxyPort(): number {
    return localProxyState?.port ?? 0;
  }

  function resetState(): void {
    localProxyState = undefined;
    localProxyTunnelState = undefined;
    localProxyConfigKey = undefined;
  }

  return {
    cancelPersistentSessionJob,
    ensureLocalProxyPlan,
    buildRemoteCommand,
    stopLocalProxyServer,
    resumeLocalProxyForRemoteSession,
    getCurrentLocalProxyPort,
    resetState
  };
}
