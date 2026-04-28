import * as dns from 'dns';
import * as http from 'http';
import * as net from 'net';
import * as os from 'os';

import { afterEach, describe, expect, it, vi } from 'vitest';

import type { SlurmConnectConfig } from '../../config/types';

const {
  execFileMock,
  netConnectOverrides,
  showErrorMessageMock,
  showInformationMessageMock,
  showWarningMessageMock
} =
  vi.hoisted(() => ({
    execFileMock: vi.fn(
      (
        _file: string,
        _args: string[],
        _options: unknown,
        callback?: (error: Error | null, stdout: string, stderr: string) => void
      ) => {
        const done =
          typeof _options === 'function'
            ? (_options as (error: Error | null, stdout: string, stderr: string) => void)
            : callback;
        done?.(null, '', '');
      }
    ),
    netConnectOverrides: new Map<string, { host: string; port: number }>(),
    showInformationMessageMock: vi.fn(),
    showWarningMessageMock: vi.fn(),
    showErrorMessageMock: vi.fn()
  }));

// The runtime rejects literal loopback proxy targets, so these tests use allowed
// hostnames and redirect only the outbound transport back to local fixtures.
vi.mock('net', async (importOriginal) => {
  const actual = await importOriginal<typeof import('net')>();
  const connect = (...args: unknown[]): net.Socket => {
    if (typeof args[0] === 'number') {
      const port = args[0];
      const host = typeof args[1] === 'string' ? args[1] : undefined;
      const callback =
        typeof args[1] === 'function'
          ? (args[1] as () => void)
          : typeof args[2] === 'function'
            ? (args[2] as () => void)
            : undefined;
      const override = host ? netConnectOverrides.get(`${host}:${port}`) : undefined;
      if (override) {
        return callback
          ? actual.connect(override.port, override.host, callback)
          : actual.connect(override.port, override.host);
      }
    }
    if (typeof args[0] === 'object' && args[0]) {
      const options = args[0] as net.NetConnectOpts;
      const host = typeof options.host === 'string' ? options.host : undefined;
      const port = Number(options.port);
      const callback = typeof args[1] === 'function' ? (args[1] as () => void) : undefined;
      const override =
        host && Number.isFinite(port) ? netConnectOverrides.get(`${host}:${port}`) : undefined;
      if (override) {
        return actual.connect({ ...options, host: override.host, port: override.port }, callback);
      }
    }
    return Reflect.apply(actual.connect, actual, args) as net.Socket;
  };
  return {
    ...actual,
    connect,
    createConnection: connect,
    default: {
      ...actual,
      connect,
      createConnection: connect
    }
  };
});

vi.mock('child_process', () => ({
  execFile: execFileMock
}));

vi.mock('vscode', () => ({
  workspace: {
    getConfiguration: (section: string) => ({
      get: (key: string, fallback?: unknown) => {
        if (section === 'slurmConnect' && key === 'sshConnectTimeoutSeconds') {
          return 15;
        }
        if (section === 'remote.SSH' && key === 'connectTimeout') {
          return undefined;
        }
        return fallback;
      }
    })
  },
  window: {
    withProgress: async (_options: unknown, task: () => Promise<unknown>) => await task(),
    showInformationMessage: showInformationMessageMock,
    showWarningMessage: showWarningMessageMock,
    showErrorMessage: showErrorMessageMock
  },
  ProgressLocation: {
    Notification: 15
  },
  env: {
    remoteName: ''
  }
}));

import { createLocalProxyRuntime } from '../../localProxy/runtime';
import type { LocalProxyRuntime } from '../../localProxy/runtime';

function createConfig(overrides: Partial<SlurmConnectConfig> = {}): SlurmConnectConfig {
  return {
    loginHosts: ['login.example.com'],
    loginHostsCommand: '',
    loginHostsQueryHost: '',
    partitionCommand: '',
    partitionInfoCommand: '',
    filterFreeResources: true,
    qosCommand: '',
    accountCommand: '',
    user: 'alice',
    identityFile: '',
    preSshCommand: '',
    preSshCheckCommand: '',
    autoInstallProxyScriptOnClusterInfo: true,
    forwardAgent: true,
    requestTTY: true,
    moduleLoad: '',
    startupCommand: '',
    proxyCommand: '',
    proxyArgs: [],
    proxyDebugLogging: false,
    localProxyEnabled: true,
    localProxyNoProxy: [],
    localProxyPort: 0,
    localProxyRemoteBind: '0.0.0.0',
    localProxyRemoteHost: '',
    localProxyComputeTunnel: false,
    localProxyTunnelMode: 'dedicated',
    extraSallocArgs: [],
    promptForExtraSallocArgs: false,
    sessionMode: 'persistent',
    sessionKey: '',
    sessionIdleTimeoutSeconds: 600,
    sessionStateDir: '~/.slurm-connect',
    defaultPartition: '',
    defaultNodes: 1,
    defaultTasksPerNode: 1,
    defaultCpusPerTask: 1,
    defaultTime: '24:00:00',
    defaultMemoryMb: 0,
    defaultGpuType: '',
    defaultGpuCount: 0,
    sshHostPrefix: 'slurm-',
    openInNewWindow: false,
    remoteWorkspacePath: '',
    temporarySshConfigPath: '',
    additionalSshOptions: {},
    sshQueryConfigPath: '~/.ssh/config',
    sshHostKeyChecking: 'accept-new',
    sshConnectTimeoutSeconds: 15,
    ...overrides
  };
}

describe('localProxy runtime', () => {
  afterEach(() => {
    execFileMock.mockClear();
    showInformationMessageMock.mockClear();
    showWarningMessageMock.mockClear();
    showErrorMessageMock.mockClear();
    vi.restoreAllMocks();
    netConnectOverrides.clear();
  });

  function createRuntime(): LocalProxyRuntime {
    return createLocalProxyRuntime({
      getOutputChannel: () =>
        ({
          appendLine: vi.fn()
        }) as never,
      formatError: (error) => String(error),
      getGlobalState: () => undefined,
      getConfig: () => createConfig(),
      getConnectionState: () => 'idle',
      getStoredConnectionState: () => undefined,
      resolveRemoteSshAlias: () => undefined,
      hasSessionE2EContext: () => false,
      resolveSshToolPath: async () => '/usr/bin/ssh',
      normalizeSshErrorText: (error) => String(error),
      pickSshErrorSummary: (text) => text,
      runSshCommand: async (_host, _cfg, command) =>
        command.includes('getsockname') ? '45000\n' : '',
      runPreSshCommandInTerminal: async () => undefined
    });
  }

  function proxyAuthHeader(proxyUrl: string): string {
    const parsed = new URL(proxyUrl);
    return `Basic ${Buffer.from(`${parsed.username}:${parsed.password}`).toString('base64')}`;
  }

  function setHttpLookupOverride(hostname: string, address: string): () => void {
    const originalLookup = http.globalAgent.options.lookup;
    http.globalAgent.options.lookup = ((host: string, options: unknown, callback?: unknown) => {
      const cb = typeof options === 'function' ? options : callback;
      if (host === hostname && typeof cb === 'function') {
        if (typeof options === 'object' && options && 'all' in options && options.all) {
          cb(null, [{ address, family: 4 }]);
          return;
        }
        cb(null, address, 4);
        return;
      }
      dns.lookup(host, options as never, callback as never);
    }) as typeof dns.lookup;

    return () => {
      if (originalLookup) {
        http.globalAgent.options.lookup = originalLookup;
      } else {
        delete http.globalAgent.options.lookup;
      }
    };
  }

  async function listen(server: http.Server | net.Server, host = '127.0.0.1'): Promise<number> {
    await new Promise<void>((resolve) => {
      server.listen(0, host, resolve);
    });
    const address = server.address() as net.AddressInfo;
    return address.port;
  }

  async function close(server: http.Server | net.Server): Promise<void> {
    if (!server.listening) {
      return;
    }
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  }

  it('expands sshQueryConfigPath before starting a dedicated tunnel', async () => {
    const runtime = createRuntime();

    try {
      await runtime.ensureLocalProxyPlan(createConfig(), 'login.example.com');

      expect(execFileMock).toHaveBeenCalled();
      const sshArgs = execFileMock.mock.calls[0]?.[1] as string[];
      expect(sshArgs.slice(0, 2)).toEqual(['-F', `${os.homedir()}/.ssh/config`]);
    } finally {
      runtime.stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: true });
    }
  });

  it('passes session e2e proxy probe args to compute tunnel proxy commands', () => {
    const runtime = createRuntime();
    const originalUrl = process.env.SLURM_CONNECT_SESSION_E2E_PROXY_PROBE_TARGET_URL;
    const originalToken = process.env.SLURM_CONNECT_SESSION_E2E_PROXY_PROBE_EXPECTED_TOKEN;
    process.env.SLURM_CONNECT_SESSION_E2E_PROXY_PROBE_TARGET_URL =
      'http://client-probe.example:4321/slurm-connect-local-proxy-e2e/token';
    process.env.SLURM_CONNECT_SESSION_E2E_PROXY_PROBE_EXPECTED_TOKEN = 'probe-token';

    try {
      const command = runtime.buildRemoteCommand(
        createConfig({
          proxyCommand: 'python3 ~/.slurm-connect/vscode-proxy.py',
          sessionMode: 'ephemeral'
        }),
        [],
        'session-key',
        'client-id',
        undefined,
        undefined,
        {
          enabled: true,
          computeTunnel: {
            loginHost: 'login.example.com',
            port: 43123,
            authUser: 'slurm-connect',
            authToken: 'auth-token'
          }
        }
      );

      expect(command).toContain(
        '--local-proxy-probe-url=http://client-probe.example:4321/slurm-connect-local-proxy-e2e/token'
      );
      expect(command).toContain('--local-proxy-probe-token=probe-token');
    } finally {
      if (originalUrl === undefined) {
        delete process.env.SLURM_CONNECT_SESSION_E2E_PROXY_PROBE_TARGET_URL;
      } else {
        process.env.SLURM_CONNECT_SESSION_E2E_PROXY_PROBE_TARGET_URL = originalUrl;
      }
      if (originalToken === undefined) {
        delete process.env.SLURM_CONNECT_SESSION_E2E_PROXY_PROBE_EXPECTED_TOKEN;
      } else {
        process.env.SLURM_CONNECT_SESSION_E2E_PROXY_PROBE_EXPECTED_TOKEN = originalToken;
      }
    }
  });

  it('forwards HTTP requests to non-loopback proxy targets', async () => {
    const runtime = createRuntime();
    const targetHost = 'proxy-target.test';
    const restoreLookup = setHttpLookupOverride(targetHost, '127.0.0.1');
    let upstreamUrl = '';
    let upstreamHost = '';
    let upstreamProxyAuth: string | string[] | undefined;
    const upstream = http.createServer((req, res) => {
      upstreamUrl = req.url || '';
      upstreamHost = String(req.headers.host || '');
      upstreamProxyAuth = req.headers['proxy-authorization'];
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('proxied-http');
    });

    try {
      const upstreamPort = await listen(upstream);
      const plan = await runtime.ensureLocalProxyPlan(
        createConfig({
          localProxyComputeTunnel: false,
          localProxyTunnelMode: 'remoteSsh'
        }),
        'login.example.com'
      );
      expect(plan.proxyUrl).toBeDefined();

      const response = await new Promise<{ statusCode: number; body: string }>((resolve, reject) => {
        const req = http.request(
          {
            hostname: '127.0.0.1',
            port: runtime.getCurrentLocalProxyPort(),
            method: 'GET',
            path: `http://${targetHost}:${upstreamPort}/resource?q=1`,
            headers: {
              'Proxy-Authorization': proxyAuthHeader(plan.proxyUrl!),
              'Proxy-Connection': 'keep-alive',
              Connection: 'keep-alive'
            }
          },
          (res) => {
            const chunks: Buffer[] = [];
            res.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
            res.on('end', () =>
              resolve({
                statusCode: res.statusCode || 0,
                body: Buffer.concat(chunks).toString('utf8')
              })
            );
          }
        );
        req.on('error', reject);
        req.end();
      });

      expect(response).toEqual({ statusCode: 200, body: 'proxied-http' });
      expect(upstreamUrl).toBe('/resource?q=1');
      expect(upstreamHost).toBe(`${targetHost}:${upstreamPort}`);
      expect(upstreamProxyAuth).toBeUndefined();
    } finally {
      restoreLookup();
      runtime.stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: true });
      await close(upstream);
    }
  });

  it('opens CONNECT tunnels for HTTPS proxy targets', async () => {
    const runtime = createRuntime();
    const targetHost = 'secure-proxy-target.test';
    let tunneledPayload = '';
    const upstream = net.createServer((socket) => {
      socket.once('data', (chunk) => {
        tunneledPayload = chunk.toString('utf8');
        socket.end('tunnel-ok');
      });
    });

    try {
      const upstreamPort = await listen(upstream);
      netConnectOverrides.set(`${targetHost}:${upstreamPort}`, {
        host: '127.0.0.1',
        port: upstreamPort
      });
      const plan = await runtime.ensureLocalProxyPlan(
        createConfig({
          localProxyComputeTunnel: false,
          localProxyTunnelMode: 'remoteSsh'
        }),
        'login.example.com'
      );
      expect(plan.proxyUrl).toBeDefined();

      const tunnelResponse = await new Promise<string>((resolve, reject) => {
        const socket = net.connect(runtime.getCurrentLocalProxyPort(), '127.0.0.1');
        let connected = false;
        let received = Buffer.alloc(0);
        socket.setTimeout(2000, () => {
          socket.destroy(new Error('CONNECT tunnel timed out'));
        });
        socket.on('connect', () => {
          socket.write(
            [
              `CONNECT ${targetHost}:${upstreamPort} HTTP/1.1`,
              `Host: ${targetHost}:${upstreamPort}`,
              `Proxy-Authorization: ${proxyAuthHeader(plan.proxyUrl!)}`,
              '',
              ''
            ].join('\r\n')
          );
        });
        socket.on('data', (chunk) => {
          received = Buffer.concat([received, chunk]);
          if (!connected) {
            const headerEnd = received.indexOf('\r\n\r\n');
            if (headerEnd < 0) {
              return;
            }
            const header = received.slice(0, headerEnd).toString('utf8');
            if (!header.startsWith('HTTP/1.1 200')) {
              socket.destroy();
              reject(new Error(`Unexpected CONNECT response: ${header}`));
              return;
            }
            connected = true;
            received = received.slice(headerEnd + 4);
            socket.write('hello-through-connect');
          }
          const body = received.toString('utf8');
          if (body.includes('tunnel-ok')) {
            socket.end();
            resolve(body);
          }
        });
        socket.on('error', reject);
      });

      expect(tunnelResponse).toContain('tunnel-ok');
      expect(tunneledPayload).toBe('hello-through-connect');
    } finally {
      runtime.stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: true });
      await close(upstream);
    }
  });
});
