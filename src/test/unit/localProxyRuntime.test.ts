import * as os from 'os';

import { afterEach, describe, expect, it, vi } from 'vitest';

import type { SlurmConnectConfig } from '../../config/types';

const { execFileMock, showErrorMessageMock, showInformationMessageMock, showWarningMessageMock } =
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
    showInformationMessageMock: vi.fn(),
    showWarningMessageMock: vi.fn(),
    showErrorMessageMock: vi.fn()
  }));

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
  });

  it('expands sshQueryConfigPath before starting a dedicated tunnel', async () => {
    const runtime = createLocalProxyRuntime({
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

    try {
      await runtime.ensureLocalProxyPlan(createConfig(), 'login.example.com');

      expect(execFileMock).toHaveBeenCalled();
      const sshArgs = execFileMock.mock.calls[0]?.[1] as string[];
      expect(sshArgs.slice(0, 2)).toEqual(['-F', `${os.homedir()}/.ssh/config`]);
    } finally {
      runtime.stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: true });
    }
  });
});
