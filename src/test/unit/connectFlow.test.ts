import { describe, expect, it, vi } from 'vitest';

vi.mock('vscode', () => ({
  env: {
    sessionId: 'test-session-id'
  },
  ProgressLocation: {
    Notification: 15
  }
}));

import {
  buildDefaultAlias,
  buildSallocArgs,
  runConnectFlow,
  type ConnectFlowRuntime
} from '../../connect/flow';
import type { ConnectToken } from '../../state/connectionState';
import type { SlurmConnectConfig } from '../../config/types';

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
    proxyCommand: 'vscode-proxy',
    proxyArgs: [],
    proxyDebugLogging: false,
    localProxyEnabled: false,
    localProxyNoProxy: [],
    localProxyPort: 0,
    localProxyRemoteBind: '0.0.0.0',
    localProxyRemoteHost: '',
    localProxyComputeTunnel: false,
    localProxyTunnelMode: 'remoteSsh',
    extraSallocArgs: ['--constraint=a100'],
    promptForExtraSallocArgs: false,
    sessionMode: 'persistent',
    sessionKey: '',
    sessionIdleTimeoutSeconds: 600,
    sessionStateDir: '~/.slurm-connect',
    defaultPartition: 'gpu',
    defaultNodes: 1,
    defaultTasksPerNode: 1,
    defaultCpusPerTask: 1,
    defaultTime: '01:00:00',
    defaultMemoryMb: 0,
    defaultGpuType: '',
    defaultGpuCount: 0,
    sshHostPrefix: 'slurm',
    openInNewWindow: false,
    remoteWorkspacePath: '/work/project',
    temporarySshConfigPath: '',
    additionalSshOptions: {},
    sshQueryConfigPath: '',
    sshHostKeyChecking: 'accept-new',
    sshConnectTimeoutSeconds: 15,
    ...overrides
  };
}

function createRuntime(
  overrides: Partial<ConnectFlowRuntime> = {},
  token?: ConnectToken
): ConnectFlowRuntime {
  const log = {
    clear: vi.fn(),
    appendLine: vi.fn(),
    show: vi.fn()
  };
  return {
    getOutputChannel: () => log as never,
    formatError: (error) => String(error),
    showErrorMessage: vi.fn(),
    showWarningMessage: vi.fn(async () => undefined),
    showInputBox: vi.fn(async () => undefined),
    showQuickPick: vi.fn(async () => undefined),
    withProgress: vi.fn(async (_options, task) => await task()),
    runSshCommand: vi.fn(async () => ''),
    maybePromptForSshAuthOnConnect: vi.fn(async () => true),
    queryPartitions: vi.fn(async () => ({ partitions: ['gpu'], defaultPartition: 'gpu' })),
    querySimpleList: vi.fn(async () => []),
    resolveDefaultPartitionForHost: vi.fn(async () => undefined),
    resolvePartitionDefaultTimeForHost: vi.fn(async () => undefined),
    validateGpuRequestAgainstCachedClusterInfo: vi.fn(() => ({})),
    resolveSessionKey: vi.fn((_cfg, alias) => `session-${alias}`),
    getProxyInstallSnippet: vi.fn(async () => undefined),
    normalizeLocalProxyTunnelMode: vi.fn((value) => (value === 'dedicated' ? 'dedicated' : 'remoteSsh')),
    ensureRemoteSshSettings: vi.fn(async () => undefined),
    migrateStaleRemoteSshConfigIfNeeded: vi.fn(async () => undefined),
    resolveRemoteConfigContext: vi.fn(async () => ({
      baseConfigPath: '/home/alice/.ssh/config',
      includePath: '/home/alice/.ssh/slurm-connect.conf',
      includeFilePath: '/home/alice/.ssh/slurm-connect.conf'
    })),
    ensureLocalProxyPlan: vi.fn(async () => ({ enabled: false })),
    buildRemoteCommand: vi.fn(() => 'vscode-proxy --session-mode=persistent'),
    ensureSshAgentEnvForCurrentSsh: vi.fn(async () => undefined),
    resolveSshToolPath: vi.fn(async () => 'ssh'),
    hasSshOption: vi.fn(() => false),
    redactRemoteCommandForLog: vi.fn((entry) => entry),
    writeSlurmConnectIncludeFile: vi.fn(async () => '/home/alice/.ssh/slurm-connect.conf'),
    ensureSshIncludeInstalled: vi.fn(async () => 'added'),
    refreshRemoteSshHosts: vi.fn(async () => undefined),
    ensurePreSshCommand: vi.fn(async () => undefined),
    connectToHost: vi.fn(async () => true),
    delay: vi.fn(async () => undefined),
    waitForRemoteConnectionOrTimeout: vi.fn(async () => true),
    resolveSshIdentityAgentOption: vi.fn(() => undefined),
    getCurrentLocalProxyPort: vi.fn(() => 3128),
    showOutput: vi.fn(),
    isConnectCancelled: vi.fn((candidate?: ConnectToken) => Boolean(candidate?.cancelled ?? token?.cancelled)),
    stopLocalProxyServer: vi.fn(),
    ...overrides
  };
}

describe('connect flow helpers', () => {
  it('builds salloc args with optional resources', () => {
    expect(
      buildSallocArgs({
        partition: 'gpu',
        nodes: 2,
        tasksPerNode: 4,
        cpusPerTask: 8,
        time: '04:00:00',
        memoryMb: 65536,
        gpuType: 'a100',
        gpuCount: 2,
        qos: 'interactive',
        account: 'research'
      })
    ).toEqual([
      '--partition=gpu',
      '--nodes=2',
      '--ntasks-per-node=4',
      '--cpus-per-task=8',
      '--time=04:00:00',
      '--qos=interactive',
      '--account=research',
      '--mem=65536',
      '--gres=gpu:a100:2'
    ]);
  });

  it('omits optional salloc args when values are unset', () => {
    expect(
      buildSallocArgs({
        partition: undefined,
        nodes: 1,
        tasksPerNode: 1,
        cpusPerTask: 1,
        time: '',
        memoryMb: 0,
        gpuType: '',
        gpuCount: 0
      })
    ).toEqual(['--nodes=1', '--ntasks-per-node=1', '--cpus-per-task=1']);
  });

  it('builds sanitized default aliases from connect selections', () => {
    expect(buildDefaultAlias('slurm', 'login01.cluster.example', 'gpu/a100', 2, 8)).toBe(
      'slurm-login01-gpu-a100-2n-8c'
    );
  });

  it('runs the non-interactive connect orchestration and writes the generated SSH host entry', async () => {
    const cfg = createConfig();
    const runtime = createRuntime();

    const result = await runConnectFlow(runtime, cfg, { interactive: false });

    expect(result).toEqual({
      didConnect: true,
      alias: 'slurm-login-gpu-1n-1c',
      sessionKey: 'session-slurm-login-gpu-1n-1c',
      loginHost: 'login.example.com',
      sessionMode: 'persistent'
    });
    expect(runtime.buildRemoteCommand).toHaveBeenCalledWith(
      cfg,
      [
        '--partition=gpu',
        '--nodes=1',
        '--ntasks-per-node=1',
        '--cpus-per-task=1',
        '--time=01:00:00',
        '--constraint=a100'
      ],
      'session-slurm-login-gpu-1n-1c',
      'test-session-id',
      undefined,
      undefined,
      { enabled: false }
    );
    expect(runtime.writeSlurmConnectIncludeFile).toHaveBeenCalledWith(
      expect.stringContaining('Host slurm-login-gpu-1n-1c'),
      '/home/alice/.ssh/slurm-connect.conf'
    );
    expect(runtime.writeSlurmConnectIncludeFile).toHaveBeenCalledWith(
      expect.stringContaining('RemoteCommand vscode-proxy --session-mode=persistent'),
      '/home/alice/.ssh/slurm-connect.conf'
    );
    expect(runtime.connectToHost).toHaveBeenCalledWith(
      'slurm-login-gpu-1n-1c',
      false,
      '/work/project'
    );
    expect(runtime.waitForRemoteConnectionOrTimeout).toHaveBeenCalledWith(30_000, 500);
  });

  it('retries Remote-SSH local proxy forwards with an alternate port after connect failure', async () => {
    const randomValues = [0, 0.1];
    const randomSpy = vi.spyOn(Math, 'random').mockImplementation(() => randomValues.shift() ?? 0.2);
    const cfg = createConfig({
      localProxyEnabled: true,
      localProxyPort: 3128,
      localProxyTunnelMode: 'remoteSsh'
    });
    const runtime = createRuntime({
      ensureLocalProxyPlan: vi.fn(async () => ({
        enabled: true,
        proxyUrl: 'http://slurm-connect:secret@127.0.0.1:3128',
        sshOptions: ['RemoteForward 0.0.0.0:3128 127.0.0.1:3128']
      })),
      connectToHost: vi.fn()
        .mockResolvedValueOnce(false)
        .mockResolvedValueOnce(true),
      getCurrentLocalProxyPort: vi.fn(() => 3128)
    });

    try {
      const result = await runConnectFlow(runtime, cfg, { interactive: false });

      expect(result.didConnect).toBe(true);
      expect(runtime.ensureLocalProxyPlan).toHaveBeenNthCalledWith(
        1,
        cfg,
        'login.example.com',
        { remotePortOverride: undefined }
      );
      expect(runtime.ensureLocalProxyPlan).toHaveBeenNthCalledWith(
        2,
        cfg,
        'login.example.com',
        { remotePortOverride: 49152 }
      );
      expect(runtime.ensurePreSshCommand).toHaveBeenCalledTimes(1);
      expect(runtime.connectToHost).toHaveBeenCalledTimes(2);
    } finally {
      randomSpy.mockRestore();
    }
  });

  it('stops local proxy setup when cancellation arrives during connect orchestration', async () => {
    const token: ConnectToken = { id: 1, cancelled: false };
    const runtime = createRuntime({
      ensureLocalProxyPlan: vi.fn(async () => {
        token.cancelled = true;
        return { enabled: false };
      })
    }, token);

    const result = await runConnectFlow(runtime, createConfig(), {
      interactive: false,
      token
    });

    expect(result).toEqual({
      didConnect: false,
      alias: 'slurm-login-gpu-1n-1c'
    });
    expect(runtime.stopLocalProxyServer).toHaveBeenCalledTimes(1);
    expect(runtime.buildRemoteCommand).not.toHaveBeenCalled();
    expect(runtime.writeSlurmConnectIncludeFile).not.toHaveBeenCalled();
  });
});
