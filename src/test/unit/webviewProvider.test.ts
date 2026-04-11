import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('vscode', () => ({
  commands: {
    executeCommand: vi.fn()
  },
  window: {
    showOpenDialog: vi.fn(),
    showInputBox: vi.fn(),
    showWarningMessage: vi.fn()
  },
  ConfigurationTarget: {
    Global: 1,
    Workspace: 2,
    WorkspaceFolder: 3
  }
}));

import { SlurmConnectViewProvider, type SlurmConnectWebviewDependencies } from '../../webview/provider';
import type { UiValues } from '../../state/types';

function createUiDefaults(): UiValues {
  return {
    loginHosts: 'login1',
    loginHostsCommand: '',
    loginHostsQueryHost: '',
    partitionCommand: '',
    partitionInfoCommand: '',
    filterFreeResources: true,
    qosCommand: '',
    accountCommand: '',
    user: 'alice',
    identityFile: '~/.ssh/id_ed25519',
    preSshCommand: '',
    preSshCheckCommand: '',
    autoInstallProxyScriptOnClusterInfo: true,
    additionalSshOptions: '',
    moduleLoad: '',
    startupCommand: '',
    moduleSelections: [],
    moduleCustomCommand: '',
    proxyCommand: '',
    proxyArgs: '',
    proxyDebugLogging: false,
    localProxyEnabled: false,
    localProxyNoProxy: '',
    localProxyPort: '0',
    localProxyRemoteBind: '',
    localProxyRemoteHost: '',
    localProxyComputeTunnel: true,
    extraSallocArgs: '',
    promptForExtraSallocArgs: false,
    sessionMode: 'persistent',
    sessionKey: '',
    sessionIdleTimeoutSeconds: '600',
    sessionStateDir: '~/.slurm-connect',
    defaultPartition: '',
    defaultNodes: '',
    defaultTasksPerNode: '',
    defaultCpusPerTask: '',
    defaultTime: '24:00:00',
    defaultMemoryMb: '',
    defaultGpuType: '',
    defaultGpuCount: '',
    sshHostPrefix: 'slurm-',
    forwardAgent: true,
    requestTTY: true,
    temporarySshConfigPath: '',
    sshQueryConfigPath: '',
    openInNewWindow: false,
    remoteWorkspacePath: ''
  };
}

function createDependencies(overrides: Partial<SlurmConnectWebviewDependencies> = {}): SlurmConnectWebviewDependencies {
  const defaults = createUiDefaults();
  return {
    getWebviewHtml: () => '',
    syncConnectionStateFromEnvironment: () => {},
    getReadyPayload: async () => ({
      values: {},
      defaults: {},
      profiles: [{ name: 'current', updatedAt: '2026-01-01T00:00:00Z' }],
      profileSummaries: {},
      activeProfile: 'current',
      connectionState: 'idle',
      connectionSessionMode: 'persistent',
      remoteActive: false,
      saveTarget: 'global'
    }),
    getActiveProfileName: () => 'current',
    getProfileStore: () => ({
      current: {
        name: 'current',
        values: { loginHosts: 'login1' },
        createdAt: '2026-01-01T00:00:00Z',
        updatedAt: '2026-01-01T00:00:00Z'
      }
    }),
    getProfileSummaries: (store) =>
      Object.values(store).map((profile) => ({ name: profile.name, updatedAt: profile.updatedAt })),
    buildProfileSummaryMap: () => ({}),
    getUiGlobalDefaults: () => defaults,
    getProfileValues: () => undefined,
    resolvePreferredSaveTarget: () => 'global',
    firstLoginHostFromInput: () => undefined,
    getCachedClusterInfo: () => undefined,
    pickProfileValues: (values) => values,
    setActiveProfileName: async () => {},
    updateProfileStore: async () => {},
    buildOverridesFromUi: () => ({}),
    updateConfigFromUi: async () => {},
    resolveSshHostFromConfig: async () => ({
      host: 'login1',
      hasProxyCommand: false,
      hasProxyJump: false,
      hasExplicitHost: true
    }),
    getConfigWithOverrides: () => ({
      loginHosts: ['login1'],
      loginHostsCommand: '',
      loginHostsQueryHost: '',
      partitionCommand: '',
      partitionInfoCommand: '',
      filterFreeResources: true,
      qosCommand: '',
      accountCommand: '',
      user: 'alice',
      identityFile: '~/.ssh/id_ed25519',
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
      localProxyEnabled: false,
      localProxyNoProxy: [],
      localProxyPort: 0,
      localProxyRemoteBind: '',
      localProxyRemoteHost: '',
      localProxyComputeTunnel: true,
      localProxyTunnelMode: 'remoteSsh',
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
      sshQueryConfigPath: '',
      sshHostKeyChecking: 'accept-new',
      sshConnectTimeoutSeconds: 15
    }),
    buildAgentStatusMessage: async () => ({ text: '', isError: false }),
    handleConnectMessage: async () => {},
    handleCancelConnectMessage: () => {},
    handleDisconnectMessage: async () => {},
    handleCancelSessionMessage: async () => {},
    handleClusterInfoRequest: async () => {},
    loadSshHosts: async () => ({ hosts: [] }),
    openLogs: async () => {},
    openRemoteSshLog: async () => {},
    openSettings: () => {},
    formatError: (error) => String(error),
    ...overrides
  };
}

describe('SlurmConnectViewProvider', () => {
  let onDidReceiveMessage:
    | ((message: { command: string; name?: string }) => void | Promise<void>)
    | undefined;
  let postedMessages: unknown[];

  beforeEach(() => {
    onDidReceiveMessage = undefined;
    postedMessages = [];
  });

  it('keeps the active profile when loading a missing profile fails', async () => {
    const provider = new SlurmConnectViewProvider(
      { extensionUri: {} } as never,
      createDependencies()
    );

    const webview = {
      options: undefined,
      html: '',
      onDidReceiveMessage(handler: typeof onDidReceiveMessage) {
        onDidReceiveMessage = handler;
        return { dispose() {} };
      },
      async postMessage(message: unknown) {
        postedMessages.push(message);
        return true;
      }
    };

    provider.resolveWebviewView(
      {
        webview,
        onDidDispose() {
          return { dispose() {} };
        }
      } as never
    );

    expect(onDidReceiveMessage).toBeTypeOf('function');
    await onDidReceiveMessage?.({ command: 'loadProfile', name: 'missing' });

    expect(postedMessages).toHaveLength(1);
    expect(postedMessages[0]).toMatchObject({
      command: 'profiles',
      activeProfile: 'current',
      profileStatus: 'Profile "missing" not found.',
      profileError: true
    });
  });
});
