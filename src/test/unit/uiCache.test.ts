import { describe, expect, it, vi } from 'vitest';

vi.mock('vscode', () => ({}));

import type { SlurmConnectConfig } from '../../config/types';
import { getUiValuesFromConfig, pickProfileValues } from '../../state/uiCache';

function createConfig(overrides: Partial<SlurmConnectConfig> = {}): SlurmConnectConfig {
  return {
    loginHosts: ['login1', 'login2'],
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
    moduleLoad: 'module load cluster',
    startupCommand: 'echo ready',
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
    additionalSshOptions: { ServerAliveInterval: '30' },
    sshQueryConfigPath: '',
    sshHostKeyChecking: 'accept-new',
    sshConnectTimeoutSeconds: 15,
    ...overrides
  };
}

describe('uiCache helpers', () => {
  it('keeps only profile-backed values and drops blank optional numerics', () => {
    const picked = pickProfileValues({
      loginHosts: 'login1',
      sessionIdleTimeoutSeconds: '  ',
      defaultGpuCount: '',
      localProxyEnabled: true
    });

    expect(picked).toEqual({
      loginHosts: 'login1'
    });
  });

  it('prefers explicit config values over cache while still reading cache-only fields', () => {
    const uiValues = getUiValuesFromConfig({
      cfg: createConfig(),
      cache: {
        loginHosts: 'cached-login',
        moduleSelections: ['gcc/13'],
        sessionKey: 'cached-session'
      },
      hasConfigValue: (key) => key === 'loginHosts' || key === 'additionalSshOptions',
      formatAdditionalSshOptions: (options) => JSON.stringify(options || {})
    });

    expect(uiValues.loginHosts).toBe('login1\nlogin2');
    expect(uiValues.moduleSelections).toEqual(['gcc/13']);
    expect(uiValues.sessionKey).toBe('cached-session');
    expect(uiValues.additionalSshOptions).toBe('{"ServerAliveInterval":"30"}');
  });
});
