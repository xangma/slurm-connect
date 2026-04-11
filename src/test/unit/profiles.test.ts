import { describe, expect, it, vi } from 'vitest';

vi.mock('vscode', () => ({}));

import { buildProfileSummaryMap, getProfileValues, type ProfileSummaryHelpers } from '../../state/profiles';
import type { ProfileStore, UiValues } from '../../state/types';
import { mergeUiValuesWithDefaults, pickProfileValues } from '../../state/uiCache';

const defaults: UiValues = {
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

const helpers: ProfileSummaryHelpers = {
  firstLoginHostFromInput(input) {
    return input.split(/\s+/).filter(Boolean)[0];
  },
  parsePositiveNumberInput(input) {
    return Number.parseInt(input, 10);
  },
  parseNonNegativeNumberInput(input) {
    return Number.parseInt(input, 10);
  }
};

describe('profile helpers', () => {
  it('merges saved values with defaults when loading a profile', () => {
    const store: ProfileStore = {
      gpu: {
        name: 'gpu',
        createdAt: '2026-01-01T00:00:00Z',
        updatedAt: '2026-01-01T00:00:00Z',
        values: {
          loginHosts: 'login2',
          defaultPartition: 'gpu',
          defaultGpuCount: '2'
        }
      }
    };

    expect(getProfileValues('gpu', store, mergeUiValuesWithDefaults, pickProfileValues, defaults)).toEqual({
      ...defaults,
      loginHosts: 'login2',
      defaultPartition: 'gpu',
      defaultGpuCount: '2'
    });
  });

  it('builds profile summaries with computed totals and overrides', () => {
    const store: ProfileStore = {
      batch: {
        name: 'batch',
        createdAt: '2026-01-01T00:00:00Z',
        updatedAt: '2026-01-02T00:00:00Z',
        values: {
          loginHosts: 'login2 login3',
          defaultPartition: 'gpu',
          defaultNodes: '2',
          defaultTasksPerNode: '4',
          defaultCpusPerTask: '8',
          defaultMemoryMb: '32000',
          defaultGpuCount: '1',
          defaultGpuType: 'a100'
        }
      }
    };

    const summaries = buildProfileSummaryMap(store, defaults, mergeUiValuesWithDefaults, pickProfileValues, helpers);

    expect(summaries.batch).toMatchObject({
      host: 'login2',
      partition: 'gpu',
      nodes: 2,
      tasksPerNode: 4,
      cpusPerTask: 8,
      memoryMb: 32000,
      gpuCount: 1,
      gpuType: 'a100',
      totalCpu: 64,
      totalMemoryMb: 64000,
      totalGpu: 2
    });
    expect(summaries.batch?.overrides?.some((override) => override.label === 'Partition')).toBe(false);
    expect(summaries.batch?.overrides?.some((override) => override.label === 'Login hosts')).toBe(true);
  });
});
