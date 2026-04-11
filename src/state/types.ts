export interface UiValues {
  loginHosts: string;
  loginHostsCommand: string;
  loginHostsQueryHost: string;
  partitionCommand: string;
  partitionInfoCommand: string;
  filterFreeResources: boolean;
  qosCommand: string;
  accountCommand: string;
  user: string;
  identityFile: string;
  preSshCommand: string;
  preSshCheckCommand: string;
  autoInstallProxyScriptOnClusterInfo: boolean;
  additionalSshOptions: string;
  moduleLoad: string;
  startupCommand: string;
  moduleSelections?: string[];
  moduleCustomCommand?: string;
  proxyCommand: string;
  proxyArgs: string;
  proxyDebugLogging: boolean;
  localProxyEnabled: boolean;
  localProxyNoProxy: string;
  localProxyPort: string;
  localProxyRemoteBind: string;
  localProxyRemoteHost: string;
  localProxyComputeTunnel: boolean;
  extraSallocArgs: string;
  promptForExtraSallocArgs: boolean;
  sessionMode: string;
  sessionKey: string;
  sessionIdleTimeoutSeconds: string;
  sessionStateDir: string;
  defaultPartition: string;
  defaultNodes: string;
  defaultTasksPerNode: string;
  defaultCpusPerTask: string;
  defaultTime: string;
  defaultMemoryMb: string;
  defaultGpuType: string;
  defaultGpuCount: string;
  sshHostPrefix: string;
  forwardAgent: boolean;
  requestTTY: boolean;
  temporarySshConfigPath: string;
  sshQueryConfigPath: string;
  openInNewWindow: boolean;
  remoteWorkspacePath: string;
}

export const PROFILE_UI_KEYS = [
  'loginHosts',
  'loginHostsCommand',
  'loginHostsQueryHost',
  'partitionCommand',
  'partitionInfoCommand',
  'filterFreeResources',
  'qosCommand',
  'accountCommand',
  'user',
  'identityFile',
  'preSshCommand',
  'preSshCheckCommand',
  'additionalSshOptions',
  'moduleLoad',
  'startupCommand',
  'moduleSelections',
  'moduleCustomCommand',
  'extraSallocArgs',
  'promptForExtraSallocArgs',
  'sessionMode',
  'sessionKey',
  'sessionIdleTimeoutSeconds',
  'defaultPartition',
  'defaultNodes',
  'defaultTasksPerNode',
  'defaultCpusPerTask',
  'defaultTime',
  'defaultMemoryMb',
  'defaultGpuType',
  'defaultGpuCount',
  'forwardAgent',
  'requestTTY',
  'openInNewWindow',
  'remoteWorkspacePath'
] as const;

export type ProfileUiKey = typeof PROFILE_UI_KEYS[number];
export type ProfileValues = { [K in ProfileUiKey]?: UiValues[K] };
export type ClusterUiCache = Partial<UiValues>;

export const PROFILE_UI_KEY_SET = new Set<keyof UiValues>(PROFILE_UI_KEYS);
export const OPTIONAL_NUMERIC_PROFILE_UI_KEYS = new Set<ProfileUiKey>([
  'sessionIdleTimeoutSeconds',
  'defaultNodes',
  'defaultTasksPerNode',
  'defaultCpusPerTask',
  'defaultMemoryMb',
  'defaultGpuCount'
]);

export interface ProfileEntry {
  name: string;
  values: ProfileValues;
  createdAt: string;
  updatedAt: string;
}

export interface ProfileSummary {
  name: string;
  updatedAt: string;
}

export type ProfileStore = Record<string, ProfileEntry>;

export interface ProfileOverrideDetail {
  label: string;
  value: string;
}

export interface ProfileResourceSummary {
  host?: string;
  partition?: string;
  nodes?: number;
  tasksPerNode?: number;
  cpusPerTask?: number;
  memoryMb?: number;
  gpuType?: string;
  gpuCount?: number;
  time?: string;
  totalCpu?: number;
  totalMemoryMb?: number;
  totalGpu?: number;
  overrides?: ProfileOverrideDetail[];
}
