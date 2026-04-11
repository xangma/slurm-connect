export interface SlurmConnectConfig {
  loginHosts: string[];
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
  forwardAgent: boolean;
  requestTTY: boolean;
  moduleLoad: string;
  startupCommand: string;
  proxyCommand: string;
  proxyArgs: string[];
  proxyDebugLogging: boolean;
  localProxyEnabled: boolean;
  localProxyNoProxy: string[];
  localProxyPort: number;
  localProxyRemoteBind: string;
  localProxyRemoteHost: string;
  localProxyComputeTunnel: boolean;
  localProxyTunnelMode: LocalProxyTunnelMode;
  extraSallocArgs: string[];
  promptForExtraSallocArgs: boolean;
  sessionMode: SessionMode;
  sessionKey: string;
  sessionIdleTimeoutSeconds: number;
  sessionStateDir: string;
  defaultPartition: string;
  defaultNodes: number;
  defaultTasksPerNode: number;
  defaultCpusPerTask: number;
  defaultTime: string;
  defaultMemoryMb: number;
  defaultGpuType: string;
  defaultGpuCount: number;
  sshHostPrefix: string;
  openInNewWindow: boolean;
  remoteWorkspacePath: string;
  temporarySshConfigPath: string;
  additionalSshOptions: Record<string, string>;
  sshQueryConfigPath: string;
  sshHostKeyChecking: SshHostKeyCheckingMode;
  sshConnectTimeoutSeconds: number;
}

export type SessionMode = 'ephemeral' | 'persistent';
export type LocalProxyTunnelMode = 'remoteSsh' | 'dedicated';
export type SshHostKeyCheckingMode = 'accept-new' | 'ask' | 'yes' | 'no';

export interface ResolvedSshHostInfo {
  host: string;
  hostname?: string;
  user?: string;
  identityFile?: string;
  port?: string;
  certificateFile?: string;
  hasProxyCommand: boolean;
  hasProxyJump: boolean;
  hasExplicitHost: boolean;
}
