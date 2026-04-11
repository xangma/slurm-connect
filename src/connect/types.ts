import type { SessionMode } from '../config/types';

export type { SessionSummary } from '../utils/sessionInfo';

export interface PartitionResult {
  partitions: string[];
  defaultPartition?: string;
}

export interface PartitionDefaults {
  defaultTime?: string;
  defaultNodes?: number;
  defaultTasksPerNode?: number;
  defaultCpusPerTask?: number;
  defaultMemoryMb?: number;
  defaultGpuType?: string;
  defaultGpuCount?: number;
}

export interface CachedGpuValidationResult {
  blocked: boolean;
  message?: string;
  note?: string;
}

export interface LocalProxyPlan {
  enabled: boolean;
  proxyUrl?: string;
  noProxy?: string;
  sshOptions?: string[];
  computeTunnel?: {
    loginHost: string;
    loginUser?: string;
    port: number;
    authUser: string;
    authToken: string;
    noProxy?: string;
  };
}

export interface ConnectFlowResult {
  didConnect: boolean;
  alias?: string;
  sessionKey?: string;
  loginHost?: string;
  sessionMode?: SessionMode;
  clearSessionState?: boolean;
}
