import * as vscode from 'vscode';

import type { SlurmConnectConfig } from '../config/types';
import {
  ClusterUiCache,
  OPTIONAL_NUMERIC_PROFILE_UI_KEYS,
  PROFILE_UI_KEY_SET,
  ProfileValues,
  ProfileUiKey,
  UiValues
} from './types';

export function mergeUiValuesWithDefaults(values: Partial<UiValues> | undefined, defaults: UiValues): UiValues {
  if (!values) {
    return defaults;
  }
  return {
    ...defaults,
    ...values
  } as UiValues;
}

export function pickProfileValues(values: Partial<UiValues>): ProfileValues {
  const picked: ProfileValues = {};
  const target = picked as Record<ProfileUiKey, UiValues[ProfileUiKey]>;
  for (const key of PROFILE_UI_KEY_SET) {
    if (Object.prototype.hasOwnProperty.call(values, key)) {
      const typedKey = key as ProfileUiKey;
      const value = values[typedKey];
      if (OPTIONAL_NUMERIC_PROFILE_UI_KEYS.has(typedKey) && typeof value === 'string' && value.trim().length === 0) {
        continue;
      }
      target[typedKey] = value as UiValues[ProfileUiKey];
    }
  }
  return picked;
}

export function buildWebviewValues(values: UiValues): Partial<UiValues> {
  return {
    ...pickProfileValues(values),
    proxyDebugLogging: values.proxyDebugLogging,
    localProxyEnabled: values.localProxyEnabled,
    localProxyNoProxy: values.localProxyNoProxy,
    localProxyPort: values.localProxyPort,
    localProxyRemoteBind: values.localProxyRemoteBind,
    localProxyRemoteHost: values.localProxyRemoteHost,
    localProxyComputeTunnel: values.localProxyComputeTunnel
  };
}

export function filterProfileValues(values: Partial<UiValues>): { next: ProfileValues; changed: boolean } {
  const next: ProfileValues = {};
  let changed = false;
  for (const [key, value] of Object.entries(values)) {
    if (PROFILE_UI_KEY_SET.has(key as keyof UiValues)) {
      (next as Record<string, unknown>)[key] = value;
    } else {
      changed = true;
    }
  }
  return { next, changed };
}

export function getClusterUiCache(state: vscode.Memento | undefined, cacheKey: string): ClusterUiCache | undefined {
  if (!state) {
    return undefined;
  }
  return state.get<ClusterUiCache>(cacheKey);
}

export function getMergedClusterUiCache(options: {
  globalState?: vscode.Memento;
  workspaceState?: vscode.Memento;
  hasWorkspace: boolean;
  cacheKey: string;
}): ClusterUiCache | undefined {
  const globalCache = getClusterUiCache(options.globalState, options.cacheKey) || {};
  const workspaceCache = options.hasWorkspace ? getClusterUiCache(options.workspaceState, options.cacheKey) || {} : {};
  const merged = { ...globalCache, ...workspaceCache };
  return Object.keys(merged).length > 0 ? merged : undefined;
}

export function resolvePreferredSaveTarget(options: {
  workspaceFolders?: readonly vscode.WorkspaceFolder[];
  workspaceState?: vscode.Memento;
  cacheKey: string;
}): 'global' | 'workspace' {
  if (!options.workspaceFolders || options.workspaceFolders.length === 0) {
    return 'global';
  }
  const workspaceCache = getClusterUiCache(options.workspaceState, options.cacheKey);
  if (workspaceCache && Object.keys(workspaceCache).length > 0) {
    return 'workspace';
  }
  return 'global';
}

export async function updateClusterUiCache(options: {
  values: ProfileValues;
  target: vscode.ConfigurationTarget;
  globalState?: vscode.Memento;
  workspaceState?: vscode.Memento;
  workspaceFolders?: readonly vscode.WorkspaceFolder[];
  cacheKey: string;
}): Promise<void> {
  const hasWorkspace = Boolean(options.workspaceFolders && options.workspaceFolders.length > 0);
  const useWorkspace =
    hasWorkspace &&
    (options.target === vscode.ConfigurationTarget.Workspace ||
      options.target === vscode.ConfigurationTarget.WorkspaceFolder);
  const state = useWorkspace ? options.workspaceState : options.globalState;
  if (!state) {
    return;
  }
  await state.update(options.cacheKey, pickProfileValues(options.values));
}

export function hasCachedUiValue(cache: ClusterUiCache | undefined, key: keyof UiValues): boolean {
  return Boolean(cache && Object.prototype.hasOwnProperty.call(cache, key));
}

export function getCachedUiValue<T extends keyof UiValues>(
  cache: ClusterUiCache | undefined,
  key: T
): UiValues[T] | undefined {
  if (!hasCachedUiValue(cache, key)) {
    return undefined;
  }
  return cache?.[key];
}

export function getUiValuesFromConfig(options: {
  cfg: SlurmConnectConfig;
  cache?: ClusterUiCache;
  hasConfigValue: (key: string) => boolean;
  formatAdditionalSshOptions: (options: Record<string, string> | undefined) => string;
}): UiValues {
  const { cfg, cache, hasConfigValue, formatAdditionalSshOptions } = options;
  const hasValue = (key: string, cacheKey?: keyof UiValues): boolean =>
    hasConfigValue(key) || (cacheKey ? hasCachedUiValue(cache, cacheKey) : false);
  const fromCache = <T extends keyof UiValues>(key: T, fallback: UiValues[T]): UiValues[T] => {
    const cached = getCachedUiValue(cache, key);
    return cached !== undefined ? cached : fallback;
  };
  const fromConfigOrCache = <T extends keyof UiValues>(
    configKey: string,
    cacheKey: T,
    fallback: UiValues[T]
  ): UiValues[T] => (hasConfigValue(configKey) ? fallback : fromCache(cacheKey, fallback));
  const hasDefaultPartition = hasValue('defaultPartition', 'defaultPartition');
  const hasDefaultNodes = hasValue('defaultNodes', 'defaultNodes');
  const hasDefaultTasksPerNode = hasValue('defaultTasksPerNode', 'defaultTasksPerNode');
  const hasDefaultCpusPerTask = hasValue('defaultCpusPerTask', 'defaultCpusPerTask');
  const hasDefaultTime = hasValue('defaultTime', 'defaultTime');
  const hasDefaultMemoryMb = hasValue('defaultMemoryMb', 'defaultMemoryMb');
  const hasDefaultGpuType = hasValue('defaultGpuType', 'defaultGpuType');
  const hasDefaultGpuCount = hasValue('defaultGpuCount', 'defaultGpuCount');

  return {
    loginHosts: fromConfigOrCache('loginHosts', 'loginHosts', cfg.loginHosts.join('\n')),
    loginHostsCommand: fromConfigOrCache('loginHostsCommand', 'loginHostsCommand', cfg.loginHostsCommand || ''),
    loginHostsQueryHost: fromConfigOrCache('loginHostsQueryHost', 'loginHostsQueryHost', cfg.loginHostsQueryHost || ''),
    partitionCommand: fromConfigOrCache('partitionCommand', 'partitionCommand', cfg.partitionCommand || ''),
    partitionInfoCommand: fromConfigOrCache(
      'partitionInfoCommand',
      'partitionInfoCommand',
      cfg.partitionInfoCommand || ''
    ),
    filterFreeResources: fromConfigOrCache('filterFreeResources', 'filterFreeResources', cfg.filterFreeResources),
    qosCommand: fromConfigOrCache('qosCommand', 'qosCommand', cfg.qosCommand || ''),
    accountCommand: fromConfigOrCache('accountCommand', 'accountCommand', cfg.accountCommand || ''),
    user: fromConfigOrCache('user', 'user', cfg.user || ''),
    identityFile: fromConfigOrCache('identityFile', 'identityFile', cfg.identityFile || ''),
    preSshCommand: fromConfigOrCache('preSshCommand', 'preSshCommand', cfg.preSshCommand || ''),
    preSshCheckCommand: fromConfigOrCache('preSshCheckCommand', 'preSshCheckCommand', cfg.preSshCheckCommand || ''),
    autoInstallProxyScriptOnClusterInfo: cfg.autoInstallProxyScriptOnClusterInfo,
    additionalSshOptions: fromConfigOrCache(
      'additionalSshOptions',
      'additionalSshOptions',
      formatAdditionalSshOptions(cfg.additionalSshOptions)
    ),
    moduleLoad: fromConfigOrCache('moduleLoad', 'moduleLoad', cfg.moduleLoad || ''),
    startupCommand: fromConfigOrCache('startupCommand', 'startupCommand', cfg.startupCommand || ''),
    moduleSelections: getCachedUiValue(cache, 'moduleSelections'),
    moduleCustomCommand: getCachedUiValue(cache, 'moduleCustomCommand'),
    proxyCommand: fromConfigOrCache('proxyCommand', 'proxyCommand', cfg.proxyCommand || ''),
    proxyArgs: fromConfigOrCache('proxyArgs', 'proxyArgs', cfg.proxyArgs.join('\n')),
    proxyDebugLogging: cfg.proxyDebugLogging,
    localProxyEnabled: cfg.localProxyEnabled,
    localProxyNoProxy: cfg.localProxyNoProxy.join('\n'),
    localProxyPort: String(cfg.localProxyPort ?? 0),
    localProxyRemoteBind: cfg.localProxyRemoteBind || '',
    localProxyRemoteHost: cfg.localProxyRemoteHost || '',
    localProxyComputeTunnel: cfg.localProxyComputeTunnel,
    extraSallocArgs: fromConfigOrCache('extraSallocArgs', 'extraSallocArgs', cfg.extraSallocArgs.join('\n')),
    promptForExtraSallocArgs: fromConfigOrCache(
      'promptForExtraSallocArgs',
      'promptForExtraSallocArgs',
      cfg.promptForExtraSallocArgs
    ),
    sessionMode: fromConfigOrCache('sessionMode', 'sessionMode', cfg.sessionMode || 'persistent'),
    sessionKey: fromConfigOrCache('sessionKey', 'sessionKey', cfg.sessionKey || ''),
    sessionIdleTimeoutSeconds: fromConfigOrCache(
      'sessionIdleTimeoutSeconds',
      'sessionIdleTimeoutSeconds',
      String(cfg.sessionIdleTimeoutSeconds ?? 600)
    ),
    sessionStateDir: cfg.sessionStateDir || '',
    defaultPartition: hasDefaultPartition
      ? fromConfigOrCache('defaultPartition', 'defaultPartition', cfg.defaultPartition || '')
      : '',
    defaultNodes: hasDefaultNodes
      ? fromConfigOrCache('defaultNodes', 'defaultNodes', String(cfg.defaultNodes || ''))
      : '',
    defaultTasksPerNode: hasDefaultTasksPerNode
      ? fromConfigOrCache('defaultTasksPerNode', 'defaultTasksPerNode', String(cfg.defaultTasksPerNode || ''))
      : '',
    defaultCpusPerTask: hasDefaultCpusPerTask
      ? fromConfigOrCache('defaultCpusPerTask', 'defaultCpusPerTask', String(cfg.defaultCpusPerTask || ''))
      : '',
    defaultTime: hasDefaultTime ? fromConfigOrCache('defaultTime', 'defaultTime', cfg.defaultTime || '') : '24:00:00',
    defaultMemoryMb: hasDefaultMemoryMb
      ? fromConfigOrCache('defaultMemoryMb', 'defaultMemoryMb', String(cfg.defaultMemoryMb || ''))
      : '',
    defaultGpuType: hasDefaultGpuType
      ? fromConfigOrCache('defaultGpuType', 'defaultGpuType', cfg.defaultGpuType || '')
      : '',
    defaultGpuCount: hasDefaultGpuCount
      ? fromConfigOrCache('defaultGpuCount', 'defaultGpuCount', String(cfg.defaultGpuCount || ''))
      : '',
    sshHostPrefix: cfg.sshHostPrefix || '',
    forwardAgent: fromConfigOrCache('forwardAgent', 'forwardAgent', cfg.forwardAgent),
    requestTTY: fromConfigOrCache('requestTTY', 'requestTTY', cfg.requestTTY),
    openInNewWindow: fromConfigOrCache('openInNewWindow', 'openInNewWindow', cfg.openInNewWindow),
    remoteWorkspacePath: fromConfigOrCache(
      'remoteWorkspacePath',
      'remoteWorkspacePath',
      cfg.remoteWorkspacePath || ''
    ),
    temporarySshConfigPath: cfg.temporarySshConfigPath || '',
    sshQueryConfigPath: cfg.sshQueryConfigPath || ''
  };
}
