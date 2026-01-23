import * as vscode from 'vscode';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs/promises';
import * as crypto from 'crypto';
import * as jsonc from 'jsonc-parser';
import * as http from 'http';
import * as https from 'https';
import * as net from 'net';
import * as zlib from 'zlib';
import { execFile, spawn } from 'child_process';
import { promisify } from 'util';
import {
  buildHostEntry,
  buildSlurmConnectIncludeBlock,
  buildSlurmConnectIncludeContent,
  expandHome,
  formatSshConfigValue,
  SLURM_CONNECT_INCLUDE_END,
  SLURM_CONNECT_INCLUDE_START
} from './utils/sshConfig';
import {
  ClusterInfo,
  getMaxFieldCount,
  hasMeaningfulClusterInfo,
  parsePartitionInfoOutput
} from './utils/clusterInfo';
import { isShellOperatorToken, joinShellCommand, quoteShellArg, splitShellArgs } from './utils/shellArgs';

const execFileAsync = promisify(execFile);

interface SlurmConnectConfig {
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
  proxyCommand: string;
  proxyArgs: string[];
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

interface PartitionResult {
  partitions: string[];
  defaultPartition?: string;
}

type ConnectionState = 'idle' | 'connecting' | 'connected' | 'disconnecting';
type SshAuthMode = 'agent' | 'terminal';
type SessionMode = 'ephemeral' | 'persistent';
type LocalProxyTunnelMode = 'remoteSsh' | 'dedicated';
type SshHostKeyCheckingMode = 'accept-new' | 'ask' | 'yes' | 'no';

interface ConnectToken {
  id: number;
  cancelled: boolean;
}

interface SessionSummary {
  sessionKey: string;
  jobId: string;
  state: string;
  jobName?: string;
  createdAt?: string;
  partition?: string;
  nodes?: number;
  cpus?: number;
  timeLimit?: string;
  clients?: number;
  lastSeenEpoch?: number;
  idleRemainingSeconds?: number;
  idleTimeoutSeconds?: number;
}

interface ResolvedSshHostInfo {
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

interface LocalProxyState {
  server: http.Server;
  port: number;
  authUser: string;
  authToken: string;
}

interface LocalProxyRuntimeState {
  port: number;
  authUser: string;
  authToken: string;
  loginHost: string;
  remoteBind: string;
}

interface LocalProxyTunnelState {
  remotePort: number;
  configKey: string;
  controlPath: string;
  target: string;
}

interface LocalProxyPlan {
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

interface LocalProxyEnv {
  proxyUrl: string;
  noProxy?: string;
}

let outputChannel: vscode.OutputChannel | undefined;
let logFilePath: string | undefined;
let extensionStoragePath: string | undefined;
let extensionRootPath: string | undefined;
let extensionGlobalState: vscode.Memento | undefined;
let extensionWorkspaceState: vscode.Memento | undefined;
let activeWebview: vscode.Webview | undefined;
let connectionState: ConnectionState = 'idle';
let lastConnectionAlias: string | undefined;
let lastConnectionSessionKey: string | undefined;
let lastConnectionSessionMode: SessionMode | undefined;
let lastConnectionLoginHost: string | undefined;
let activeConnectToken: ConnectToken | undefined;
let connectTokenCounter = 0;
let lastSshAuthPrompt: { identityPath: string; timestamp: number; mode: SshAuthMode } | undefined;
let preSshCommandInFlight: Promise<void> | undefined;
let logWriteQueue: Promise<void> = Promise.resolve();
let localProxyState: LocalProxyState | undefined;
let localProxyTunnelState: LocalProxyTunnelState | undefined;
let localProxyConfigKey: string | undefined;

const LOG_MAX_BYTES = 5 * 1024 * 1024;
const LOG_TRUNCATE_KEEP_BYTES = 4 * 1024 * 1024;
let clusterInfoRequestInFlight = false;
let sshHostsRequestInFlight = false;
const SETTINGS_SECTION = 'slurmConnect';
const LEGACY_SETTINGS_SECTION = 'sciamaSlurm';
const PROFILE_STORE_KEY = 'slurmConnect.profiles';
const ACTIVE_PROFILE_KEY = 'slurmConnect.activeProfile';
const CLUSTER_INFO_CACHE_KEY = 'slurmConnect.clusterInfoCache';
const CLUSTER_UI_CACHE_KEY = 'slurmConnect.clusterUiCache';
const LEGACY_PROFILE_STORE_KEY = 'sciamaSlurm.profiles';
const LEGACY_ACTIVE_PROFILE_KEY = 'sciamaSlurm.activeProfile';
const LEGACY_CLUSTER_INFO_CACHE_KEY = 'sciamaSlurm.clusterInfoCache';
const DEFAULT_SESSION_STATE_DIR = '~/.slurm-connect';
const DEFAULT_PROXY_SCRIPT_INSTALL_PATH = '~/.slurm-connect/vscode-proxy.py';
const PROXY_OVERRIDE_RESET_KEY = 'slurmConnect.proxyOverrideReset';
const SSH_AGENT_ENV_KEY = 'slurmConnect.sshAgentEnv';
const LOCAL_PROXY_TUNNEL_STATE_KEY = 'slurmConnect.localProxyTunnelState';
const LOCAL_PROXY_RUNTIME_STATE_KEY = 'slurmConnect.localProxyRuntimeState';
const CONFIG_KEYS = [
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
  'autoInstallProxyScriptOnClusterInfo',
  'forwardAgent',
  'requestTTY',
  'moduleLoad',
  'proxyCommand',
  'proxyArgs',
  'localProxyEnabled',
  'localProxyNoProxy',
  'localProxyPort',
  'localProxyRemoteBind',
  'localProxyRemoteHost',
  'localProxyComputeTunnel',
  'localProxyTunnelMode',
  'extraSallocArgs',
  'promptForExtraSallocArgs',
  'sessionMode',
  'sessionKey',
  'sessionIdleTimeoutSeconds',
  'sessionStateDir',
  'defaultPartition',
  'defaultNodes',
  'defaultTasksPerNode',
  'defaultCpusPerTask',
  'defaultTime',
  'defaultMemoryMb',
  'defaultGpuType',
  'defaultGpuCount',
  'sshHostPrefix',
  'openInNewWindow',
  'remoteWorkspacePath',
  'temporarySshConfigPath',
  'additionalSshOptions',
  'sshQueryConfigPath',
  'sshHostKeyChecking',
  'sshConnectTimeoutSeconds'
];
const CLUSTER_SETTING_KEYS = [
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

type ClusterSettingKey = typeof CLUSTER_SETTING_KEYS[number];

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  extensionStoragePath = context.globalStorageUri.fsPath;
  extensionRootPath = context.extensionUri.fsPath;
  extensionGlobalState = context.globalState;
  extensionWorkspaceState = context.workspaceState;
  await migrateLegacyState();
  await migrateLegacySettings();
  await migrateLegacyModuleCommands();
  await migrateClusterSettingsToCache();
  await resetProxyOverridesToDefaults();
  syncConnectionStateFromEnvironment();
  void resumeLocalProxyForRemoteSession();
  const disposable = vscode.commands.registerCommand('slurmConnect.connect', () => {
    void connectCommand();
  });
  context.subscriptions.push(disposable);

  void migrateStaleRemoteSshConfigIfNeeded();

  const viewProvider = new SlurmConnectViewProvider(context);
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider('slurmConnect.connectView', viewProvider)
  );
}

export function deactivate(): void {
  stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: false });
}

async function migrateLegacyState(): Promise<void> {
  if (!extensionGlobalState) {
    return;
  }
  const migrations: Array<[string, string]> = [
    [LEGACY_PROFILE_STORE_KEY, PROFILE_STORE_KEY],
    [LEGACY_ACTIVE_PROFILE_KEY, ACTIVE_PROFILE_KEY],
    [LEGACY_CLUSTER_INFO_CACHE_KEY, CLUSTER_INFO_CACHE_KEY]
  ];
  for (const [legacyKey, newKey] of migrations) {
    const current = extensionGlobalState.get(newKey);
    const legacyValue = extensionGlobalState.get(legacyKey);
    if (current === undefined && legacyValue !== undefined) {
      try {
        await extensionGlobalState.update(newKey, legacyValue);
      } catch {
        // Ignore migration failures.
      }
    }
    if (legacyValue !== undefined) {
      try {
        await extensionGlobalState.update(legacyKey, undefined);
      } catch {
        // Ignore cleanup failures.
      }
    }
  }
}

async function migrateLegacySettings(): Promise<void> {
  const legacyConfig = vscode.workspace.getConfiguration(LEGACY_SETTINGS_SECTION);
  const newConfig = vscode.workspace.getConfiguration(SETTINGS_SECTION);

  for (const key of CONFIG_KEYS) {
    const oldInspect = legacyConfig.inspect(key);
    const newInspect = newConfig.inspect(key);
    if (!oldInspect) {
      continue;
    }
    if (oldInspect.globalValue !== undefined && newInspect?.globalValue === undefined) {
      try {
        await newConfig.update(key, oldInspect.globalValue, vscode.ConfigurationTarget.Global);
      } catch {
        // Ignore migration failures.
      }
    }
    if (oldInspect.workspaceValue !== undefined && newInspect?.workspaceValue === undefined) {
      try {
        await newConfig.update(key, oldInspect.workspaceValue, vscode.ConfigurationTarget.Workspace);
      } catch {
        // Ignore migration failures.
      }
    }
    if (oldInspect.globalValue !== undefined) {
      try {
        await legacyConfig.update(key, undefined, vscode.ConfigurationTarget.Global);
      } catch {
        // Ignore cleanup failures.
      }
    }
    if (oldInspect.workspaceValue !== undefined) {
      try {
        await legacyConfig.update(key, undefined, vscode.ConfigurationTarget.Workspace);
      } catch {
        // Ignore cleanup failures.
      }
    }
  }

  const folders = vscode.workspace.workspaceFolders || [];
  for (const folder of folders) {
    const legacyFolderConfig = vscode.workspace.getConfiguration(LEGACY_SETTINGS_SECTION, folder.uri);
    const newFolderConfig = vscode.workspace.getConfiguration(SETTINGS_SECTION, folder.uri);
    for (const key of CONFIG_KEYS) {
      const oldInspect = legacyFolderConfig.inspect(key);
      const newInspect = newFolderConfig.inspect(key);
      if (!oldInspect) {
        continue;
      }
      if (oldInspect.workspaceFolderValue !== undefined && newInspect?.workspaceFolderValue === undefined) {
        try {
          await newFolderConfig.update(
            key,
            oldInspect.workspaceFolderValue,
            vscode.ConfigurationTarget.WorkspaceFolder
          );
        } catch {
          // Ignore migration failures.
        }
      }
      if (oldInspect.workspaceFolderValue !== undefined) {
        try {
          await legacyFolderConfig.update(key, undefined, vscode.ConfigurationTarget.WorkspaceFolder);
        } catch {
          // Ignore cleanup failures.
        }
      }
    }
  }
}

function normalizeModuleSelections(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .filter((entry) => typeof entry === 'string')
    .map((entry) => stripModuleTagSuffix(entry.trim()))
    .filter((entry) => entry.length > 0);
}

function stripModuleTagSuffix(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    return trimmed;
  }
  const defaultMatch = trimmed.match(/^(.*?)(?:\s*[<(]\s*(?:default|d)\s*[>)]\s*)$/i);
  if (defaultMatch) {
    return defaultMatch[1].trim();
  }
  const shortTagMatch = trimmed.match(/^(.*?)(?:\s*[<(]\s*[a-zA-Z]{1,3}\s*[>)]\s*)$/);
  if (shortTagMatch) {
    return shortTagMatch[1].trim();
  }
  return trimmed;
}

function normalizeModuleCustomCommand(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

function parseLegacyModuleLoad(
  value: string
): { selections: string[]; customCommand: string } | undefined {
  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }
  const moduleMatch = trimmed.match(/^module\s+load\s+(.+)$/i);
  const mlMatch = trimmed.match(/^ml\s+(.+)$/i);
  const match = moduleMatch || mlMatch;
  if (match) {
    const selections = match[1]
      .split(/[\s,]+/)
      .map((entry) => stripModuleTagSuffix(entry.trim()))
      .filter((entry) => entry.length > 0);
    if (selections.length === 0) {
      return undefined;
    }
    return { selections, customCommand: '' };
  }
  return { selections: [], customCommand: trimmed };
}

function sanitizeModuleCache(cache?: ClusterUiCache): { next: ClusterUiCache; changed: boolean } {
  if (!cache) {
    return { next: {}, changed: false };
  }
  const next: ClusterUiCache = { ...cache };
  let changed = false;

  const selections = normalizeModuleSelections(next.moduleSelections);
  const custom = normalizeModuleCustomCommand(next.moduleCustomCommand);
  const moduleLoad = typeof next.moduleLoad === 'string' ? next.moduleLoad.trim() : '';

  if (selections.length > 0) {
    next.moduleSelections = selections;
  } else if (Object.prototype.hasOwnProperty.call(next, 'moduleSelections')) {
    delete next.moduleSelections;
    changed = true;
  }

  if (custom) {
    next.moduleCustomCommand = custom;
  } else if (Object.prototype.hasOwnProperty.call(next, 'moduleCustomCommand')) {
    delete next.moduleCustomCommand;
    changed = true;
  }

  const hasSelections = selections.length > 0;
  const hasCustom = custom.length > 0;
  if (!hasSelections && !hasCustom && moduleLoad) {
    const migrated = parseLegacyModuleLoad(moduleLoad);
    if (migrated) {
      if (migrated.selections.length > 0) {
        next.moduleSelections = migrated.selections;
      }
      if (migrated.customCommand) {
        next.moduleCustomCommand = migrated.customCommand;
      }
      if (Object.prototype.hasOwnProperty.call(next, 'moduleLoad')) {
        delete next.moduleLoad;
      }
      changed = true;
    }
  }

  if (!changed) {
    const cachedSelections = normalizeModuleSelections(cache.moduleSelections);
    const selectionsChanged =
      selections.length !== cachedSelections.length ||
      selections.some((entry, index) => entry !== cachedSelections[index]);
    const customChanged = custom !== normalizeModuleCustomCommand(cache.moduleCustomCommand);
    if (selectionsChanged || customChanged) {
      changed = true;
    }
  }

  return { next, changed };
}

function stripProxyOverridesFromCache(cache?: ClusterUiCache): { next: ClusterUiCache; changed: boolean } {
  if (!cache) {
    return { next: {}, changed: false };
  }
  const next: ClusterUiCache = { ...cache };
  let changed = false;
  if (Object.prototype.hasOwnProperty.call(next, 'proxyCommand')) {
    delete (next as Partial<UiValues>).proxyCommand;
    changed = true;
  }
  if (Object.prototype.hasOwnProperty.call(next, 'proxyArgs')) {
    delete (next as Partial<UiValues>).proxyArgs;
    changed = true;
  }
  return { next, changed };
}

function stripProxyOverridesFromProfile(values: ProfileValues): { next: ProfileValues; changed: boolean } {
  const next = { ...values };
  let changed = false;
  if (Object.prototype.hasOwnProperty.call(next, 'proxyCommand')) {
    delete (next as Partial<UiValues>).proxyCommand;
    changed = true;
  }
  if (Object.prototype.hasOwnProperty.call(next, 'proxyArgs')) {
    delete (next as Partial<UiValues>).proxyArgs;
    changed = true;
  }
  return { next: next as ProfileValues, changed };
}

function sanitizeProfileModuleCommands(values: ProfileValues): { next: ProfileValues; changed: boolean } {
  let changed = false;
  const next = { ...values };
  const selections = normalizeModuleSelections(next.moduleSelections);
  const custom = normalizeModuleCustomCommand(next.moduleCustomCommand);
  const moduleLoad = typeof next.moduleLoad === 'string' ? next.moduleLoad.trim() : '';
  const originalSelections = normalizeModuleSelections(values.moduleSelections);
  const selectionsChanged =
    selections.length !== originalSelections.length ||
    selections.some((entry, index) => entry !== originalSelections[index]);
  const customChanged = custom !== normalizeModuleCustomCommand(values.moduleCustomCommand);

  next.moduleSelections = selections;
  next.moduleCustomCommand = custom;

  if (selections.length === 0 && !custom && moduleLoad) {
    const migrated = parseLegacyModuleLoad(moduleLoad);
    if (migrated) {
      next.moduleSelections = migrated.selections;
      next.moduleCustomCommand = migrated.customCommand;
      next.moduleLoad = '';
      changed = true;
    }
  }

  if (selectionsChanged || customChanged || next.moduleLoad !== values.moduleLoad) {
    changed = true;
  }

  return { next: next as ProfileValues, changed };
}

async function migrateLegacyModuleCommands(): Promise<void> {
  if (extensionGlobalState) {
    const current = getClusterUiCache(extensionGlobalState);
    const { next, changed } = sanitizeModuleCache(current);
    if (changed) {
      await extensionGlobalState.update(CLUSTER_UI_CACHE_KEY, next);
    }
  }

  if (extensionWorkspaceState) {
    const current = getClusterUiCache(extensionWorkspaceState);
    const { next, changed } = sanitizeModuleCache(current);
    if (changed) {
      await extensionWorkspaceState.update(CLUSTER_UI_CACHE_KEY, next);
    }
  }

  if (extensionGlobalState) {
    const store = getProfileStore();
    let changed = false;
    for (const [key, profile] of Object.entries(store)) {
      const filtered = filterProfileValues(profile.values as Partial<UiValues>);
      const sanitized = sanitizeProfileModuleCommands(filtered.next);
      if (filtered.changed || sanitized.changed) {
        store[key] = {
          ...profile,
          values: sanitized.next,
          updatedAt: new Date().toISOString()
        };
        changed = true;
      }
    }
    if (changed) {
      await extensionGlobalState.update(PROFILE_STORE_KEY, store);
    }
  }
}

async function resetProxyOverridesToDefaults(): Promise<void> {
  const cfg = vscode.workspace.getConfiguration(SETTINGS_SECTION);
  const folders = vscode.workspace.workspaceFolders || [];
  const resetKeys = ['proxyCommand', 'proxyArgs'];
  const resetConfig = async (
    targetCfg: vscode.WorkspaceConfiguration,
    target: vscode.ConfigurationTarget
  ): Promise<void> => {
    for (const key of resetKeys) {
      try {
        await targetCfg.update(key, undefined, target);
      } catch {
        // Ignore reset failures.
      }
    }
  };

  if (extensionGlobalState && !extensionGlobalState.get<boolean>(PROXY_OVERRIDE_RESET_KEY)) {
    await resetConfig(cfg, vscode.ConfigurationTarget.Global);
    const current = getClusterUiCache(extensionGlobalState);
    const { next, changed } = stripProxyOverridesFromCache(current);
    if (changed) {
      await extensionGlobalState.update(CLUSTER_UI_CACHE_KEY, next);
    }
    const store = getProfileStore();
    let storeChanged = false;
    for (const [key, profile] of Object.entries(store)) {
      const stripped = stripProxyOverridesFromProfile(profile.values);
      if (stripped.changed) {
        store[key] = {
          ...profile,
          values: stripped.next,
          updatedAt: new Date().toISOString()
        };
        storeChanged = true;
      }
    }
    if (storeChanged) {
      await extensionGlobalState.update(PROFILE_STORE_KEY, store);
    }
    await extensionGlobalState.update(PROXY_OVERRIDE_RESET_KEY, true);
  }

  if (extensionWorkspaceState && !extensionWorkspaceState.get<boolean>(PROXY_OVERRIDE_RESET_KEY)) {
    await resetConfig(cfg, vscode.ConfigurationTarget.Workspace);
    for (const folder of folders) {
      const folderCfg = vscode.workspace.getConfiguration(SETTINGS_SECTION, folder.uri);
      await resetConfig(folderCfg, vscode.ConfigurationTarget.WorkspaceFolder);
    }
    const current = getClusterUiCache(extensionWorkspaceState);
    const { next, changed } = stripProxyOverridesFromCache(current);
    if (changed) {
      await extensionWorkspaceState.update(CLUSTER_UI_CACHE_KEY, next);
    }
    await extensionWorkspaceState.update(PROXY_OVERRIDE_RESET_KEY, true);
  }
}

function mapConfigValueToUi(key: ClusterSettingKey, value: unknown): UiValues[ClusterSettingKey] {
  switch (key) {
    case 'loginHosts':
    case 'extraSallocArgs': {
      if (Array.isArray(value)) {
        return value.map((entry) => String(entry)).join('\n');
      }
      if (typeof value === 'string') {
        return value;
      }
      return value === undefined || value === null ? '' : String(value);
    }
    case 'defaultNodes':
    case 'defaultTasksPerNode':
    case 'defaultCpusPerTask':
    case 'defaultMemoryMb':
    case 'defaultGpuCount': {
      const numeric = typeof value === 'number' ? value : Number(value);
      if (!Number.isFinite(numeric) || numeric <= 0) {
        return '';
      }
      return String(Math.floor(numeric));
    }
    case 'sessionIdleTimeoutSeconds': {
      const numeric = typeof value === 'number' ? value : Number(value);
      if (!Number.isFinite(numeric) || numeric < 0) {
        return '0';
      }
      return String(Math.floor(numeric));
    }
    case 'filterFreeResources':
    case 'promptForExtraSallocArgs':
    case 'forwardAgent':
    case 'requestTTY':
    case 'openInNewWindow':
      return Boolean(value) as UiValues[ClusterSettingKey];
    case 'additionalSshOptions': {
      if (value && typeof value === 'object') {
        return formatAdditionalSshOptions(value as Record<string, string>) as UiValues[ClusterSettingKey];
      }
      return value === undefined || value === null ? '' : String(value);
    }
    default:
      return value === undefined || value === null ? '' : String(value);
  }
}

async function migrateClusterSettingsToCache(): Promise<void> {
  if (!extensionGlobalState) {
    return;
  }

  const cfg = vscode.workspace.getConfiguration(SETTINGS_SECTION);
  const folders = vscode.workspace.workspaceFolders || [];

  const nextGlobalCache: ClusterUiCache = { ...(getClusterUiCache(extensionGlobalState) || {}) };
  const nextWorkspaceCache: ClusterUiCache = { ...(getClusterUiCache(extensionWorkspaceState) || {}) };
  let updatedGlobal = false;
  let updatedWorkspace = false;

  const globalKeysToClear = new Set<ClusterSettingKey>();
  const workspaceKeysToClear = new Set<ClusterSettingKey>();
  const folderKeysToClear: Array<{ key: ClusterSettingKey; folder: vscode.WorkspaceFolder }> = [];

  for (const key of CLUSTER_SETTING_KEYS) {
    const inspect = cfg.inspect(key);
    if (inspect?.globalValue !== undefined) {
      if (!Object.prototype.hasOwnProperty.call(nextGlobalCache, key)) {
        (nextGlobalCache as Record<ClusterSettingKey, UiValues[ClusterSettingKey]>)[key] = mapConfigValueToUi(
          key,
          inspect.globalValue
        );
        updatedGlobal = true;
      }
      globalKeysToClear.add(key);
    }
    if (inspect?.workspaceValue !== undefined) {
      if (!Object.prototype.hasOwnProperty.call(nextWorkspaceCache, key)) {
        (nextWorkspaceCache as Record<ClusterSettingKey, UiValues[ClusterSettingKey]>)[key] = mapConfigValueToUi(
          key,
          inspect.workspaceValue
        );
        updatedWorkspace = true;
      }
      workspaceKeysToClear.add(key);
    }

    if (folders.length > 0) {
      const folderValues: Array<{ folder: vscode.WorkspaceFolder; value: unknown }> = [];
      for (const folder of folders) {
        const folderCfg = vscode.workspace.getConfiguration(SETTINGS_SECTION, folder.uri);
        const folderInspect = folderCfg.inspect(key);
        if (folderInspect?.workspaceFolderValue !== undefined) {
          folderValues.push({ folder, value: folderInspect.workspaceFolderValue });
        }
      }

      if (folderValues.length > 0) {
        const serialized = folderValues.map((entry) => JSON.stringify(entry.value));
        const unique = new Set(serialized);
        if (unique.size === 1) {
          if (inspect?.workspaceValue === undefined) {
            if (!Object.prototype.hasOwnProperty.call(nextWorkspaceCache, key)) {
              (nextWorkspaceCache as Record<ClusterSettingKey, UiValues[ClusterSettingKey]>)[key] =
                mapConfigValueToUi(key, folderValues[0].value);
              updatedWorkspace = true;
            }
          }
          folderValues.forEach((entry) => {
            folderKeysToClear.push({ key, folder: entry.folder });
          });
        } else {
          const log = getOutputChannel();
          log.appendLine(
            `Skipping migration for ${SETTINGS_SECTION}.${key} workspace-folder values because they differ across folders.`
          );
        }
      }
    }
  }

  if (updatedGlobal) {
    await extensionGlobalState.update(CLUSTER_UI_CACHE_KEY, nextGlobalCache);
  }
  if (updatedWorkspace && extensionWorkspaceState) {
    await extensionWorkspaceState.update(CLUSTER_UI_CACHE_KEY, nextWorkspaceCache);
  }

  for (const key of globalKeysToClear) {
    try {
      await cfg.update(key, undefined, vscode.ConfigurationTarget.Global);
    } catch {
      // Ignore cleanup failures.
    }
  }
  for (const key of workspaceKeysToClear) {
    try {
      await cfg.update(key, undefined, vscode.ConfigurationTarget.Workspace);
    } catch {
      // Ignore cleanup failures.
    }
  }
  for (const entry of folderKeysToClear) {
    try {
      const folderCfg = vscode.workspace.getConfiguration(SETTINGS_SECTION, entry.folder.uri);
      await folderCfg.update(entry.key, undefined, vscode.ConfigurationTarget.WorkspaceFolder);
    } catch {
      // Ignore cleanup failures.
    }
  }
}

function getOutputChannel(): vscode.OutputChannel {
  if (!outputChannel) {
    const baseChannel = vscode.window.createOutputChannel('Slurm Connect');
    outputChannel = {
      name: baseChannel.name,
      append: (value: string) => {
        baseChannel.append(value);
        appendLogFile(value);
      },
      appendLine: (value: string) => {
        baseChannel.appendLine(value);
        appendLogFile(`${value}
`);
      },
      replace: (value: string) => {
        baseChannel.replace(value);
        appendLogFile(`
--- Output replaced at ${new Date().toISOString()} ---
${value}
`);
      },
      clear: () => {
        baseChannel.clear();
        appendLogFile(`
--- Output cleared at ${new Date().toISOString()} ---
`);
      },
      show: (columnOrPreserveFocus?: vscode.ViewColumn | boolean, preserveFocus?: boolean) => {
        if (typeof columnOrPreserveFocus === 'boolean' || columnOrPreserveFocus === undefined) {
          baseChannel.show(columnOrPreserveFocus);
        } else {
          baseChannel.show(columnOrPreserveFocus, preserveFocus);
        }
      },
      hide: () => {
        baseChannel.hide();
      },
      dispose: () => {
        baseChannel.dispose();
      }
    };
  }
  return outputChannel;
}

function resolveLogFilePath(): string {
  if (logFilePath) {
    return logFilePath;
  }
  const baseDir = extensionStoragePath || os.tmpdir();
  logFilePath = path.join(baseDir, 'slurm-connect.log');
  return logFilePath;
}

async function ensureLogFile(): Promise<string> {
  const filePath = resolveLogFilePath();
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  try {
    await fs.access(filePath);
  } catch {
    await fs.writeFile(filePath, '', 'utf8');
  }
  return filePath;
}

function enqueueLogWrite(task: () => Promise<void>): void {
  logWriteQueue = logWriteQueue.then(task).catch(() => {
    // Ignore logging failures to avoid breaking the main flow.
  });
}

async function truncateLogFileIfNeeded(filePath: string): Promise<void> {
  let stat: { size: number };
  try {
    stat = await fs.stat(filePath);
  } catch {
    return;
  }
  if (stat.size <= LOG_MAX_BYTES) {
    return;
  }
  const keepBytes = Math.min(LOG_TRUNCATE_KEEP_BYTES, LOG_MAX_BYTES);
  const start = Math.max(0, stat.size - keepBytes);
  let handle: fs.FileHandle | undefined;
  try {
    handle = await fs.open(filePath, 'r');
    const buffer = Buffer.alloc(keepBytes);
    const { bytesRead } = await handle.read(buffer, 0, keepBytes, start);
    const header = `--- Log truncated at ${new Date().toISOString()} (kept last ${bytesRead} bytes) ---\n`;
    const payload = header + buffer.slice(0, bytesRead).toString('utf8');
    await fs.writeFile(filePath, payload, 'utf8');
  } catch {
    // Ignore truncation failures.
  } finally {
    try {
      await handle?.close();
    } catch {
      // Ignore close failures.
    }
  }
}

function appendLogFile(text: string): void {
  enqueueLogWrite(async () => {
    const filePath = await ensureLogFile();
    await fs.appendFile(filePath, text, 'utf8');
    await truncateLogFileIfNeeded(filePath);
  });
}

async function openLogFile(): Promise<void> {
  const filePath = await ensureLogFile();
  const doc = await vscode.workspace.openTextDocument(vscode.Uri.file(filePath));
  await vscode.window.showTextDocument(doc, { preview: false });
}

async function openRemoteSshLog(): Promise<void> {
  try {
    await vscode.commands.executeCommand('opensshremotes.showLog');
    return;
  } catch {
    // Fall through to output channel fallback.
  }
  try {
    await vscode.commands.executeCommand(
      'workbench.action.output.show.extension-output-ms-vscode-remote.remote-ssh'
    );
  } catch {
    void vscode.window.showWarningMessage('Unable to open Remote-SSH logs.');
  }
}

interface UiValues {
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
  moduleSelections?: string[];
  moduleCustomCommand?: string;
  proxyCommand: string;
  proxyArgs: string;
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

const PROFILE_UI_KEYS = [
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

type ProfileUiKey = typeof PROFILE_UI_KEYS[number];
type ProfileValues = { [K in ProfileUiKey]?: UiValues[K] };
type ClusterUiCache = Partial<UiValues>;

const PROFILE_UI_KEY_SET = new Set<keyof UiValues>(PROFILE_UI_KEYS);

interface ProfileEntry {
  name: string;
  values: ProfileValues;
  createdAt: string;
  updatedAt: string;
}

interface ProfileSummary {
  name: string;
  updatedAt: string;
}

type ProfileStore = Record<string, ProfileEntry>;

interface ProfileOverrideDetail {
  label: string;
  value: string;
}

interface ProfileResourceSummary {
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

function postToWebview(message: unknown): void {
  if (!activeWebview) {
    return;
  }
  void activeWebview.postMessage(message);
}

async function loadSshHostsForWebview(overrides?: Partial<SlurmConnectConfig>): Promise<void> {
  if (!activeWebview || sshHostsRequestInFlight) {
    return;
  }
  sshHostsRequestInFlight = true;
  try {
    const cfg = getConfigWithOverrides(overrides);
    const configPath = await resolveSshHostsConfigPath(cfg);
    if (!configPath) {
      postToWebview({
        command: 'sshHosts',
        hosts: [],
        error: 'SSH config not found.'
      });
      return;
    }
    const hosts = await collectSshConfigHosts(configPath);
    postToWebview({
      command: 'sshHosts',
      hosts,
      source: configPath
    });
  } catch (error) {
    postToWebview({
      command: 'sshHosts',
      hosts: [],
      error: formatError(error)
    });
  } finally {
    sshHostsRequestInFlight = false;
  }
}

async function openSlurmConnectView(): Promise<void> {
  try {
    await vscode.commands.executeCommand('workbench.view.extension.slurmConnect');
  } catch {
    // Ignore if the view cannot be opened.
  }
}

async function refreshAgentStatus(identityFile: string): Promise<void> {
  if (!activeWebview) {
    return;
  }
  const agentStatus = await buildAgentStatusMessage(identityFile);
  postToWebview({
    command: 'agentStatus',
    agentStatus: agentStatus.text,
    agentStatusError: agentStatus.isError
  });
}

function setConnectionState(state: ConnectionState): void {
  connectionState = state;
  postToWebview({
    command: 'connectionState',
    state,
    sessionMode: lastConnectionSessionMode
  });
}

function createConnectToken(): ConnectToken {
  const token = { id: connectTokenCounter + 1, cancelled: false };
  connectTokenCounter += 1;
  activeConnectToken = token;
  return token;
}

function cancelActiveConnectToken(): void {
  if (activeConnectToken) {
    activeConnectToken.cancelled = true;
  }
}

function isConnectCancelled(token?: ConnectToken): boolean {
  return Boolean(token?.cancelled);
}

function finalizeConnectToken(token?: ConnectToken): void {
  if (token && activeConnectToken === token) {
    activeConnectToken = undefined;
  }
}

function resolveRemoteSshAlias(): string | undefined {
  if (vscode.env.remoteName !== 'ssh-remote') {
    return undefined;
  }
  const folders = vscode.workspace.workspaceFolders;
  const authority = folders && folders.length > 0 ? folders[0].uri.authority : undefined;
  if (!authority) {
    return undefined;
  }
  const plusIndex = authority.indexOf('+');
  const raw = plusIndex >= 0 ? authority.slice(plusIndex + 1) : authority;
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

function syncConnectionStateFromEnvironment(): void {
  const remoteActive = vscode.env.remoteName === 'ssh-remote';
  if (remoteActive) {
    if (connectionState === 'idle') {
      connectionState = 'connected';
    }
    const alias = resolveRemoteSshAlias();
    if (alias) {
      lastConnectionAlias = alias;
    }
    const cfg = getConfig();
    if (!lastConnectionSessionMode) {
      lastConnectionSessionMode = cfg.sessionMode;
    }
    if (!lastConnectionSessionKey && alias) {
      lastConnectionSessionKey = resolveSessionKey(cfg, alias);
    }
    if (!lastConnectionLoginHost && cfg.loginHosts.length > 0) {
      lastConnectionLoginHost = cfg.loginHosts[0];
    }
    return;
  }
  if (connectionState === 'connected') {
    connectionState = 'idle';
  }
}

function mergeUiValuesWithDefaults(values?: Partial<UiValues>, defaults?: UiValues): UiValues {
  const base = defaults ?? getUiValuesFromStorage();
  if (!values) {
    return base;
  }
  return {
    ...base,
    ...values
  } as UiValues;
}

function pickProfileValues(values: Partial<UiValues>): ProfileValues {
  const picked: ProfileValues = {};
  const target = picked as Record<ProfileUiKey, UiValues[ProfileUiKey]>;
  for (const key of PROFILE_UI_KEY_SET) {
    if (Object.prototype.hasOwnProperty.call(values, key)) {
      target[key as ProfileUiKey] = values[key] as UiValues[ProfileUiKey];
    }
  }
  return picked;
}

function buildWebviewValues(values: UiValues): Partial<UiValues> {
  return {
    ...pickProfileValues(values),
    localProxyEnabled: values.localProxyEnabled,
    localProxyNoProxy: values.localProxyNoProxy,
    localProxyPort: values.localProxyPort,
    localProxyRemoteBind: values.localProxyRemoteBind,
    localProxyRemoteHost: values.localProxyRemoteHost,
    localProxyComputeTunnel: values.localProxyComputeTunnel
  };
}

function filterProfileValues(values: Partial<UiValues>): { next: ProfileValues; changed: boolean } {
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

function getClusterUiCache(state?: vscode.Memento): ClusterUiCache | undefined {
  if (!state) {
    return undefined;
  }
  return state.get<ClusterUiCache>(CLUSTER_UI_CACHE_KEY);
}

function getMergedClusterUiCache(): ClusterUiCache | undefined {
  const globalCache = getClusterUiCache(extensionGlobalState) || {};
  const hasWorkspace = Boolean(vscode.workspace.workspaceFolders && vscode.workspace.workspaceFolders.length > 0);
  const workspaceCache = hasWorkspace ? getClusterUiCache(extensionWorkspaceState) || {} : {};
  const merged = { ...globalCache, ...workspaceCache };
  return Object.keys(merged).length > 0 ? merged : undefined;
}

function resolvePreferredSaveTarget(): 'global' | 'workspace' {
  if (!vscode.workspace.workspaceFolders || vscode.workspace.workspaceFolders.length === 0) {
    return 'global';
  }
  const workspaceCache = getClusterUiCache(extensionWorkspaceState);
  if (workspaceCache && Object.keys(workspaceCache).length > 0) {
    return 'workspace';
  }
  return 'global';
}

async function updateClusterUiCache(values: ProfileValues, target: vscode.ConfigurationTarget): Promise<void> {
  const hasWorkspace = Boolean(vscode.workspace.workspaceFolders && vscode.workspace.workspaceFolders.length > 0);
  const useWorkspace =
    hasWorkspace &&
    (target === vscode.ConfigurationTarget.Workspace || target === vscode.ConfigurationTarget.WorkspaceFolder);
  const state = useWorkspace ? extensionWorkspaceState : extensionGlobalState;
  if (!state) {
    return;
  }
  await state.update(CLUSTER_UI_CACHE_KEY, pickProfileValues(values));
}

function getProfileStore(): ProfileStore {
  if (!extensionGlobalState) {
    return {};
  }
  return extensionGlobalState.get<ProfileStore>(PROFILE_STORE_KEY) || {};
}

function getActiveProfileName(): string | undefined {
  if (!extensionGlobalState) {
    return undefined;
  }
  const name = extensionGlobalState.get<string>(ACTIVE_PROFILE_KEY);
  return name || undefined;
}

async function setActiveProfileName(name?: string): Promise<void> {
  if (!extensionGlobalState) {
    return;
  }
  await extensionGlobalState.update(ACTIVE_PROFILE_KEY, name);
}

function getProfileSummaries(store: ProfileStore): ProfileSummary[] {
  return Object.values(store)
    .map((profile) => ({ name: profile.name, updatedAt: profile.updatedAt }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

function getProfileValues(name: string): UiValues | undefined {
  const store = getProfileStore();
  const entry = store[name];
  if (!entry) {
    return undefined;
  }
  return mergeUiValuesWithDefaults(pickProfileValues(entry.values), getUiGlobalDefaults());
}

function normalizeWhitespace(value: string | undefined | null): string {
  if (!value) {
    return '';
  }
  return value.replace(/\s+/g, ' ').trim();
}

function normalizeList(values: string[] | undefined): string[] {
  if (!Array.isArray(values)) {
    return [];
  }
  return values.map((entry) => entry.trim()).filter(Boolean);
}

function listsEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}

function buildProfileOverrides(values: UiValues, defaults: UiValues): ProfileOverrideDetail[] {
  const overrides: ProfileOverrideDetail[] = [];
  const add = (label: string, value: string): void => {
    const normalized = normalizeWhitespace(value);
    if (normalized) {
      overrides.push({ label, value: normalized });
    }
  };
  const diffString = (value: string, fallback: string): boolean => {
    const normalized = normalizeWhitespace(value);
    if (!normalized) {
      return false;
    }
    return normalized !== normalizeWhitespace(fallback);
  };
  const diffBool = (value: boolean, fallback: boolean): boolean => value !== fallback;

  const loginHostsValue = normalizeWhitespace(values.loginHosts);
  const loginHostTokens = loginHostsValue.split(/[,\s]+/).filter(Boolean);
  if (loginHostTokens.length > 1 && diffString(values.loginHosts, defaults.loginHosts)) {
    add('Login hosts', values.loginHosts);
  }
  if (diffString(values.user, defaults.user)) add('User', values.user);
  if (diffString(values.identityFile, defaults.identityFile)) add('Identity file', values.identityFile);
  if (diffString(values.preSshCommand, defaults.preSshCommand)) {
    add('Pre-SSH command', values.preSshCommand);
  }
  if (diffString(values.preSshCheckCommand, defaults.preSshCheckCommand)) {
    add('Pre-SSH check command', values.preSshCheckCommand);
  }
  if (diffString(values.additionalSshOptions, defaults.additionalSshOptions)) {
    add('Additional SSH options', values.additionalSshOptions);
  }
  if (diffString(values.remoteWorkspacePath, defaults.remoteWorkspacePath)) {
    add('Remote folder', values.remoteWorkspacePath);
  }
  if (diffBool(values.openInNewWindow, defaults.openInNewWindow)) {
    add('Open in new window', values.openInNewWindow ? 'Yes' : 'No');
  }
  if (diffBool(values.filterFreeResources, defaults.filterFreeResources)) {
    add('Free-resource filter', values.filterFreeResources ? 'On' : 'Off');
  }

  const selections = normalizeList(values.moduleSelections);
  const defaultSelections = normalizeList(defaults.moduleSelections);
  const custom = normalizeWhitespace(values.moduleCustomCommand || '');
  const defaultCustom = normalizeWhitespace(defaults.moduleCustomCommand || '');
  const moduleLoad = normalizeWhitespace(values.moduleLoad || '');
  const defaultModuleLoad = normalizeWhitespace(defaults.moduleLoad || '');
  if (selections.length > 0 && !listsEqual(selections, defaultSelections)) {
    add('Modules', selections.join(', '));
  } else if (custom && custom !== defaultCustom) {
    add('Module command', custom);
  } else if (moduleLoad && moduleLoad !== defaultModuleLoad) {
    add('Module load', moduleLoad);
  }

  if (diffString(values.extraSallocArgs, defaults.extraSallocArgs)) add('Extra salloc args', values.extraSallocArgs);
  if (diffBool(values.promptForExtraSallocArgs, defaults.promptForExtraSallocArgs)) {
    add('Prompt for extra args', values.promptForExtraSallocArgs ? 'Yes' : 'No');
  }

  if (diffString(values.sessionMode, defaults.sessionMode)) add('Session mode', values.sessionMode);
  if (diffString(values.sessionKey, defaults.sessionKey)) add('Session key', values.sessionKey);
  if (diffString(values.sessionIdleTimeoutSeconds, defaults.sessionIdleTimeoutSeconds)) {
    add('Session idle timeout', values.sessionIdleTimeoutSeconds + 's');
  }

  if (diffString(values.loginHostsCommand, defaults.loginHostsCommand)) {
    add('Login hosts command', values.loginHostsCommand);
  }
  if (diffString(values.loginHostsQueryHost, defaults.loginHostsQueryHost)) {
    add('Login hosts query host', values.loginHostsQueryHost);
  }
  if (diffString(values.partitionInfoCommand, defaults.partitionInfoCommand)) {
    add('Partition info command', values.partitionInfoCommand);
  }
  if (diffString(values.partitionCommand, defaults.partitionCommand)) {
    add('Partition list command', values.partitionCommand);
  }
  if (diffString(values.qosCommand, defaults.qosCommand)) add('QoS command', values.qosCommand);
  if (diffString(values.accountCommand, defaults.accountCommand)) add('Account command', values.accountCommand);

  if (diffBool(values.forwardAgent, defaults.forwardAgent)) {
    add('Forward agent', values.forwardAgent ? 'Enabled' : 'Disabled');
  }
  if (diffBool(values.requestTTY, defaults.requestTTY)) {
    add('Request TTY', values.requestTTY ? 'Enabled' : 'Disabled');
  }

  return overrides;
}

function buildProfileResourceSummary(values: UiValues, defaults: UiValues): ProfileResourceSummary {
  const host = firstLoginHostFromInput(values.loginHosts);
  const partition = values.defaultPartition.trim() || undefined;
  const nodesRaw = values.defaultNodes.trim();
  const tasksRaw = values.defaultTasksPerNode.trim();
  const cpusRaw = values.defaultCpusPerTask.trim();
  const memoryRaw = values.defaultMemoryMb.trim();
  const gpuCountRaw = values.defaultGpuCount.trim();
  const gpuType = values.defaultGpuType.trim() || undefined;
  const time = values.defaultTime.trim() || undefined;

  const nodes = nodesRaw ? parsePositiveNumberInput(nodesRaw) || undefined : undefined;
  const tasksPerNode = tasksRaw ? parsePositiveNumberInput(tasksRaw) || undefined : undefined;
  const cpusPerTask = cpusRaw ? parsePositiveNumberInput(cpusRaw) || undefined : undefined;
  const memoryMb = memoryRaw ? parseNonNegativeNumberInput(memoryRaw) || undefined : undefined;
  const gpuCount = gpuCountRaw ? parseNonNegativeNumberInput(gpuCountRaw) : undefined;

  const totalCpu =
    nodes && (tasksPerNode || cpusPerTask)
      ? nodes * (tasksPerNode || 1) * (cpusPerTask || 1)
      : undefined;
  const totalMemoryMb = nodes && memoryMb ? nodes * memoryMb : undefined;
  const totalGpu = nodes && gpuCount !== undefined ? nodes * gpuCount : undefined;

  return {
    host,
    partition,
    nodes,
    tasksPerNode,
    cpusPerTask,
    memoryMb,
    gpuType,
    gpuCount,
    time,
    totalCpu,
    totalMemoryMb,
    totalGpu,
    overrides: buildProfileOverrides(values, defaults)
  };
}

function buildProfileSummaryMap(
  store: ProfileStore,
  defaults: UiValues
): Record<string, ProfileResourceSummary> {
  const summaries: Record<string, ProfileResourceSummary> = {};
  Object.entries(store).forEach(([name, entry]) => {
    const merged = mergeUiValuesWithDefaults(pickProfileValues(entry.values), defaults);
    summaries[name] = buildProfileResourceSummary(merged, defaults);
  });
  return summaries;
}

function getConfigFromSettings(): SlurmConnectConfig {
  const cfg = vscode.workspace.getConfiguration(SETTINGS_SECTION);
  const user = (cfg.get<string>('user') || '').trim();
  return {
    loginHosts: cfg.get<string[]>('loginHosts', []),
    loginHostsCommand: (cfg.get<string>('loginHostsCommand') || '').trim(),
    loginHostsQueryHost: (cfg.get<string>('loginHostsQueryHost') || '').trim(),
    partitionCommand: (cfg.get<string>('partitionCommand') || '').trim(),
    partitionInfoCommand: (cfg.get<string>('partitionInfoCommand') || '').trim(),
    filterFreeResources: cfg.get<boolean>('filterFreeResources', true),
    qosCommand: (cfg.get<string>('qosCommand') || '').trim(),
    accountCommand: (cfg.get<string>('accountCommand') || '').trim(),
    user,
    identityFile: (cfg.get<string>('identityFile') || '').trim(),
    preSshCommand: (cfg.get<string>('preSshCommand') || '').trim(),
    preSshCheckCommand: (cfg.get<string>('preSshCheckCommand') || '').trim(),
    autoInstallProxyScriptOnClusterInfo: cfg.get<boolean>('autoInstallProxyScriptOnClusterInfo', true),
    forwardAgent: cfg.get<boolean>('forwardAgent', true),
    requestTTY: cfg.get<boolean>('requestTTY', true),
    moduleLoad: (cfg.get<string>('moduleLoad') || '').trim(),
    proxyCommand: (cfg.get<string>('proxyCommand') || '').trim(),
    proxyArgs: cfg.get<string[]>('proxyArgs', []),
    localProxyEnabled: cfg.get<boolean>('localProxyEnabled', false),
    localProxyNoProxy: cfg.get<string[]>('localProxyNoProxy', []),
    localProxyPort: cfg.get<number>('localProxyPort', 0),
    localProxyRemoteBind: (cfg.get<string>('localProxyRemoteBind') || '').trim(),
    localProxyRemoteHost: (cfg.get<string>('localProxyRemoteHost') || '').trim(),
    localProxyComputeTunnel: cfg.get<boolean>('localProxyComputeTunnel', true),
    localProxyTunnelMode: normalizeLocalProxyTunnelMode(cfg.get<string>('localProxyTunnelMode') || 'remoteSsh'),
    extraSallocArgs: cfg.get<string[]>('extraSallocArgs', []),
    promptForExtraSallocArgs: cfg.get<boolean>('promptForExtraSallocArgs', false),
    sessionMode: normalizeSessionMode(cfg.get<string>('sessionMode') || 'persistent'),
    sessionKey: (cfg.get<string>('sessionKey') || '').trim(),
    sessionIdleTimeoutSeconds: normalizeNonNegativeInteger(cfg.get<number>('sessionIdleTimeoutSeconds', 600)),
    sessionStateDir: (cfg.get<string>('sessionStateDir') || '').trim(),
    defaultPartition: (cfg.get<string>('defaultPartition') || '').trim(),
    defaultNodes: cfg.get<number>('defaultNodes', 1),
    defaultTasksPerNode: cfg.get<number>('defaultTasksPerNode', 1),
    defaultCpusPerTask: cfg.get<number>('defaultCpusPerTask', 1),
    defaultTime: (cfg.get<string>('defaultTime') || '').trim(),
    defaultMemoryMb: cfg.get<number>('defaultMemoryMb', 0),
    defaultGpuType: (cfg.get<string>('defaultGpuType') || '').trim(),
    defaultGpuCount: cfg.get<number>('defaultGpuCount', 0),
    sshHostPrefix: (cfg.get<string>('sshHostPrefix') || '').trim(),
    openInNewWindow: cfg.get<boolean>('openInNewWindow', false),
    remoteWorkspacePath: (cfg.get<string>('remoteWorkspacePath') || '').trim(),
    temporarySshConfigPath: (cfg.get<string>('temporarySshConfigPath') || '').trim(),
    additionalSshOptions: cfg.get<Record<string, string>>('additionalSshOptions', {}),
    sshQueryConfigPath: (cfg.get<string>('sshQueryConfigPath') || '').trim(),
    sshHostKeyChecking: normalizeSshHostKeyChecking(cfg.get<string>('sshHostKeyChecking') || 'accept-new'),
    sshConnectTimeoutSeconds: cfg.get<number>('sshConnectTimeoutSeconds', 15)
  };
}

function getConfigDefaultsFromSettings(): SlurmConnectConfig {
  const cfg = vscode.workspace.getConfiguration(SETTINGS_SECTION);
  const getDefault = <T>(key: string, fallback: T): T => {
    const inspected = cfg.inspect<T>(key);
    if (inspected && inspected.defaultValue !== undefined) {
      return inspected.defaultValue as T;
    }
    return fallback;
  };
  const user = String(getDefault<string>('user', '') || '').trim();
  return {
    loginHosts: getDefault<string[]>('loginHosts', []),
    loginHostsCommand: String(getDefault<string>('loginHostsCommand', '') || '').trim(),
    loginHostsQueryHost: String(getDefault<string>('loginHostsQueryHost', '') || '').trim(),
    partitionCommand: String(getDefault<string>('partitionCommand', '') || '').trim(),
    partitionInfoCommand: String(getDefault<string>('partitionInfoCommand', '') || '').trim(),
    filterFreeResources: Boolean(getDefault<boolean>('filterFreeResources', true)),
    qosCommand: String(getDefault<string>('qosCommand', '') || '').trim(),
    accountCommand: String(getDefault<string>('accountCommand', '') || '').trim(),
    user,
    identityFile: String(getDefault<string>('identityFile', '') || '').trim(),
    preSshCommand: String(getDefault<string>('preSshCommand', '') || '').trim(),
    preSshCheckCommand: String(getDefault<string>('preSshCheckCommand', '') || '').trim(),
    autoInstallProxyScriptOnClusterInfo: Boolean(getDefault<boolean>('autoInstallProxyScriptOnClusterInfo', true)),
    forwardAgent: Boolean(getDefault<boolean>('forwardAgent', true)),
    requestTTY: Boolean(getDefault<boolean>('requestTTY', true)),
    moduleLoad: String(getDefault<string>('moduleLoad', '') || '').trim(),
    proxyCommand: String(getDefault<string>('proxyCommand', '') || '').trim(),
    proxyArgs: getDefault<string[]>('proxyArgs', []),
    localProxyEnabled: Boolean(getDefault<boolean>('localProxyEnabled', false)),
    localProxyNoProxy: getDefault<string[]>('localProxyNoProxy', []),
    localProxyPort: Number(getDefault<number>('localProxyPort', 0)),
    localProxyRemoteBind: String(getDefault<string>('localProxyRemoteBind', '') || '').trim(),
    localProxyRemoteHost: String(getDefault<string>('localProxyRemoteHost', '') || '').trim(),
    localProxyComputeTunnel: Boolean(getDefault<boolean>('localProxyComputeTunnel', true)),
    localProxyTunnelMode: normalizeLocalProxyTunnelMode(String(getDefault<string>('localProxyTunnelMode', 'remoteSsh') || 'remoteSsh')),
    extraSallocArgs: getDefault<string[]>('extraSallocArgs', []),
    promptForExtraSallocArgs: Boolean(getDefault<boolean>('promptForExtraSallocArgs', false)),
    sessionMode: normalizeSessionMode(String(getDefault<string>('sessionMode', 'persistent') || 'persistent')),
    sessionKey: String(getDefault<string>('sessionKey', '') || '').trim(),
    sessionIdleTimeoutSeconds: normalizeNonNegativeInteger(getDefault<number>('sessionIdleTimeoutSeconds', 600)),
    sessionStateDir: String(getDefault<string>('sessionStateDir', '') || '').trim(),
    defaultPartition: String(getDefault<string>('defaultPartition', '') || '').trim(),
    defaultNodes: Number(getDefault<number>('defaultNodes', 1)),
    defaultTasksPerNode: Number(getDefault<number>('defaultTasksPerNode', 1)),
    defaultCpusPerTask: Number(getDefault<number>('defaultCpusPerTask', 1)),
    defaultTime: String(getDefault<string>('defaultTime', '') || '').trim(),
    defaultMemoryMb: Number(getDefault<number>('defaultMemoryMb', 0)),
    defaultGpuType: String(getDefault<string>('defaultGpuType', '') || '').trim(),
    defaultGpuCount: Number(getDefault<number>('defaultGpuCount', 0)),
    sshHostPrefix: String(getDefault<string>('sshHostPrefix', '') || '').trim(),
    openInNewWindow: Boolean(getDefault<boolean>('openInNewWindow', false)),
    remoteWorkspacePath: String(getDefault<string>('remoteWorkspacePath', '') || '').trim(),
    temporarySshConfigPath: String(getDefault<string>('temporarySshConfigPath', '') || '').trim(),
    additionalSshOptions: getDefault<Record<string, string>>('additionalSshOptions', {}),
    sshQueryConfigPath: String(getDefault<string>('sshQueryConfigPath', '') || '').trim(),
    sshHostKeyChecking: normalizeSshHostKeyChecking(String(getDefault<string>('sshHostKeyChecking', 'accept-new') || 'accept-new')),
    sshConnectTimeoutSeconds: Number(getDefault<number>('sshConnectTimeoutSeconds', 15))
  };
}

function getConfig(): SlurmConnectConfig {
  const base = getConfigFromSettings();
  const cachedUi = getMergedClusterUiCache();
  if (!cachedUi) {
    return base;
  }
  const overrides = buildClusterOverridesFromUi(cachedUi);
  return {
    ...base,
    ...overrides
  };
}

function getConfigWithOverrides(overrides?: Partial<SlurmConnectConfig>): SlurmConnectConfig {
  const base = getConfig();
  if (!overrides) {
    return base;
  }
  return {
    ...base,
    ...overrides
  };
}

class SlurmConnectViewProvider implements vscode.WebviewViewProvider {
  private view?: vscode.WebviewView;

  constructor(private readonly context: vscode.ExtensionContext) {}

  resolveWebviewView(view: vscode.WebviewView): void {
    this.view = view;
    const webview = view.webview;
    activeWebview = webview;
    view.onDidDispose(() => {
      if (activeWebview === webview) {
        activeWebview = undefined;
      }
    });
    webview.options = {
      enableScripts: true,
      localResourceRoots: [this.context.extensionUri]
    };

    webview.html = getWebviewHtml(webview);
    webview.onDidReceiveMessage(async (message) => {
      switch (message.command) {
        case 'ready': {
          syncConnectionStateFromEnvironment();
          let activeProfile = getActiveProfileName();
          const defaults = getUiValuesFromStorage();
          const values = buildWebviewValues(defaults);
          if (activeProfile && !getProfileValues(activeProfile)) {
            activeProfile = undefined;
          }
          const agentStatus = await buildAgentStatusMessage(defaults.identityFile);
          const host = firstLoginHostFromInput(defaults.loginHosts);
          const cached = host ? getCachedClusterInfo(host) : undefined;
          webview.postMessage({
            command: 'load',
            values,
            defaults: buildWebviewValues(getUiDefaultsFromConfig()),
            clusterInfo: cached?.info,
            clusterInfoCachedAt: cached?.fetchedAt,
            sessions: [],
            profiles: getProfileSummaries(getProfileStore()),
            profileSummaries: buildProfileSummaryMap(getProfileStore(), getUiGlobalDefaults()),
            activeProfile,
            connectionState,
            connectionSessionMode: lastConnectionSessionMode,
            agentStatus: agentStatus.text,
            agentStatusError: agentStatus.isError,
            remoteActive: vscode.env.remoteName === 'ssh-remote',
            saveTarget: resolvePreferredSaveTarget()
          });
          void loadSshHostsForWebview(buildOverridesFromUi(pickProfileValues(values)));
          break;
        }
        case 'connect': {
          const target = message.target === 'workspace'
            ? vscode.ConfigurationTarget.Workspace
            : vscode.ConfigurationTarget.Global;
          const uiValues = message.values as Partial<UiValues>;
          const sessionSelection =
            typeof message.sessionSelection === 'string' ? message.sessionSelection.trim() : '';
          await updateConfigFromUi(uiValues, target);
          if (connectionState === 'connecting' && activeConnectToken && !activeConnectToken.cancelled) {
            break;
          }
          const token = createConnectToken();
          setConnectionState('connecting');
          const overrides = buildOverridesFromUi(uiValues);
          if (sessionSelection) {
            overrides.sessionMode = 'persistent';
            overrides.sessionKey = sessionSelection;
          }
          let connected = false;
          try {
            connected = await connectCommand(overrides, {
              interactive: false,
              aliasOverride: sessionSelection,
              token
            });
          } catch (error) {
            getOutputChannel().appendLine(`Connect failed: ${formatError(error)}`);
          }
          if (!isConnectCancelled(token)) {
            setConnectionState(connected ? 'connected' : 'idle');
          }
          finalizeConnectToken(token);
          break;
        }
        case 'cancelConnect': {
          cancelActiveConnectToken();
          stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: false });
          setConnectionState('idle');
          break;
        }
        case 'saveSettings': {
          const target = message.target === 'workspace'
            ? vscode.ConfigurationTarget.Workspace
            : vscode.ConfigurationTarget.Global;
          const uiValues = message.values as Partial<UiValues>;
          await updateConfigFromUi(uiValues, target);
          break;
        }
        case 'resolveSshHost': {
          const host = typeof message.host === 'string' ? message.host.trim() : '';
          if (!host) {
            break;
          }
          const uiValues = message.values as Partial<UiValues> | undefined;
          const overrides = uiValues ? buildOverridesFromUi(uiValues) : undefined;
          try {
            const cfg = getConfigWithOverrides(overrides);
            const resolved = await resolveSshHostFromConfig(host, cfg);
            webview.postMessage({
              command: 'sshHostResolved',
              ...resolved
            });
          } catch (error) {
            webview.postMessage({
              command: 'sshHostResolved',
              host,
              error: formatError(error)
            });
          }
          break;
        }
        case 'refreshAgentStatus': {
          const identityFile = typeof message.identityFile === 'string' ? message.identityFile : '';
          const agentStatus = await buildAgentStatusMessage(identityFile);
          webview.postMessage({
            command: 'agentStatus',
            agentStatus: agentStatus.text,
            agentStatusError: agentStatus.isError
          });
          break;
        }
        case 'pickIdentityFile': {
          const result = await vscode.window.showOpenDialog({
            title: 'Select identity file',
            canSelectFiles: true,
            canSelectFolders: false,
            canSelectMany: false
          });
          if (result && result[0]) {
            webview.postMessage({
              command: 'pickedPath',
              field: 'identityFile',
              value: result[0].fsPath
            });
          }
          break;
        }
        case 'disconnect': {
          setConnectionState('disconnecting');
          const disconnected = await disconnectFromHost(lastConnectionAlias);
          if (disconnected) {
            lastConnectionSessionKey = undefined;
            lastConnectionSessionMode = undefined;
            lastConnectionLoginHost = undefined;
          }
          setConnectionState(disconnected ? 'idle' : 'connected');
          break;
        }
        case 'cancelSession': {
          if (lastConnectionSessionMode !== 'persistent') {
            void vscode.window.showWarningMessage('No persistent Slurm session is active to cancel.');
            break;
          }
          const label = lastConnectionSessionKey || lastConnectionAlias || 'current session';
          const choice = await vscode.window.showWarningMessage(
            `Cancel job "${label}"? This will end the allocation and close the remote connection. Close this dialog to keep it running.`,
            { modal: true },
            'Cancel job'
          );
          if (choice !== 'Cancel job') {
            break;
          }
          const cfg = getConfig();
          const sessionKey =
            lastConnectionSessionKey ||
            (lastConnectionAlias ? resolveSessionKey(cfg, lastConnectionAlias) : '');
          const loginHost =
            lastConnectionLoginHost ||
            (cfg.loginHosts.length > 0 ? cfg.loginHosts[0] : undefined);
          if (loginHost && sessionKey) {
            await cancelPersistentSessionJob(loginHost, sessionKey, cfg, {
              useTerminal: true
            });
          } else {
            void vscode.window.showWarningMessage(
              'Unable to resolve the login host or session key to cancel the Slurm job.'
            );
          }
          break;
        }
        case 'saveProfile': {
          const uiValues = message.values as Partial<UiValues>;
          const suggestedRaw = typeof message.suggestedName === 'string' ? message.suggestedName : '';
          const suggestedName = suggestedRaw.trim();
          const nameInput = await vscode.window.showInputBox({
            title: 'Profile name',
            prompt: 'Enter a profile name',
            value: suggestedName || undefined
          });
          if (nameInput === undefined) {
            webview.postMessage({
              command: 'profiles',
              profiles: getProfileSummaries(getProfileStore()),
              profileSummaries: buildProfileSummaryMap(getProfileStore(), getUiGlobalDefaults()),
              activeProfile: getActiveProfileName(),
              profileStatus: 'Profile save cancelled.'
            });
            break;
          }
          const name = nameInput.trim();
          if (!name) {
            webview.postMessage({
              command: 'profiles',
              profiles: getProfileSummaries(getProfileStore()),
              profileSummaries: buildProfileSummaryMap(getProfileStore(), getUiGlobalDefaults()),
              activeProfile: getActiveProfileName(),
              profileStatus: 'Enter a profile name before saving.',
              profileError: true
            });
            break;
          }
          const store = getProfileStore();
          const existing = store[name];
          const now = new Date().toISOString();
          store[name] = {
            name,
            values: pickProfileValues(uiValues),
            createdAt: existing?.createdAt ?? now,
            updatedAt: now
          };
          if (extensionGlobalState) {
            await extensionGlobalState.update(PROFILE_STORE_KEY, store);
            await setActiveProfileName(name);
          }
          webview.postMessage({
            command: 'profiles',
            profiles: getProfileSummaries(store),
            profileSummaries: buildProfileSummaryMap(store, getUiGlobalDefaults()),
            activeProfile: name,
            profileStatus: existing ? `Profile "${name}" updated.` : `Profile "${name}" saved.`
          });
          break;
        }
        case 'loadProfile': {
          const rawName = typeof message.name === 'string' ? message.name : '';
          const name = rawName.trim();
          if (!name) {
            webview.postMessage({
              command: 'profiles',
              profiles: getProfileSummaries(getProfileStore()),
              profileSummaries: buildProfileSummaryMap(getProfileStore(), getUiGlobalDefaults()),
              activeProfile: getActiveProfileName(),
              profileStatus: 'Select a profile to load.',
              profileError: true
            });
            break;
          }
          const store = getProfileStore();
          const profile = store[name];
          if (!profile) {
            webview.postMessage({
              command: 'profiles',
              profiles: getProfileSummaries(store),
              profileSummaries: buildProfileSummaryMap(store, getUiGlobalDefaults()),
              activeProfile: getActiveProfileName(),
              profileStatus: `Profile "${name}" not found.`,
              profileError: true
            });
            break;
          }
          await setActiveProfileName(name);
          syncConnectionStateFromEnvironment();
          const resolved = getProfileValues(name);
          if (!resolved) {
            webview.postMessage({
              command: 'profiles',
              profiles: getProfileSummaries(store),
              profileSummaries: buildProfileSummaryMap(store, getUiGlobalDefaults()),
              activeProfile: getActiveProfileName(),
              profileStatus: `Profile "${name}" not found.`,
              profileError: true
            });
            break;
          }
          const host = firstLoginHostFromInput(resolved.loginHosts);
          const cached = host ? getCachedClusterInfo(host) : undefined;
          webview.postMessage({
            command: 'load',
            values: pickProfileValues(resolved),
            clusterInfo: cached?.info,
            clusterInfoCachedAt: cached?.fetchedAt,
            profiles: getProfileSummaries(store),
            profileSummaries: buildProfileSummaryMap(store, getUiGlobalDefaults()),
            activeProfile: name,
            connectionState,
            remoteActive: vscode.env.remoteName === 'ssh-remote',
            profileStatus: `Loaded profile "${name}".`,
            saveTarget: resolvePreferredSaveTarget()
          });
          break;
        }
        case 'deleteProfile': {
          const rawName = typeof message.name === 'string' ? message.name : '';
          const name = rawName.trim();
          const store = getProfileStore();
          if (!name || !store[name]) {
            webview.postMessage({
              command: 'profiles',
              profiles: getProfileSummaries(store),
              profileSummaries: buildProfileSummaryMap(store, getUiGlobalDefaults()),
              activeProfile: getActiveProfileName(),
              profileStatus: 'Select a profile to delete.',
              profileError: true
            });
            break;
          }
          const confirmDelete = await vscode.window.showWarningMessage(
            `Delete profile "${name}"?`,
            { modal: true },
            'Delete'
          );
          if (confirmDelete !== 'Delete') {
            webview.postMessage({
              command: 'profiles',
              profiles: getProfileSummaries(store),
              profileSummaries: buildProfileSummaryMap(store, getUiGlobalDefaults()),
              activeProfile: getActiveProfileName(),
              profileStatus: 'Profile deletion cancelled.'
            });
            break;
          }
          delete store[name];
          if (extensionGlobalState) {
            await extensionGlobalState.update(PROFILE_STORE_KEY, store);
          }
          let activeProfile = getActiveProfileName();
          if (activeProfile === name) {
            activeProfile = undefined;
            await setActiveProfileName(undefined);
          }
          webview.postMessage({
            command: 'profiles',
            profiles: getProfileSummaries(store),
            profileSummaries: buildProfileSummaryMap(store, getUiGlobalDefaults()),
            activeProfile,
            profileStatus: `Profile "${name}" deleted.`
          });
          break;
        }
        case 'getClusterInfo': {
          const uiValues = message.values as Partial<UiValues>;
          await handleClusterInfoRequest(uiValues, webview);
          break;
        }
        case 'openSettings': {
          void vscode.commands.executeCommand('workbench.action.openSettings', SETTINGS_SECTION);
          break;
        }
        case 'openLogs': {
          await openLogFile();
          break;
        }
        case 'openRemoteSshLog': {
          await openRemoteSshLog();
          break;
        }
        default:
          break;
      }
    });
  }
}

async function connectCommand(
  overrides?: Partial<SlurmConnectConfig>,
  options?: { interactive?: boolean; aliasOverride?: string; token?: ConnectToken }
): Promise<boolean> {
  const cfg = getConfigWithOverrides(overrides);
  const interactive = options?.interactive !== false;
  const connectToken = options?.token;
  const log = getOutputChannel();
  log.clear();
  log.appendLine('Slurm Connect started.');

  let didConnect = false;
  const wasCancelled = (): boolean => isConnectCancelled(connectToken);
  const cancelAndReturn = (): boolean => {
    if (!wasCancelled()) {
      return false;
    }
    log.appendLine('Connect cancelled by user.');
    stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: false });
    return true;
  };

  if (wasCancelled()) {
    return false;
  }

  const loginHosts = await resolveLoginHosts(cfg, { interactive });
  log.appendLine(`Login hosts resolved: ${loginHosts.join(', ') || '(none)'}`);
  if (cancelAndReturn()) {
    return false;
  }
  if (loginHosts.length === 0) {
    void vscode.window.showErrorMessage('No login hosts available. Configure slurmConnect.loginHosts or loginHostsCommand.');
    return false;
  }

  let loginHost: string | undefined;
  if (loginHosts.length === 1) {
    loginHost = loginHosts[0];
  } else if (interactive) {
    loginHost = await pickFromList('Select login host', loginHosts, true);
  } else {
    loginHost = loginHosts[0];
    log.appendLine(`Using first login host: ${loginHost}`);
  }
  if (!loginHost) {
    return false;
  }
  if (cancelAndReturn()) {
    return false;
  }

  const proceed = await maybePromptForSshAuthOnConnect(cfg, loginHost);
  if (!proceed) {
    return false;
  }
  if (cancelAndReturn()) {
    return false;
  }

  let partition: string | undefined;
  let qos: string | undefined;
  let account: string | undefined;

  if (interactive) {
    const { partitions, defaultPartition } = await vscode.window.withProgress(
      {
        location: vscode.ProgressLocation.Notification,
        title: 'Querying Slurm resources',
        cancellable: false
      },
      async () => {
        const partitionResult = await queryPartitions(loginHost, cfg);
        return partitionResult;
      }
    );

    const partitionPick = await pickPartition(partitions, cfg.defaultPartition || defaultPartition);
    if (partitionPick === null) {
      return false;
    }
    partition = partitionPick;
    if (cancelAndReturn()) {
      return false;
    }

    qos = await pickOptionalValue('Select QoS (optional)', await querySimpleList(loginHost, cfg, cfg.qosCommand));
    account = await pickOptionalValue(
      'Select account (optional)',
      await querySimpleList(loginHost, cfg, cfg.accountCommand)
    );
    if (cancelAndReturn()) {
      return false;
    }
  } else {
    partition = cfg.defaultPartition || undefined;
  }

  let nodes = cfg.defaultNodes;
  let tasksPerNode = cfg.defaultTasksPerNode;
  let cpusPerTask = cfg.defaultCpusPerTask;
  let time = cfg.defaultTime;
  let memoryMb = cfg.defaultMemoryMb;
  let gpuType = cfg.defaultGpuType;
  let gpuCount = cfg.defaultGpuCount;

  if (interactive) {
    const nodesInput = await promptNumber('Nodes', cfg.defaultNodes, 1);
    if (nodesInput === undefined) {
      return false;
    }
    nodes = nodesInput;
    if (cancelAndReturn()) {
      return false;
    }

    const tasksInput = await promptNumber('Tasks per node', cfg.defaultTasksPerNode, 1);
    if (tasksInput === undefined) {
      return false;
    }
    tasksPerNode = tasksInput;
    if (cancelAndReturn()) {
      return false;
    }

    const cpusInput = await promptNumber('CPUs per task', cfg.defaultCpusPerTask, 1);
    if (cpusInput === undefined) {
      return false;
    }
    cpusPerTask = cpusInput;
    if (cancelAndReturn()) {
      return false;
    }

    const timeInput = await promptTime('Wall time', cfg.defaultTime || '24:00:00');
    if (timeInput === undefined) {
      return false;
    }
    time = timeInput;
    if (cancelAndReturn()) {
      return false;
    }
  }

  if (!partition || partition.trim().length === 0) {
    const resolvedPartition = await resolveDefaultPartitionForHost(loginHost, cfg);
    if (resolvedPartition) {
      partition = resolvedPartition;
      log.appendLine(`Using default partition: ${partition}`);
    }
  }
  if (cancelAndReturn()) {
    return false;
  }

  if (!time || time.trim().length === 0) {
    const resolvedTime = await resolvePartitionDefaultTimeForHost(loginHost, partition, cfg);
    if (resolvedTime) {
      time = resolvedTime;
      log.appendLine(`Using partition default time: ${time}`);
    } else {
      time = '24:00:00';
      log.appendLine(`No partition default time found; using fallback wall time: ${time}`);
    }
  }
  if (cancelAndReturn()) {
    return false;
  }

  let extraArgs: string[] = [];
  if (interactive && cfg.promptForExtraSallocArgs) {
    const extra = await vscode.window.showInputBox({
      title: 'Extra salloc args (optional)',
      prompt: 'Example: --gres=gpu:1 --mem=32G',
      placeHolder: '--gres=gpu:1'
    });
    if (extra && extra.trim().length > 0) {
      extraArgs = splitArgs(extra.trim());
    }
  }
  if (cancelAndReturn()) {
    return false;
  }

  const sallocArgs = buildSallocArgs({
    partition,
    nodes,
    tasksPerNode,
    cpusPerTask,
    time,
    memoryMb,
    gpuType,
    gpuCount,
    qos,
    account
  });

  const defaultAlias = buildDefaultAlias(cfg.sshHostPrefix || 'slurm', loginHost, partition, nodes, cpusPerTask);
  const aliasOverride = options?.aliasOverride ? options.aliasOverride.trim() : '';
  let alias = aliasOverride || defaultAlias;
  if (interactive && !aliasOverride) {
    const aliasInput = await vscode.window.showInputBox({
      title: 'SSH host alias',
      value: defaultAlias,
      prompt: 'This name will appear in your SSH config and Remote-SSH hosts list.',
      validateInput: (value) => (value.trim().length === 0 ? 'Alias is required.' : undefined)
    });
    if (!aliasInput) {
      return false;
    }
    alias = aliasInput.trim();
  }
  if (cancelAndReturn()) {
    return false;
  }
  if (!alias) {
    alias = defaultAlias;
  }

  lastConnectionAlias = alias.trim();

  const sessionKey = resolveSessionKey(cfg, alias.trim());
  const clientId = vscode.env.sessionId || '';
  const installSnippet = await getProxyInstallSnippet(cfg);
  const tunnelMode = normalizeLocalProxyTunnelMode(cfg.localProxyTunnelMode || 'remoteSsh');
  const shouldRetryProxyPort = cfg.localProxyEnabled && tunnelMode === 'remoteSsh';
  const maxProxyPortAttempts = shouldRetryProxyPort ? 3 : 1;
  let proxyPortCandidates: number[] = [];
  let localProxyPlan: LocalProxyPlan = { enabled: false };

  await ensureRemoteSshSettings(cfg);
  if (cancelAndReturn()) {
    return false;
  }
  await migrateStaleRemoteSshConfigIfNeeded();
  if (cancelAndReturn()) {
    return false;
  }
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const currentRemoteConfig = normalizeRemoteConfigPath(remoteCfg.get<string>('configFile'));
  const baseConfigPath = normalizeSshPath(currentRemoteConfig || defaultSshConfigPath());
  if (!currentRemoteConfig) {
    try {
      await fs.access(baseConfigPath);
    } catch {
      log.appendLine(`SSH config not found at ${baseConfigPath}; creating.`);
    }
  }
  const includePath = resolveSlurmConnectIncludePath(cfg);
  const includeFilePath = resolveSlurmConnectIncludeFilePath(cfg);
  if (normalizeSshPath(baseConfigPath) === includeFilePath) {
    const message = 'Slurm Connect include file path must not be the same as the SSH config path.';
    log.appendLine(message);
    void vscode.window.showErrorMessage(message);
    return false;
  }
  if (cancelAndReturn()) {
    return false;
  }

  let preSshReady = false;
  let connected = false;
  for (let attempt = 0; attempt < maxProxyPortAttempts; attempt += 1) {
    if (cancelAndReturn()) {
      return false;
    }
    const overridePort =
      shouldRetryProxyPort && proxyPortCandidates.length > 0 ? proxyPortCandidates[attempt] : undefined;
    try {
      localProxyPlan = await ensureLocalProxyPlan(cfg, loginHost, { remotePortOverride: overridePort });
    } catch (error) {
      if (!wasCancelled()) {
        const message = `Failed to start local proxy: ${formatError(error)}`;
        log.appendLine(message);
        void vscode.window.showErrorMessage(message);
      }
      return false;
    }
    if (cancelAndReturn()) {
      return false;
    }
    if (shouldRetryProxyPort && proxyPortCandidates.length === 0) {
      const basePort = localProxyState?.port ?? 0;
      proxyPortCandidates = buildLocalProxyRemotePortCandidates(basePort, maxProxyPortAttempts);
    }
    const localProxyEnv =
      localProxyPlan.enabled && localProxyPlan.proxyUrl && !localProxyPlan.computeTunnel
        ? { proxyUrl: localProxyPlan.proxyUrl, noProxy: localProxyPlan.noProxy }
        : undefined;
    const remoteCommand = buildRemoteCommand(
      cfg,
      [...sallocArgs, ...cfg.extraSallocArgs, ...extraArgs],
      sessionKey,
      clientId,
      installSnippet,
      localProxyEnv,
      localProxyPlan
    );
    if (!remoteCommand) {
      if (!wasCancelled()) {
        void vscode.window.showErrorMessage('RemoteCommand is empty. Check slurmConnect.proxyCommand.');
      }
      return false;
    }
    if (cancelAndReturn()) {
      return false;
    }

    await ensureSshAgentEnvForCurrentSsh();
    const additionalSshOptions: Record<string, string> = { ...(cfg.additionalSshOptions || {}) };
    if (process.platform === 'win32') {
      const sshPath = await resolveSshToolPath('ssh');
      const sock = process.env.SSH_AUTH_SOCK || '';
      if (sock && isGitSshPath(sshPath) && !hasSshOption(additionalSshOptions, 'IdentityAgent')) {
        additionalSshOptions.IdentityAgent = sock;
      }
    }

    const hostEntry = buildHostEntry(
      alias.trim(),
      loginHost,
      { ...cfg, additionalSshOptions, extraSshOptions: localProxyPlan.sshOptions },
      remoteCommand
    );
    log.appendLine('Generated SSH host entry:');
    log.appendLine(redactRemoteCommandForLog(hostEntry));

    try {
      await writeSlurmConnectIncludeFile(hostEntry, includeFilePath);
      const status = await ensureSshIncludeInstalled(baseConfigPath, includePath);
      log.appendLine(`SSH config include ${status} in ${baseConfigPath}.`);
    } catch (error) {
      if (!wasCancelled()) {
        const message = `Failed to install SSH Include block: ${formatError(error)}`;
        log.appendLine(message);
        void vscode.window.showErrorMessage(message);
      }
      return false;
    }
    if (cancelAndReturn()) {
      return false;
    }
    await delay(300);
    await refreshRemoteSshHosts();
    if (cancelAndReturn()) {
      return false;
    }

    if (!preSshReady) {
      try {
        await ensurePreSshCommand(cfg, `Remote-SSH connect to ${alias.trim()}`);
      } catch (error) {
        if (!wasCancelled()) {
          const message = `Pre-SSH command failed: ${formatError(error)}`;
          log.appendLine(message);
          void vscode.window.showErrorMessage(message);
        }
        return false;
      }
      preSshReady = true;
    }
    if (cancelAndReturn()) {
      return false;
    }

    connected = await connectToHost(
      alias.trim(),
      cfg.openInNewWindow,
      cfg.remoteWorkspacePath
    );
    if (cancelAndReturn()) {
      return false;
    }
    if (connected) {
      break;
    }
    if (shouldRetryProxyPort && attempt < maxProxyPortAttempts - 1) {
      const nextPort = proxyPortCandidates[attempt + 1];
      const message = nextPort
        ? `Remote-SSH connect failed; retrying local proxy forward on port ${nextPort}.`
        : 'Remote-SSH connect failed; retrying with an alternate local proxy port.';
      log.appendLine(message);
    }
  }
  didConnect = connected;
  if (connected) {
    lastConnectionSessionKey = sessionKey || undefined;
    lastConnectionSessionMode = cfg.sessionMode;
    lastConnectionLoginHost = loginHost;
  } else {
    lastConnectionSessionKey = undefined;
    lastConnectionSessionMode = undefined;
    lastConnectionLoginHost = undefined;
  }
  if (!connected) {
    if (!wasCancelled()) {
      void vscode.window.showWarningMessage(
        `SSH host "${alias.trim()}" created, but auto-connect failed. Use Remote-SSH to connect.`,
        'Show Output'
      ).then((selection) => {
        if (selection === 'Show Output') {
          getOutputChannel().show(true);
        }
      });
    }
    return didConnect;
  }
  if (cfg.openInNewWindow) {
    return didConnect;
  }
  await waitForRemoteConnectionOrTimeout(30_000, 500);

  return didConnect;
}

async function resolveLoginHosts(
  cfg: SlurmConnectConfig,
  options?: { interactive?: boolean }
): Promise<string[]> {
  const interactive = options?.interactive !== false;
  const log = getOutputChannel();
  let hosts = cfg.loginHosts.slice();
  if (cfg.loginHostsCommand) {
    let queryHost: string | undefined = cfg.loginHostsQueryHost || hosts[0];
    if (!queryHost && interactive) {
      queryHost = await vscode.window.showInputBox({
        title: 'Login host for discovery',
        prompt: 'Enter a login host to run the loginHostsCommand on.'
      });
    }
    if (!queryHost && !interactive) {
      log.appendLine('Login host discovery skipped (no query host and prompts disabled).');
    }
    if (queryHost) {
      try {
        const output = await runSshCommand(queryHost, cfg, cfg.loginHostsCommand);
        const discovered = parseSimpleList(output);
        if (discovered.length > 0) {
          hosts = discovered;
        }
      } catch (error) {
        void vscode.window.showWarningMessage(`Failed to query login hosts: ${formatError(error)}`);
      }
    }
  }

  hosts = uniqueList(hosts);
  if (hosts.length === 0 && interactive) {
    const manual = await vscode.window.showInputBox({
      title: 'Login host',
      prompt: 'Enter a login host'
    });
    if (manual) {
      hosts = [manual.trim()];
    }
  }
  if (hosts.length === 0 && !interactive) {
    log.appendLine('No login hosts available and prompts are disabled.');
  }
  return hosts;
}

async function queryPartitions(loginHost: string, cfg: SlurmConnectConfig): Promise<PartitionResult> {
  if (!cfg.partitionCommand) {
    return { partitions: [] };
  }
  try {
    const output = await runSshCommand(loginHost, cfg, cfg.partitionCommand);
    return parsePartitionOutput(output);
  } catch (error) {
    void vscode.window.showWarningMessage(`Failed to query partitions: ${formatError(error)}`);
    return { partitions: [] };
  }
}

async function querySimpleList(loginHost: string, cfg: SlurmConnectConfig, command: string): Promise<string[]> {
  if (!command) {
    return [];
  }
  try {
    const output = await runSshCommand(loginHost, cfg, command);
    return parseSimpleList(output);
  } catch (error) {
    void vscode.window.showWarningMessage(`Failed to query resources: ${formatError(error)}`);
    return [];
  }
}

async function queryAvailableModules(loginHost: string, cfg: SlurmConnectConfig): Promise<string[]> {
  const log = getOutputChannel();
  const commands = [
    'module -t avail 2>&1',
    'bash -lc "module -t avail 2>&1"'
  ];

  for (const command of commands) {
    try {
      log.appendLine(`Module list command: ${command}`);
      const output = await runSshCommand(loginHost, cfg, command);
      const modules = parseModulesOutput(output);
      if (modules.length > 0) {
        return modules;
      }
    } catch (error) {
      log.appendLine(`Module list command failed: ${formatError(error)}`);
    }
  }
  return [];
}

type PartitionDefaults = {
  defaultTime?: string;
  defaultNodes?: number;
  defaultTasksPerNode?: number;
  defaultCpusPerTask?: number;
  defaultMemoryMb?: number;
  defaultGpuType?: string;
  defaultGpuCount?: number;
};

function mergePartitionDefaults(
  base: Record<string, PartitionDefaults>,
  incoming: Record<string, PartitionDefaults>
): Record<string, PartitionDefaults> {
  const merged: Record<string, PartitionDefaults> = { ...base };
  for (const [name, defaults] of Object.entries(incoming)) {
    if (!merged[name]) {
      merged[name] = { ...defaults };
      continue;
    }
    const existing = merged[name] as Record<string, unknown>;
    for (const [key, value] of Object.entries(defaults)) {
      if (value !== undefined) {
        existing[key] = value;
      }
    }
  }
  return merged;
}

function applyPartitionDefaultTimes(info: ClusterInfo, defaults: Record<string, PartitionDefaults>): void {
  if (!info.partitions || info.partitions.length === 0) {
    return;
  }
  for (const partition of info.partitions) {
    const entry = defaults[partition.name];
    if (!entry) {
      continue;
    }
    if (entry.defaultTime) {
      partition.defaultTime = entry.defaultTime;
    }
    if (entry.defaultNodes !== undefined) {
      partition.defaultNodes = entry.defaultNodes;
    }
    if (entry.defaultTasksPerNode !== undefined) {
      partition.defaultTasksPerNode = entry.defaultTasksPerNode;
    }
    if (entry.defaultCpusPerTask !== undefined) {
      partition.defaultCpusPerTask = entry.defaultCpusPerTask;
    }
    if (entry.defaultMemoryMb !== undefined) {
      partition.defaultMemoryMb = entry.defaultMemoryMb;
    }
    if (entry.defaultGpuType !== undefined) {
      partition.defaultGpuType = entry.defaultGpuType;
    }
    if (entry.defaultGpuCount !== undefined) {
      partition.defaultGpuCount = entry.defaultGpuCount;
    }
  }
}

function resolveDefaultPartitionName(info: ClusterInfo): string | undefined {
  if (info.defaultPartition) {
    return info.defaultPartition;
  }
  const flagged = info.partitions.find((partition) => partition.isDefault);
  return flagged ? flagged.name : undefined;
}

function resolvePartitionDefaultTime(info: ClusterInfo, partition?: string): string | undefined {
  const effective = partition || resolveDefaultPartitionName(info);
  if (!effective) {
    return undefined;
  }
  const match = info.partitions.find((item) => item.name === effective);
  return match?.defaultTime;
}

async function resolveDefaultPartitionForHost(
  loginHost: string,
  cfg: SlurmConnectConfig
): Promise<string | undefined> {
  const cached = getCachedClusterInfo(loginHost)?.info;
  if (cached) {
    const cachedPartition = resolveDefaultPartitionName(cached);
    if (cachedPartition) {
      return cachedPartition;
    }
  }
  try {
    const info = await fetchClusterInfo(loginHost, cfg);
    cacheClusterInfo(loginHost, info);
    return resolveDefaultPartitionName(info);
  } catch (error) {
    getOutputChannel().appendLine(`Failed to resolve default partition: ${formatError(error)}`);
    return undefined;
  }
}

async function resolvePartitionDefaultTimeForHost(
  loginHost: string,
  partition: string | undefined,
  cfg: SlurmConnectConfig
): Promise<string | undefined> {
  const cached = getCachedClusterInfo(loginHost)?.info;
  if (cached) {
    const cachedTime = resolvePartitionDefaultTime(cached, partition);
    if (cachedTime) {
      return cachedTime;
    }
  }
  try {
    const info = await fetchClusterInfo(loginHost, cfg);
    cacheClusterInfo(loginHost, info);
    return resolvePartitionDefaultTime(info, partition);
  } catch (error) {
    getOutputChannel().appendLine(`Failed to resolve partition default time: ${formatError(error)}`);
    return undefined;
  }
}

async function fetchExistingSessions(loginHost: string, cfg: SlurmConnectConfig): Promise<SessionSummary[]> {
  const stateDir = cfg.sessionStateDir.trim() || DEFAULT_SESSION_STATE_DIR;
  const command = buildSessionQueryCommand(stateDir);
  const output = await runSshCommand(loginHost, cfg, command);
  const sessions = parseSessionListOutput(output);
  return applySessionIdleInfo(sessions);
}

async function fetchClusterInfoWithSessions(
  loginHost: string,
  cfg: SlurmConnectConfig
): Promise<{ info: ClusterInfo; sessions: SessionSummary[] }> {
  const { commands, freeResourceIndexes } = buildClusterInfoCommandSet(cfg);
  const log = getOutputChannel();
  const sessionsCommand = buildSessionQueryCommand(cfg.sessionStateDir.trim() || DEFAULT_SESSION_STATE_DIR);
  const combinedCommand = buildCombinedClusterInfoCommand(commands, sessionsCommand);
  log.appendLine(`Cluster info single-call (with sessions) command: ${combinedCommand}`);
  const output = await runSshCommand(loginHost, cfg, combinedCommand);
  const { commandOutputs, modulesOutput, sessionsOutput } = parseCombinedClusterInfoOutput(output, commands.length);
  const info = buildClusterInfoFromOutputs(commandOutputs, modulesOutput, freeResourceIndexes);
  const sessions = applySessionIdleInfo(parseSessionListOutput(sessionsOutput));
  return { info, sessions };
}

function buildSessionQueryCommand(stateDir: string): string {
  const dirLiteral = JSON.stringify(stateDir);
  const script = [
    'import json, os, subprocess, sys, re, time',
    `state_dir = os.path.expanduser(${dirLiteral})`,
    'sessions_dir = os.path.join(state_dir, "sessions")',
    'if not os.path.isdir(sessions_dir):',
    '    print("[]")',
    '    sys.exit(0)',
    'session_key_re = re.compile(r"[^A-Za-z0-9_.-]+")',
    'def sanitize(value):',
    '    trimmed = (value or "").strip()',
    '    if not trimmed:',
    '        return "default"',
    '    sanitized = session_key_re.sub("-", trimmed).strip(".-")',
    '    return sanitized[:64] if sanitized else "default"',
    'try:',
    '    user = os.environ.get("USER") or subprocess.check_output(["id", "-un"], text=True).strip()',
    'except Exception:',
    '    user = None',
    'safe_user = sanitize(user or "unknown")',
    'state_map = {}',
    'job_ids = []',
    'try:',
    '    cmd = ["squeue", "-h", "-o", "%i|%T|%j"]',
    '    if user:',
    '        cmd[2:2] = ["-u", user]',
    '    output = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)',
    '    for line in output.splitlines():',
    '        parts = line.split("|", 2)',
    '        if len(parts) >= 2:',
    '            job_id = parts[0].strip()',
    '            state = parts[1].strip()',
    '            job_name = parts[2].strip() if len(parts) > 2 else ""',
    '            if job_id:',
    '                state_map[job_id] = {"state": state, "jobName": job_name}',
    '                job_ids.append(job_id)',
    'except Exception:',
    '    pass',
    'job_details = {}',
    'if job_ids:',
    '    try:',
    '        detail_out = subprocess.check_output(["scontrol", "show", "job", "-o"] + job_ids, text=True, stderr=subprocess.DEVNULL)',
    '        for line in detail_out.splitlines():',
    '            fields = {}',
    '            for token in line.split():',
    '                if "=" not in token:',
    '                    continue',
    '                key, value = token.split("=", 1)',
    '                fields[key] = value',
    '            job_id = fields.get("JobId") or fields.get("JobID")',
    '            if not job_id:',
    '                continue',
    '            detail = {}',
    '            if fields.get("Partition"):',
    '                detail["partition"] = fields.get("Partition")',
    '            if fields.get("NumNodes"):',
    '                detail["nodes"] = fields.get("NumNodes")',
    '            if fields.get("NumCPUs"):',
    '                detail["cpus"] = fields.get("NumCPUs")',
    '            if fields.get("TimeLimit"):',
    '                detail["timeLimit"] = fields.get("TimeLimit")',
    '            if detail:',
    '                job_details[job_id] = detail',
    '    except Exception:',
    '        pass',
    'session_dirs = []',
    'user_root = os.path.join(sessions_dir, safe_user)',
    'if os.path.isdir(user_root):',
    '    for entry in sorted(os.listdir(user_root)):',
    '        session_dirs.append((entry, os.path.join(user_root, entry)))',
    'for entry in sorted(os.listdir(sessions_dir)):',
    '    path = os.path.join(sessions_dir, entry)',
    '    job_path = os.path.join(path, "job.json")',
    '    if not os.path.isfile(job_path):',
    '        continue',
    '    session_dirs.append((entry, path))',
    'sessions = []',
    'seen_job_ids = set()',
    'for entry, session_dir in session_dirs:',
    '    job_path = os.path.join(session_dir, "job.json")',
    '    if not os.path.isfile(job_path):',
    '        continue',
    '    try:',
    '        with open(job_path, "r") as handle:',
    '            data = json.load(handle)',
    '    except Exception:',
    '        continue',
    '    job_id = str(data.get("job_id", "")).strip()',
    '    if not job_id:',
    '        continue',
    '    if job_id in seen_job_ids:',
    '        continue',
    '    info = state_map.get(job_id)',
    '    if not info:',
    '        continue',
    '    seen_job_ids.add(job_id)',
    '    detail = job_details.get(job_id, {})',
    '    clients_dir = os.path.join(session_dir, "clients")',
    '    client_ids = set()',
    '    now = time.time()',
    '    try:',
    '        raw_stale = data.get("stale_seconds")',
    '        stale_seconds = int(raw_stale) if raw_stale is not None else 90',
    '    except Exception:',
    '        stale_seconds = 90',
    '    try:',
    '        if os.path.isdir(clients_dir):',
    '            for name in os.listdir(clients_dir):',
    '                path = os.path.join(clients_dir, name)',
    '                if not os.path.isfile(path):',
    '                    continue',
    '                if stale_seconds > 0:',
    '                    try:',
    '                        age = now - os.path.getmtime(path)',
    '                        if age > stale_seconds:',
    '                            continue',
    '                    except Exception:',
    '                        pass',
    '                client_key = name',
    '                if name.startswith("client-"):',
    '                    prefix = name.split(".", 1)[0]',
    '                    client_key = prefix[7:] if len(prefix) > 7 else name',
    '                client_ids.add(client_key)',
    '    except Exception:',
    '        client_ids = set()',
    '    last_seen = ""',
    '    last_seen_path = os.path.join(session_dir, "last_seen")',
    '    if os.path.isfile(last_seen_path):',
    '        try:',
    '            with open(last_seen_path, "r") as handle:',
    '                last_seen = handle.read().strip()',
    '        except Exception:',
    '            last_seen = ""',
    '    sessions.append({',
    '        "sessionKey": str(data.get("session_key") or entry),',
    '        "jobId": job_id,',
    '        "state": info.get("state", ""),',
    '        "jobName": (info.get("jobName") or data.get("job_name") or ""),',
    '        "createdAt": str(data.get("created_at") or ""),',
    '        "clients": len(client_ids),',
    '        "lastSeen": last_seen,',
    '        "idleTimeoutSeconds": data.get("idle_timeout_seconds", ""),',
    '        "partition": detail.get("partition", ""),',
    '        "nodes": detail.get("nodes", ""),',
    '        "cpus": detail.get("cpus", ""),',
    '        "timeLimit": detail.get("timeLimit", "")',
    '    })',
    'print(json.dumps(sessions))'
  ].join('\n');
  const encodedScript = Buffer.from(script, 'utf8').toString('base64');
  return wrapPythonScriptCommand(encodedScript, '"[]"');
}

function parseSessionListOutput(output: string): SessionSummary[] {
  const trimmed = output.trim();
  if (!trimmed) {
    return [];
  }
  try {
    const parsed = JSON.parse(trimmed);
    if (!Array.isArray(parsed)) {
      return [];
    }
    const results: SessionSummary[] = [];
    for (const entry of parsed) {
      if (!entry || typeof entry !== 'object') {
        continue;
      }
      const sessionKey = typeof entry.sessionKey === 'string' ? entry.sessionKey.trim() : '';
      const jobId = typeof entry.jobId === 'string' ? entry.jobId.trim() : '';
      if (!sessionKey || !jobId) {
        continue;
      }
      const state = typeof entry.state === 'string' ? entry.state.trim() : '';
      const jobName = typeof entry.jobName === 'string' ? entry.jobName.trim() : '';
      const createdAt = typeof entry.createdAt === 'string' ? entry.createdAt.trim() : '';
      const partition = typeof entry.partition === 'string' ? entry.partition.trim() : '';
      const timeLimit = typeof entry.timeLimit === 'string' ? entry.timeLimit.trim() : '';
      const nodes = Number(entry.nodes);
      const cpus = Number(entry.cpus);
      const clientsValue = Number(entry.clients);
      const lastSeenValue = Number(entry.lastSeen);
      const idleTimeoutValue = Number(entry.idleTimeoutSeconds);
      results.push({
        sessionKey,
        jobId,
        state,
        jobName: jobName || undefined,
        createdAt: createdAt || undefined,
        partition: partition || undefined,
        nodes: Number.isFinite(nodes) && nodes > 0 ? nodes : undefined,
        cpus: Number.isFinite(cpus) && cpus > 0 ? cpus : undefined,
        timeLimit: timeLimit || undefined,
        clients: Number.isFinite(clientsValue) && clientsValue >= 0 ? clientsValue : undefined,
        lastSeenEpoch: Number.isFinite(lastSeenValue) && lastSeenValue > 0 ? lastSeenValue : undefined,
        idleTimeoutSeconds:
          Number.isFinite(idleTimeoutValue) && idleTimeoutValue > 0 ? idleTimeoutValue : undefined
      });
    }
    return results;
  } catch {
    return [];
  }
}

function applySessionIdleInfo(sessions: SessionSummary[]): SessionSummary[] {
  const nowSeconds = Math.floor(Date.now() / 1000);
  return sessions.map((session) => {
    const next: SessionSummary = { ...session };
    const timeout = typeof session.idleTimeoutSeconds === 'number' ? session.idleTimeoutSeconds : 0;
    if (timeout > 0) {
      const clients = typeof session.clients === 'number' ? session.clients : 0;
      const lastSeen = session.lastSeenEpoch;
      if (clients === 0 && typeof lastSeen === 'number' && Number.isFinite(lastSeen)) {
        const idleSeconds = Math.max(0, nowSeconds - lastSeen);
        next.idleRemainingSeconds = Math.max(0, timeout - idleSeconds);
      }
    }
    return next;
  });
}

async function handleClusterInfoRequest(values: Partial<UiValues>, webview: vscode.Webview): Promise<void> {
  if (clusterInfoRequestInFlight) {
    return;
  }
  clusterInfoRequestInFlight = true;
  const overrides = buildOverridesFromUi(values);
  const cfg = getConfigWithOverrides(overrides);
  const loginHosts = parseListInput(String(values.loginHosts ?? ''));
  const log = getOutputChannel();
  let sessions: SessionSummary[] = [];

  try {
    if (loginHosts.length === 0) {
      const message = 'Enter a login host before fetching cluster info.';
      void vscode.window.showErrorMessage(message);
      const agentStatus = await buildAgentStatusMessage(String(values.identityFile ?? ''));
      webview.postMessage({
        command: 'clusterInfoError',
        message,
        sessions: [],
        agentStatus: agentStatus.text,
        agentStatusError: agentStatus.isError
      });
      return;
    }

    const loginHost = loginHosts[0];
    log.appendLine(`Fetching cluster info from ${loginHost}...`);
    const proceed = await maybePromptForSshAuthOnConnect(cfg, loginHost);
    if (!proceed) {
      const message = 'Set an SSH identity file in the Slurm Connect view, then retry.';
      const agentStatus = await buildAgentStatusMessage(String(values.identityFile ?? ''));
      webview.postMessage({
        command: 'clusterInfoError',
        message,
        sessions: [],
        agentStatus: agentStatus.text,
        agentStatusError: agentStatus.isError
      });
      return;
    }

    let info: ClusterInfo;
    try {
      const result = await fetchClusterInfoWithSessions(loginHost, cfg);
      info = result.info;
      sessions = result.sessions;
    } catch (error) {
      log.appendLine(`Single-call cluster info failed, falling back. ${formatError(error)}`);
      info = await fetchClusterInfo(loginHost, cfg);
    }
    cacheClusterInfo(loginHost, info);
    const cached = getCachedClusterInfo(loginHost);
    const agentStatus = await buildAgentStatusMessage(String(values.identityFile ?? ''));
    webview.postMessage({
      command: 'clusterInfo',
      info,
      sessions,
      fetchedAt: cached?.fetchedAt,
      agentStatus: agentStatus.text,
      agentStatusError: agentStatus.isError
    });
  } catch (error) {
    const message = formatError(error);
    void vscode.window.showErrorMessage(`Failed to fetch cluster info: ${message}`);
    const agentStatus = await buildAgentStatusMessage(String(values.identityFile ?? ''));
    webview.postMessage({
      command: 'clusterInfoError',
      message,
      sessions,
      agentStatus: agentStatus.text,
      agentStatusError: agentStatus.isError
    });
  } finally {
    clusterInfoRequestInFlight = false;
  }
}

async function fetchClusterInfo(loginHost: string, cfg: SlurmConnectConfig): Promise<ClusterInfo> {
  const { commands, freeResourceIndexes } = buildClusterInfoCommandSet(cfg);

  const log = getOutputChannel();
  let info: ClusterInfo | undefined;
  try {
    info = await fetchClusterInfoSingleCall(loginHost, cfg, commands, freeResourceIndexes);
  } catch (error) {
    log.appendLine(`Single-call cluster info failed, falling back. ${formatError(error)}`);
  }

  if (!info) {
    let lastInfo: ClusterInfo = { partitions: [] };
    let bestInfo: ClusterInfo | undefined;
    let partitionDefaults: Record<string, PartitionDefaults> = {};
    let defaultPartitionFromScontrol: string | undefined;
    const fallbackCommands = commands.slice(0, freeResourceIndexes.infoCommandCount);
    for (const command of fallbackCommands) {
      log.appendLine(`Cluster info command: ${command}`);
      const output = await runSshCommand(loginHost, cfg, command);
      const defaultsResult = parsePartitionDefaultTimesOutput(output);
      if (Object.keys(defaultsResult.defaults).length > 0 || defaultsResult.defaultPartition) {
        partitionDefaults = mergePartitionDefaults(partitionDefaults, defaultsResult.defaults);
        if (!defaultPartitionFromScontrol && defaultsResult.defaultPartition) {
          defaultPartitionFromScontrol = defaultsResult.defaultPartition;
        }
        continue;
      }
      const parsed = parsePartitionInfoOutput(output);
      lastInfo = parsed;
      const maxFields = getMaxFieldCount(output);
      const outputHasGpu = output.includes('gpu:');
      const hasGpu = parsed.partitions.some((partition) => partition.gpuMax > 0);
      log.appendLine(
        `Cluster info fields: ${maxFields}, partitions: ${parsed.partitions.length}, outputHasGpu: ${outputHasGpu}, hasGpu: ${hasGpu}`
      );
      if (outputHasGpu && !hasGpu) {
        log.appendLine('GPU data present but parse yielded none; trying next command.');
        continue;
      }
      if (maxFields < 5) {
        continue;
      }
      if (hasMeaningfulClusterInfo(parsed)) {
        bestInfo = parsed;
        break;
      }
    }
    info = bestInfo ?? lastInfo;
    const modules = await queryAvailableModules(loginHost, cfg);
    info.modules = modules;
    if (defaultPartitionFromScontrol && !info.defaultPartition) {
      info.defaultPartition = defaultPartitionFromScontrol;
    }
    applyPartitionDefaultTimes(info, partitionDefaults);
    if (cfg.filterFreeResources) {
      log.appendLine('Free resource filtering skipped because cluster info used the multi-call fallback.');
    }
  }

  return info;
}

const CLUSTER_CMD_START = '__SC_CMD_START__';
const CLUSTER_CMD_END = '__SC_CMD_END__';
const CLUSTER_MODULES_START = '__SC_MODULES_START__';
const CLUSTER_MODULES_END = '__SC_MODULES_END__';
const CLUSTER_SESSIONS_START = '__SC_SESSIONS_START__';
const CLUSTER_SESSIONS_END = '__SC_SESSIONS_END__';

function buildCombinedClusterInfoCommand(
  commands: string[],
  sessionsCommand?: string
): string {
  const encodedCommands = commands.map((command) => Buffer.from(command, 'utf8').toString('base64'));
  const commandArray = encodedCommands.map((encoded) => `'${encoded}'`).join(' ');
  const script = [
    'set +e',
    `cmds=(${commandArray})`,
    'i=0',
    'for b64 in "${cmds[@]}"; do',
    `  echo "${CLUSTER_CMD_START}\${i}__"`,
    '  cmd=$(printf %s "$b64" | base64 -d)',
    '  eval "$cmd"',
    `  echo "${CLUSTER_CMD_END}\${i}__"`,
    '  i=$((i+1))',
    'done',
    sessionsCommand ? `echo "${CLUSTER_SESSIONS_START}"` : '',
    sessionsCommand ? sessionsCommand : '',
    sessionsCommand ? `echo "${CLUSTER_SESSIONS_END}"` : '',
    `echo "${CLUSTER_MODULES_START}"`,
    'modules=$(module -t avail 2>&1)',
    'status=$?',
    'if [ $status -ne 0 ] || echo "$modules" | grep -qi "command not found\\|module: not found"; then',
    '  modules=$(bash -lc "module -t avail 2>&1")',
    'fi',
    'printf "%s\\n" "$modules"',
    `echo "${CLUSTER_MODULES_END}"`
  ].join('\n');

  const encodedScript = Buffer.from(script, 'utf8').toString('base64');
  return `bash -lc 'printf %s ${encodedScript} | base64 -d | bash'`;
}

function parseCombinedClusterInfoOutput(
  output: string,
  commandCount: number
): { commandOutputs: string[]; modulesOutput: string; sessionsOutput: string } {
  const commandOutputs: string[] = Array.from({ length: commandCount }, () => '');
  let currentIndex: number | null = null;
  let inModules = false;
  let inSessions = false;
  const moduleLines: string[] = [];
  const sessionLines: string[] = [];
  const lines = output.split(/\r?\n/);

  for (const line of lines) {
    if (line === CLUSTER_SESSIONS_START) {
      inSessions = true;
      currentIndex = null;
      continue;
    }
    if (line === CLUSTER_SESSIONS_END) {
      inSessions = false;
      continue;
    }
    if (line === CLUSTER_MODULES_START) {
      inModules = true;
      currentIndex = null;
      continue;
    }
    if (line === CLUSTER_MODULES_END) {
      inModules = false;
      continue;
    }
    if (line.startsWith(CLUSTER_CMD_START)) {
      const raw = line.slice(CLUSTER_CMD_START.length);
      const match = raw.match(/^(\d+)__$/);
      currentIndex = match ? Number(match[1]) : null;
      continue;
    }
    if (line.startsWith(CLUSTER_CMD_END)) {
      currentIndex = null;
      continue;
    }
    if (inModules) {
      moduleLines.push(line);
      continue;
    }
    if (inSessions) {
      sessionLines.push(line);
      continue;
    }
    if (currentIndex !== null && currentIndex >= 0 && currentIndex < commandOutputs.length) {
      commandOutputs[currentIndex] += line + '\n';
    }
  }

  return {
    commandOutputs: commandOutputs.map((text) => text.trim()),
    modulesOutput: moduleLines.join('\n').trim(),
    sessionsOutput: sessionLines.join('\n').trim()
  };
}

function sanitizeModuleOutput(output: string): string {
  const withoutOsc = output.replace(/\u001B\][^\u0007]*(?:\u0007|\u001B\\)/g, '');
  const withoutAnsi = withoutOsc.replace(
    /[\u001B\u009B][[()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nq-uy=><]/g,
    ''
  );
  return withoutAnsi.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F\u0080-\u009F]/g, '');
}

function parseModulesOutput(output: string): string[] {
  if (!output) {
    return [];
  }
  const sanitized = sanitizeModuleOutput(output);
  const lowered = sanitized.toLowerCase();
  if (lowered.includes('command not found') || lowered.includes('module: not found')) {
    return [];
  }
  const lines = sanitized.split(/\r?\n/);
  const entries: string[] = [];
  const isSeparatorToken = (token: string): boolean => /^-+$/.test(token);
  const isLegendLine = (line: string): boolean => {
    const lower = line.toLowerCase();
    if (lower.startsWith('where:')) {
      return true;
    }
    if (lower.includes('module is loaded') || lower.includes('module is auto-loaded')) {
      return true;
    }
    if (lower.includes('module is inactive') || lower.includes('module is hidden')) {
      return true;
    }
    if (lower.includes('module spider') || lower.includes('module help')) {
      return true;
    }
    if (lower.startsWith('to get ') || lower.startsWith('to find ') || lower.startsWith('to list ')) {
      return true;
    }
    if (lower.startsWith('use "module') || lower.startsWith('use module')) {
      return true;
    }
    return false;
  };
  const stripTrailingTag = (value: string): string => {
    let trimmed = value.trim();
    if (!trimmed) {
      return trimmed;
    }
    const defaultMatch = trimmed.match(/^(.*?)(?:\s*[<(]\s*(?:default|d)\s*[>)]\s*)$/i);
    if (defaultMatch) {
      const base = defaultMatch[1].trim();
      return base ? `${base} (default)` : trimmed;
    }
    const shortTagPattern = /^(.*?)(?:\s*[<(]\s*[a-zA-Z]{1,3}\s*[>)]\s*)$/;
    while (shortTagPattern.test(trimmed)) {
      trimmed = trimmed.replace(shortTagPattern, '$1').trim();
    }
    return trimmed;
  };
  const normalizeHeader = (value: string): string => {
    const trimmed = value.trim();
    if (!trimmed) {
      return '';
    }
    return trimmed.endsWith(':') ? trimmed : `${trimmed}:`;
  };
  const headerFromDashLine = (line: string): string | null => {
    const match = line.match(/^-+\s*(\/\S.*?)\s*-+$/);
    return match ? match[1] : null;
  };
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) {
      continue;
    }
    if (isLegendLine(trimmed)) {
      continue;
    }
    if (trimmed.startsWith('/') && trimmed.endsWith(':')) {
      entries.push(trimmed);
      continue;
    }
    const dashedHeader = headerFromDashLine(trimmed);
    if (dashedHeader) {
      const normalized = normalizeHeader(dashedHeader);
      if (normalized) {
        entries.push(normalized);
      }
      continue;
    }
    const tokens = /\s/.test(trimmed)
      ? trimmed.split(/\s+/).map((value) => value.trim()).filter(Boolean)
      : [trimmed];
    for (const token of tokens) {
      if (isSeparatorToken(token)) {
        continue;
      }
      if (token.startsWith('/') && token.endsWith(':')) {
        entries.push(token);
        continue;
      }
      const cleaned = stripTrailingTag(token);
      if (!cleaned) {
        continue;
      }
      entries.push(cleaned);
    }
  }
  return uniqueList(entries);
}

async function fetchClusterInfoSingleCall(
  loginHost: string,
  cfg: SlurmConnectConfig,
  commands: string[],
  freeResourceIndexes?: FreeResourceCommandIndexes
): Promise<ClusterInfo> {
  const log = getOutputChannel();
  const combinedCommand = buildCombinedClusterInfoCommand(commands);
  log.appendLine(`Cluster info single-call command: ${combinedCommand}`);
  const output = await runSshCommand(loginHost, cfg, combinedCommand);
  const { commandOutputs, modulesOutput } = parseCombinedClusterInfoOutput(output, commands.length);
  return buildClusterInfoFromOutputs(commandOutputs, modulesOutput, freeResourceIndexes);
}

function buildClusterInfoFromOutputs(
  commandOutputs: string[],
  modulesOutput: string,
  freeResourceIndexes?: FreeResourceCommandIndexes
): ClusterInfo {
  const log = getOutputChannel();
  let lastInfo: ClusterInfo = { partitions: [] };
  let bestInfo: ClusterInfo | undefined;
  let partitionDefaults: Record<string, PartitionDefaults> = {};
  let defaultPartitionFromScontrol: string | undefined;
  const infoCommandCount = freeResourceIndexes ? freeResourceIndexes.infoCommandCount : commandOutputs.length;
  for (let i = 0; i < infoCommandCount; i += 1) {
    const cmdOutput = commandOutputs[i];
    if (!cmdOutput) {
      continue;
    }
    const defaultsResult = parsePartitionDefaultTimesOutput(cmdOutput);
    if (Object.keys(defaultsResult.defaults).length > 0 || defaultsResult.defaultPartition) {
      partitionDefaults = mergePartitionDefaults(partitionDefaults, defaultsResult.defaults);
      if (!defaultPartitionFromScontrol && defaultsResult.defaultPartition) {
        defaultPartitionFromScontrol = defaultsResult.defaultPartition;
      }
      continue;
    }
    const info = parsePartitionInfoOutput(cmdOutput);
    lastInfo = info;
    const maxFields = getMaxFieldCount(cmdOutput);
    const outputHasGpu = cmdOutput.includes('gpu:');
    const hasGpu = info.partitions.some((partition) => partition.gpuMax > 0);
    log.appendLine(
      `Cluster info fields: ${maxFields}, partitions: ${info.partitions.length}, outputHasGpu: ${outputHasGpu}, hasGpu: ${hasGpu}`
    );
    if (outputHasGpu && !hasGpu) {
      log.appendLine('GPU data present but parse yielded none; trying next command.');
      continue;
    }
    if (maxFields < 5) {
      continue;
    }
    if (hasMeaningfulClusterInfo(info)) {
      bestInfo = info;
      break;
    }
  }

  const info = bestInfo ?? lastInfo;
  info.modules = parseModulesOutput(modulesOutput);
  if (defaultPartitionFromScontrol && !info.defaultPartition) {
    info.defaultPartition = defaultPartitionFromScontrol;
  }
  applyPartitionDefaultTimes(info, partitionDefaults);
  if (freeResourceIndexes) {
    const nodeOutput = commandOutputs[freeResourceIndexes.nodeIndex] || '';
    const jobOutput = commandOutputs[freeResourceIndexes.jobIndex] || '';
    const freeSummary = computeFreeResourceSummary(nodeOutput, jobOutput);
    if (freeSummary) {
      applyFreeResourceSummary(info, freeSummary);
    }
  }
  return info;
}


function normalizeSshErrorText(error: unknown): string {
  if (error && typeof error === 'object') {
    const err = error as { stderr?: string; stdout?: string; message?: string };
    return [err.message, err.stderr, err.stdout].filter(Boolean).join('\n');
  }
  return String(error);
}

function getSshAgentEnv(): { sock: string; pid: string } {
  return {
    sock: process.env.SSH_AUTH_SOCK || '',
    pid: process.env.SSH_AGENT_PID || ''
  };
}

interface StoredSshAgentEnv {
  sock: string;
  pid?: string;
}

function getStoredSshAgentEnv(): StoredSshAgentEnv | undefined {
  if (!extensionGlobalState) {
    return undefined;
  }
  const stored = extensionGlobalState.get<StoredSshAgentEnv>(SSH_AGENT_ENV_KEY);
  if (!stored || !stored.sock) {
    return undefined;
  }
  return stored;
}

function storeSshAgentEnv(env: { sock: string; pid?: string }): void {
  if (!extensionGlobalState || !env.sock) {
    return;
  }
  void extensionGlobalState.update(SSH_AGENT_ENV_KEY, { sock: env.sock, pid: env.pid });
}

type SshToolName = 'ssh' | 'ssh-add' | 'ssh-keygen';
const sshToolPathCache: Partial<Record<SshToolName, string>> = {};
const WINDOWS_OPENSSH_PATH = 'C:\\Windows\\System32\\OpenSSH\\ssh.exe';
const WINDOWS_OPENSSH_AGENT_PIPE = '\\\\.\\pipe\\openssh-ssh-agent';
const SSH_VERSION_TIMEOUT_MS = 10_000;
const HAS_WOW64 = Object.prototype.hasOwnProperty.call(process.env, 'PROCESSOR_ARCHITEW6432');
let cachedRemoteSshPathSetting = '';
let cachedRemoteUseLocalServer: boolean | undefined;

enum SshCommandKind {
  NotFound = 0,
  WindowsSsh = 1,
  Other = 2
}

function stripOuterQuotes(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length < 2) {
    return trimmed;
  }
  const first = trimmed[0];
  const last = trimmed[trimmed.length - 1];
  if ((first === '"' && last === '"') || (first === "'" && last === "'")) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function resolveRemoteSshPathSetting(): string {
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const configured = String(remoteCfg.get<string>('path') || '').trim();
  return configured ? stripOuterQuotes(configured) : '';
}

async function listSshPathsFromWhere(): Promise<string[]> {
  try {
    const { stdout } = await execFileAsync('where', ['ssh']);
    return stdout
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);
  } catch {
    return [];
  }
}

function isGitSshPath(value: string): boolean {
  const normalized = value.toLowerCase();
  return normalized.includes('\\git\\') || normalized.includes('/git/');
}

async function resolveGitSshAgentPath(sshPath: string): Promise<string | undefined> {
  if (!sshPath) {
    return undefined;
  }
  const candidate = path.join(path.dirname(sshPath), 'ssh-agent.exe');
  if (await fileExists(candidate)) {
    return candidate;
  }
  return undefined;
}

async function ensureSshAgentEnvForCurrentSsh(identityPathRaw?: string): Promise<void> {
  if (process.platform !== 'win32') {
    return;
  }
  const log = getOutputChannel();
  const sshPath = await resolveSshToolPath('ssh');
  if (!isGitSshPath(sshPath)) {
    log.appendLine(`Skipping Git ssh-agent setup (SSH path: ${sshPath}).`);
    return;
  }
  const sshAdd = await resolveSshToolPath('ssh-add');
  const identityPath = identityPathRaw ? expandHome(identityPathRaw) : '';
  const identityFingerprint = identityPath ? await getPublicKeyFingerprint(identityPath) : undefined;

  const candidates: Array<{ sock: string; pid?: string; label: string }> = [];
  const seen = new Set<string>();
  const currentEnv = getSshAgentEnv();
  if (currentEnv.sock && !seen.has(currentEnv.sock)) {
    seen.add(currentEnv.sock);
    candidates.push({ sock: currentEnv.sock, pid: currentEnv.pid, label: 'current' });
  }
  const storedEnv = getStoredSshAgentEnv();
  if (storedEnv?.sock && !seen.has(storedEnv.sock)) {
    seen.add(storedEnv.sock);
    candidates.push({ sock: storedEnv.sock, pid: storedEnv.pid, label: 'stored' });
  }
  if (!seen.has(WINDOWS_OPENSSH_AGENT_PIPE)) {
    seen.add(WINDOWS_OPENSSH_AGENT_PIPE);
    candidates.push({ sock: WINDOWS_OPENSSH_AGENT_PIPE, label: 'windows' });
  }

  const probes: Array<{
    candidate: { sock: string; pid?: string; label: string };
    info: SshAgentInfo;
    matchesIdentity: boolean;
  }> = [];

  for (const candidate of candidates) {
    const info = await probeSshAgentWithEnv(sshAdd, candidate);
    if (info.status === 'unavailable') {
      continue;
    }
    const matchesIdentity = identityFingerprint ? info.output.includes(identityFingerprint) : false;
    probes.push({ candidate, info, matchesIdentity });
  }

  if (probes.length > 0) {
    const matched = probes.find((probe) => probe.matchesIdentity);
    const available = probes.find((probe) => probe.info.status === 'available');
    const fallback = probes.find((probe) => probe.info.status === 'empty');
    const selected = matched ?? available ?? fallback;
    if (selected) {
      process.env.SSH_AUTH_SOCK = selected.candidate.sock;
      if (selected.candidate.pid) {
        process.env.SSH_AGENT_PID = selected.candidate.pid;
      } else {
        delete process.env.SSH_AGENT_PID;
      }
      if (selected.candidate.label === 'windows') {
        log.appendLine(`Using Windows OpenSSH agent pipe for Git SSH: ${WINDOWS_OPENSSH_AGENT_PIPE}`);
      } else if (selected.candidate.label === 'stored') {
        log.appendLine(`Restored SSH agent socket from state: ${selected.candidate.sock}`);
      }
      if (selected.info.status === 'available') {
        storeSshAgentEnv({ sock: selected.candidate.sock, pid: selected.candidate.pid });
      }
      return;
    }
  }

  const agentPath = await resolveGitSshAgentPath(sshPath);
  if (!agentPath) {
    log.appendLine(`Git ssh-agent not found next to ${sshPath}.`);
    return;
  }
  try {
    log.appendLine(`Starting Git ssh-agent: ${agentPath}`);
    const result = await execFileAsync(agentPath, ['-s']);
    const output = [result.stdout, result.stderr].filter(Boolean).join('\n');
    const sockMatch = /SSH_AUTH_SOCK=([^;\r\n]+)/.exec(output);
    const pidMatch = /SSH_AGENT_PID=([^;\r\n]+)/.exec(output);
    if (sockMatch && sockMatch[1]) {
      process.env.SSH_AUTH_SOCK = sockMatch[1].trim();
    }
    if (pidMatch && pidMatch[1]) {
      process.env.SSH_AGENT_PID = pidMatch[1].trim();
    }
    if (process.env.SSH_AUTH_SOCK) {
      log.appendLine(`Git SSH_AUTH_SOCK set to ${process.env.SSH_AUTH_SOCK}`);
      storeSshAgentEnv({ sock: process.env.SSH_AUTH_SOCK, pid: process.env.SSH_AGENT_PID });
    } else {
      log.appendLine(`Git ssh-agent did not return SSH_AUTH_SOCK. Output: ${output.trim() || '(empty)'}`);
    }
  } catch {
    log.appendLine('Failed to start Git ssh-agent.');
    // Ignore agent startup failures; ssh-add will surface errors.
  }
}

function hasSshOption(
  options: Record<string, string> | undefined,
  key: string
): boolean {
  if (!options) {
    return false;
  }
  const target = key.toLowerCase();
  return Object.keys(options).some((optionKey) => optionKey.toLowerCase() === target);
}

function resetSshToolCacheIfNeeded(): void {
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const pathSetting = String(remoteCfg.get<string>('path') || '').trim();
  const useLocalServer = remoteCfg.get<boolean>('useLocalServer', false);
  if (pathSetting === cachedRemoteSshPathSetting && useLocalServer === cachedRemoteUseLocalServer) {
    return;
  }
  cachedRemoteSshPathSetting = pathSetting;
  cachedRemoteUseLocalServer = useLocalServer;
  for (const key of Object.keys(sshToolPathCache)) {
    delete sshToolPathCache[key as SshToolName];
  }
}

function collectWindowsSshCandidates(): string[] {
  const candidates: string[] = [];
  const pathEnv = process.env.PATH;
  if (pathEnv) {
    const parts = pathEnv
      .split(';')
      .map((entry) => entry.trim())
      .filter(Boolean)
      .filter((entry) => path.isAbsolute(entry))
      .map((entry) => path.join(entry, 'ssh.exe'));
    candidates.push(...parts);
  }
  if (process.env.windir) {
    candidates.push(path.join(process.env.windir, 'System32', 'OpenSSH', 'ssh.exe'));
  }
  if (process.env.ProgramFiles) {
    candidates.push(path.join(process.env.ProgramFiles, 'Git', 'usr', 'bin', 'ssh.exe'));
  }
  const programFilesX86 = process.env['ProgramFiles(x86)'];
  if (programFilesX86) {
    candidates.push(path.join(programFilesX86, 'Git', 'usr', 'bin', 'ssh.exe'));
  }
  if (process.env.LOCALAPPDATA) {
    candidates.push(path.join(process.env.LOCALAPPDATA, 'Programs', 'Git', 'usr', 'bin', 'ssh.exe'));
  }
  if (HAS_WOW64) {
    const root = process.env.SystemRoot || 'C:\\WINDOWS';
    candidates.unshift(path.join(root, 'Sysnative', 'OpenSSH', 'ssh.exe'));
  }
  const seen = new Set<string>();
  return candidates.filter((candidate) => {
    const key = candidate.toLowerCase();
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

async function detectSshCommandKind(sshCommand: string, log: vscode.OutputChannel): Promise<SshCommandKind> {
  return new Promise((resolve) => {
    log.appendLine(`Checking ssh with "${sshCommand} -V"`);
    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];
    let command = sshCommand;
    let options: { shell?: boolean } | undefined;
    if (process.platform === 'win32' && (sshCommand.endsWith('.bat') || sshCommand.endsWith('.cmd'))) {
      command = `"${sshCommand}"`;
      options = { shell: true };
    }
    const child = spawn(command, ['-V'], options);
    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      log.appendLine('ssh is not exiting, continuing');
      resolve(SshCommandKind.NotFound);
      child.kill();
    }, SSH_VERSION_TIMEOUT_MS);
    child.stdout.on('data', (chunk) => {
      stdoutChunks.push(chunk);
    });
    child.stderr.on('data', (chunk) => {
      stderrChunks.push(chunk);
    });
    child.on('error', (error) => {
      if (timedOut) {
        return;
      }
      clearTimeout(timeout);
      log.appendLine(`Got error from ssh: ${error.message}`);
      resolve(SshCommandKind.NotFound);
    });
    child.on('exit', (code) => {
      if (timedOut) {
        return;
      }
      clearTimeout(timeout);
      if (code) {
        log.appendLine(`ssh exited with code: ${code}`);
        resolve(SshCommandKind.NotFound);
        return;
      }
      const output = Buffer.concat([...stdoutChunks, ...stderrChunks]).toString('utf8').trim();
      if (output.match(/OpenSSH_for_Windows/i)) {
        resolve(SshCommandKind.WindowsSsh);
        return;
      }
      if (output.match(/OpenSSH/i)) {
        resolve(SshCommandKind.Other);
        return;
      }
      log.appendLine('ssh output did not match /OpenSSH/');
      resolve(SshCommandKind.NotFound);
    });
  });
}

async function isValidSshCommand(sshCommand: string, log: vscode.OutputChannel): Promise<boolean> {
  return (await detectSshCommandKind(sshCommand, log)) !== SshCommandKind.NotFound;
}

async function resolveSshCommandPath(): Promise<string> {
  resetSshToolCacheIfNeeded();
  const cached = sshToolPathCache.ssh;
  if (cached) {
    return cached;
  }
  const log = getOutputChannel();
  const configured = resolveRemoteSshPathSetting();
  if (configured) {
    const kind = await detectSshCommandKind(configured, log);
    if (kind !== SshCommandKind.NotFound) {
      sshToolPathCache.ssh = configured;
      return configured;
    }
    log.appendLine(`The specified path ${configured} is not a valid SSH binary`);
  }

  if (process.platform !== 'win32') {
    if (await isValidSshCommand('ssh', log)) {
      sshToolPathCache.ssh = 'ssh';
      return 'ssh';
    }
    throw new Error('ssh is not on the PATH');
  }

  const candidates = collectWindowsSshCandidates();
  const useLocalServer = vscode.workspace.getConfiguration('remote.SSH').get<boolean>('useLocalServer', false);
  if (useLocalServer) {
    for (const candidate of candidates) {
      if (await detectSshCommandKind(candidate, log) === SshCommandKind.Other) {
        sshToolPathCache.ssh = candidate;
        return candidate;
      }
      log.appendLine('Preferring non-Windows OpenSSH, skipping');
    }
  }
  for (const candidate of candidates) {
    if (await isValidSshCommand(candidate, log)) {
      sshToolPathCache.ssh = candidate;
      return candidate;
    }
  }

  throw new Error('ssh installation not found');
}

async function resolveSshToolPath(tool: SshToolName): Promise<string> {
  resetSshToolCacheIfNeeded();
  const cached = sshToolPathCache[tool];
  if (cached) {
    return cached;
  }
  const sshPath = await resolveSshCommandPath();
  sshToolPathCache.ssh = sshPath;
  if (tool === 'ssh') {
    return sshPath;
  }
  if (sshPath === 'ssh') {
    sshToolPathCache[tool] = tool;
    return tool;
  }
  const ext = path.extname(sshPath);
  const candidate = path.join(path.dirname(sshPath), ext ? `${tool}${ext}` : tool);
  if (await fileExists(candidate)) {
    sshToolPathCache[tool] = candidate;
    return candidate;
  }
  sshToolPathCache[tool] = candidate;
  return candidate;
}

function pickSshErrorSummary(text: string): string {
  const lines = text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  if (lines.length === 0) {
    return text.trim();
  }
  const patterns = [
    /permission denied/i,
    /no identities/i,
    /could not open a connection to your authentication agent/i,
    /agent has no identities/i,
    /sign_and_send_pubkey/i,
    /identity file .* not accessible/i
  ];
  for (const line of lines) {
    if (patterns.some((pattern) => pattern.test(line))) {
      return line;
    }
  }
  return lines[lines.length - 1];
}

function looksLikeAuthFailure(text: string): boolean {
  return [
    /permission denied/i,
    /no identities/i,
    /authentication agent/i,
    /read_passphrase/i,
    /enter passphrase/i,
    /sign_and_send_pubkey/i,
    /identity file .* not accessible/i
  ].some((pattern) => pattern.test(text));
}

function appendSshHostKeyCheckingArgs(args: string[], cfg: SlurmConnectConfig): void {
  const mode = normalizeSshHostKeyChecking(cfg.sshHostKeyChecking);
  args.push('-o', `StrictHostKeyChecking=${mode}`);
}

function buildSshArgs(
  host: string,
  cfg: SlurmConnectConfig,
  command: string,
  options: { batchMode: boolean }
): string[] {
  const args: string[] = [];
  if (cfg.sshQueryConfigPath) {
    args.push('-F', expandHome(cfg.sshQueryConfigPath));
  }
  args.push('-T', '-o', `BatchMode=${options.batchMode ? 'yes' : 'no'}`, '-o', `ConnectTimeout=${cfg.sshConnectTimeoutSeconds}`);
  appendSshHostKeyCheckingArgs(args, cfg);
  if (cfg.identityFile) {
    args.push('-i', expandHome(cfg.identityFile));
  }
  const target = cfg.user ? `${cfg.user}@${host}` : host;
  args.push(target, command);
  return args;
}

function quotePowerShellString(value: string): string {
  return "'" + value.replace(/'/g, "''") + "'";
}

function normalizeRemoteCommandForWindowsTerminal(command: string): string {
  if (!command.includes('"')) {
    return command;
  }
  const quotedSegments = command.match(/"[^"]*"/g);
  if (quotedSegments && quotedSegments.some((segment) => segment.includes("'"))) {
    return command;
  }
  return command.replace(/"/g, "'");
}

async function createTerminalSshRunFiles(): Promise<{
  dirPath: string;
  stdoutPath: string;
  statusPath: string;
}> {
  const dirPath = await fs.mkdtemp(path.join(os.tmpdir(), 'slurm-connect-ssh-'));
  return {
    dirPath,
    stdoutPath: path.join(dirPath, 'stdout.txt'),
    statusPath: path.join(dirPath, 'status.txt')
  };
}

async function resolveLocalTerminalCwd(): Promise<string | undefined> {
  const candidates: string[] = [];
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri;
  if (workspaceFolder?.scheme === 'file') {
    candidates.push(workspaceFolder.fsPath);
  }
  const homeDir = os.homedir();
  if (homeDir) {
    candidates.push(homeDir);
  }
  const currentDir = process.cwd();
  if (currentDir) {
    candidates.push(currentDir);
  }
  candidates.push(os.tmpdir());

  for (const candidate of candidates) {
    if (await fileExists(candidate)) {
      return candidate;
    }
  }
  return undefined;
}

async function createLocalTerminal(name: string): Promise<vscode.Terminal> {
  const options: vscode.TerminalOptions = { name };
  const cwd = await resolveLocalTerminalCwd();
  if (cwd) {
    options.cwd = cwd;
  }
  return vscode.window.createTerminal(options);
}

async function waitForFile(filePath: string, pollMs: number): Promise<void> {
  while (true) {
    try {
      await fs.access(filePath);
      return;
    } catch {
      // Keep waiting.
    }
    await delay(pollMs);
  }
}

async function runSshCommandInTerminal(
  host: string,
  cfg: SlurmConnectConfig,
  command: string
): Promise<string> {
  const normalizedCommand =
    process.platform === 'win32' ? normalizeRemoteCommandForWindowsTerminal(command) : command;
  const args = buildSshArgs(host, cfg, normalizedCommand, { batchMode: false });
  const sshPath = await resolveSshToolPath('ssh');
  const { dirPath, stdoutPath, statusPath } = await createTerminalSshRunFiles();
  const isWindows = process.platform === 'win32';
  const shellCommand = isWindows
    ? (() => {
        const psArgs = args.map((arg) => quotePowerShellString(arg)).join(', ');
        const psScript = [
          "$ErrorActionPreference = 'Continue'",
          `$sshPath = ${quotePowerShellString(sshPath)}`,
          `$stdoutPath = ${quotePowerShellString(stdoutPath)}`,
          `$statusPath = ${quotePowerShellString(statusPath)}`,
          '$exitCode = 1',
          'try {',
          `  & $sshPath @(${psArgs}) | Out-File -FilePath $stdoutPath -Encoding utf8`,
          '  $exitCode = $LASTEXITCODE',
          '} catch {',
          '  $exitCode = 1',
          '}',
          '$exitCode | Out-File -FilePath $statusPath -Encoding ascii -NoNewline'
        ].join('; ');
        const encoded = Buffer.from(psScript, 'utf16le').toString('base64');
        return `powershell -NoProfile -EncodedCommand ${encoded}`;
      })()
    : (() => {
        const sshCommand = [sshPath, ...args].map(quoteShellArg).join(' ');
        return `${sshCommand} > ${quoteShellArg(stdoutPath)}; printf "%s" $? > ${quoteShellArg(statusPath)}`;
      })();
  const terminal = await createLocalTerminal('Slurm Connect SSH');
  terminal.show(true);
  terminal.sendText(shellCommand, true);
  void vscode.window.showInformationMessage('Enter your SSH key passphrase in the terminal.');

  let stdout = '';
  let exitCode = NaN;
  try {
    await waitForFile(statusPath, 500);
    const statusText = await fs.readFile(statusPath, 'utf8');
    exitCode = Number(statusText.trim());
    try {
      stdout = await fs.readFile(stdoutPath, 'utf8');
    } catch {
      stdout = '';
    }
  } finally {
    try {
      await fs.rm(dirPath, { recursive: true, force: true });
    } catch {
      // Ignore cleanup failures.
    }
  }

  if (!Number.isFinite(exitCode) || exitCode !== 0) {
    throw new Error('SSH command failed in the terminal. Check the terminal output for details.');
  }
  terminal.dispose();
  return stdout.trim();
}

async function runPreSshCommandInTerminal(command: string): Promise<void> {
  const { dirPath, statusPath } = await createTerminalSshRunFiles();
  const isWindows = process.platform === 'win32';
  const shellCommand = isWindows
    ? (() => {
        const psScript = [
          "$ErrorActionPreference = 'Continue'",
          `$statusPath = ${quotePowerShellString(statusPath)}`,
          '$exitCode = 1',
          'try {',
          `  cmd /c ${quotePowerShellString(command)}`,
          '  $exitCode = $LASTEXITCODE',
          '} catch {',
          '  $exitCode = 1',
          '}',
          '$exitCode | Out-File -FilePath $statusPath -Encoding ascii -NoNewline'
        ].join('; ');
        const encoded = Buffer.from(psScript, 'utf16le').toString('base64');
        return `powershell -NoProfile -EncodedCommand ${encoded}`;
      })()
    : `${command}; printf "%s" $? > ${quoteShellArg(statusPath)}`;

  const terminal = await createLocalTerminal('Slurm Connect Auth');
  terminal.show(true);
  terminal.sendText(shellCommand, true);
  void vscode.window.showInformationMessage('Complete the pre-SSH authentication in the terminal.');

  let exitCode = NaN;
  try {
    await waitForFile(statusPath, 500);
    const statusText = await fs.readFile(statusPath, 'utf8');
    exitCode = Number(statusText.trim());
  } finally {
    try {
      await fs.rm(dirPath, { recursive: true, force: true });
    } catch {
      // Ignore cleanup failures.
    }
  }

  if (!Number.isFinite(exitCode) || exitCode !== 0) {
    throw new Error('Pre-SSH command failed in the terminal. Check the terminal output for details.');
  }
  terminal.dispose();
}

async function runPreSshCheckCommand(command: string): Promise<boolean> {
  const trimmed = command.trim();
  if (!trimmed) {
    return false;
  }
  const isWindows = process.platform === 'win32';
  const shell = isWindows ? 'cmd' : 'sh';
  const args = isWindows ? ['/c', trimmed] : ['-lc', trimmed];
  try {
    await execFileAsync(shell, args);
    return true;
  } catch (error) {
    const log = getOutputChannel();
    log.appendLine(`Pre-SSH check command failed: ${normalizeSshErrorText(error)}`);
    return false;
  }
}

async function ensurePreSshCommand(cfg: SlurmConnectConfig, reason: string): Promise<void> {
  const command = (cfg.preSshCommand || '').trim();
  if (!command) {
    return;
  }
  if (preSshCommandInFlight) {
    await preSshCommandInFlight;
  }
  const log = getOutputChannel();
  preSshCommandInFlight = (async () => {
    log.appendLine(`Running pre-SSH command (${reason}).`);
    try {
      const checkCommand = (cfg.preSshCheckCommand || '').trim();
      if (checkCommand) {
        log.appendLine('Running pre-SSH check command.');
        const ready = await runPreSshCheckCommand(checkCommand);
        if (ready) {
          log.appendLine('Pre-SSH check succeeded; skipping pre-SSH command.');
          return;
        }
        log.appendLine('Pre-SSH check did not pass; running pre-SSH command.');
      }
      await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: 'Waiting for pre-SSH authentication',
          cancellable: false
        },
        async () => await runPreSshCommandInTerminal(command)
      );
    } catch (error) {
      log.appendLine(`Pre-SSH command failed: ${formatError(error)}`);
      throw new Error('Pre-SSH command failed. Check the terminal output for details.');
    }
  })();
  try {
    await preSshCommandInFlight;
  } finally {
    preSshCommandInFlight = undefined;
  }
}

async function getPublicKeyFingerprint(identityPath: string): Promise<string | undefined> {
  if (!identityPath) {
    return undefined;
  }
  const pubPath = getPublicKeyPath(identityPath);
  try {
    await fs.access(pubPath);
  } catch {
    return undefined;
  }
  try {
    const sshKeygen = await resolveSshToolPath('ssh-keygen');
    const { stdout } = await execFileAsync(sshKeygen, ['-lf', pubPath, '-E', 'sha256']);
    const match = stdout.match(/SHA256:[A-Za-z0-9+/=]+/);
    return match ? match[0] : undefined;
  } catch {
    return undefined;
  }
}

function getPublicKeyPath(identityPath: string): string {
  return identityPath.endsWith('.pub') ? identityPath : `${identityPath}.pub`;
}

async function hasPublicKeyFile(identityPath: string): Promise<boolean> {
  if (!identityPath) {
    return false;
  }
  return await fileExists(getPublicKeyPath(identityPath));
}

function parsePublicKeyTokens(line: string): { type: string; key: string } | undefined {
  const trimmed = line.trim();
  if (!trimmed) {
    return undefined;
  }
  const parts = trimmed.split(/\s+/);
  if (parts.length < 2) {
    return undefined;
  }
  const [type, key] = parts;
  if (!type || !key) {
    return undefined;
  }
  return { type, key };
}

async function readPublicKeyTokens(identityPath: string): Promise<{ type: string; key: string } | undefined> {
  const pubPath = getPublicKeyPath(identityPath);
  try {
    const text = await fs.readFile(pubPath, 'utf8');
    const line = text
      .split(/\r?\n/)
      .map((entry) => entry.trim())
      .find((entry) => entry.length > 0);
    return line ? parsePublicKeyTokens(line) : undefined;
  } catch {
    return undefined;
  }
}

async function getSshAgentPublicKeys(): Promise<Array<{ type: string; key: string }>> {
  try {
    const sshAdd = await resolveSshToolPath('ssh-add');
    const result = await execFileAsync(sshAdd, ['-L']);
    const output = [result.stdout, result.stderr].filter(Boolean).join('\n');
    if (!output.trim() || /no identities/i.test(output)) {
      return [];
    }
    return output
      .split(/\r?\n/)
      .map((line) => parsePublicKeyTokens(line))
      .filter((entry): entry is { type: string; key: string } => Boolean(entry));
  } catch {
    return [];
  }
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

type SshAgentStatus = 'available' | 'empty' | 'unavailable';

interface SshAgentInfo {
  status: SshAgentStatus;
  output: string;
}

function looksLikeAgentUnavailable(text: string): boolean {
  return [
    /could not open a connection to your authentication agent/i,
    /error connecting to agent/i,
    /communication with agent failed/i,
    /agent refused operation/i,
    /no such file or directory/i
  ].some((pattern) => pattern.test(text));
}

async function probeSshAgentWithEnv(
  sshAdd: string,
  env: { sock: string; pid?: string },
  timeoutMs = 3000
): Promise<SshAgentInfo> {
  if (!env.sock) {
    return { status: 'unavailable', output: '' };
  }
  const execEnv: NodeJS.ProcessEnv = { ...process.env, SSH_AUTH_SOCK: env.sock };
  if (env.pid) {
    execEnv.SSH_AGENT_PID = env.pid;
  }
  try {
    const result = await execFileAsync(sshAdd, ['-l'], { env: execEnv, timeout: timeoutMs });
    const output = [result.stdout, result.stderr].filter(Boolean).join('\n');
    if (!output.trim() || /no identities/i.test(output)) {
      return { status: 'empty', output };
    }
    return { status: 'available', output };
  } catch (error) {
    const errorText = normalizeSshErrorText(error);
    if (/no identities/i.test(errorText)) {
      return { status: 'empty', output: errorText };
    }
    if (looksLikeAgentUnavailable(errorText)) {
      return { status: 'unavailable', output: errorText };
    }
    return { status: 'unavailable', output: errorText };
  }
}

async function getSshAgentInfo(identityPathRaw?: string): Promise<SshAgentInfo> {
  try {
    await ensureSshAgentEnvForCurrentSsh(identityPathRaw);
    const sshAdd = await resolveSshToolPath('ssh-add');
    const agentEnv = getSshAgentEnv();
    const info = await probeSshAgentWithEnv(sshAdd, agentEnv);
    if (info.status === 'available') {
      storeSshAgentEnv(agentEnv);
    }
    return info;
  } catch (error) {
    const errorText = normalizeSshErrorText(error);
    if (/no identities/i.test(errorText)) {
      return { status: 'empty', output: errorText };
    }
    if (looksLikeAgentUnavailable(errorText)) {
      return { status: 'unavailable', output: errorText };
    }
    return { status: 'unavailable', output: errorText };
  }
}

async function isSshKeyListedInAgentOutput(identityPath: string, output: string): Promise<boolean> {
  if (!identityPath) {
    return false;
  }
  if (!output.trim()) {
    return false;
  }
  if (/no identities/i.test(output)) {
    return false;
  }

  const fingerprint = await getPublicKeyFingerprint(identityPath);
  if (fingerprint) {
    const pattern = new RegExp(escapeRegExp(fingerprint));
    if (pattern.test(output)) {
      return true;
    }
  }
  const basename = path.basename(identityPath);
  if (basename && output.includes(basename)) {
    return true;
  }
  if (output.includes(identityPath)) {
    return true;
  }
  const pubTokens = await readPublicKeyTokens(identityPath);
  if (pubTokens) {
    const agentKeys = await getSshAgentPublicKeys();
    if (agentKeys.some((entry) => entry.type === pubTokens.type && entry.key === pubTokens.key)) {
      return true;
    }
  }
  return false;
}

async function buildAgentStatusMessage(identityPathRaw: string): Promise<{ text: string; isError: boolean }> {
  const agentInfo = await getSshAgentInfo(identityPathRaw);
  const agentBase = agentInfo.status === 'available'
    ? 'SSH agent running'
    : agentInfo.status === 'empty'
      ? 'SSH agent running (no keys loaded)'
      : 'SSH agent unavailable';
  if (!identityPathRaw) {
    return { text: `${agentBase}. No identity file set (using SSH agent/config).`, isError: false };
  }
  const identityPath = expandHome(identityPathRaw);
  const identityExists = await fileExists(identityPath);
  if (!identityExists) {
    return { text: `${agentBase}. Identity file not found.`, isError: true };
  }
  const pubExists = await hasPublicKeyFile(identityPath);
  if (agentInfo.status === 'unavailable') {
    return {
      text: `SSH agent unavailable. Passphrase required for ${identityPath}.`,
      isError: true
    };
  }
  if (agentInfo.status === 'empty') {
    return { text: 'SSH agent running, no keys loaded.', isError: true };
  }
  if (!pubExists) {
    return {
      text: `SSH agent running, public key file missing (${getPublicKeyPath(identityPath)}). Generate it to verify the loaded key.`,
      isError: true
    };
  }
  const keyLoaded = await isSshKeyListedInAgentOutput(identityPath, agentInfo.output);
  if (keyLoaded) {
    return { text: 'SSH agent running, key loaded.', isError: false };
  }
  return { text: 'SSH agent running, key not loaded.', isError: true };
}

async function isSshKeyLoaded(identityPath: string): Promise<boolean> {
  if (!identityPath) {
    return false;
  }
  const agentInfo = await getSshAgentInfo(identityPath);
  if (agentInfo.status !== 'available') {
    return false;
  }
  return await isSshKeyListedInAgentOutput(identityPath, agentInfo.output);
}


async function runSshAddInTerminal(identityPath: string): Promise<void> {
  await ensureSshAgentEnvForCurrentSsh();
  const sshAdd = await resolveSshToolPath('ssh-add');
  const trimmed = identityPath.trim();
  const args = trimmed ? [trimmed] : [];
  const { dirPath, stdoutPath, statusPath } = await createTerminalSshRunFiles();
  const agentEnv = getSshAgentEnv();
  const isWindows = process.platform === 'win32';
  const shellCommand = isWindows
    ? (() => {
        const psArgs = args.map((arg) => quotePowerShellString(arg)).join(', ');
        const psScript = [
          "$ErrorActionPreference = 'Continue'",
          `$stdoutPath = ${quotePowerShellString(stdoutPath)}`,
          `$statusPath = ${quotePowerShellString(statusPath)}`,
          `$cmd = ${quotePowerShellString(sshAdd)}`,
          `$env:SSH_AUTH_SOCK = ${quotePowerShellString(agentEnv.sock)}`,
          `$env:SSH_AGENT_PID = ${quotePowerShellString(agentEnv.pid)}`,
          psArgs ? `$cmdArgs = @(${psArgs})` : '$cmdArgs = @()',
          '$exitCode = 1',
          'try {',
          '  & $cmd @cmdArgs | Out-File -FilePath $stdoutPath -Encoding utf8',
          '  $exitCode = $LASTEXITCODE',
          '} catch {',
          '  $exitCode = 1',
          '}',
          '$exitCode | Out-File -FilePath $statusPath -Encoding ascii -NoNewline'
        ].join('; ');
        const encoded = Buffer.from(psScript, 'utf16le').toString('base64');
        return `powershell -NoProfile -EncodedCommand ${encoded}`;
      })()
    : (() => {
        const envPrefix = `SSH_AUTH_SOCK=${quoteShellArg(agentEnv.sock)} SSH_AGENT_PID=${quoteShellArg(agentEnv.pid)}`;
        const sshCommand = [quoteShellArg(sshAdd), ...args.map(quoteShellArg)].join(' ');
        return `${envPrefix} ${sshCommand} > ${quoteShellArg(stdoutPath)}; printf "%s" $? > ${quoteShellArg(statusPath)}`;
      })();

  const terminal = await createLocalTerminal('Slurm Connect SSH Add');
  terminal.show(true);
  terminal.sendText(shellCommand, true);
  void vscode.window.showInformationMessage('Follow the terminal prompt to add your SSH key with ssh-add.');

  let exitCode = NaN;
  try {
    await waitForFile(statusPath, 500);
    const statusText = await fs.readFile(statusPath, 'utf8');
    exitCode = Number(statusText.trim());
  } finally {
    try {
      await fs.rm(dirPath, { recursive: true, force: true });
    } catch {
      // Ignore cleanup failures.
    }
  }

  if (!Number.isFinite(exitCode) || exitCode !== 0) {
    throw new Error('ssh-add failed in the terminal. Check the terminal output for details.');
  }
  terminal.dispose();
}

async function runSshKeygenPublicKeyInTerminal(identityPath: string): Promise<void> {
  const sshKeygen = await resolveSshToolPath('ssh-keygen');
  const pubPath = getPublicKeyPath(identityPath);
  const { dirPath, stdoutPath, statusPath } = await createTerminalSshRunFiles();
  const isWindows = process.platform === 'win32';
  const shellCommand = isWindows
    ? (() => {
        const args = ['-y', '-f', identityPath].map((arg) => quotePowerShellString(arg)).join(', ');
        const psScript = [
          "$ErrorActionPreference = 'Continue'",
          `$stdoutPath = ${quotePowerShellString(stdoutPath)}`,
          `$statusPath = ${quotePowerShellString(statusPath)}`,
          `$cmd = ${quotePowerShellString(sshKeygen)}`,
          `$cmdArgs = @(${args})`,
          '$exitCode = 1',
          'try {',
          `  & $cmd @cmdArgs | Out-File -FilePath ${quotePowerShellString(pubPath)} -Encoding ascii`,
          '  $exitCode = $LASTEXITCODE',
          '} catch {',
          '  $exitCode = 1',
          '}',
          '$exitCode | Out-File -FilePath $statusPath -Encoding ascii -NoNewline'
        ].join('; ');
        const encoded = Buffer.from(psScript, 'utf16le').toString('base64');
        return `powershell -NoProfile -EncodedCommand ${encoded}`;
      })()
    : (() => {
        const cmd = `${quoteShellArg(sshKeygen)} -y -f ${quoteShellArg(identityPath)} > ${quoteShellArg(pubPath)}`;
        return `${cmd}; printf "%s" $? > ${quoteShellArg(statusPath)}`;
      })();

  const terminal = await createLocalTerminal('Slurm Connect SSH Keygen');
  terminal.show(true);
  terminal.sendText(shellCommand, true);
  void vscode.window.showInformationMessage('Follow the terminal prompt to generate the public key.');

  let exitCode = NaN;
  try {
    await waitForFile(statusPath, 500);
    const statusText = await fs.readFile(statusPath, 'utf8');
    exitCode = Number(statusText.trim());
  } finally {
    try {
      await fs.rm(dirPath, { recursive: true, force: true });
    } catch {
      // Ignore cleanup failures.
    }
  }

  if (!Number.isFinite(exitCode) || exitCode !== 0) {
    throw new Error('ssh-keygen failed in the terminal. Check the terminal output for details.');
  }
  terminal.dispose();
}

async function fileExists(filePath: string): Promise<boolean> {
  if (!filePath) {
    return false;
  }
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

type SshAuthRetry =
  | { kind: 'agent' }
  | { kind: 'terminal' };

async function maybePromptForSshAuth(
  cfg: SlurmConnectConfig,
  errorText: string
): Promise<SshAuthRetry | undefined> {
  if (!looksLikeAuthFailure(errorText)) {
    return undefined;
  }
  const identityPath = cfg.identityFile ? expandHome(cfg.identityFile) : '';
  const identityExists = identityPath ? await fileExists(identityPath) : false;
  const pubExists = identityPath && identityExists ? await hasPublicKeyFile(identityPath) : false;
  const canTerminalPassphrase = Boolean(identityPath && identityExists);
  const agentInfo = identityPath && identityExists ? await getSshAgentInfo(identityPath) : undefined;
  if (identityPath && identityExists && agentInfo?.status === 'available') {
    if (await isSshKeyListedInAgentOutput(identityPath, agentInfo.output)) {
      return undefined;
    }
  }
  const agentStatus = agentInfo?.status ?? 'unavailable';
  const canAddToAgent = Boolean(identityPath && identityExists && agentStatus !== 'unavailable');
  const now = Date.now();
  if (lastSshAuthPrompt) {
    const sameIdentity = lastSshAuthPrompt.identityPath === identityPath;
    const recentlyPrompted = now - lastSshAuthPrompt.timestamp < 60_000;
    if (sameIdentity && recentlyPrompted) {
      if (lastSshAuthPrompt.mode === 'terminal' && canTerminalPassphrase) {
        lastSshAuthPrompt = { identityPath, timestamp: now, mode: 'terminal' };
        return { kind: 'terminal' };
      }
      return undefined;
    }
  }

  const actions: string[] = [];
  if (canTerminalPassphrase) {
    actions.push('Enter Passphrase in Terminal');
  }
  if (canAddToAgent) {
    actions.push('Add to Agent');
  }
  if (identityPath && identityExists && !pubExists) {
    actions.push('Generate Public Key');
  }
  if (!identityPath || !identityExists) {
    actions.push('Open Slurm Connect');
  }
  actions.push('Dismiss');
  const message = identityPath && !identityExists
    ? `SSH authentication failed while querying the cluster. The identity file ${identityPath} was not found. Update it in the Slurm Connect view.`
    : identityPath
      ? agentStatus === 'unavailable'
        ? `SSH authentication failed while querying the cluster. SSH agent is unavailable, enter the passphrase for ${identityPath} in the terminal.`
        : !pubExists
          ? `SSH authentication failed while querying the cluster. Public key file is missing for ${identityPath}; generate it to verify the agent key.`
          : `SSH authentication failed while querying the cluster. Add ${identityPath} to your ssh-agent or enter its passphrase in a terminal.`
      : 'SSH authentication failed while querying the cluster. Ensure your SSH config/agent can authenticate, or set an identity file in the Slurm Connect view.';
  const choice = await vscode.window.showWarningMessage(message, { modal: true }, ...actions);
  const defaultMode: SshAuthMode = canAddToAgent ? 'agent' : 'terminal';
  const mode: SshAuthMode = choice === 'Enter Passphrase in Terminal'
    ? 'terminal'
    : choice === 'Add to Agent'
      ? 'agent'
      : defaultMode;
  lastSshAuthPrompt = { identityPath, timestamp: now, mode };
  if (choice === 'Generate Public Key' && identityPath && identityExists && !pubExists) {
    try {
      await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: 'Generating public key',
          cancellable: false
        },
        async () => await runSshKeygenPublicKeyInTerminal(identityPath)
      );
      await refreshAgentStatus(cfg.identityFile);
      if (agentInfo?.status === 'available') {
        const refreshed = await getSshAgentInfo(identityPath);
        if (await isSshKeyListedInAgentOutput(identityPath, refreshed.output)) {
          return { kind: 'agent' };
        }
      }
    } catch (error) {
      void vscode.window.showWarningMessage(`Failed to generate public key: ${formatError(error)}`);
    }
    return undefined;
  }
  if (choice === 'Enter Passphrase in Terminal' && identityPath && identityExists && canTerminalPassphrase) {
    return { kind: 'terminal' };
  }
  if (choice === 'Add to Agent' && identityPath && canAddToAgent) {
    try {
      await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: 'Waiting for ssh-add to complete',
          cancellable: false
        },
        async () => await runSshAddInTerminal(identityPath)
      );
      await refreshAgentStatus(cfg.identityFile);
      return { kind: 'agent' };
    } catch (error) {
      void vscode.window.showWarningMessage(`ssh-add failed: ${formatError(error)}`);
      return undefined;
    }
  }
  if (choice === 'Open Slurm Connect') {
    await openSlurmConnectView();
  }
  return undefined;
}

async function maybePromptForSshAuthOnConnect(cfg: SlurmConnectConfig, loginHost: string): Promise<boolean> {
  const identityPath = cfg.identityFile ? expandHome(cfg.identityFile) : '';
  if (!identityPath) {
    return true;
  }
  const identityExists = await fileExists(identityPath);
  const pubExists = identityExists ? await hasPublicKeyFile(identityPath) : false;
  const agentInfo = identityExists ? await getSshAgentInfo(identityPath) : undefined;
  if (identityExists && agentInfo?.status === 'available') {
    const loaded = await isSshKeyListedInAgentOutput(identityPath, agentInfo.output);
    if (loaded) {
      return true;
    }
  }
  if (identityExists && agentInfo?.status === 'unavailable') {
    // Remote-SSH will handle passphrase prompts during connect.
    return true;
  }

  const now = Date.now();
  if (lastSshAuthPrompt) {
    const sameIdentity = lastSshAuthPrompt.identityPath === identityPath;
    const recentlyPrompted = now - lastSshAuthPrompt.timestamp < 60_000;
    if (sameIdentity && recentlyPrompted) {
      return true;
    }
  }

  const canAddToAgent = Boolean(identityExists && agentInfo?.status !== 'unavailable');

  const actions: string[] = [];
  if (canAddToAgent) {
    actions.push('Add to Agent');
  }
  if (identityExists && !pubExists) {
    actions.push('Generate Public Key');
  }
  if (!identityExists) {
    actions.push('Open Slurm Connect');
  }
  actions.push('Dismiss');

  const message = !identityExists
    ? `SSH identity file ${identityPath} was not found. Update it in the Slurm Connect view.`
    : !pubExists
      ? `Public key file is missing for ${identityPath}; generate it to verify the agent key.`
      : `SSH key is not loaded in the agent. Add ${identityPath} to avoid extra prompts.`;

  const choice = await vscode.window.showWarningMessage(message, { modal: true }, ...actions);
  const mode: SshAuthMode = choice === 'Add to Agent' ? 'agent' : 'terminal';
  lastSshAuthPrompt = { identityPath, timestamp: now, mode };

  if (choice === 'Add to Agent' && canAddToAgent) {
    try {
      await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: 'Waiting for ssh-add to complete',
          cancellable: false
        },
        async () => await runSshAddInTerminal(identityPath)
      );
      await refreshAgentStatus(cfg.identityFile);
    } catch (error) {
      void vscode.window.showWarningMessage(`ssh-add failed: ${formatError(error)}`);
    }
    return true;
  }

  if (choice === 'Generate Public Key' && identityExists && !pubExists) {
    try {
      await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: 'Generating public key',
          cancellable: false
        },
        async () => await runSshKeygenPublicKeyInTerminal(identityPath)
      );
      await refreshAgentStatus(cfg.identityFile);
    } catch (error) {
      void vscode.window.showWarningMessage(`Failed to generate public key: ${formatError(error)}`);
    }
    return true;
  }

  if (choice === 'Open Slurm Connect') {
    await openSlurmConnectView();
    return false;
  }
  return true;
}

async function runSshCommand(host: string, cfg: SlurmConnectConfig, command: string): Promise<string> {
  await ensurePreSshCommand(cfg, `SSH query to ${host}`);
  const args = buildSshArgs(host, cfg, command, { batchMode: true });
  const sshPath = await resolveSshToolPath('ssh');

  try {
    const { stdout } = await execFileAsync(sshPath, args, { timeout: cfg.sshConnectTimeoutSeconds * 1000 });
    return stdout.trim();
  } catch (error) {
    const errorText = normalizeSshErrorText(error);
    const retry = await maybePromptForSshAuth(cfg, errorText);
    if (retry?.kind === 'agent') {
      try {
        const { stdout } = await execFileAsync(sshPath, args, { timeout: cfg.sshConnectTimeoutSeconds * 1000 });
        return stdout.trim();
      } catch (retryError) {
        const retryText = normalizeSshErrorText(retryError);
        const retrySummary = pickSshErrorSummary(retryText);
        throw new Error(retrySummary || 'SSH command failed');
      }
    }
    if (retry?.kind === 'terminal') {
      try {
        return await vscode.window.withProgress(
          {
            location: vscode.ProgressLocation.Notification,
            title: 'Waiting for SSH passphrase in terminal',
            cancellable: false
          },
          async () => await runSshCommandInTerminal(host, cfg, command)
        );
      } catch (retryError) {
        const retryText = normalizeSshErrorText(retryError);
        const retrySummary = pickSshErrorSummary(retryText);
        throw new Error(retrySummary || 'SSH command failed');
      }
    }
    const summary = pickSshErrorSummary(errorText);
    throw new Error(summary || 'SSH command failed');
  }
}

function parsePartitionOutput(output: string): PartitionResult {
  const tokens = output.split(/\s+/).map((token) => token.trim()).filter(Boolean);
  const partitions: string[] = [];
  let defaultPartition: string | undefined;

  for (const token of tokens) {
    const isDefault = token.includes('*');
    const cleaned = token.replace(/\*/g, '');
    if (!cleaned) {
      continue;
    }
    if (isDefault && !defaultPartition) {
      defaultPartition = cleaned;
    }
    if (!partitions.includes(cleaned)) {
      partitions.push(cleaned);
    }
  }

  return { partitions, defaultPartition };
}

function parseSimpleList(output: string): string[] {
  return uniqueList(
    output
      .split(/\s+/)
      .map((value) => value.trim())
      .filter(Boolean)
  );
}

function parsePartitionDefaultTimesOutput(
  output: string
): { defaults: Record<string, PartitionDefaults>; defaultPartition?: string } {
  const defaults: Record<string, PartitionDefaults> = {};
  let defaultPartition: string | undefined;
  const lines = output.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  const timePattern = /^(\d+-)?\d{1,2}:\d{2}:\d{2}$/;
  const normalizeTimeValue = (value: string): string | undefined => {
    const trimmed = value.trim();
    if (!trimmed) {
      return undefined;
    }
    const upper = trimmed.toUpperCase();
    if (upper === 'NONE' || upper === 'UNLIMITED') {
      return undefined;
    }
    return timePattern.test(trimmed) ? trimmed : undefined;
  };
  const parsePositiveInt = (value: string): number | undefined => {
    const match = value.match(/\d+/);
    if (!match) {
      return undefined;
    }
    const parsed = Number(match[0]);
    if (!Number.isFinite(parsed) || parsed < 1) {
      return undefined;
    }
    return Math.floor(parsed);
  };
  const parseMemoryMb = (value: string): number | undefined => {
    const trimmed = value.trim();
    if (!trimmed) {
      return undefined;
    }
    const upper = trimmed.toUpperCase();
    if (upper === 'NONE' || upper === 'UNLIMITED') {
      return undefined;
    }
    const match = upper.match(/^(\d+(?:\.\d+)?)([KMGTP])?$/);
    if (!match) {
      return undefined;
    }
    const amount = Number(match[1]);
    if (!Number.isFinite(amount) || amount <= 0) {
      return undefined;
    }
    const unit = match[2] || 'M';
    const multipliers: Record<string, number> = {
      K: 1 / 1024,
      M: 1,
      G: 1024,
      T: 1024 * 1024,
      P: 1024 * 1024 * 1024
    };
    const multiplier = multipliers[unit] ?? 1;
    const mb = Math.round(amount * multiplier);
    return mb > 0 ? mb : undefined;
  };
  const parseGpuDefaults = (value: string): { count?: number; type?: string } | undefined => {
    const parts = value.split(',').map((entry) => entry.trim()).filter(Boolean);
    for (const part of parts) {
      if (part.includes('gres/gpu')) {
        const [left, right] = part.split('=');
        if (!right) {
          continue;
        }
        const count = parsePositiveInt(right);
        if (!count) {
          continue;
        }
        const typeMatch = left.match(/gres\/gpu:([^=]+)/);
        const type = typeMatch ? typeMatch[1] : undefined;
        return { count, type };
      }
      if (part.startsWith('gpu=')) {
        const count = parsePositiveInt(part.slice('gpu='.length));
        if (count) {
          return { count };
        }
      }
      if (part.startsWith('gpu:')) {
        const segments = part.split(':').map((entry) => entry.trim()).filter(Boolean);
        if (segments.length >= 2 && segments[0] === 'gpu') {
          const count = parsePositiveInt(segments[segments.length - 1]);
          if (!count) {
            continue;
          }
          const type = segments.length >= 3 ? segments[1] : undefined;
          return { count, type };
        }
      }
    }
    return undefined;
  };
  const applyGpuDefaults = (value: string, target: PartitionDefaults): void => {
    const gpu = parseGpuDefaults(value);
    if (!gpu) {
      return;
    }
    if (gpu.count && target.defaultGpuCount === undefined) {
      target.defaultGpuCount = gpu.count;
    }
    if (gpu.type && target.defaultGpuType === undefined) {
      target.defaultGpuType = gpu.type;
    }
  };
  const applyDefaultToken = (key: string, value: string, target: PartitionDefaults): void => {
    const keyLower = key.toLowerCase();
    if (keyLower === 'defaulttime' || keyLower === 'deftime') {
      const timeValue = normalizeTimeValue(value);
      if (timeValue && !target.defaultTime) {
        target.defaultTime = timeValue;
      }
      return;
    }
    if (keyLower === 'defmempernode') {
      const memValue = parseMemoryMb(value);
      if (memValue !== undefined && target.defaultMemoryMb === undefined) {
        target.defaultMemoryMb = memValue;
      }
      return;
    }
    if (
      keyLower === 'defcpupertask' ||
      keyLower === 'defcpuspertask' ||
      keyLower === 'defaultcpupertask' ||
      keyLower === 'defaultcpuspertask'
    ) {
      const cpuValue = parsePositiveInt(value);
      if (cpuValue !== undefined && target.defaultCpusPerTask === undefined) {
        target.defaultCpusPerTask = cpuValue;
      }
      return;
    }
    if (keyLower === 'defaultnodes') {
      const nodesValue = parsePositiveInt(value);
      if (nodesValue !== undefined && target.defaultNodes === undefined) {
        target.defaultNodes = nodesValue;
      }
      return;
    }
    if (keyLower === 'defaulttaskspernode') {
      const tasksValue = parsePositiveInt(value);
      if (tasksValue !== undefined && target.defaultTasksPerNode === undefined) {
        target.defaultTasksPerNode = tasksValue;
      }
      return;
    }
    if (
      keyLower === 'defaultgres' ||
      keyLower === 'defgres' ||
      keyLower.startsWith('defaulttres') ||
      keyLower.startsWith('deftres')
    ) {
      applyGpuDefaults(value, target);
    }
  };
  const applyJobDefaults = (value: string, target: PartitionDefaults): void => {
    const trimmed = value.trim();
    if (!trimmed || trimmed.toLowerCase() === '(null)') {
      return;
    }
    const entries = trimmed.split(',').map((entry) => entry.trim()).filter(Boolean);
    for (const entry of entries) {
      const equalsIndex = entry.indexOf('=');
      if (equalsIndex === -1) {
        continue;
      }
      const key = entry.slice(0, equalsIndex);
      const val = entry.slice(equalsIndex + 1);
      if (!key) {
        continue;
      }
      if (key.toLowerCase() === 'gres' || key.toLowerCase() === 'defgres') {
        applyGpuDefaults(val, target);
      } else {
        applyDefaultToken(key, val, target);
      }
    }
  };

  for (const line of lines) {
    const tokens = line.split(/\s+/).filter(Boolean);
    let rawName = '';
    let isDefault = false;
    const lineDefaults: PartitionDefaults = {};
    for (const token of tokens) {
      const equalsIndex = token.indexOf('=');
      if (equalsIndex === -1) {
        continue;
      }
      const key = token.slice(0, equalsIndex);
      const value = token.slice(equalsIndex + 1);
      if (!key) {
        continue;
      }
      if (key === 'PartitionName') {
        rawName = value;
        continue;
      }
      if (key === 'Default' && value === 'YES') {
        isDefault = true;
        continue;
      }
      if (key === 'JobDefaults') {
        applyJobDefaults(value, lineDefaults);
        continue;
      }
      if (key.startsWith('Def') || key.startsWith('Default')) {
        applyDefaultToken(key, value, lineDefaults);
      }
    }
    if (!rawName) {
      continue;
    }
    const names = rawName
      .split(',')
      .map((entry) => entry.replace(/\*/g, '').trim())
      .filter(Boolean);
    for (const name of names) {
      if (!defaults[name]) {
        defaults[name] = {};
      }
      const existing = defaults[name] as Record<string, unknown>;
      for (const [key, value] of Object.entries(lineDefaults)) {
        if (value !== undefined) {
          existing[key] = value;
        }
      }
    }
    if (isDefault && !defaultPartition) {
      defaultPartition = names[0];
    }
  }
  return { defaults, defaultPartition };
}

const FREE_RESOURCE_NODE_INFO_COMMAND = 'sinfo -h -N -o "%n|%c|%t|%P|%G"';
const FREE_RESOURCE_SQUEUE_COMMAND = 'squeue -h -o "%t|%C|%b|%N"';
const FREE_RESOURCE_BAD_STATES = new Set(['down', 'drain', 'drng', 'maint', 'fail']);

interface NodeInfo {
  name: string;
  cpus: number;
  state: string;
  partitions: string[];
  gpuTypes: Record<string, number>;
}

interface PartitionFreeSummary {
  freeNodes: number;
  freeCpuTotal: number;
  freeCpusPerNode: number;
  freeGpuTotal: number;
  freeGpuMax: number;
  freeGpuTypes: Record<string, number>;
}

interface FreeResourceCommandIndexes {
  infoCommandCount: number;
  nodeIndex: number;
  jobIndex: number;
}

function buildClusterInfoCommandSet(
  cfg: SlurmConnectConfig
): { commands: string[]; freeResourceIndexes: FreeResourceCommandIndexes } {
  const baseCommands = [
    cfg.partitionInfoCommand,
    'sinfo -h -N -o "%P|%n|%c|%m|%G"',
    'sinfo -h -o "%P|%D|%c|%m|%G"',
    'scontrol show partition -o'
  ].filter(Boolean);
  const commands = baseCommands.slice();
  const nodeIndex = commands.length;
  commands.push(FREE_RESOURCE_NODE_INFO_COMMAND);
  const jobIndex = commands.length;
  commands.push(FREE_RESOURCE_SQUEUE_COMMAND);
  return {
    commands,
    freeResourceIndexes: { infoCommandCount: baseCommands.length, nodeIndex, jobIndex }
  };
}

function normalizePartitionName(value: string): string {
  return value.replace(/\*/g, '').trim();
}

function parseGresField(raw: string): Record<string, number> {
  const result: Record<string, number> = {};
  if (!raw || raw === '(null)' || raw === 'N/A') {
    return result;
  }
  const tokens = raw
    .split(',')
    .map((token) => token.trim())
    .filter(Boolean);
  for (const token of tokens) {
    if (!token.includes('gpu')) {
      continue;
    }
    let cleaned = token.replace(/\(.*?\)/g, '').trim();
    if (!cleaned) {
      continue;
    }
    if (cleaned.startsWith('gres/')) {
      cleaned = cleaned.slice('gres/'.length);
    }
    if (cleaned === 'gpu') {
      cleaned = '';
    } else if (cleaned.startsWith('gpu:')) {
      cleaned = cleaned.slice(4);
    } else if (cleaned.startsWith('gpu')) {
      cleaned = cleaned.slice(3);
      if (cleaned.startsWith(':')) {
        cleaned = cleaned.slice(1);
      }
    }

    const parts = cleaned
      .split(':')
      .map((part) => part.trim())
      .filter(Boolean);
    let count = 0;
    let type = '';
    if (parts.length === 0) {
      count = 1;
    } else {
      const last = parts[parts.length - 1];
      if (/^\d+$/.test(last)) {
        count = Number(last);
        type = parts.slice(0, -1).join(':');
      } else {
        count = 1;
        type = parts.join(':');
      }
    }
    if (!Number.isFinite(count) || count <= 0) {
      continue;
    }
    result[type] = (result[type] || 0) + count;
  }
  return result;
}

function parseNodeInfoOutput(output: string): Map<string, NodeInfo> {
  const nodes = new Map<string, NodeInfo>();
  const lines = output.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  for (const line of lines) {
    const fields = line.split('|').map((value) => value.trim());
    if (fields.length < 5) {
      continue;
    }
    const name = fields[0];
    const cpuStr = fields[1];
    const stateRaw = fields[2] || '';
    const partitionsRaw = fields[3] || '';
    const gresRaw = fields[4] || '';
    if (!name) {
      continue;
    }
    const cpus = Number(cpuStr);
    const state = stateRaw.toLowerCase().replace(/[\*\+~]+/g, '');
    const partitions = partitionsRaw
      .split(',')
      .map((entry) => normalizePartitionName(entry))
      .filter(Boolean);
    const gpuTypes = parseGresField(gresRaw);
    nodes.set(name, {
      name,
      cpus: Number.isFinite(cpus) ? Math.max(0, Math.floor(cpus)) : 0,
      state,
      partitions: partitions.length ? partitions : ['unknown'],
      gpuTypes
    });
  }
  return nodes;
}

function splitSlurmList(input: string): string[] {
  const result: string[] = [];
  let current = '';
  let depth = 0;
  for (const char of input) {
    if (char === '[') {
      depth += 1;
    } else if (char === ']') {
      depth = Math.max(0, depth - 1);
    }
    if (char === ',' && depth === 0) {
      if (current) {
        result.push(current);
      }
      current = '';
      continue;
    }
    current += char;
  }
  if (current) {
    result.push(current);
  }
  return result.map((entry) => entry.trim()).filter(Boolean);
}

function padNumber(value: number, width: number): string {
  const raw = String(value);
  if (raw.length >= width) {
    return raw;
  }
  return '0'.repeat(width - raw.length) + raw;
}

function expandNumericRange(value: string): string[] {
  const trimmed = value.trim();
  if (!trimmed) {
    return [];
  }
  const parts = trimmed.split(':');
  const rangePart = parts[0];
  const step = parts.length > 1 ? Number(parts[1]) : 1;
  const match = rangePart.match(/^(\d+)-(\d+)$/);
  if (!match) {
    return [rangePart];
  }
  const startRaw = match[1];
  const endRaw = match[2];
  const start = Number(startRaw);
  const end = Number(endRaw);
  if (!Number.isFinite(start) || !Number.isFinite(end) || !Number.isFinite(step) || step <= 0) {
    return [rangePart];
  }
  const width = Math.max(startRaw.length, endRaw.length);
  const results: string[] = [];
  if (start <= end) {
    for (let value = start; value <= end; value += step) {
      results.push(padNumber(value, width));
    }
  } else {
    for (let value = start; value >= end; value -= step) {
      results.push(padNumber(value, width));
    }
  }
  return results;
}

function expandBracketValues(value: string): string[] {
  const entries = value
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean);
  const result: string[] = [];
  for (const entry of entries) {
    result.push(...expandNumericRange(entry));
  }
  return result;
}

function expandHostToken(token: string): string[] {
  const openIndex = token.indexOf('[');
  if (openIndex === -1) {
    return [token];
  }
  const closeIndex = token.indexOf(']', openIndex + 1);
  if (closeIndex === -1) {
    return [token];
  }
  const prefix = token.slice(0, openIndex);
  const inner = token.slice(openIndex + 1, closeIndex);
  const suffix = token.slice(closeIndex + 1);
  const expandedInner = expandBracketValues(inner);
  const expandedSuffix = suffix ? expandHostToken(suffix) : [''];
  const result: string[] = [];
  for (const innerValue of expandedInner) {
    for (const suffixValue of expandedSuffix) {
      result.push(prefix + innerValue + suffixValue);
    }
  }
  return result;
}

function expandSlurmHostList(input: string): string[] {
  const trimmed = input.trim();
  if (!trimmed || trimmed === '(null)' || trimmed === 'N/A') {
    return [];
  }
  const tokens = splitSlurmList(trimmed);
  const result: string[] = [];
  for (const token of tokens) {
    result.push(...expandHostToken(token));
  }
  return result;
}

function parseSqueueUsageOutput(output: string): {
  nodeCpuUsed: Map<string, number>;
  nodeGpuUsedTypes: Map<string, Record<string, number>>;
} {
  const nodeCpuUsed = new Map<string, number>();
  const nodeGpuUsedTypes = new Map<string, Record<string, number>>();
  const lines = output.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  for (const line of lines) {
    const fields = line.split('|');
    if (fields.length < 4) {
      continue;
    }
    const state = fields[0].trim();
    if (!state || state.startsWith('PD')) {
      continue;
    }
    const cpuStr = fields[1].trim();
    const gresRaw = fields[2] || '';
    const nodelist = fields.slice(3).join('|').trim();
    const cpus = Number(cpuStr);
    const nodes = expandSlurmHostList(nodelist);
    if (!Number.isFinite(cpus) || nodes.length === 0) {
      continue;
    }
    const perNodeCpu = Math.max(Math.floor(cpus / nodes.length), 1);
    const remCpu = cpus % nodes.length;
    const gpuReq = parseGresField(gresRaw);
    const perNodeGpu: Record<string, number> = {};
    const remGpu: Record<string, number> = {};
    for (const [type, count] of Object.entries(gpuReq)) {
      perNodeGpu[type] = Math.floor(count / nodes.length);
      remGpu[type] = count % nodes.length;
    }
    nodes.forEach((node, index) => {
      const incCpu = perNodeCpu + (index < remCpu ? 1 : 0);
      nodeCpuUsed.set(node, (nodeCpuUsed.get(node) || 0) + incCpu);
      if (Object.keys(gpuReq).length === 0) {
        return;
      }
      const current = nodeGpuUsedTypes.get(node) || {};
      for (const [type, base] of Object.entries(perNodeGpu)) {
        const incGpu = base + (index < (remGpu[type] || 0) ? 1 : 0);
        if (incGpu <= 0) {
          continue;
        }
        current[type] = (current[type] || 0) + incGpu;
      }
      nodeGpuUsedTypes.set(node, current);
    });
  }
  return { nodeCpuUsed, nodeGpuUsedTypes };
}

function computeFreeResourceSummary(
  nodeInfoOutput: string,
  squeueOutput: string
): Map<string, PartitionFreeSummary> | undefined {
  const nodesInfo = parseNodeInfoOutput(nodeInfoOutput);
  if (nodesInfo.size === 0) {
    return undefined;
  }
  const { nodeCpuUsed, nodeGpuUsedTypes } = parseSqueueUsageOutput(squeueOutput);
  const summary = new Map<string, PartitionFreeSummary>();

  for (const node of nodesInfo.values()) {
    const bad = FREE_RESOURCE_BAD_STATES.has(node.state);
    const usedCpu = nodeCpuUsed.get(node.name) || 0;
    const freeCpu = bad ? 0 : Math.max(node.cpus - usedCpu, 0);
    const usedGpuTypes = nodeGpuUsedTypes.get(node.name) || {};
    let freeGpuTotal = 0;
    const nodeFreeGpuTypes: Record<string, number> = {};
    for (const [type, total] of Object.entries(node.gpuTypes)) {
      const used = usedGpuTypes[type] || 0;
      const free = bad ? 0 : Math.max(total - used, 0);
      if (free > 0) {
        nodeFreeGpuTypes[type] = free;
      }
      freeGpuTotal += free;
    }
    const partitions = node.partitions.length
      ? node.partitions.map((entry) => normalizePartitionName(entry)).filter(Boolean)
      : ['unknown'];
    const uniquePartitions = Array.from(new Set(partitions));
    for (const partition of uniquePartitions) {
      const entry = summary.get(partition) || {
        freeNodes: 0,
        freeCpuTotal: 0,
        freeCpusPerNode: 0,
        freeGpuTotal: 0,
        freeGpuMax: 0,
        freeGpuTypes: {}
      };
      if (freeCpu > 0) {
        entry.freeNodes += 1;
      }
      entry.freeCpuTotal += freeCpu;
      entry.freeCpusPerNode = Math.max(entry.freeCpusPerNode, freeCpu);
      entry.freeGpuTotal += freeGpuTotal;
      entry.freeGpuMax = Math.max(entry.freeGpuMax, freeGpuTotal);
      for (const [type, free] of Object.entries(nodeFreeGpuTypes)) {
        const current = entry.freeGpuTypes[type] || 0;
        if (free > current) {
          entry.freeGpuTypes[type] = free;
        }
      }
      summary.set(partition, entry);
    }
  }

  return summary;
}

function applyFreeResourceSummary(info: ClusterInfo, summary: Map<string, PartitionFreeSummary>): void {
  if (!info.partitions || info.partitions.length === 0) {
    return;
  }
  for (const partition of info.partitions) {
    const free = summary.get(partition.name);
    if (!free) {
      continue;
    }
    partition.freeNodes = free.freeNodes;
    partition.freeCpuTotal = free.freeCpuTotal;
    partition.freeCpusPerNode = free.freeCpusPerNode;
    partition.freeGpuTotal = free.freeGpuTotal;
    partition.freeGpuMax = free.freeGpuMax;
    partition.freeGpuTypes = free.freeGpuTypes;
  }
}

async function pickFromList(title: string, items: string[], allowManual: boolean): Promise<string | undefined> {
  const picks: vscode.QuickPickItem[] = items.map((item) => ({ label: item }));
  if (allowManual) {
    picks.unshift({ label: 'Enter manually' });
  }

  const picked = await vscode.window.showQuickPick(picks, {
    title,
    placeHolder: items.length ? 'Select an item' : 'Enter a value'
  });

  if (!picked) {
    return undefined;
  }

  if (allowManual && picked.label === 'Enter manually') {
    const manual = await vscode.window.showInputBox({ title, prompt: 'Enter value' });
    return manual?.trim() || undefined;
  }

  return picked.label;
}

async function pickPartition(
  partitions: string[],
  defaultPartition?: string
): Promise<string | undefined | null> {
  if (partitions.length === 0) {
    const manual = await vscode.window.showInputBox({
      title: 'Partition',
      prompt: 'Enter partition or leave blank to use cluster default'
    });
    if (manual === undefined) {
      return null;
    }
    return manual.trim() || undefined;
  }

  const picks: vscode.QuickPickItem[] = [
    {
      label: 'Use cluster default',
      description: defaultPartition ? `(${defaultPartition})` : undefined
    },
    ...partitions.map((partition) => ({
      label: partition,
      description: partition === defaultPartition ? 'default' : undefined
    }))
  ];

  const picked = await vscode.window.showQuickPick(picks, {
    title: 'Select partition',
    placeHolder: 'Choose a partition'
  });

  if (!picked) {
    return null;
  }

  if (picked.label === 'Use cluster default') {
    return defaultPartition || undefined;
  }

  return picked.label;
}

async function pickOptionalValue(title: string, items: string[]): Promise<string | undefined> {
  if (items.length === 0) {
    return undefined;
  }

  const picks: vscode.QuickPickItem[] = [
    { label: 'None' },
    ...items.map((item) => ({ label: item }))
  ];
  const picked = await vscode.window.showQuickPick(picks, {
    title,
    placeHolder: 'Select a value'
  });
  if (!picked || picked.label === 'None') {
    return undefined;
  }
  return picked.label;
}

async function promptNumber(title: string, defaultValue: number, minValue: number): Promise<number | undefined> {
  const value = await vscode.window.showInputBox({
    title,
    value: String(defaultValue),
    prompt: 'Leave blank to use cluster default.',
    validateInput: (input) => {
      const trimmed = input.trim();
      if (!trimmed) {
        return undefined;
      }
      const parsed = Number(trimmed);
      if (!Number.isInteger(parsed) || parsed < minValue) {
        return `Enter an integer >= ${minValue}, or leave blank for default.`;
      }
      return undefined;
    }
  });
  if (value === undefined) {
    return undefined;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return 0;
  }
  return Number(trimmed);
}

async function promptTime(title: string, defaultValue: string): Promise<string | undefined> {
  const timePattern = /^(\d+-)?\d{1,2}:\d{2}:\d{2}$/;
  const value = await vscode.window.showInputBox({
    title,
    value: defaultValue,
    prompt: 'HH:MM:SS or D-HH:MM:SS (leave blank for cluster default)',
    validateInput: (input) => {
      const trimmed = input.trim();
      if (!trimmed) {
        return undefined;
      }
      return timePattern.test(trimmed) ? undefined : 'Invalid time format.';
    }
  });
  if (value === undefined) {
    return undefined;
  }
  return value.trim();
}

function buildSallocArgs(params: {
  partition?: string;
  nodes: number;
  tasksPerNode: number;
  cpusPerTask: number;
  time: string;
  memoryMb?: number;
  gpuType?: string;
  gpuCount?: number;
  qos?: string;
  account?: string;
}): string[] {
  const args: string[] = [];
  if (params.partition) {
    args.push(`--partition=${params.partition}`);
  }
  if (params.nodes > 0) {
    args.push(`--nodes=${params.nodes}`);
  }
  if (params.tasksPerNode > 0) {
    args.push(`--ntasks-per-node=${params.tasksPerNode}`);
  }
  if (params.cpusPerTask > 0) {
    args.push(`--cpus-per-task=${params.cpusPerTask}`);
  }
  if (params.time && params.time.trim().length > 0) {
    args.push(`--time=${params.time}`);
  }
  if (params.qos) {
    args.push(`--qos=${params.qos}`);
  }
  if (params.account) {
    args.push(`--account=${params.account}`);
  }
  if (params.memoryMb && params.memoryMb > 0) {
    args.push(`--mem=${params.memoryMb}`);
  }
  if (params.gpuCount && params.gpuCount > 0) {
    const type = params.gpuType ? params.gpuType.trim() : '';
    const gres = type ? `gpu:${type}:${params.gpuCount}` : `gpu:${params.gpuCount}`;
    args.push(`--gres=${gres}`);
  }
  return args;
}

function resolveSessionKey(cfg: SlurmConnectConfig, alias: string): string {
  const trimmed = (cfg.sessionKey || '').trim();
  if (trimmed) {
    return trimmed;
  }
  return alias.trim();
}

function wrapPythonScriptCommand(encodedScript: string, emptyFallback: string): string {
  const lines = [
    'set -e',
    'PYTHON=$(command -v python3 || command -v python || true)',
    'if [ -z "$PYTHON" ]; then',
    `  echo ${emptyFallback}`,
    '  exit 0',
    'fi',
    `printf %s ${encodedScript} | base64 -d | "$PYTHON" -`
  ];
  const script = lines.join('\n');
  return `bash -lc ${quoteShellArg(script)}`;
}

function buildSessionJobLookupScript(stateDir: string, sessionKey: string): string {
  const dirLiteral = JSON.stringify(stateDir);
  const keyLiteral = JSON.stringify(sessionKey);
  return [
    'import json, os, subprocess, sys, re',
    `state_dir = os.path.expanduser(${dirLiteral})`,
    `raw_key = ${keyLiteral}`,
    'def sanitize(value):',
    '    trimmed = (value or "").strip()',
    '    if not trimmed:',
    '        return "default"',
    '    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "-", trimmed).strip(".-")',
    '    sanitized = sanitized or "default"',
    '    return sanitized[:64]',
    'sessions_dir = os.path.join(state_dir, "sessions")',
    'safe_key = sanitize(raw_key)',
    'try:',
    '    user = os.environ.get("USER") or subprocess.check_output(["id", "-un"], text=True).strip()',
    'except Exception:',
    '    user = None',
    'safe_user = sanitize(user or "unknown")',
    'candidates = []',
    'user_root = os.path.join(sessions_dir, safe_user)',
    'if os.path.isdir(user_root):',
    '    candidates.append(os.path.join(user_root, safe_key, "job.json"))',
    'candidates.append(os.path.join(sessions_dir, safe_key, "job.json"))',
    'job_id = ""',
    'for path in candidates:',
    '    if not os.path.isfile(path):',
    '        continue',
    '    try:',
    '        with open(path, "r") as handle:',
    '            data = json.load(handle)',
    '        job_id = str(data.get("job_id") or "").strip()',
    '        if job_id:',
    '            break',
    '    except Exception:',
    '        pass',
    'if not job_id:',
    '    search_roots = []',
    '    if os.path.isdir(user_root):',
    '        search_roots.append(user_root)',
    '    if os.path.isdir(sessions_dir):',
    '        search_roots.append(sessions_dir)',
    '    for root in search_roots:',
    '        for entry in sorted(os.listdir(root)):',
    '            path = os.path.join(root, entry, "job.json")',
    '            if not os.path.isfile(path):',
    '                continue',
    '            try:',
    '                with open(path, "r") as handle:',
    '                    data = json.load(handle)',
    '            except Exception:',
    '                continue',
    '            entry_key = str(data.get("session_key") or entry).strip()',
    '            if entry_key == raw_key or sanitize(entry_key) == safe_key:',
    '                job_id = str(data.get("job_id") or "").strip()',
    '                if job_id:',
    '                    break',
    '        if job_id:',
    '            break',
    'print(job_id)'
  ].join('\n');
}

function buildLocalCancelPersistentSessionCommand(stateDir: string, sessionKey: string): string {
  const script = buildSessionJobLookupScript(stateDir, sessionKey);
  return [
    'set +e',
    'JOB_ID="${SLURM_JOB_ID:-${SLURM_JOBID:-}}"',
    'if [ -n "$JOB_ID" ]; then',
    '  echo "Slurm Connect: cancelling job $JOB_ID (from SLURM_JOB_ID)"',
    '  scancel "$JOB_ID"',
    '  exit $?',
    'fi',
    'PYTHON=$(command -v python3 || command -v python || true)',
    'if [ -z "$PYTHON" ]; then',
    '  echo "Slurm Connect: python not available to resolve session job."',
    '  exit 1',
    'fi',
    'JOB_ID=$("$PYTHON" - <<\'PY\'',
    script,
    'PY',
    ')',
    'JOB_ID=$(printf "%s" "$JOB_ID" | tr -d "\\r" | tail -n 1)',
    'if [ -z "$JOB_ID" ]; then',
    '  echo "Slurm Connect: no session job found."',
    '  exit 1',
    'fi',
    'echo "Slurm Connect: cancelling job $JOB_ID"',
    'scancel "$JOB_ID"'
  ].join('\n');
}

async function cancelPersistentSessionJob(
  loginHost: string,
  sessionKey: string,
  cfg: SlurmConnectConfig,
  options?: { useTerminal?: boolean }
): Promise<boolean> {
  const log = getOutputChannel();
  try {
    const stateDir = cfg.sessionStateDir.trim() || DEFAULT_SESSION_STATE_DIR;
    if (vscode.env.remoteName === 'ssh-remote') {
      const localCommand = buildLocalCancelPersistentSessionCommand(stateDir, sessionKey);
      log.appendLine(`Cancelling session "${sessionKey}" in remote terminal.`);
      const terminal = vscode.window.createTerminal({ name: 'Slurm Connect Cancel' });
      terminal.show(true);
      terminal.sendText(localCommand, true);
      void vscode.window.showInformationMessage(
        `Cancel command sent to the terminal. Check terminal output to confirm cancellation for "${sessionKey}".`
      );
      return true;
    }
    // NOTE: Non-remote cancel path is disabled for now; cancel works only when connected.
    void vscode.window.showWarningMessage(
      'Cancel job is only supported from an active remote session. Connect to the session first.'
    );
    return false;
  } catch (error) {
    const message = `Failed to cancel session ${sessionKey}: ${formatError(error)}`;
    log.appendLine(message);
    void vscode.window.showErrorMessage(message);
    return false;
  }
}

function buildProxyArgs(
  cfg: SlurmConnectConfig,
  sessionKey?: string,
  clientId?: string,
  localProxyPlan?: LocalProxyPlan
): string[] {
  const args = cfg.proxyArgs.filter(Boolean);
  if (localProxyPlan?.computeTunnel) {
    const tunnel = localProxyPlan.computeTunnel;
    args.push('--local-proxy-tunnel');
    args.push(`--local-proxy-tunnel-host=${tunnel.loginHost}`);
    args.push(`--local-proxy-tunnel-port=${tunnel.port}`);
    if (tunnel.loginUser) {
      args.push(`--local-proxy-tunnel-user=${tunnel.loginUser}`);
    }
    args.push(`--local-proxy-auth-user=${tunnel.authUser}`);
    args.push(`--local-proxy-auth-token=${tunnel.authToken}`);
    const tunnelTimeout = resolveLocalProxyTunnelTimeoutSeconds();
    if (tunnelTimeout) {
      args.push(`--local-proxy-tunnel-timeout=${tunnelTimeout}`);
    }
    if (tunnel.noProxy) {
      args.push(`--local-proxy-no-proxy=${tunnel.noProxy}`);
    }
  }
  const key = (sessionKey || cfg.sessionKey || '').trim();
  if (cfg.sessionMode !== 'persistent') {
    if (key) {
      args.push(`--session-key=${key}`);
    }
    return args;
  }
  const sessionArgs = ['--session-mode=persistent'];
  if (key) {
    sessionArgs.push(`--session-key=${key}`);
  }
  if (clientId && clientId.trim().length > 0) {
    sessionArgs.push(`--session-client-id=${clientId.trim()}`);
  }
  if (cfg.sessionIdleTimeoutSeconds > 0) {
    sessionArgs.push(`--session-idle-timeout=${Math.floor(cfg.sessionIdleTimeoutSeconds)}`);
  }
  if (cfg.sessionStateDir && cfg.sessionStateDir.trim().length > 0) {
    sessionArgs.push(`--session-state-dir=${cfg.sessionStateDir.trim()}`);
  }
  return args.concat(sessionArgs);
}

const LOCAL_PROXY_AUTH_USER = 'slurm-connect';

function resolveLocalProxyTunnelTimeoutSeconds(): number | undefined {
  const cfg = vscode.workspace.getConfiguration(SETTINGS_SECTION);
  const configured = Number(cfg.get<number>('sshConnectTimeoutSeconds', 15));
  if (Number.isFinite(configured) && configured > 0) {
    return Math.floor(configured);
  }
  const remoteTimeout = vscode.workspace.getConfiguration('remote.SSH').get<number>('connectTimeout');
  if (Number.isFinite(remoteTimeout) && Number(remoteTimeout) > 0) {
    return Math.floor(Number(remoteTimeout));
  }
  return undefined;
}

function normalizeProxyPort(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  const port = Math.floor(value);
  if (port < 0 || port > 65535) {
    return 0;
  }
  return port;
}

function buildLocalProxyRemotePortCandidates(basePort: number, attempts: number): number[] {
  const total = Math.max(1, Math.floor(attempts || 1));
  const ports = new Set<number>();
  const base = normalizeProxyPort(basePort);
  if (base > 0) {
    ports.add(base);
  }
  const min = 49152;
  const max = 65535;
  while (ports.size < total) {
    const port = Math.floor(Math.random() * (max - min + 1)) + min;
    if (port > 0) {
      ports.add(port);
    }
  }
  return Array.from(ports);
}

async function pickRemoteForwardPort(
  loginHost: string,
  cfg: SlurmConnectConfig,
  bindHost: string
): Promise<number> {
  const host = (bindHost || '').trim() || '127.0.0.1';
  const script = [
    'import socket',
    `host=${JSON.stringify(host)}`,
    's=socket.socket()',
    's.bind((host,0))',
    'print(s.getsockname()[1])',
    's.close()'
  ].join('\n');
  const command = [
    'PYTHON=$(command -v python3 || command -v python || true)',
    'if [ -z "$PYTHON" ]; then exit 1; fi',
    '"$PYTHON" - <<\'PY\'',
    script,
    'PY'
  ].join('\n');
  const output = await runSshCommand(loginHost, cfg, command);
  const raw = output
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .pop();
  const port = raw ? Number(raw) : NaN;
  if (!Number.isFinite(port) || port <= 0 || port > 65535) {
    throw new Error(`Unable to parse remote port from: ${output}`);
  }
  return Math.floor(port);
}

function normalizeHostForMatch(host: string): string {
  const trimmed = (host || '').trim().toLowerCase();
  if (!trimmed) {
    return '';
  }
  if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
    return trimmed.slice(1, -1);
  }
  return trimmed.endsWith('.') ? trimmed.slice(0, -1) : trimmed;
}

function extractIpv4FromMappedIpv6(host: string): string | undefined {
  const match = /^(?:0:0:0:0:0:ffff:|::ffff:)(.+)$/.exec(host);
  if (!match) {
    return undefined;
  }
  const tail = match[1];
  if (tail.includes('.')) {
    return net.isIP(tail) === 4 ? tail : undefined;
  }
  const parts = tail.split(':');
  const isHex = (value: string): boolean => /^[0-9a-f]{1,4}$/.test(value);
  let num: number | undefined;
  if (parts.length === 2 && parts.every(isHex)) {
    num = (parseInt(parts[0], 16) << 16) | parseInt(parts[1], 16);
  } else if (parts.length === 1 && /^[0-9a-f]{1,8}$/.test(parts[0])) {
    num = parseInt(parts[0], 16);
  }
  if (num === undefined || !Number.isFinite(num)) {
    return undefined;
  }
  const bytes = [
    (num >>> 24) & 0xff,
    (num >>> 16) & 0xff,
    (num >>> 8) & 0xff,
    num & 0xff
  ];
  return bytes.join('.');
}

function isLoopbackHost(host: string): boolean {
  const normalized = normalizeHostForMatch(host);
  if (!normalized) {
    return false;
  }
  if (normalized === 'localhost' || normalized === '0.0.0.0' || normalized === '::' || normalized === '::1') {
    return true;
  }
  if (normalized === 'ip6-localhost' || normalized === 'ip6-loopback') {
    return true;
  }
  const ipType = net.isIP(normalized);
  if (ipType === 4) {
    return normalized.startsWith('127.') || normalized === '0.0.0.0';
  }
  if (ipType === 6) {
    if (normalized === '0:0:0:0:0:0:0:1') {
      return true;
    }
    const mapped = extractIpv4FromMappedIpv6(normalized);
    if (mapped) {
      return mapped.startsWith('127.') || mapped === '0.0.0.0';
    }
  }
  return false;
}

function splitHostPort(input: string): { host: string; port?: number } {
  const trimmed = (input || '').trim();
  if (!trimmed) {
    return { host: '' };
  }
  if (trimmed.startsWith('[')) {
    const closeIndex = trimmed.indexOf(']');
    if (closeIndex > 0) {
      const host = trimmed.slice(1, closeIndex);
      const rest = trimmed.slice(closeIndex + 1);
      if (rest.startsWith(':')) {
        const port = Number(rest.slice(1));
        return { host, port: Number.isFinite(port) ? port : undefined };
      }
      return { host };
    }
  }
  const lastColon = trimmed.lastIndexOf(':');
  if (lastColon > 0 && trimmed.indexOf(':') === lastColon) {
    const host = trimmed.slice(0, lastColon);
    const port = Number(trimmed.slice(lastColon + 1));
    return { host, port: Number.isFinite(port) ? port : undefined };
  }
  return { host: trimmed };
}

function isHostAllowed(host: string): boolean {
  if (!host) {
    return false;
  }
  return !isLoopbackHost(host);
}

function buildNoProxyValue(values: string[]): string {
  const cleaned = values.map((entry) => entry.trim()).filter(Boolean);
  if (cleaned.length === 0) {
    return '';
  }
  return uniqueList(cleaned).join(',');
}

function buildLocalProxyTunnelConfigKey(
  cfg: SlurmConnectConfig,
  loginHost: string,
  remoteBind: string,
  localPort: number
): string {
  return JSON.stringify({
    loginHost,
    user: cfg.user || '',
    identityFile: cfg.identityFile || '',
    sshQueryConfigPath: cfg.sshQueryConfigPath || '',
    remoteBind,
    localPort
  });
}

function buildLocalProxyTunnelTarget(cfg: SlurmConnectConfig, loginHost: string): string {
  return cfg.user ? `${cfg.user}@${loginHost}` : loginHost;
}

function buildLocalProxyControlPath(key: string): string {
  const hash = crypto.createHash('sha256').update(key).digest('hex').slice(0, 12);
  const baseDir = process.platform === 'win32' ? os.tmpdir() : '/tmp';
  return path.join(baseDir, `slurm-connect-tunnel-${hash}.sock`);
}

function resolveRemoteBindConnectHost(bindHost: string): string {
  const trimmed = (bindHost || '').trim().toLowerCase();
  if (!trimmed || trimmed === '0.0.0.0' || trimmed === '::' || trimmed === '[::]') {
    return '127.0.0.1';
  }
  return trimmed;
}

async function verifyRemoteForwardListener(
  cfg: SlurmConnectConfig,
  loginHost: string,
  bindHost: string,
  remotePort: number
): Promise<boolean> {
  const host = resolveRemoteBindConnectHost(bindHost);
  const script = [
    'import socket,sys',
    `host=${JSON.stringify(host)}`,
    `port=${Math.floor(remotePort)}`,
    's=socket.socket()',
    's.settimeout(1.5)',
    'err=s.connect_ex((host,port))',
    's.close()',
    'sys.exit(0 if err==0 else 1)'
  ].join('\n');
  const command = [
    'PYTHON=$(command -v python3 || command -v python || true)',
    'if [ -z "$PYTHON" ]; then exit 1; fi',
    '"$PYTHON" - <<\'PY\'',
    script,
    'PY'
  ].join('\n');
  try {
    await runSshCommand(loginHost, cfg, command);
    return true;
  } catch {
    return false;
  }
}

function loadStoredLocalProxyTunnelState(): LocalProxyTunnelState | undefined {
  if (!extensionGlobalState) {
    return undefined;
  }
  const stored = extensionGlobalState.get<LocalProxyTunnelState>(LOCAL_PROXY_TUNNEL_STATE_KEY);
  if (!stored || typeof stored !== 'object') {
    return undefined;
  }
  if (!stored.controlPath || !stored.target || !stored.configKey || !stored.remotePort) {
    persistLocalProxyTunnelState(undefined);
    return undefined;
  }
  return stored;
}

function persistLocalProxyTunnelState(state?: LocalProxyTunnelState): void {
  if (!extensionGlobalState) {
    return;
  }
  void extensionGlobalState.update(LOCAL_PROXY_TUNNEL_STATE_KEY, state);
}

function loadStoredLocalProxyRuntimeState(): LocalProxyRuntimeState | undefined {
  if (!extensionGlobalState) {
    return undefined;
  }
  const stored = extensionGlobalState.get<LocalProxyRuntimeState>(LOCAL_PROXY_RUNTIME_STATE_KEY);
  if (!stored || typeof stored !== 'object') {
    return undefined;
  }
  if (!stored.port || !stored.authUser || !stored.authToken || !stored.loginHost || !stored.remoteBind) {
    persistLocalProxyRuntimeState(undefined);
    return undefined;
  }
  return stored;
}

function persistLocalProxyRuntimeState(state?: LocalProxyRuntimeState): void {
  if (!extensionGlobalState) {
    return;
  }
  void extensionGlobalState.update(LOCAL_PROXY_RUNTIME_STATE_KEY, state);
}

async function cleanupControlPath(controlPath: string): Promise<void> {
  if (!controlPath) {
    return;
  }
  try {
    await fs.unlink(controlPath);
  } catch {
    // Ignore cleanup failures.
  }
}

async function checkLocalProxyTunnel(state: LocalProxyTunnelState, cfg: SlurmConnectConfig): Promise<boolean> {
  const sshPath = await resolveSshToolPath('ssh');
  const args: string[] = [];
  if (cfg.sshQueryConfigPath) {
    args.push('-F', expandHome(cfg.sshQueryConfigPath));
  }
  args.push('-S', state.controlPath, '-O', 'check');
  args.push('-o', 'BatchMode=yes', '-o', `ConnectTimeout=${cfg.sshConnectTimeoutSeconds}`);
  appendSshHostKeyCheckingArgs(args, cfg);
  if (cfg.identityFile) {
    args.push('-i', expandHome(cfg.identityFile));
  }
  args.push(state.target);
  try {
    await execFileAsync(sshPath, args, { timeout: cfg.sshConnectTimeoutSeconds * 1000 });
    return true;
  } catch {
    await cleanupControlPath(state.controlPath);
    return false;
  }
}

function stopLocalProxyTunnel(): void {
  const state = localProxyTunnelState ?? loadStoredLocalProxyTunnelState();
  if (!state) {
    return;
  }
  const args = ['-S', state.controlPath, '-O', 'exit', state.target];
  void resolveSshToolPath('ssh')
    .then((sshPath) => execFileAsync(sshPath, args, { timeout: 5000 }))
    .catch(() => undefined);
  localProxyTunnelState = undefined;
  persistLocalProxyTunnelState(undefined);
}

function buildLocalProxyTunnelArgs(
  cfg: SlurmConnectConfig,
  loginHost: string,
  remoteBind: string,
  remotePort: number,
  localPort: number,
  controlPath: string
): string[] {
  const args: string[] = [];
  if (cfg.sshQueryConfigPath) {
    args.push('-F', expandHome(cfg.sshQueryConfigPath));
  }
  args.push('-M', '-S', controlPath);
  args.push('-N', '-f');
  args.push('-o', 'BatchMode=yes');
  args.push('-o', `ConnectTimeout=${cfg.sshConnectTimeoutSeconds}`);
  appendSshHostKeyCheckingArgs(args, cfg);
  args.push('-o', 'ExitOnForwardFailure=yes');
  args.push('-o', 'ServerAliveInterval=30');
  args.push('-o', 'ServerAliveCountMax=3');
  args.push('-R', `${remoteBind}:${remotePort}:127.0.0.1:${localPort}`);
  if (cfg.identityFile) {
    args.push('-i', expandHome(cfg.identityFile));
  }
  args.push(buildLocalProxyTunnelTarget(cfg, loginHost));
  return args;
}

async function startLocalProxyTunnel(
  cfg: SlurmConnectConfig,
  loginHost: string,
  remoteBind: string,
  localPort: number,
  configKey: string
): Promise<LocalProxyTunnelState> {
  const log = getOutputChannel();
  const sshPath = await resolveSshToolPath('ssh');
  const maxAttempts = 3;
  let lastError: Error | undefined;
  const target = buildLocalProxyTunnelTarget(cfg, loginHost);

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    let remotePort: number;
    try {
      remotePort = await pickRemoteForwardPort(loginHost, cfg, remoteBind);
    } catch (error) {
      lastError = error as Error;
      break;
    }
    const controlPath = buildLocalProxyControlPath(`${configKey}:${remotePort}`);
    await cleanupControlPath(controlPath);
    log.appendLine(
      `Starting local proxy tunnel (${attempt}/${maxAttempts}): ${remoteBind}:${remotePort} -> 127.0.0.1:${localPort}`
    );
    const args = buildLocalProxyTunnelArgs(cfg, loginHost, remoteBind, remotePort, localPort, controlPath);
    try {
      const { stderr } = await execFileAsync(sshPath, args, { timeout: cfg.sshConnectTimeoutSeconds * 1000 });
      if (stderr && /remote port forwarding failed/i.test(stderr)) {
        log.appendLine(`Local proxy tunnel SSH warning: ${stderr.trim()}`);
        lastError = new Error('Remote port forwarding failed');
        await cleanupControlPath(controlPath);
        continue;
      }
    } catch (error) {
      const errorText = normalizeSshErrorText(error);
      const summary = pickSshErrorSummary(errorText);
      log.appendLine(`Local proxy tunnel SSH failed: ${summary || errorText}`);
      lastError = new Error(summary || 'SSH tunnel failed');
      continue;
    }
    const state: LocalProxyTunnelState = {
      remotePort,
      configKey,
      controlPath,
      target
    };
    if (!(await checkLocalProxyTunnel(state, cfg))) {
      log.appendLine('Local proxy tunnel did not respond to control check.');
      await cleanupControlPath(controlPath);
      lastError = new Error('SSH tunnel failed to start.');
      continue;
    }
    if (!(await verifyRemoteForwardListener(cfg, loginHost, remoteBind, remotePort))) {
      log.appendLine(`Local proxy tunnel listener not detected on ${remoteBind}:${remotePort}.`);
      await cleanupControlPath(controlPath);
      lastError = new Error('Remote proxy listener not available.');
      continue;
    }
    persistLocalProxyTunnelState(state);
    log.appendLine(`Local proxy tunnel ready on ${remoteBind}:${remotePort}`);
    return state;
  }

  throw lastError ?? new Error('Failed to start local proxy tunnel.');
}

async function ensureLocalProxyTunnel(
  cfg: SlurmConnectConfig,
  loginHost: string,
  remoteBind: string,
  localPort: number
): Promise<number> {
  const configKey = buildLocalProxyTunnelConfigKey(cfg, loginHost, remoteBind, localPort);
  const inMemory = localProxyTunnelState;
  if (inMemory && inMemory.configKey === configKey) {
    if (await checkLocalProxyTunnel(inMemory, cfg)) {
      return inMemory.remotePort;
    }
    stopLocalProxyTunnel();
  }
  const stored = loadStoredLocalProxyTunnelState();
  if (stored && stored.configKey === configKey) {
    if (await checkLocalProxyTunnel(stored, cfg)) {
      localProxyTunnelState = stored;
      return stored.remotePort;
    }
    stopLocalProxyTunnel();
  }
  stopLocalProxyTunnel();
  const state = await startLocalProxyTunnel(cfg, loginHost, remoteBind, localPort, configKey);
  localProxyTunnelState = state;
  return state.remotePort;
}

function parseProxyTarget(req: http.IncomingMessage): {
  hostname: string;
  port: number;
  path: string;
  isHttps: boolean;
  hostHeader: string;
} | null {
  const rawUrl = String(req.url || '');
  let parsed: URL;
  if (/^https?:\/\//i.test(rawUrl)) {
    try {
      parsed = new URL(rawUrl);
    } catch {
      return null;
    }
  } else {
    const hostHeader = String(req.headers.host || '').trim();
    if (!hostHeader) {
      return null;
    }
    try {
      parsed = new URL(`http://${hostHeader}${rawUrl}`);
    } catch {
      return null;
    }
  }
  const hostname = parsed.hostname;
  const isHttps = parsed.protocol === 'https:';
  const port = parsed.port ? Number(parsed.port) : isHttps ? 443 : 80;
  if (!hostname || !Number.isFinite(port)) {
    return null;
  }
  const path = `${parsed.pathname || '/'}${parsed.search || ''}`;
  const hostHeader = (() => {
    const isIpv6 = net.isIP(hostname) === 6;
    if (parsed.port) {
      return isIpv6 ? `[${hostname}]:${parsed.port}` : `${hostname}:${parsed.port}`;
    }
    return isIpv6 ? `[${hostname}]` : hostname;
  })();
  return {
    hostname,
    port,
    path,
    isHttps,
    hostHeader
  };
}

function isProxyAuthValid(req: http.IncomingMessage, authUser: string, authToken: string): boolean {
  const header = req.headers['proxy-authorization'];
  if (!header) {
    return false;
  }
  const value = Array.isArray(header) ? header[0] : header;
  const match = /^Basic\s+(.+)$/i.exec(value.trim());
  if (!match) {
    return false;
  }
  let decoded = '';
  try {
    decoded = Buffer.from(match[1], 'base64').toString('utf8');
  } catch {
    return false;
  }
  const separatorIndex = decoded.indexOf(':');
  if (separatorIndex < 0) {
    return false;
  }
  const user = decoded.slice(0, separatorIndex);
  const pass = decoded.slice(separatorIndex + 1);
  return user === authUser && pass === authToken;
}

function respondProxyAuthRequired(res: http.ServerResponse): void {
  res.writeHead(407, {
    'Proxy-Authenticate': 'Basic realm="Slurm Connect"',
    Connection: 'close'
  });
  res.end('Proxy authentication required.');
}

function respondProxyAuthRequiredSocket(socket: net.Socket): void {
  socket.write('HTTP/1.1 407 Proxy Authentication Required\r\n');
  socket.write('Proxy-Authenticate: Basic realm="Slurm Connect"\r\n');
  socket.write('Connection: close\r\n\r\n');
  socket.destroy();
}

async function startLocalProxyServer(
  port: number,
  authOverride?: { authUser: string; authToken: string }
): Promise<LocalProxyState> {
  const authUser = authOverride?.authUser || LOCAL_PROXY_AUTH_USER;
  const authToken = authOverride?.authToken || crypto.randomBytes(18).toString('hex');
  const server = http.createServer((req, res) => {
    if (!isProxyAuthValid(req, authUser, authToken)) {
      respondProxyAuthRequired(res);
      return;
    }
    const target = parseProxyTarget(req);
    if (!target) {
      res.writeHead(400, { Connection: 'close' });
      res.end('Unable to parse proxy target.');
      return;
    }
    if (!isHostAllowed(target.hostname)) {
      res.writeHead(403, { Connection: 'close' });
      res.end('Proxy target not allowed.');
      return;
    }
    const headers = { ...req.headers };
    delete headers['proxy-authorization'];
    delete headers['proxy-connection'];
    delete headers['connection'];
    headers.host = target.hostHeader;
    const requestOptions = {
      hostname: target.hostname,
      port: target.port,
      method: req.method,
      path: target.path,
      headers
    };
    const proxyReq = (target.isHttps ? https : http).request(requestOptions, (proxyRes) => {
      res.writeHead(proxyRes.statusCode || 502, proxyRes.headers);
      proxyRes.pipe(res);
    });
    proxyReq.on('error', () => {
      res.writeHead(502, { Connection: 'close' });
      res.end('Proxy request failed.');
    });
    req.pipe(proxyReq);
  });

  server.on('connect', (req: http.IncomingMessage, clientSocket: net.Socket, head: Buffer) => {
    if (!isProxyAuthValid(req, authUser, authToken)) {
      respondProxyAuthRequiredSocket(clientSocket);
      return;
    }
    const targetValue = String(req.url || '').trim();
    const { host, port: targetPort } = splitHostPort(targetValue);
    if (!host || !targetPort) {
      clientSocket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
      clientSocket.destroy();
      return;
    }
    if (!isHostAllowed(host)) {
      clientSocket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
      clientSocket.destroy();
      return;
    }
    const upstream = net.connect(targetPort, host, () => {
      clientSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
      if (head && head.length > 0) {
        upstream.write(head);
      }
      clientSocket.pipe(upstream);
      upstream.pipe(clientSocket);
    });
    upstream.on('error', () => {
      clientSocket.write('HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n');
      clientSocket.destroy();
    });
  });

  server.on('clientError', (_err, socket) => {
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
  });

  return await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => {
      server.removeListener('error', reject);
      const address = server.address() as net.AddressInfo | null;
      if (!address) {
        reject(new Error('Failed to resolve local proxy server address.'));
        return;
      }
      resolve({
        server,
        port: address.port,
        authUser,
        authToken
      });
    });
  });
}

function stopLocalProxyServer(options?: { stopTunnel?: boolean; clearRuntimeState?: boolean }): void {
  const stopTunnel = options?.stopTunnel !== false;
  const clearRuntimeState = options?.clearRuntimeState !== false;
  if (stopTunnel) {
    stopLocalProxyTunnel();
  }
  if (!localProxyState) {
    if (clearRuntimeState) {
      persistLocalProxyRuntimeState(undefined);
    }
    return;
  }
  try {
    localProxyState.server.close();
  } catch {
    // Ignore shutdown errors.
  }
  localProxyState = undefined;
  localProxyConfigKey = undefined;
  if (clearRuntimeState) {
    persistLocalProxyRuntimeState(undefined);
  }
}

function buildLocalProxyRuntimeState(
  loginHost: string,
  remoteBind: string,
  state: LocalProxyState
): LocalProxyRuntimeState {
  return {
    port: state.port,
    authUser: state.authUser,
    authToken: state.authToken,
    loginHost,
    remoteBind
  };
}

async function resumeLocalProxyForRemoteSession(): Promise<void> {
  if (vscode.env.remoteName !== 'ssh-remote') {
    return;
  }
  const cfg = getConfig();
  if (!cfg.localProxyEnabled) {
    stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: true });
    return;
  }
  const storedRuntime = loadStoredLocalProxyRuntimeState();
  if (!storedRuntime) {
    return;
  }
  const log = getOutputChannel();
  const configKey = JSON.stringify({ port: normalizeProxyPort(cfg.localProxyPort) });
  const tunnelMode = normalizeLocalProxyTunnelMode(cfg.localProxyTunnelMode || 'remoteSsh');
  if (!localProxyState) {
    try {
      localProxyState = await startLocalProxyServer(storedRuntime.port, {
        authUser: storedRuntime.authUser,
        authToken: storedRuntime.authToken
      });
      localProxyConfigKey = configKey;
      log.appendLine(`Local proxy resumed on 127.0.0.1:${localProxyState.port}`);
    } catch (error) {
      log.appendLine(`Failed to resume local proxy: ${formatError(error)}`);
      persistLocalProxyRuntimeState(undefined);
      return;
    }
  }
  if (tunnelMode === 'dedicated') {
    try {
      await ensureLocalProxyTunnel(cfg, storedRuntime.loginHost, storedRuntime.remoteBind, storedRuntime.port);
    } catch (error) {
      log.appendLine(`Failed to resume local proxy tunnel: ${formatError(error)}`);
    }
  } else {
    stopLocalProxyTunnel();
  }
}

async function ensureLocalProxyPlan(
  cfg: SlurmConnectConfig,
  loginHost: string,
  options?: { remotePortOverride?: number }
): Promise<LocalProxyPlan> {
  if (!cfg.localProxyEnabled) {
    if (localProxyState && connectionState !== 'connected') {
      stopLocalProxyServer();
    }
    return { enabled: false };
  }
  const log = getOutputChannel();
  const remoteBindInput = (cfg.localProxyRemoteBind || '').trim() || '0.0.0.0';
  const remoteBind = cfg.localProxyComputeTunnel ? '127.0.0.1' : remoteBindInput;
  const tunnelMode = normalizeLocalProxyTunnelMode(cfg.localProxyTunnelMode || 'remoteSsh');
  const localPort = normalizeProxyPort(cfg.localProxyPort);
  const configKey = JSON.stringify({ port: localPort });
  if (!localProxyState || localProxyConfigKey !== configKey) {
    stopLocalProxyServer({ stopTunnel: false, clearRuntimeState: false });
    const storedRuntime = loadStoredLocalProxyRuntimeState();
    const canResume = Boolean(
      storedRuntime &&
        storedRuntime.loginHost === loginHost &&
        storedRuntime.remoteBind === remoteBind &&
        (localPort === 0 || storedRuntime.port === localPort)
    );
    if (canResume && storedRuntime) {
      try {
        localProxyState = await startLocalProxyServer(storedRuntime.port, {
          authUser: storedRuntime.authUser,
          authToken: storedRuntime.authToken
        });
        localProxyConfigKey = configKey;
        log.appendLine(`Local proxy resumed on 127.0.0.1:${localProxyState.port}`);
      } catch (error) {
        log.appendLine(`Failed to resume local proxy: ${formatError(error)}`);
        persistLocalProxyRuntimeState(undefined);
      }
    }
    if (!localProxyState) {
      try {
        localProxyState = await startLocalProxyServer(localPort);
        localProxyConfigKey = configKey;
        log.appendLine(`Local proxy listening on 127.0.0.1:${localProxyState.port}`);
      } catch (error) {
        log.appendLine(`Failed to start local proxy: ${formatError(error)}`);
        throw error;
      }
    }
  }
  const remoteHost = (cfg.localProxyRemoteHost || '').trim() || loginHost;
  const proxyPort = localProxyState.port;
  let remotePort: number;
  let sshOptions: string[] | undefined;
  if (tunnelMode === 'dedicated') {
    try {
      remotePort = await ensureLocalProxyTunnel(cfg, loginHost, remoteBind, proxyPort);
    } catch (error) {
      getOutputChannel().appendLine(`Failed to start local proxy tunnel: ${formatError(error)}`);
      throw error;
    }
  } else {
    stopLocalProxyTunnel();
    const overridePort = normalizeProxyPort(options?.remotePortOverride ?? proxyPort);
    remotePort = overridePort || proxyPort;
    sshOptions = [
      `RemoteForward ${remoteBind}:${remotePort} 127.0.0.1:${proxyPort}`,
      'ExitOnForwardFailure yes'
    ];
  }
  persistLocalProxyRuntimeState(buildLocalProxyRuntimeState(loginHost, remoteBind, localProxyState));
  const noProxy = buildNoProxyValue(cfg.localProxyNoProxy);
  const computeTunnel = cfg.localProxyComputeTunnel
    ? {
        loginHost: remoteHost || loginHost,
        loginUser: cfg.user || undefined,
        port: remotePort,
        authUser: localProxyState.authUser,
        authToken: localProxyState.authToken,
        noProxy: noProxy || undefined
      }
    : undefined;
  const proxyHostForEnv = computeTunnel ? '127.0.0.1' : remoteHost;
  const proxyUrl = `http://${localProxyState.authUser}:${localProxyState.authToken}@${proxyHostForEnv}:${remotePort}`;
  return {
    enabled: true,
    proxyUrl,
    noProxy: noProxy || undefined,
    sshOptions,
    computeTunnel
  };
}

function escapeModuleLoadCommand(command: string): string {
  const trimmed = command.trim();
  if (!trimmed) {
    return trimmed;
  }
  const tokens = splitSshConfigArgs(trimmed);
  if (tokens.length === 0) {
    return trimmed;
  }
  const moduleSubcommands = new Set(['load', 'add', 'switch', 'swap', 'try-load', 'unload']);
  const result: string[] = [];
  let changed = false;
  let i = 0;
  while (i < tokens.length) {
    const token = tokens[i];
    if (isShellOperatorToken(token)) {
      result.push(token);
      i += 1;
      continue;
    }
    const lower = token.toLowerCase();
    if (lower === 'module' && i + 1 < tokens.length) {
      const sub = tokens[i + 1];
      const lowerSub = sub.toLowerCase();
      if (moduleSubcommands.has(lowerSub)) {
        result.push(token, sub);
        i += 2;
        while (i < tokens.length && !isShellOperatorToken(tokens[i])) {
          const cleaned = stripModuleTagSuffix(tokens[i]);
          if (!cleaned) {
            i += 1;
            continue;
          }
          const escaped = quoteShellArg(cleaned);
          if (cleaned !== tokens[i] || escaped !== cleaned) {
            changed = true;
          }
          result.push(escaped);
          i += 1;
        }
        continue;
      }
    }
    if (lower === 'ml') {
      result.push(token);
      i += 1;
      while (i < tokens.length && !isShellOperatorToken(tokens[i])) {
        const cleaned = stripModuleTagSuffix(tokens[i]);
        if (!cleaned) {
          i += 1;
          continue;
        }
        const escaped = quoteShellArg(cleaned);
        if (cleaned !== tokens[i] || escaped !== cleaned) {
          changed = true;
        }
        result.push(escaped);
        i += 1;
      }
      continue;
    }
    result.push(token);
    i += 1;
  }
  if (!changed) {
    return trimmed;
  }
  return result.join(' ');
}

function buildProxyEnvTokens(env?: LocalProxyEnv): string[] {
  if (!env || !env.proxyUrl) {
    return [];
  }
  const tokens = [
    'env',
    `HTTP_PROXY=${env.proxyUrl}`,
    `HTTPS_PROXY=${env.proxyUrl}`,
    `http_proxy=${env.proxyUrl}`,
    `https_proxy=${env.proxyUrl}`,
    `ALL_PROXY=${env.proxyUrl}`,
    `all_proxy=${env.proxyUrl}`
  ];
  if (env.noProxy) {
    tokens.push(`NO_PROXY=${env.noProxy}`, `no_proxy=${env.noProxy}`);
  }
  return tokens;
}

function buildRemoteCommand(
  cfg: SlurmConnectConfig,
  sallocArgs: string[],
  sessionKey?: string,
  clientId?: string,
  installSnippet?: string,
  localProxyEnv?: LocalProxyEnv,
  localProxyPlan?: LocalProxyPlan
): string {
  const proxyTokens = splitShellArgs(cfg.proxyCommand);
  if (proxyTokens.length === 0) {
    return '';
  }
  const envTokens = buildProxyEnvTokens(localProxyEnv);
  const proxyArgs = buildProxyArgs(cfg, sessionKey, clientId, localProxyPlan);
  const sallocFlags = sallocArgs.map((arg) => `--salloc-arg=${arg}`);
  const fullProxyCommand = joinShellCommand([...envTokens, ...proxyTokens, ...proxyArgs, ...sallocFlags]);
  const moduleLoad = cfg.moduleLoad ? escapeModuleLoadCommand(cfg.moduleLoad) : '';
  const baseCommand = moduleLoad
    ? `${moduleLoad} && ${fullProxyCommand}`.trim()
    : fullProxyCommand;
  if (!baseCommand) {
    return '';
  }
  if (!installSnippet) {
    return baseCommand;
  }
  const encoded = Buffer.from(installSnippet, 'utf8').toString('base64');
  const installScript = `echo ${encoded} | base64 -d | bash`;
  const wrapped = `${installScript}; ${baseCommand}`;
  return joinShellCommand(['bash', '-lc', wrapped]);
}

function buildDefaultAlias(
  prefix: string,
  loginHost: string,
  partition: string | undefined,
  nodes?: number,
  cpusPerTask?: number
): string {
  const hostShort = loginHost.split('.')[0];
  const pieces = [prefix || 'slurm', hostShort];
  if (partition) {
    pieces.push(partition);
  }
  if (nodes && nodes > 0) {
    pieces.push(`${nodes}n`);
  }
  if (cpusPerTask && cpusPerTask > 0) {
    pieces.push(`${cpusPerTask}c`);
  }
  return pieces.join('-').replace(/[^a-zA-Z0-9_-]/g, '-');
}

function defaultSshConfigPath(): string {
  return path.join(os.homedir(), '.ssh', 'config');
}

function resolveSlurmConnectIncludePath(cfg: SlurmConnectConfig): string {
  const trimmed = (cfg.temporarySshConfigPath || '').trim();
  return trimmed || '~/.ssh/slurm-connect.conf';
}

function resolveSlurmConnectIncludeFilePath(cfg: SlurmConnectConfig): string {
  return normalizeSshPath(resolveSlurmConnectIncludePath(cfg));
}

function normalizeSshPath(value: string): string {
  const expanded = expandHome(value);
  if (!path.isAbsolute(expanded)) {
    return path.resolve(os.homedir(), expanded);
  }
  return path.resolve(expanded);
}

function detectLineEnding(content: string): string {
  return content.includes('\r\n') ? '\r\n' : '\n';
}

function splitSshConfigArgs(input: string): string[] {
  const result: string[] = [];
  let current = '';
  let inSingle = false;
  let inDouble = false;
  let escaping = false;
  for (const ch of input) {
    if (escaping) {
      current += ch;
      escaping = false;
      continue;
    }
    if (inSingle) {
      if (ch === '\\') {
        escaping = true;
        continue;
      }
      if (ch === "'") {
        inSingle = false;
      } else {
        current += ch;
      }
      continue;
    }
    if (inDouble) {
      if (ch === '\\') {
        escaping = true;
        continue;
      }
      if (ch === '"') {
        inDouble = false;
      } else {
        current += ch;
      }
      continue;
    }
    if (ch === "'") {
      inSingle = true;
      continue;
    }
    if (ch === '"') {
      inDouble = true;
      continue;
    }
    if (/\s/.test(ch)) {
      if (current.length > 0) {
        result.push(current);
        current = '';
      }
      continue;
    }
    current += ch;
  }
  if (current.length > 0) {
    result.push(current);
  }
  return result;
}

function extractIncludeTargets(line: string): string[] {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith('#')) {
    return [];
  }
  const match = /^Include\s+(.+)$/i.exec(trimmed);
  if (!match) {
    return [];
  }
  return splitSshConfigArgs(match[1]);
}

function sshConfigHasIncludePath(content: string, includePath: string): boolean {
  const target = normalizeSshPath(includePath);
  const lines = content.split(/\r?\n/);
  for (const line of lines) {
    const targets = extractIncludeTargets(line);
    for (const candidate of targets) {
      if (normalizeSshPath(candidate) === target) {
        return true;
      }
    }
  }
  return false;
}

function parseSshConfigIncludePaths(content: string): string[] {
  const results: string[] = [];
  const lines = content.split(/\r?\n/);
  for (const line of lines) {
    const targets = extractIncludeTargets(line);
    if (targets.length > 0) {
      results.push(...targets);
    }
  }
  return results;
}

function isSshHostPattern(value: string): boolean {
  return value.startsWith('!') || /[*?\[]/.test(value);
}

function parseSshConfigHosts(content: string): string[] {
  const results: string[] = [];
  const lines = content.split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }
    const match = /^Host\s+(.+)$/i.exec(trimmed);
    if (!match) {
      continue;
    }
    const tokens = splitSshConfigArgs(match[1]);
    for (const token of tokens) {
      if (!token || isSshHostPattern(token)) {
        continue;
      }
      results.push(token);
    }
  }
  return uniqueList(results);
}

function hasGlobPattern(value: string): boolean {
  return /[*?\[]/.test(value);
}

function globSegmentToRegex(segment: string): RegExp {
  const escaped = segment.replace(/[.+^${}()|\\]/g, '\\$&');
  const withWildcards = escaped.replace(/\\\*/g, '.*').replace(/\\\?/g, '.');
  return new RegExp(`^${withWildcards}$`);
}

async function expandGlobPattern(pattern: string): Promise<string[]> {
  const resolved = path.resolve(pattern);
  const root = path.parse(resolved).root;
  const relative = resolved.slice(root.length);
  const segments = relative.split(path.sep).filter(Boolean);
  let candidates = [root];
  for (const segment of segments) {
    if (!hasGlobPattern(segment)) {
      candidates = candidates.map((base) => path.join(base, segment));
      continue;
    }
    const regex = globSegmentToRegex(segment);
    const next: string[] = [];
    for (const base of candidates) {
      let entries: Array<{ name: string }> = [];
      try {
        entries = await fs.readdir(base, { withFileTypes: true });
      } catch {
        continue;
      }
      for (const entry of entries) {
        if (regex.test(entry.name)) {
          next.push(path.join(base, entry.name));
        }
      }
    }
    candidates = next;
  }
  const results: string[] = [];
  for (const candidate of candidates) {
    try {
      const stat = await fs.stat(candidate);
      if (stat.isFile()) {
        results.push(candidate);
      }
    } catch {
      // Ignore missing files.
    }
  }
  return results;
}

async function expandSshIncludeTarget(value: string, baseDir: string): Promise<string[]> {
  const expanded = expandHome(value);
  const resolved = path.isAbsolute(expanded) ? expanded : path.resolve(baseDir, expanded);
  if (hasGlobPattern(resolved)) {
    return await expandGlobPattern(resolved);
  }
  return (await fileExists(resolved)) ? [resolved] : [];
}

async function collectSshConfigHosts(configPath: string | undefined, seen = new Set<string>()): Promise<string[]> {
  if (!configPath) {
    return [];
  }
  const resolved = normalizeSshPath(configPath);
  if (seen.has(resolved)) {
    return [];
  }
  seen.add(resolved);
  let content = '';
  try {
    content = await fs.readFile(resolved, 'utf8');
  } catch {
    return [];
  }
  const hosts = parseSshConfigHosts(content);
  const includeTargets = parseSshConfigIncludePaths(content);
  if (includeTargets.length === 0) {
    return hosts;
  }
  const baseDir = path.dirname(resolved);
  for (const target of includeTargets) {
    const expandedPaths = await expandSshIncludeTarget(target, baseDir);
    for (const includePath of expandedPaths) {
      const nested = await collectSshConfigHosts(includePath, seen);
      hosts.push(...nested);
    }
  }
  return uniqueList(hosts);
}

interface ExplicitSshHostConfig {
  user?: string;
  identityFiles: string[];
  hasExplicitHost: boolean;
}

function normalizeHostValue(value: string): string {
  return value.trim().toLowerCase();
}

function isExplicitHostToken(token: string): boolean {
  return !isSshHostPattern(token);
}

function matchesExplicitHostToken(host: string, token: string): boolean {
  return normalizeHostValue(host) === normalizeHostValue(token);
}

function isHostExplicitMatch(host: string, tokens: string[]): boolean {
  let explicitMatch = false;
  for (const raw of tokens) {
    if (!raw) {
      continue;
    }
    if (raw.startsWith('!')) {
      const negated = raw.slice(1);
      if (isExplicitHostToken(negated) && matchesExplicitHostToken(host, negated)) {
        return false;
      }
      continue;
    }
    if (isExplicitHostToken(raw) && matchesExplicitHostToken(host, raw)) {
      explicitMatch = true;
    }
  }
  return explicitMatch;
}

function parseSshConfigKeyValue(line: string): { key: string; values: string[] } | undefined {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith('#')) {
    return undefined;
  }
  const match = /^(\S+)\s+(.+)$/.exec(trimmed);
  if (!match) {
    return undefined;
  }
  const key = match[1].toLowerCase();
  const values = splitSshConfigArgs(match[2]);
  return { key, values };
}

async function collectExplicitHostConfig(
  configPath: string | undefined,
  host: string,
  seen = new Set<string>()
): Promise<ExplicitSshHostConfig> {
  const explicit: ExplicitSshHostConfig = { identityFiles: [], hasExplicitHost: false };
  if (!configPath) {
    return explicit;
  }
  const resolved = normalizeSshPath(configPath);
  if (seen.has(resolved)) {
    return explicit;
  }
  seen.add(resolved);
  let content = '';
  try {
    content = await fs.readFile(resolved, 'utf8');
  } catch {
    return explicit;
  }
  const lines = content.split(/\r?\n/);
  let inExplicitHost = false;
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }
    const includeTargets = extractIncludeTargets(line);
    if (includeTargets.length > 0) {
      if (!inExplicitHost) {
        continue;
      }
      const baseDir = path.dirname(resolved);
      for (const target of includeTargets) {
        const expandedPaths = await expandSshIncludeTarget(target, baseDir);
        for (const includePath of expandedPaths) {
          const nested = await collectExplicitHostConfig(includePath, host, seen);
          if (!explicit.user && nested.user) {
            explicit.user = nested.user;
          }
          if (nested.identityFiles.length > 0) {
            explicit.identityFiles.push(...nested.identityFiles);
          }
          if (nested.hasExplicitHost) {
            explicit.hasExplicitHost = true;
          }
        }
      }
      continue;
    }
    const hostMatch = /^Host\s+(.+)$/i.exec(trimmed);
    if (hostMatch) {
      const tokens = splitSshConfigArgs(hostMatch[1]);
      inExplicitHost = isHostExplicitMatch(host, tokens);
      if (inExplicitHost) {
        explicit.hasExplicitHost = true;
      }
      continue;
    }
    if (/^Match\b/i.test(trimmed)) {
      inExplicitHost = false;
      continue;
    }
    if (!inExplicitHost) {
      continue;
    }
    const parsed = parseSshConfigKeyValue(trimmed);
    if (!parsed) {
      continue;
    }
    if (parsed.key === 'user') {
      const value = parsed.values[0];
      if (value && !explicit.user) {
        explicit.user = value;
      }
      continue;
    }
    if (parsed.key === 'identityfile') {
      for (const value of parsed.values) {
        if (!value) {
          continue;
        }
        if (value.toLowerCase() === 'none') {
          continue;
        }
        explicit.identityFiles.push(value);
      }
      continue;
    }
  }
  explicit.identityFiles = uniqueList(explicit.identityFiles);
  return explicit;
}

async function resolveSshHostsConfigPath(cfg: SlurmConnectConfig): Promise<string | undefined> {
  const override = (cfg.sshQueryConfigPath || '').trim();
  const basePath = override ? normalizeSshPath(override) : normalizeSshPath(defaultSshConfigPath());
  return (await fileExists(basePath)) ? basePath : undefined;
}

function parseSshConfigOutput(output: string): Record<string, string[]> {
  const result: Record<string, string[]> = {};
  const lines = output.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  for (const line of lines) {
    const parts = line.split(/\s+/);
    if (parts.length < 2) {
      continue;
    }
    const key = parts.shift();
    if (!key) {
      continue;
    }
    const value = parts.join(' ').trim();
    if (!value) {
      continue;
    }
    const lowerKey = key.toLowerCase();
    if (!result[lowerKey]) {
      result[lowerKey] = [];
    }
    result[lowerKey].push(value);
  }
  return result;
}

function pickFirstValue(values?: string[]): string | undefined {
  if (!values || values.length === 0) {
    return undefined;
  }
  const trimmed = values.map((value) => value.trim()).filter(Boolean);
  return trimmed.length > 0 ? trimmed[0] : undefined;
}

async function pickFirstExistingPath(values?: string[]): Promise<string | undefined> {
  if (!values || values.length === 0) {
    return undefined;
  }
  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed) {
      continue;
    }
    const expanded = expandHome(trimmed);
    if (await fileExists(expanded)) {
      return trimmed;
    }
  }
  return pickFirstValue(values);
}

async function resolveSshHostFromConfig(
  host: string,
  cfg: SlurmConnectConfig
): Promise<ResolvedSshHostInfo> {
  const configPath = await resolveSshHostsConfigPath(cfg);
  const sshPath = await resolveSshToolPath('ssh');
  const args: string[] = [];
  if (configPath) {
    args.push('-F', configPath);
  }
  args.push('-G', host);
  try {
    const { stdout } = await execFileAsync(sshPath, args);
    const parsed = parseSshConfigOutput(stdout);
    const hostname = pickFirstValue(parsed.hostname);
    const port = pickFirstValue(parsed.port);
    const explicit = await collectExplicitHostConfig(configPath, host);
    const user = explicit.user;
    const identityFile = explicit.identityFiles.length > 0
      ? await pickFirstExistingPath(explicit.identityFiles)
      : undefined;
    const certificateFile = await pickFirstExistingPath(parsed.certificatefile);
    const proxyCommand = pickFirstValue(parsed.proxycommand);
    const proxyJump = pickFirstValue(parsed.proxyjump);
    const hasProxyCommand = Boolean(proxyCommand && proxyCommand.toLowerCase() !== 'none');
    const hasProxyJump = Boolean(proxyJump && proxyJump.toLowerCase() !== 'none');
    return {
      host,
      hostname,
      user,
      port,
      identityFile,
      certificateFile,
      hasProxyCommand,
      hasProxyJump,
      hasExplicitHost: explicit.hasExplicitHost
    };
  } catch (error) {
    const summary = pickSshErrorSummary(normalizeSshErrorText(error));
    throw new Error(summary || 'Failed to resolve SSH host.');
  }
}

function replaceIncludeLineWithBlock(content: string, includePath: string, block: string): string {
  const lineEnding = detectLineEnding(content);
  const lines = content.split(/\r?\n/);
  const blockLines = block.split(/\r?\n/);
  const target = normalizeSshPath(includePath);
  for (let i = 0; i < lines.length; i += 1) {
    const targets = extractIncludeTargets(lines[i]);
    if (targets.length !== 1) {
      continue;
    }
    for (const candidate of targets) {
      if (normalizeSshPath(candidate) === target) {
        lines.splice(i, 1, ...blockLines);
        return lines.join(lineEnding);
      }
    }
  }
  return content;
}

function removeIncludePathFromLines(
  content: string,
  includePath: string
): { content: string; removed: boolean } {
  const lineEnding = detectLineEnding(content);
  const lines = content.split(/\r?\n/);
  const target = normalizeSshPath(includePath);
  let removed = false;
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = lines[i];
    const targets = extractIncludeTargets(line);
    if (targets.length === 0) {
      continue;
    }
    const remaining = targets.filter((candidate) => normalizeSshPath(candidate) !== target);
    if (remaining.length === targets.length) {
      continue;
    }
    removed = true;
    if (remaining.length === 0) {
      lines.splice(i, 1);
      continue;
    }
    const indentMatch = line.match(/^(\s*)Include\b/i);
    const indent = indentMatch ? indentMatch[1] : '';
    const rebuilt = `${indent}Include ${remaining.map((value) => formatSshConfigValue(value)).join(' ')}`;
    lines[i] = rebuilt;
  }
  return { content: lines.join(lineEnding), removed };
}

function stripManagedIncludeBlock(content: string): { content: string; removed: boolean } {
  const blockRegex = new RegExp(
    `${SLURM_CONNECT_INCLUDE_START.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}[\\s\\S]*?${SLURM_CONNECT_INCLUDE_END.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`,
    'm'
  );
  if (!blockRegex.test(content)) {
    return { content, removed: false };
  }
  return { content: content.replace(blockRegex, ''), removed: true };
}

function ensureTrailingNewline(value: string, lineEnding: string): string {
  if (!value) {
    return value;
  }
  return value.endsWith(lineEnding) ? value : `${value}${lineEnding}`;
}

async function createSshConfigBackupPath(baseConfigPath: string): Promise<string> {
  const dir = path.dirname(baseConfigPath);
  const baseName = path.basename(baseConfigPath);
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  let candidate = path.join(dir, `${baseName}.slurm-connect.backup-${timestamp}`);
  let counter = 1;
  while (true) {
    try {
      await fs.access(candidate);
      candidate = path.join(dir, `${baseName}.slurm-connect.backup-${timestamp}-${counter}`);
      counter += 1;
    } catch {
      break;
    }
  }
  return candidate;
}

async function backupSshConfigFile(baseConfigPath: string, content: string): Promise<void> {
  const backupPath = await createSshConfigBackupPath(baseConfigPath);
  await fs.writeFile(backupPath, content, 'utf8');
  getOutputChannel().appendLine(`Backed up SSH config to ${backupPath}.`);
}

async function ensureSshIncludeInstalled(baseConfigPath: string, includePath: string): Promise<'added' | 'updated' | 'already'> {
  const resolvedBase = normalizeSshPath(baseConfigPath);
  const resolvedInclude = normalizeSshPath(includePath);
  if (resolvedBase === resolvedInclude) {
    throw new Error('Slurm Connect include path must not be the same as the SSH config path.');
  }
  let content = '';
  let fileExists = true;
  try {
    content = await fs.readFile(resolvedBase, 'utf8');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
      throw error;
    }
    fileExists = false;
  }
  const lineEnding = detectLineEnding(content);
  const block = buildSlurmConnectIncludeBlock(includePath).split('\n').join(lineEnding);
  const hadInclude = sshConfigHasIncludePath(content, includePath);
  const stripped = stripManagedIncludeBlock(content);
  const withoutBlock = stripped.content;
  const withoutTarget = removeIncludePathFromLines(withoutBlock, includePath);
  const bom = withoutTarget.content.startsWith('\uFEFF') ? '\uFEFF' : '';
  const bodyRaw = bom ? withoutTarget.content.slice(1) : withoutTarget.content;
  const body = bodyRaw.replace(/^[\r\n]+/, '');
  const combined = body ? `${bom}${block}${lineEnding}${lineEnding}${body}` : `${bom}${block}${lineEnding}`;
  const next = ensureTrailingNewline(combined, lineEnding);
  let status: 'added' | 'updated' | 'already' = 'already';
  if (!stripped.removed && !hadInclude) {
    status = 'added';
  } else if (next === content) {
    status = 'already';
  } else {
    status = 'updated';
  }
  if (next !== content) {
    await fs.mkdir(path.dirname(resolvedBase), { recursive: true });
    if (fileExists) {
      await backupSshConfigFile(resolvedBase, content);
    }
    await fs.writeFile(resolvedBase, ensureTrailingNewline(next, lineEnding), 'utf8');
  }
  return status;
}

async function writeSlurmConnectIncludeFile(entry: string, includePath: string): Promise<string> {
  const dir = path.dirname(includePath);
  await fs.mkdir(dir, { recursive: true });
  const content = buildSlurmConnectIncludeContent(entry);
  await fs.writeFile(includePath, content, 'utf8');
  return includePath;
}

function normalizeRemoteConfigPath(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

async function isSlurmConnectTempConfigFile(configPath: string): Promise<boolean> {
  try {
    const content = await fs.readFile(configPath, 'utf8');
    return content.includes('# Temporary SSH config generated by Slurm Connect');
  } catch {
    return false;
  }
}

async function guessPreviousConfigFromTempConfig(configPath: string): Promise<string | undefined> {
  try {
    const content = await fs.readFile(configPath, 'utf8');
    const includes = parseSshConfigIncludePaths(content).map((value) => normalizeSshPath(value));
    const defaultConfig = normalizeSshPath(defaultSshConfigPath());
    for (const includePath of includes) {
      if (includePath !== defaultConfig) {
        return includePath;
      }
    }
    return undefined;
  } catch {
    return undefined;
  }
}

async function migrateStaleRemoteSshConfigIfNeeded(): Promise<void> {
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const current = normalizeRemoteConfigPath(remoteCfg.get<string>('configFile'));
  if (!current) {
    return;
  }
  if (!(await isSlurmConnectTempConfigFile(current))) {
    return;
  }
  const restored = await guessPreviousConfigFromTempConfig(current);
  await remoteCfg.update('configFile', restored, vscode.ConfigurationTarget.Global);
  const log = getOutputChannel();
  log.appendLine(`Detected stale Slurm Connect temporary SSH config. Restored Remote.SSH configFile ${restored ? `to ${restored}` : 'to default'}.`);
}

function resolveWindowsUserSettingsPath(): string | undefined {
  const appData = process.env.APPDATA;
  if (!appData) {
    return undefined;
  }
  const appName = vscode.env.appName || '';
  const normalized = appName.toLowerCase();
  let folderName = 'Code';
  if (normalized.includes('insiders')) {
    folderName = 'Code - Insiders';
  } else if (normalized.includes('oss')) {
    folderName = 'Code - OSS';
  } else if (normalized.includes('vscodium')) {
    folderName = 'VSCodium';
  }
  return path.join(appData, folderName, 'User', 'settings.json');
}

function detectJsonFormattingOptions(text: string): jsonc.FormattingOptions {
  const eol = text.includes('\r\n') ? '\r\n' : text.includes('\n') ? '\n' : os.EOL;
  const indentMatch = text.match(/^[ \t]+(?="[^"]+")/m);
  if (indentMatch) {
    const indent = indentMatch[0];
    const usesTabs = indent.includes('\t');
    return {
      insertSpaces: !usesTabs,
      tabSize: usesTabs ? 1 : indent.length,
      eol
    };
  }
  return { insertSpaces: true, tabSize: 2, eol };
}

async function readUserSettingsText(settingsPath: string): Promise<string> {
  try {
    return await fs.readFile(settingsPath, 'utf8');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return '';
    }
    throw error;
  }
}

async function userSettingsHasKey(settingsPath: string, key: string): Promise<boolean> {
  const text = await readUserSettingsText(settingsPath);
  if (text.trim().length === 0) {
    return false;
  }
  const errors: jsonc.ParseError[] = [];
  const data = jsonc.parse(text, errors, {
    allowTrailingComma: true,
    disallowComments: false
  });
  if (errors.length > 0) {
    throw new Error('Unable to parse user settings JSON.');
  }
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    throw new Error('User settings JSON is not an object.');
  }
  return Object.prototype.hasOwnProperty.call(data as Record<string, unknown>, key);
}

async function addSettingToUserSettingsJson(
  settingsPath: string,
  key: string,
  value: boolean
): Promise<void> {
  const text = await readUserSettingsText(settingsPath);
  const errors: jsonc.ParseError[] = [];
  if (text.trim().length > 0) {
    jsonc.parse(text, errors, {
      allowTrailingComma: true,
      disallowComments: false
    });
  }
  if (errors.length > 0) {
    throw new Error('Unable to parse user settings JSON.');
  }
  const edits = jsonc.modify(text, [key], value, {
    formattingOptions: detectJsonFormattingOptions(text)
  });
  if (edits.length === 0) {
    return;
  }
  const updated = jsonc.applyEdits(text, edits);
  await fs.mkdir(path.dirname(settingsPath), { recursive: true });
  await fs.writeFile(settingsPath, updated, 'utf8');
}

async function ensureLocalServerSetting(): Promise<void> {
  const platform = os.platform();
  const osVersion = typeof os.version === 'function' ? os.version() : os.release();
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const useLocalServer = remoteCfg.get<boolean>('useLocalServer', false);

  if (platform === 'win32') {
    const settingsPath = resolveWindowsUserSettingsPath();
    if (!settingsPath) {
      void vscode.window.showWarningMessage(
        'Slurm Connect could not locate your Windows user settings.json file. Please add "remote.SSH.useLocalServer": true manually.'
      );
      return;
    }
    let hasKey = false;
    try {
      hasKey = await userSettingsHasKey(settingsPath, 'remote.SSH.useLocalServer');
      if (!hasKey) {
        const action = await vscode.window.showInformationMessage(
          `Slurm Connect detected ${platform} (${osVersion}). To work around a Remote-SSH issue, it can add "remote.SSH.useLocalServer": true to ${settingsPath} (required even if the UI shows it enabled).`,
          'Add Setting',
          'Not Now'
        );
        if (action !== 'Add Setting') {
          return;
        }
        try {
          await addSettingToUserSettingsJson(settingsPath, 'remote.SSH.useLocalServer', true);
        } catch (error) {
          void vscode.window.showWarningMessage(
            `Failed to update ${settingsPath} with remote.SSH.useLocalServer: ${formatError(error)}`
          );
        }
        return;
      }
    } catch (error) {
      void vscode.window.showWarningMessage(
        `Slurm Connect could not read ${settingsPath}. Please add "remote.SSH.useLocalServer": true manually. (${formatError(error)})`
      );
      return;
    }

    if (useLocalServer) {
      return;
    }

    const enable = await vscode.window.showWarningMessage(
      'Remote.SSH: Use Local Server is disabled. This is required for Slurm Connect.',
      'Enable',
      'Ignore'
    );
    if (enable !== 'Enable') {
      return;
    }
    try {
      await remoteCfg.update('useLocalServer', true, vscode.ConfigurationTarget.Global);
    } catch (error) {
      void vscode.window.showWarningMessage(
        `Failed to update user settings with remote.SSH.useLocalServer: ${formatError(error)}`
      );
    }
    return;
  }

  if (useLocalServer) {
    return;
  }

  const enable = await vscode.window.showWarningMessage(
    'Remote.SSH: Use Local Server is disabled. This is required for Slurm Connect.',
    'Enable',
    'Ignore'
  );
  if (enable !== 'Enable') {
    return;
  }

  try {
    await remoteCfg.update('useLocalServer', true, vscode.ConfigurationTarget.Global);
  } catch (error) {
    void vscode.window.showWarningMessage(
      `Failed to update user settings with remote.SSH.useLocalServer: ${formatError(error)}`
    );
  }
}

async function ensureRemoteSshPathOnWindows(): Promise<void> {
  if (process.platform !== 'win32') {
    return;
  }
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const configured = String(remoteCfg.get<string>('path') || '').trim();
  if (configured) {
    return;
  }
  const paths = await listSshPathsFromWhere();
  if (paths.length === 0) {
    return;
  }
  const primary = paths[0].toLowerCase();
  const isGitSsh = primary.includes('\\git\\') || primary.includes('/git/');
  if (!isGitSsh) {
    return;
  }
  if (!(await fileExists(WINDOWS_OPENSSH_PATH))) {
    return;
  }
  const choice = await vscode.window.showWarningMessage(
    'Remote-SSH is using Git SSH on Windows, which does not share the OpenSSH agent. Set remote.SSH.path to the Windows OpenSSH client to avoid passphrase prompts?',
    'Set remote.SSH.path',
    'Not Now'
  );
  if (choice !== 'Set remote.SSH.path') {
    return;
  }
  try {
    await remoteCfg.update('path', WINDOWS_OPENSSH_PATH, vscode.ConfigurationTarget.Global);
  } catch (error) {
    void vscode.window.showWarningMessage(
      `Failed to update remote.SSH.path: ${formatError(error)}`
    );
  }
}


async function ensureRemoteSshSettings(cfg: SlurmConnectConfig): Promise<void> {
  await ensureRemoteSshPathOnWindows();
  await ensureSshAgentEnvForCurrentSsh();
  await ensureLocalServerSetting();
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const enableRemoteCommand = remoteCfg.get<boolean>('enableRemoteCommand', false);
  if (!enableRemoteCommand) {
    const enable = await vscode.window.showWarningMessage(
      'Remote.SSH: Enable Remote Command is disabled. This is required for Slurm Connect.',
      'Enable',
      'Ignore'
    );
    if (enable === 'Enable') {
      await remoteCfg.update('enableRemoteCommand', true, vscode.ConfigurationTarget.Global);
    }
  }

  const lockfilesInTmp = remoteCfg.get<boolean>('lockfilesInTmp', false);
  if (!lockfilesInTmp) {
    const enable = await vscode.window.showWarningMessage(
      'Remote.SSH: Lockfiles In Tmp is disabled. This is recommended on shared filesystems.',
      'Enable',
      'Ignore'
    );
    if (enable === 'Enable') {
      await remoteCfg.update('lockfilesInTmp', true, vscode.ConfigurationTarget.Global);
    }
  }

  const tunnelMode = normalizeLocalProxyTunnelMode(cfg.localProxyTunnelMode || 'remoteSsh');
  const useExecServer = remoteCfg.get<boolean>('useExecServer', true);
  if (useExecServer && cfg.localProxyEnabled && tunnelMode === 'remoteSsh') {
    const disable = await vscode.window.showWarningMessage(
      'Remote.SSH: Use Exec Server opens a second SSH connection, which breaks the single-connection local proxy. Disable it?',
      'Disable',
      'Ignore'
    );
    if (disable === 'Disable') {
      await remoteCfg.update('useExecServer', false, vscode.ConfigurationTarget.Global);
    }
  }

  const updatedUseExecServer = remoteCfg.get<boolean>('useExecServer', true);
  if (updatedUseExecServer && cfg.sessionMode === 'persistent') {
    const disable = await vscode.window.showWarningMessage(
      'Remote.SSH: Use Exec Server is enabled. This can prevent reconnecting to persistent Slurm sessions.',
      'Disable',
      'Ignore'
    );
    if (disable === 'Disable') {
      await remoteCfg.update('useExecServer', false, vscode.ConfigurationTarget.Global);
    }
  }
}

async function refreshRemoteSshHosts(): Promise<void> {
  const commands = [
    'opensshremotes.refresh',
    'opensshremotes.refreshExplorer',
    'remote-ssh.refresh'
  ];
  for (const command of commands) {
    try {
      await vscode.commands.executeCommand(command);
      return;
    } catch {
      // ignore
    }
  }
}

async function connectToHost(
  alias: string,
  openInNewWindow: boolean,
  remoteWorkspacePath?: string
): Promise<boolean> {
  const remoteExtension = vscode.extensions.getExtension('ms-vscode-remote.remote-ssh');
  if (!remoteExtension) {
    return false;
  }
  const log = getOutputChannel();
  await ensureSshAgentEnvForCurrentSsh();
  if (process.platform === 'win32') {
    const sshPath = await resolveSshToolPath('ssh');
    if (isGitSshPath(sshPath) && !process.env.SSH_AUTH_SOCK) {
      log.appendLine('Git SSH agent socket is not set; Remote-SSH may prompt for a passphrase.');
    }
  }
  await remoteExtension.activate();
  const availableCommands = await vscode.commands.getCommands(true);
  const sshCommands = availableCommands.filter((command) => /ssh|openssh/i.test(command));

  const trimmedPath = remoteWorkspacePath?.trim();
  if (trimmedPath) {
    const normalizedPath = trimmedPath.startsWith('/') ? trimmedPath : `/${trimmedPath}`;
    try {
      const remoteUri = vscode.Uri.from({
        scheme: 'vscode-remote',
        authority: `ssh-remote+${encodeURIComponent(alias)}`,
        path: normalizedPath
      });
      log.appendLine(`Opening remote folder: ${remoteUri.toString()}`);
      await vscode.commands.executeCommand('vscode.openFolder', remoteUri, openInNewWindow);
      return true;
    } catch (error) {
      log.appendLine(`Failed to open folder via vscode.openFolder: ${formatError(error)}`);
      // Fall through to connect without a folder.
    }
  }

  const hostArg = { host: alias };

  if (openInNewWindow) {
    const commandCandidates: Array<[string, unknown]> = [
      ['opensshremotes.openEmptyWindow', hostArg],
      ['remote-ssh.connectToHost', alias],
      ['opensshremotes.connectToHost', hostArg],
      ['remote-ssh.openEmptyWindow', alias],
      ['remote-ssh.openEmptyWindowInCurrentWindow', alias]
    ];
    for (const [command, args] of commandCandidates) {
      try {
        log.appendLine(`Trying command: ${command}`);
        await vscode.commands.executeCommand(command, args);
        log.appendLine(`Command succeeded: ${command}`);
        return true;
      } catch (error) {
        log.appendLine(`Command failed: ${command} -> ${formatError(error)}`);
        // try next command
      }
    }
    log.appendLine(`Available SSH commands: ${sshCommands.join(', ') || '(none)'}`);
    return false;
  }

  try {
    const commandCandidates: Array<[string, unknown]> = [
      ['opensshremotes.openEmptyWindowInCurrentWindow', hostArg],
      ['opensshremotes.openEmptyWindow', hostArg],
      ['remote-ssh.connectToHost', alias],
      ['opensshremotes.connectToHost', hostArg],
      ['remote-ssh.openEmptyWindowInCurrentWindow', alias],
      ['remote-ssh.openEmptyWindow', alias]
    ];
    for (const [command, args] of commandCandidates) {
      try {
        log.appendLine(`Trying command: ${command}`);
        await vscode.commands.executeCommand(command, args);
        log.appendLine(`Command succeeded: ${command}`);
        return true;
      } catch (error) {
        log.appendLine(`Command failed: ${command} -> ${formatError(error)}`);
        // try next command
      }
    }
    log.appendLine(`Available SSH commands: ${sshCommands.join(', ') || '(none)'}`);
    return false;
  } finally {
    // No-op
  }
}

async function disconnectFromHost(alias?: string): Promise<boolean> {
  const remoteExtension = vscode.extensions.getExtension('ms-vscode-remote.remote-ssh');
  if (!remoteExtension) {
    return false;
  }
  const log = getOutputChannel();
  await remoteExtension.activate();
  const availableCommands = await vscode.commands.getCommands(true);
  const sshCommands = availableCommands.filter((command) => /ssh|openssh|remote\.close/i.test(command));
  const finalizeDisconnect = (): boolean => {
    stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: true });
    return true;
  };

  if (vscode.env.remoteName === 'ssh-remote') {
    const directCommands = [
      'workbench.action.remote.close',
      'workbench.action.remote.closeRemoteConnection'
    ];
    for (const command of directCommands) {
      if (!availableCommands.includes(command)) {
        continue;
      }
      try {
        log.appendLine(`Trying disconnect command: ${command}`);
        await vscode.commands.executeCommand(command);
        log.appendLine(`Disconnect command succeeded: ${command}`);
        return finalizeDisconnect();
      } catch (error) {
        log.appendLine(`Disconnect command failed: ${command} -> ${formatError(error)}`);
      }
    }
  }

  const candidates: Array<[string, unknown]> = [];
  if (alias) {
    candidates.push(
      ['opensshremotes.closeRemote', { host: alias }],
      ['opensshremotes.closeRemote', alias],
      ['opensshremotes.disconnect', { host: alias }],
      ['opensshremotes.disconnect', alias],
      ['opensshremotes.close', { host: alias }],
      ['opensshremotes.close', alias],
      ['remote-ssh.disconnectFromHost', alias],
      ['remote-ssh.disconnectFromHost', { host: alias }]
    );
  }
  candidates.push(
    ['opensshremotes.closeRemote', undefined],
    ['opensshremotes.disconnect', undefined],
    ['opensshremotes.close', undefined],
    ['remote-ssh.closeRemote', undefined],
    ['remote-ssh.closeRemoteConnection', undefined],
    ['remote-ssh.disconnect', undefined],
    ['workbench.action.remote.close', undefined],
    ['workbench.action.remote.closeRemoteConnection', undefined]
  );

  for (const [command, args] of candidates) {
    if (!availableCommands.includes(command)) {
      continue;
    }
    try {
      log.appendLine(`Trying disconnect command: ${command}`);
      await vscode.commands.executeCommand(command, args);
      log.appendLine(`Disconnect command succeeded: ${command}`);
      return finalizeDisconnect();
    } catch (error) {
      log.appendLine(`Disconnect command failed: ${command} -> ${formatError(error)}`);
    }
  }
  log.appendLine(`Available SSH commands: ${sshCommands.join(', ') || '(none)'}`);
  return false;
}

function hasCachedUiValue(cache: ClusterUiCache | undefined, key: keyof UiValues): boolean {
  return Boolean(cache && Object.prototype.hasOwnProperty.call(cache, key));
}

function getCachedUiValue<T extends keyof UiValues>(
  cache: ClusterUiCache | undefined,
  key: T
): UiValues[T] | undefined {
  if (!hasCachedUiValue(cache, key)) {
    return undefined;
  }
  return cache?.[key];
}

function getUiValuesFromStorage(): UiValues {
  return getUiValuesFromConfig(getConfigFromSettings(), getMergedClusterUiCache());
}

function getUiGlobalDefaults(): UiValues {
  return getUiValuesFromConfig(getConfigFromSettings());
}

function getUiDefaultsFromConfig(): UiValues {
  return getUiValuesFromConfig(getConfigDefaultsFromSettings());
}

function getUiValuesFromConfig(cfg: SlurmConnectConfig, cache?: ClusterUiCache): UiValues {
  const config = vscode.workspace.getConfiguration(SETTINGS_SECTION);
  const hasConfigValue = (key: string): boolean => {
    const inspected = config.inspect(key);
    if (!inspected) {
      return false;
    }
    return (
      inspected.workspaceFolderValue !== undefined ||
      inspected.workspaceValue !== undefined ||
      inspected.globalValue !== undefined
    );
  };
  const hasValue = (key: string, cacheKey?: keyof UiValues): boolean =>
    hasConfigValue(key) || (cacheKey ? hasCachedUiValue(cache, cacheKey) : false);
  const fromCache = <T extends keyof UiValues>(key: T, fallback: UiValues[T]): UiValues[T] => {
    const cached = getCachedUiValue(cache, key);
    return cached !== undefined ? cached : fallback;
  };
  const hasDefaultPartition = hasValue('defaultPartition', 'defaultPartition');
  const hasDefaultNodes = hasValue('defaultNodes', 'defaultNodes');
  const hasDefaultTasksPerNode = hasValue('defaultTasksPerNode', 'defaultTasksPerNode');
  const hasDefaultCpusPerTask = hasValue('defaultCpusPerTask', 'defaultCpusPerTask');
  const hasDefaultTime = hasValue('defaultTime', 'defaultTime');
  const hasDefaultMemoryMb = hasValue('defaultMemoryMb', 'defaultMemoryMb');
  const hasDefaultGpuType = hasValue('defaultGpuType', 'defaultGpuType');
  const hasDefaultGpuCount = hasValue('defaultGpuCount', 'defaultGpuCount');
  return {
    loginHosts: fromCache('loginHosts', cfg.loginHosts.join('\n')),
    loginHostsCommand: fromCache('loginHostsCommand', cfg.loginHostsCommand || ''),
    loginHostsQueryHost: fromCache('loginHostsQueryHost', cfg.loginHostsQueryHost || ''),
    partitionCommand: fromCache('partitionCommand', cfg.partitionCommand || ''),
    partitionInfoCommand: fromCache('partitionInfoCommand', cfg.partitionInfoCommand || ''),
    filterFreeResources: fromCache('filterFreeResources', cfg.filterFreeResources),
    qosCommand: fromCache('qosCommand', cfg.qosCommand || ''),
    accountCommand: fromCache('accountCommand', cfg.accountCommand || ''),
    user: fromCache('user', cfg.user || ''),
    identityFile: fromCache('identityFile', cfg.identityFile || ''),
    preSshCommand: fromCache('preSshCommand', cfg.preSshCommand || ''),
    preSshCheckCommand: fromCache('preSshCheckCommand', cfg.preSshCheckCommand || ''),
    autoInstallProxyScriptOnClusterInfo: cfg.autoInstallProxyScriptOnClusterInfo,
    additionalSshOptions: fromCache('additionalSshOptions', formatAdditionalSshOptions(cfg.additionalSshOptions)),
    moduleLoad: fromCache('moduleLoad', cfg.moduleLoad || ''),
    moduleSelections: getCachedUiValue(cache, 'moduleSelections'),
    moduleCustomCommand: getCachedUiValue(cache, 'moduleCustomCommand'),
    proxyCommand: fromCache('proxyCommand', cfg.proxyCommand || ''),
    proxyArgs: fromCache('proxyArgs', cfg.proxyArgs.join('\n')),
    localProxyEnabled: cfg.localProxyEnabled,
    localProxyNoProxy: cfg.localProxyNoProxy.join('\n'),
    localProxyPort: String(cfg.localProxyPort ?? 0),
    localProxyRemoteBind: cfg.localProxyRemoteBind || '',
    localProxyRemoteHost: cfg.localProxyRemoteHost || '',
    localProxyComputeTunnel: cfg.localProxyComputeTunnel,
    extraSallocArgs: fromCache('extraSallocArgs', cfg.extraSallocArgs.join('\n')),
    promptForExtraSallocArgs: fromCache('promptForExtraSallocArgs', cfg.promptForExtraSallocArgs),
    sessionMode: fromCache('sessionMode', cfg.sessionMode || 'persistent'),
    sessionKey: fromCache('sessionKey', cfg.sessionKey || ''),
    sessionIdleTimeoutSeconds: fromCache(
      'sessionIdleTimeoutSeconds',
      String(cfg.sessionIdleTimeoutSeconds ?? 600)
    ),
    sessionStateDir: cfg.sessionStateDir || '',
    defaultPartition: hasDefaultPartition ? fromCache('defaultPartition', cfg.defaultPartition || '') : '',
    defaultNodes: hasDefaultNodes ? fromCache('defaultNodes', String(cfg.defaultNodes || '')) : '',
    defaultTasksPerNode: hasDefaultTasksPerNode
      ? fromCache('defaultTasksPerNode', String(cfg.defaultTasksPerNode || ''))
      : '',
    defaultCpusPerTask: hasDefaultCpusPerTask
      ? fromCache('defaultCpusPerTask', String(cfg.defaultCpusPerTask || ''))
      : '',
    defaultTime: hasDefaultTime ? fromCache('defaultTime', cfg.defaultTime || '') : '24:00:00',
    defaultMemoryMb: hasDefaultMemoryMb ? fromCache('defaultMemoryMb', String(cfg.defaultMemoryMb || '')) : '',
    defaultGpuType: hasDefaultGpuType ? fromCache('defaultGpuType', cfg.defaultGpuType || '') : '',
    defaultGpuCount: hasDefaultGpuCount ? fromCache('defaultGpuCount', String(cfg.defaultGpuCount || '')) : '',
    sshHostPrefix: cfg.sshHostPrefix || '',
    forwardAgent: fromCache('forwardAgent', cfg.forwardAgent),
    requestTTY: fromCache('requestTTY', cfg.requestTTY),
    openInNewWindow: fromCache('openInNewWindow', cfg.openInNewWindow),
    remoteWorkspacePath: fromCache('remoteWorkspacePath', cfg.remoteWorkspacePath || ''),
    temporarySshConfigPath: cfg.temporarySshConfigPath || '',
    sshQueryConfigPath: cfg.sshQueryConfigPath || ''
  };
}

async function updateConfigFromUi(values: Partial<UiValues>, target: vscode.ConfigurationTarget): Promise<void> {
  await updateClusterUiCache(pickProfileValues(values), target);
  await updateLocalProxySettingsFromUi(values, target);
}

async function updateLocalProxySettingsFromUi(
  values: Partial<UiValues>,
  target: vscode.ConfigurationTarget
): Promise<void> {
  const has = (key: keyof UiValues): boolean => Object.prototype.hasOwnProperty.call(values, key);
  const cfg = vscode.workspace.getConfiguration(SETTINGS_SECTION);
  const updates: Array<Thenable<void>> = [];
  if (has('localProxyEnabled')) {
    updates.push(cfg.update('localProxyEnabled', Boolean(values.localProxyEnabled), target));
  }
  if (has('localProxyNoProxy')) {
    const list = parseListInput(String(values.localProxyNoProxy ?? ''));
    updates.push(cfg.update('localProxyNoProxy', list, target));
  }
  if (has('localProxyPort')) {
    const raw = String(values.localProxyPort ?? '').trim();
    const parsed = raw ? Number(raw) : 0;
    const port = normalizeProxyPort(parsed);
    updates.push(cfg.update('localProxyPort', port, target));
  }
  if (has('localProxyRemoteBind')) {
    updates.push(cfg.update('localProxyRemoteBind', String(values.localProxyRemoteBind ?? '').trim(), target));
  }
  if (has('localProxyRemoteHost')) {
    updates.push(cfg.update('localProxyRemoteHost', String(values.localProxyRemoteHost ?? '').trim(), target));
  }
  if (has('localProxyComputeTunnel')) {
    updates.push(cfg.update('localProxyComputeTunnel', Boolean(values.localProxyComputeTunnel), target));
  }
  if (updates.length === 0) {
    return;
  }
  try {
    await Promise.all(updates);
  } catch (error) {
    getOutputChannel().appendLine(`Failed to update local proxy settings: ${formatError(error)}`);
  }
}

function buildClusterOverridesFromUi(values: ClusterUiCache): Partial<SlurmConnectConfig> {
  const overrides: Partial<SlurmConnectConfig> = {};
  const has = (key: keyof ClusterUiCache): boolean => Object.prototype.hasOwnProperty.call(values, key);

  if (has('loginHosts')) overrides.loginHosts = parseListInput(String(values.loginHosts ?? ''));
  if (has('loginHostsCommand')) overrides.loginHostsCommand = String(values.loginHostsCommand ?? '').trim();
  if (has('loginHostsQueryHost')) overrides.loginHostsQueryHost = String(values.loginHostsQueryHost ?? '').trim();
  if (has('partitionCommand')) overrides.partitionCommand = String(values.partitionCommand ?? '').trim();
  if (has('partitionInfoCommand')) overrides.partitionInfoCommand = String(values.partitionInfoCommand ?? '').trim();
  if (has('filterFreeResources')) overrides.filterFreeResources = Boolean(values.filterFreeResources);
  if (has('qosCommand')) overrides.qosCommand = String(values.qosCommand ?? '').trim();
  if (has('accountCommand')) overrides.accountCommand = String(values.accountCommand ?? '').trim();
  if (has('user')) overrides.user = String(values.user ?? '').trim();
  if (has('identityFile')) overrides.identityFile = String(values.identityFile ?? '').trim();
  if (has('preSshCommand')) overrides.preSshCommand = String(values.preSshCommand ?? '').trim();
  if (has('preSshCheckCommand')) overrides.preSshCheckCommand = String(values.preSshCheckCommand ?? '').trim();
  if (has('additionalSshOptions')) {
    overrides.additionalSshOptions = parseAdditionalSshOptionsInput(String(values.additionalSshOptions ?? ''));
  }
  if (has('moduleLoad')) {
    overrides.moduleLoad = String(values.moduleLoad ?? '').trim();
  } else {
    const selections = normalizeModuleSelections(values.moduleSelections);
    const custom = normalizeModuleCustomCommand(values.moduleCustomCommand);
    if (custom) {
      overrides.moduleLoad = custom;
    } else if (selections.length > 0) {
      overrides.moduleLoad = `module load ${selections.join(' ')}`;
    }
  }
  if (has('extraSallocArgs')) overrides.extraSallocArgs = splitShellArgs(String(values.extraSallocArgs ?? ''));
  if (has('promptForExtraSallocArgs')) {
    overrides.promptForExtraSallocArgs = Boolean(values.promptForExtraSallocArgs);
  }
  if (has('sessionMode')) overrides.sessionMode = normalizeSessionMode(String(values.sessionMode ?? ''));
  if (has('sessionKey')) overrides.sessionKey = String(values.sessionKey ?? '').trim();
  if (has('sessionIdleTimeoutSeconds')) {
    overrides.sessionIdleTimeoutSeconds = parseNonNegativeNumberInput(String(values.sessionIdleTimeoutSeconds ?? ''));
  }
  if (has('defaultPartition')) overrides.defaultPartition = String(values.defaultPartition ?? '').trim();
  if (has('defaultNodes')) overrides.defaultNodes = parsePositiveNumberInput(String(values.defaultNodes ?? ''));
  if (has('defaultTasksPerNode')) {
    overrides.defaultTasksPerNode = parsePositiveNumberInput(String(values.defaultTasksPerNode ?? ''));
  }
  if (has('defaultCpusPerTask')) {
    overrides.defaultCpusPerTask = parsePositiveNumberInput(String(values.defaultCpusPerTask ?? ''));
  }
  if (has('defaultTime')) overrides.defaultTime = String(values.defaultTime ?? '').trim();
  if (has('defaultMemoryMb')) overrides.defaultMemoryMb = parseNonNegativeNumberInput(String(values.defaultMemoryMb ?? ''));
  if (has('defaultGpuType')) overrides.defaultGpuType = String(values.defaultGpuType ?? '').trim();
  if (has('defaultGpuCount')) overrides.defaultGpuCount = parseNonNegativeNumberInput(String(values.defaultGpuCount ?? ''));
  if (has('forwardAgent')) overrides.forwardAgent = Boolean(values.forwardAgent);
  if (has('requestTTY')) overrides.requestTTY = Boolean(values.requestTTY);
  if (has('openInNewWindow')) overrides.openInNewWindow = Boolean(values.openInNewWindow);
  if (has('remoteWorkspacePath')) overrides.remoteWorkspacePath = String(values.remoteWorkspacePath ?? '').trim();

  return overrides;
}

function uniqueList(values: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const value of values) {
    if (!seen.has(value)) {
      seen.add(value);
      result.push(value);
    }
  }
  return result;
}

function splitArgs(input: string): string[] {
  return splitShellArgs(input);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function redactRemoteCommandForLog(entry: string): string {
  const trimmed = entry.replace(/\r?\n$/, '');
  const lines = trimmed.split(/\r?\n/).map((line) => {
    const match = /^(\s*RemoteCommand\s+).+$/.exec(line);
    if (!match) {
      return line;
    }
    const command = line.slice(match[1].length);
    const redactedBase64 = command.replace(
      /echo\s+([A-Za-z0-9+/=]{20,})\s+\|\s*base64\s+-d/g,
      (_full, b64) => {
        if (b64.length <= 25) {
          return `echo ${b64} | base64 -d`;
        }
        const head = b64.slice(0, 10);
        const tail = b64.slice(-10);
        return `echo ${head}...${tail} | base64 -d`;
      }
    );
    const redactedProxyArgs = redactedBase64.replace(
      /--local-proxy-auth-token=([^\s]+)/g,
      '--local-proxy-auth-token=***'
    );
    const redacted = redactedProxyArgs.replace(/:\/\/([^:@\s]+):([^@/\s]+)@/g, '://$1:***@');
    return `${match[1]}${redacted}`;
  });
  return `${lines.join('\n')}\n`;
}

function formatAdditionalSshOptions(options: Record<string, string> | undefined): string {
  if (!options || typeof options !== 'object') {
    return '';
  }
  const lines: string[] = [];
  for (const [key, rawValue] of Object.entries(options)) {
    const name = String(key || '').trim();
    const value = String(rawValue ?? '').trim();
    if (!name || !value) {
      continue;
    }
    lines.push(`${name} ${value}`);
  }
  return lines.join('\n');
}

function parseAdditionalSshOptionsInput(input: string): Record<string, string> {
  const result: Record<string, string> = {};
  const lines = String(input || '').split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }
    const match = /^(\S+)\s+(.+)$/.exec(trimmed);
    if (!match) {
      continue;
    }
    const key = match[1];
    const value = match[2].trim();
    if (!value) {
      continue;
    }
    result[key] = value;
  }
  return result;
}

interface BundledProxyScript {
  base64: string;
  sha256: string;
}

interface ProxyInstallPlan extends BundledProxyScript {
  installPath: string;
}

let bundledProxyScriptCache: BundledProxyScript | undefined;

async function getBundledProxyScript(): Promise<BundledProxyScript | undefined> {
  if (bundledProxyScriptCache) {
    return bundledProxyScriptCache;
  }
  if (!extensionRootPath) {
    return undefined;
  }
  const filePath = path.join(extensionRootPath, 'media', 'vscode-proxy.py');
  try {
    const data = await fs.readFile(filePath);
    const sha256 = crypto.createHash('sha256').update(data).digest('hex');
    const compressed = zlib.gzipSync(data, { level: 9 });
    const base64 = compressed.toString('base64');
    bundledProxyScriptCache = { base64, sha256 };
    return bundledProxyScriptCache;
  } catch (error) {
    getOutputChannel().appendLine(`Failed to read bundled proxy script: ${formatError(error)}`);
    return undefined;
  }
}

function normalizeShellHomePath(value: string): string {
  const trimmed = value.trim();
  if (trimmed === '~') {
    return '$HOME';
  }
  if (trimmed.startsWith('~/')) {
    return `$HOME/${trimmed.slice(2)}`;
  }
  return trimmed;
}

function buildProxyInstallSnippet(plan: ProxyInstallPlan): string {
  const target = normalizeShellHomePath(plan.installPath);
  const targetEscaped = target.replace(/"/g, '\\"');
  const expected = plan.sha256;
  const chunks: string[] = [];
  const chunkSize = 1024;
  for (let i = 0; i < plan.base64.length; i += chunkSize) {
    chunks.push(plan.base64.slice(i, i + chunkSize));
  }
  const hashSnippet = [
    'import hashlib,sys',
    'path=sys.argv[1]',
    'try:',
    '    data=open(path,"rb").read()',
    'except Exception:',
    '    sys.exit(1)',
    'print(hashlib.sha256(data).hexdigest())'
  ].join('\n');
  const installSnippet = [
    `target="${targetEscaped}"`,
    `expected="${expected}"`,
    'PYTHON=$(command -v python3 || command -v python || true)',
    'if [ -z "$PYTHON" ]; then',
    '  echo "Slurm Connect: python not found; cannot install proxy." >&2',
    '  exit 1',
    'fi',
    'current=""',
    'if [ -f "$target" ]; then',
    '  current=$("$PYTHON" - "$target" 2>/dev/null <<\'PY\'',
    hashSnippet,
    'PY',
    '  )',
    'fi',
    'if [ "$current" != "$expected" ]; then',
    '  umask 077',
    '  mkdir -p "$(dirname "$target")"',
    '  "$PYTHON" - "$target" <<\'PY\'',
    'import base64,gzip,io,os,sys',
    'path=sys.argv[1]',
    'data=base64.b64decode("".join([',
    ...chunks.map((chunk) => `    "${chunk}",`),
    ']))',
    'try:',
    '    data=gzip.decompress(data)',
    'except AttributeError:',
    '    data=gzip.GzipFile(fileobj=io.BytesIO(data)).read()',
    'tmp=path + ".tmp"',
    'with open(tmp,"wb") as f:',
    '    f.write(data)',
    'os.chmod(tmp,0o700)',
    'os.replace(tmp,path)',
    'PY',
    '  current=$("$PYTHON" - "$target" 2>/dev/null <<\'PY\'',
    hashSnippet,
    'PY',
    '  )',
    '  if [ "$current" != "$expected" ]; then',
    '    echo "Slurm Connect: proxy script hash mismatch after install" >&2',
    '  fi',
    'fi'
  ];
  return installSnippet.join('\n');
}

async function getProxyInstallSnippet(cfg: SlurmConnectConfig): Promise<string | undefined> {
  if (!cfg.autoInstallProxyScriptOnClusterInfo) {
    return undefined;
  }
  const bundled = await getBundledProxyScript();
  if (!bundled) {
    return undefined;
  }
  return buildProxyInstallSnippet({
    ...bundled,
    installPath: DEFAULT_PROXY_SCRIPT_INSTALL_PATH
  });
}

async function waitForRemoteConnectionOrTimeout(timeoutMs: number, pollMs: number): Promise<boolean> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (vscode.env.remoteName === 'ssh-remote') {
      return true;
    }
    await delay(pollMs);
  }
  return vscode.env.remoteName === 'ssh-remote';
}

function parseListInput(input: string): string[] {
  return input
    .split(/[\s,]+/)
    .map((value) => value.trim())
    .filter(Boolean);
}

function firstLoginHostFromInput(input: string): string | undefined {
  const hosts = parseListInput(input);
  return hosts.length > 0 ? hosts[0] : undefined;
}

function getCachedClusterInfo(host: string): { info: ClusterInfo; fetchedAt: string } | undefined {
  if (!extensionGlobalState) {
    return undefined;
  }
  const cache = extensionGlobalState.get<Record<string, { info: ClusterInfo; fetchedAt: string }>>(
    CLUSTER_INFO_CACHE_KEY
  );
  return cache ? cache[host] : undefined;
}

function cacheClusterInfo(host: string, info: ClusterInfo): void {
  if (!extensionGlobalState) {
    return;
  }
  const cache =
    extensionGlobalState.get<Record<string, { info: ClusterInfo; fetchedAt: string }>>(
      CLUSTER_INFO_CACHE_KEY
    ) || {};
  cache[host] = { info, fetchedAt: new Date().toISOString() };
  void extensionGlobalState.update(CLUSTER_INFO_CACHE_KEY, cache);
}

function parsePositiveNumberInput(input: string): number {
  const trimmed = input.trim();
  if (!trimmed) {
    return 0;
  }
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return 0;
  }
  return Math.floor(parsed);
}

function parseNonNegativeNumberInput(input: string): number {
  const trimmed = input.trim();
  if (!trimmed) {
    return 0;
  }
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }
  return Math.floor(parsed);
}

function normalizeNonNegativeInteger(value: number): number {
  if (!Number.isFinite(value) || value < 0) {
    return 0;
  }
  return Math.floor(value);
}

function normalizeSessionMode(value: string): SessionMode {
  return value === 'persistent' ? 'persistent' : 'ephemeral';
}

function normalizeLocalProxyTunnelMode(value: string): LocalProxyTunnelMode {
  return value === 'dedicated' ? 'dedicated' : 'remoteSsh';
}

function normalizeSshHostKeyChecking(value: string): SshHostKeyCheckingMode {
  const normalized = value.trim().toLowerCase();
  if (normalized === 'ask' || normalized === 'yes' || normalized === 'no') {
    return normalized;
  }
  if (normalized === 'accept-new' || normalized === 'acceptnew' || normalized === 'new') {
    return 'accept-new';
  }
  return 'accept-new';
}

function buildOverridesFromUi(values: Partial<UiValues>): Partial<SlurmConnectConfig> {
  const moduleLoad = String(values.moduleLoad ?? '').trim();
  const moduleCustom = normalizeModuleCustomCommand(values.moduleCustomCommand);
  const moduleSelections = normalizeModuleSelections(values.moduleSelections);
  const resolvedModuleLoad =
    moduleLoad || moduleCustom || (moduleSelections.length > 0 ? `module load ${moduleSelections.join(' ')}` : '');
  return {
    loginHosts: parseListInput(String(values.loginHosts ?? '')),
    loginHostsCommand: String(values.loginHostsCommand ?? '').trim(),
    loginHostsQueryHost: String(values.loginHostsQueryHost ?? '').trim(),
    partitionCommand: String(values.partitionCommand ?? '').trim(),
    partitionInfoCommand: String(values.partitionInfoCommand ?? '').trim(),
    filterFreeResources: Boolean(values.filterFreeResources),
    qosCommand: String(values.qosCommand ?? '').trim(),
    accountCommand: String(values.accountCommand ?? '').trim(),
    user: String(values.user ?? '').trim(),
    identityFile: String(values.identityFile ?? '').trim(),
    preSshCommand: String(values.preSshCommand ?? '').trim(),
    preSshCheckCommand: String(values.preSshCheckCommand ?? '').trim(),
    additionalSshOptions: parseAdditionalSshOptionsInput(String(values.additionalSshOptions ?? '')),
    moduleLoad: resolvedModuleLoad,
    extraSallocArgs: splitShellArgs(String(values.extraSallocArgs ?? '')),
    promptForExtraSallocArgs: Boolean(values.promptForExtraSallocArgs),
    sessionMode: normalizeSessionMode(String(values.sessionMode ?? '')),
    sessionKey: String(values.sessionKey ?? '').trim(),
    sessionIdleTimeoutSeconds: parseNonNegativeNumberInput(String(values.sessionIdleTimeoutSeconds ?? '')),
    defaultPartition: String(values.defaultPartition ?? '').trim(),
    defaultNodes: parsePositiveNumberInput(String(values.defaultNodes ?? '')),
    defaultTasksPerNode: parsePositiveNumberInput(String(values.defaultTasksPerNode ?? '')),
    defaultCpusPerTask: parsePositiveNumberInput(String(values.defaultCpusPerTask ?? '')),
    defaultTime: String(values.defaultTime ?? '').trim(),
    defaultMemoryMb: parseNonNegativeNumberInput(String(values.defaultMemoryMb ?? '')),
    defaultGpuType: String(values.defaultGpuType ?? '').trim(),
    defaultGpuCount: parseNonNegativeNumberInput(String(values.defaultGpuCount ?? '')),
    forwardAgent: Boolean(values.forwardAgent),
    requestTTY: Boolean(values.requestTTY),
    openInNewWindow: Boolean(values.openInNewWindow),
    remoteWorkspacePath: String(values.remoteWorkspacePath ?? '').trim()
  };
}

function getWebviewHtml(webview: vscode.Webview): string {
  const nonce = String(Date.now());
  const csp = [
    "default-src 'none'",
    `style-src ${webview.cspSource} 'unsafe-inline'`,
    `script-src 'nonce-${nonce}'`
  ].join('; ');

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy" content="${csp}">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Slurm Connect</title>
  <style>
    :root { color-scheme: light dark; }
    body {
      font-family: var(--vscode-font-family, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif);
      padding: 12px 12px 12px;
      color: var(--vscode-foreground);
      background: var(--vscode-editor-background);
    }
    h2 { font-size: 16px; margin: 0 0 12px 0; }
    .section { margin-bottom: 16px; }
    label { font-size: 12px; display: block; margin-bottom: 4px; color: var(--vscode-foreground); }
    input, textarea, select, button { width: 100%; box-sizing: border-box; font-size: 12px; padding: 6px; }
    input:not([type="checkbox"]), textarea, select {
      background: var(--vscode-input-background, var(--vscode-editorWidget-background));
      color: var(--vscode-input-foreground, var(--vscode-foreground));
      border: 1px solid var(--vscode-input-border, var(--vscode-widget-border));
      box-shadow: 0 0 0 1px var(--vscode-input-border, var(--vscode-widget-border)) inset;
    }
    input:disabled, textarea:disabled, select:disabled {
      opacity: 0.6;
      cursor: not-allowed;
      color: var(--vscode-disabledForeground, var(--vscode-descriptionForeground));
    }
    input:not([type="checkbox"]):focus, textarea:focus, select:focus {
      outline: 1px solid var(--vscode-focusBorder);
      outline-offset: 0;
    }
    button {
      background: var(--vscode-button-background);
      color: var(--vscode-button-foreground);
      border: 1px solid var(--vscode-button-background);
      cursor: pointer;
    }
    button:hover { background: var(--vscode-button-hoverBackground); }
    button:disabled { opacity: 0.6; cursor: not-allowed; }
    textarea { resize: vertical; min-height: 48px; }
    .row { display: flex; gap: 8px; }
    .row > div { flex: 1; }
    .checkbox { display: flex; align-items: center; gap: 6px; }
    input[type="checkbox"] {
      appearance: none;
      -webkit-appearance: none;
      width: 14px;
      height: 14px;
      padding: 0;
      margin: 0;
      border-radius: 3px;
      border: 1px solid var(--vscode-checkbox-border, var(--vscode-input-border, #888));
      background: var(--vscode-checkbox-background, var(--vscode-input-background, #fff));
      display: inline-grid;
      place-content: center;
      cursor: pointer;
    }
    input[type="checkbox"]:focus {
      outline: 1px solid var(--vscode-focusBorder);
      outline-offset: 1px;
    }
    input[type="checkbox"]:checked {
      background: var(--vscode-checkbox-background, var(--vscode-button-background, #0e639c));
      border-color: var(--vscode-checkbox-border, var(--vscode-button-background, #0e639c));
    }
    input[type="checkbox"]:checked::after {
      content: '';
      width: 4px;
      height: 8px;
      border: solid var(--vscode-checkbox-foreground, var(--vscode-button-foreground, #fff));
      border-width: 0 2px 2px 0;
      transform: rotate(45deg);
    }
    .buttons { display: flex; gap: 8px; }
    .buttons button { flex: 1; }
    .cluster-actions { align-items: center; }
    .cluster-actions button { flex: 0 0 auto; }
    .hidden { display: none !important; }
    .hint { font-size: 11px; color: var(--vscode-descriptionForeground); margin-top: 4px; }
    .hint:empty { display: none; }
    .field-hint { margin-top: 2px; }
    .warning {
      color: var(--vscode-editorWarning-foreground, #b58900);
    }
    .summary-row {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
    }
    .summary-item {
      min-width: 120px;
      display: flex;
      flex-direction: column;
      gap: 2px;
    }
    .summary-label {
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--vscode-descriptionForeground);
    }
    .summary-value { font-size: 12px; font-weight: 600; }
    .summary-totals { margin-top: 6px; font-size: 11px; color: var(--vscode-descriptionForeground); }
    .profile-details {
      border: 1px solid var(--vscode-input-border);
      border-radius: 6px;
      padding: 8px;
      background: var(--vscode-input-background);
    }
    .profile-section-title {
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      color: var(--vscode-descriptionForeground);
      margin-bottom: 6px;
    }
    .profile-divider {
      height: 1px;
      background: var(--vscode-input-border);
      margin: 8px 0;
    }
    .profile-overrides { margin-top: 8px; }
    .profile-overrides summary {
      font-size: 11px;
      color: var(--vscode-descriptionForeground);
      cursor: pointer;
      list-style: none;
    }
    .profile-overrides summary::-webkit-details-marker { display: none; }
    .profile-overrides summary::before {
      content: '▸';
      display: inline-block;
      margin-right: 6px;
      transition: transform 0.15s ease;
    }
    .profile-overrides[open] summary::before { transform: rotate(90deg); }
    .override-list {
      display: flex;
      flex-direction: column;
      gap: 6px;
      margin-top: 6px;
    }
    .override-row {
      display: grid;
      grid-template-columns: 120px 1fr;
      gap: 8px;
      font-size: 11px;
    }
    .override-key { color: var(--vscode-descriptionForeground); }
    .override-value { color: var(--vscode-foreground); word-break: break-word; }
    .action-bar {
      position: fixed;
      left: 0;
      right: 0;
      bottom: 0;
      z-index: 10;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 10px 12px;
      border-top: 1px solid var(--vscode-panel-border, var(--vscode-input-border));
      background: var(--vscode-sideBar-background, var(--vscode-editor-background));
      box-sizing: border-box;
    }
    .action-main { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
    .action-options { display: flex; align-items: center; gap: 6px; }
    .action-options label { margin: 0; font-size: 11px; }
    .action-status { display: flex; flex-direction: column; gap: 2px; }
    .input-with-button { display: flex; gap: 6px; align-items: center; }
    .input-with-button input { flex: 1; }
    .input-with-button button { width: auto; min-width: 48px; }
    .input-unit { position: relative; }
    .input-unit input { padding-right: 78px; }
    .combo-picker.with-unit { position: relative; }
    .combo-picker.with-unit input { padding-right: 56px; }
    .combo-picker.with-unit::after { right: 34px; }
    .unit-label {
      position: absolute;
      right: 8px;
      top: 50%;
      transform: translateY(-50%);
      font-size: 10px;
      color: var(--vscode-descriptionForeground);
      pointer-events: none;
    }
    .button-secondary {
      background: var(--vscode-button-secondaryBackground, var(--vscode-button-background));
      color: var(--vscode-button-secondaryForeground, var(--vscode-button-foreground));
      border-color: var(--vscode-button-secondaryBackground, var(--vscode-button-background));
    }
    .button-secondary:hover {
      background: var(--vscode-button-secondaryHoverBackground, var(--vscode-button-hoverBackground));
    }
    .spinner {
      width: 14px;
      height: 14px;
      border: 2px solid var(--vscode-descriptionForeground);
      border-right-color: transparent;
      border-radius: 50%;
      display: inline-block;
      animation: spin 0.8s linear infinite;
    }
    .spinner.hidden { display: none; }
    @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
    .module-toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }
    .module-list {
      margin-top: 6px;
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }
    .module-chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 8px;
      border: 1px solid var(--vscode-input-border);
      border-radius: 999px;
      background: var(--vscode-input-background);
      font-size: 11px;
    }
    .module-chip button { width: auto; min-width: 20px; padding: 0 6px; }
    details summary { cursor: pointer; margin-bottom: 6px; color: var(--vscode-foreground); }
    .module-row { align-items: center; }
    .row > .module-actions { flex: 0 0 auto; }
    .row > .combo-picker { flex: 1; }
    .module-actions button { width: auto; min-width: 32px; }
    .combo-picker { position: relative; }
    .combo-picker.disabled {
      opacity: 0.6;
    }
    .combo-picker::after {
      content: '';
      position: absolute;
      right: 8px;
      top: 50%;
      width: 0;
      height: 0;
      border-left: 4px solid transparent;
      border-right: 4px solid transparent;
      border-top: 6px solid var(--vscode-descriptionForeground);
      opacity: 0.7;
      transform: translateY(-50%);
      pointer-events: none;
    }
    .combo-picker.no-arrow::after { display: none; }
    .combo-picker.no-options::after { display: none; }
    .combo-picker input { padding-right: 22px; }
    .combo-picker.no-arrow input { padding-right: 6px; }
    .dropdown-menu {
      position: absolute;
      top: calc(100% + 2px);
      left: 0;
      right: 0;
      max-height: 180px;
      overflow-y: auto;
      background: var(--vscode-input-background);
      border: 1px solid var(--vscode-input-border);
      border-radius: 4px;
      z-index: 20;
      display: none;
    }
    .dropdown-menu.visible { display: block; }
    .dropdown-option {
      padding: 4px 6px;
      cursor: pointer;
    }
    .dropdown-option:hover {
      background: var(--vscode-list-hoverBackground, var(--vscode-button-hoverBackground));
    }
    .dropdown-option.active {
      background: var(--vscode-list-activeSelectionBackground, var(--vscode-button-hoverBackground));
      color: var(--vscode-list-activeSelectionForeground, var(--vscode-foreground));
    }
    .dropdown-option.module-header {
      cursor: default;
      font-size: 11px;
      font-weight: 600;
      color: var(--vscode-descriptionForeground);
      background: transparent;
      border-top: 1px solid var(--vscode-input-border);
      margin-top: 4px;
      padding-top: 6px;
    }
    .dropdown-option.module-header:hover {
      background: transparent;
    }
    .module-item button { width: auto; min-width: 28px; padding: 2px 6px; }
    #moduleSection.disabled {
      opacity: 0.6;
      pointer-events: none;
    }
    .session-list {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .session-option {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 6px 8px;
      border: 1px solid var(--vscode-input-border);
      border-radius: 4px;
      background: var(--vscode-input-background);
      cursor: pointer;
    }
    .session-option input[type="radio"] {
      width: auto;
      margin: 0;
    }
    .session-option.selected {
      border-color: var(--vscode-focusBorder);
      box-shadow: 0 0 0 1px var(--vscode-focusBorder) inset;
    }
  </style>
</head>
<body>
  <h2>Slurm Connect</h2>

  <details class="section" id="connectionSection" open>
    <summary>Connection settings</summary>
    <label for="loginHosts">Login host</label>
    <div class="combo-picker no-arrow" id="loginHostPicker">
      <input id="loginHosts" type="text" placeholder="hostname1.com" />
      <div id="loginHostMenu" class="dropdown-menu"></div>
    </div>
    <div id="sshHostHint" class="hint"></div>
    <label for="user">SSH user</label>
    <input id="user" type="text" />
    <label for="identityFile">Identity file (optional)</label>
    <div class="input-with-button">
      <input id="identityFile" type="text" />
      <button id="pickIdentityFile" type="button">Browse</button>
    </div>
    <div id="agentStatus" class="hint"></div>
    <label for="remoteWorkspacePath">Remote folder to open (optional)</label>
    <input id="remoteWorkspacePath" type="text" placeholder="/home/user/project" />
    <div class="buttons cluster-actions" style="margin-top: 8px;">
      <button id="getClusterInfo" type="button">Get cluster info</button>
      <button id="clearClusterInfo" type="button" class="button-secondary" title="Clear cluster info">Clear</button>
      <span id="clusterInfoSpinner" class="spinner hidden" aria-hidden="true"></span>
    </div>
    <div id="clusterStatus" class="hint"></div>
    <div id="sessionSelector" class="hidden" style="margin-top: 8px;">
      <label>Existing sessions</label>
      <div id="sessionSelectionList" class="session-list"></div>
      <div id="sessionSelectionHint" class="hint"></div>
    </div>
  </details>

  <details class="section" id="resourceSection" open>
    <summary>Resource settings</summary>
    <div class="checkbox" style="margin-top: 6px;">
      <input id="filterFreeResources" type="checkbox" />
      <label for="filterFreeResources">Show only free resources</label>
    </div>
    <div id="freeResourceWarning" class="hint warning"></div>
    <div id="resourceWarning" class="hint warning"></div>

    <label for="defaultPartitionInput">Partition</label>
    <div class="combo-picker" id="defaultPartitionPicker">
      <input id="defaultPartitionInput" type="text" placeholder="Cluster default if blank" />
      <div id="defaultPartitionMenu" class="dropdown-menu"></div>
    </div>
    <div id="partitionMeta" class="hint"></div>
    <div class="row">
      <div>
        <label for="defaultNodesInput">Nodes</label>
        <div class="combo-picker" id="defaultNodesPicker">
          <input id="defaultNodesInput" type="number" min="1" />
          <div id="defaultNodesMenu" class="dropdown-menu"></div>
        </div>
        <div id="defaultNodesHint" class="hint field-hint"></div>
      </div>
      <div>
        <label for="defaultTasksPerNode">Tasks per node</label>
        <input id="defaultTasksPerNode" type="number" min="1" />
        <div id="defaultTasksPerNodeHint" class="hint field-hint"></div>
      </div>
    </div>
    <div class="row">
      <div>
        <label for="defaultCpusPerTaskInput">CPUs per task</label>
        <div class="combo-picker" id="defaultCpusPerTaskPicker">
          <input id="defaultCpusPerTaskInput" type="number" min="1" />
          <div id="defaultCpusPerTaskMenu" class="dropdown-menu"></div>
        </div>
        <div id="defaultCpusPerTaskHint" class="hint field-hint"></div>
      </div>
      <div>
        <label for="defaultMemoryMbInput">Memory per node</label>
        <div class="combo-picker with-unit" id="defaultMemoryMbPicker">
          <input id="defaultMemoryMbInput" type="number" min="0" placeholder="MB (optional)" />
          <span class="unit-label">MB</span>
          <div id="defaultMemoryMbMenu" class="dropdown-menu"></div>
        </div>
        <div id="defaultMemoryMbHint" class="hint field-hint"></div>
      </div>
    </div>
    <div class="row">
      <div>
        <label for="defaultGpuTypeInput">GPU type</label>
        <div class="combo-picker" id="defaultGpuTypePicker">
          <input id="defaultGpuTypeInput" type="text" placeholder="Any" />
          <div id="defaultGpuTypeMenu" class="dropdown-menu"></div>
        </div>
        <div id="defaultGpuTypeHint" class="hint field-hint"></div>
      </div>
      <div>
        <label for="defaultGpuCountInput">GPU count</label>
        <div class="combo-picker" id="defaultGpuCountPicker">
          <input id="defaultGpuCountInput" type="number" min="0" />
          <div id="defaultGpuCountMenu" class="dropdown-menu"></div>
        </div>
        <div id="defaultGpuCountHint" class="hint field-hint"></div>
      </div>
    </div>
    <label for="defaultTime">Wall time</label>
    <div class="input-unit">
      <input id="defaultTime" type="text" placeholder="HH:MM:SS" />
      <span class="unit-label">HH:MM:SS</span>
    </div>
    <div id="defaultTimeHint" class="hint field-hint"></div>
    <div class="buttons" style="margin-top: 8px;">
      <button id="fillPartitionDefaults" type="button">Use partition defaults</button>
      <button id="clearResources" type="button">Clear resources</button>
    </div>

    <div id="moduleSection">
      <div class="module-toolbar">
        <label for="moduleInput">Modules</label>
        <button id="moduleClearAll" type="button" class="button-secondary">Clear all</button>
      </div>
      <div class="row module-row">
        <div class="combo-picker" id="modulePicker">
          <input id="moduleInput" type="text" placeholder="Type module(s) or pick from the list" />
          <div id="moduleMenu" class="dropdown-menu"></div>
        </div>
        <div class="module-actions">
          <button id="moduleAdd" type="button">+</button>
        </div>
      </div>
      <div id="moduleList" class="module-list"></div>
      <div id="moduleHint" class="hint"></div>
      <input id="moduleLoad" type="hidden" />
    </div>
  </details>

  <details class="section" id="profilesSection">
    <summary>Profiles</summary>
    <label for="profileSelect">Saved profiles</label>
    <select id="profileSelect"></select>
    <div id="profileDetails" class="profile-details" style="margin-top: 8px;">
      <div class="profile-section-title">Resources</div>
      <div id="profileSummary" class="summary-row">
        <div class="summary-item">
          <span class="summary-label">Host</span>
          <span id="profileSummaryHost" class="summary-value">—</span>
        </div>
        <div class="summary-item">
          <span class="summary-label">Partition</span>
          <span id="profileSummaryPartition" class="summary-value">—</span>
        </div>
        <div class="summary-item">
          <span class="summary-label">Nodes</span>
          <span id="profileSummaryNodes" class="summary-value">—</span>
        </div>
        <div class="summary-item">
          <span class="summary-label">CPUs</span>
          <span id="profileSummaryCpus" class="summary-value">—</span>
        </div>
        <div class="summary-item">
          <span class="summary-label">GPUs</span>
          <span id="profileSummaryGpus" class="summary-value">—</span>
        </div>
        <div class="summary-item">
          <span class="summary-label">Time</span>
          <span id="profileSummaryTime" class="summary-value">—</span>
        </div>
        <div class="summary-item">
          <span class="summary-label">Memory</span>
          <span id="profileSummaryMemory" class="summary-value">—</span>
        </div>
      </div>
      <div id="profileSummaryTotals" class="summary-totals"></div>
      <details id="profileOverrides" class="profile-overrides hidden">
        <div class="profile-divider"></div>
        <summary id="profileOverridesSummary">Other saved settings</summary>
        <div id="profileOverrideList" class="override-list"></div>
      </details>
    </div>
    <div class="buttons" style="margin-top: 8px;">
      <button id="profileLoad">Load</button>
      <button id="profileSave">Save profile</button>
      <button id="profileDelete">Delete</button>
    </div>
    <div id="profileStatus" class="hint"></div>
  </details>

  <details class="section" id="advancedSection">
    <summary>Advanced settings</summary>
    <label for="loginHostsCommand">Login hosts command (optional)</label>
    <input id="loginHostsCommand" type="text" />
    <label for="loginHostsQueryHost">Login hosts query host (optional)</label>
    <input id="loginHostsQueryHost" type="text" />
    <label for="partitionInfoCommand">Partition info command</label>
    <input id="partitionInfoCommand" type="text" />
    <label for="partitionCommand">Partition list command</label>
    <input id="partitionCommand" type="text" />
    <label for="qosCommand">QoS command (optional)</label>
    <input id="qosCommand" type="text" />
    <label for="accountCommand">Account command (optional)</label>
    <input id="accountCommand" type="text" />
    <label for="extraSallocArgs">Extra salloc args</label>
    <textarea id="extraSallocArgs" rows="2"></textarea>
    <div class="checkbox">
      <input id="promptForExtraSallocArgs" type="checkbox" />
      <label for="promptForExtraSallocArgs">Prompt for extra salloc args</label>
    </div>
    <label for="sessionMode">Session mode</label>
    <select id="sessionMode">
      <option value="ephemeral">Ephemeral (new job per connection)</option>
      <option value="persistent">Persistent (reuse allocation)</option>
    </select>
    <label for="sessionKey">Session key (optional)</label>
    <input id="sessionKey" type="text" placeholder="Defaults to SSH alias" />
    <label for="sessionIdleTimeoutSeconds">Session idle timeout (seconds, 0 = never)</label>
    <input id="sessionIdleTimeoutSeconds" type="number" min="0" placeholder="600" />
    <label for="preSshCommand">Pre-SSH auth command (optional)</label>
    <input id="preSshCommand" type="text" placeholder="" />
    <label for="preSshCheckCommand">Pre-SSH check command (optional)</label>
    <input id="preSshCheckCommand" type="text" placeholder="" />
    <label for="additionalSshOptions">Additional SSH options (one per line)</label>
    <textarea id="additionalSshOptions" rows="3" placeholder=""></textarea>
    <div class="checkbox">
      <input id="localProxyEnabled" type="checkbox" />
      <label for="localProxyEnabled">Route remote HTTP(S) through local proxy</label>
    </div>
    <div class="hint">Advanced local proxy settings live in VS Code Settings.</div>
    <div class="checkbox">
      <input id="forwardAgent" type="checkbox" />
      <label for="forwardAgent">Forward agent</label>
    </div>
    <div class="checkbox">
      <input id="requestTTY" type="checkbox" />
      <label for="requestTTY">Request TTY</label>
    </div>
    <label for="saveTarget">Remember values in</label>
    <select id="saveTarget">
      <option value="global">User settings</option>
      <option value="workspace">Workspace settings</option>
    </select>
    <div class="buttons" style="margin-top: 8px;">
      <button id="resetAdvancedDefaults" type="button" class="button-secondary">Reset advanced to defaults</button>
    </div>
    <div class="buttons" style="margin-top: 8px;">
      <button id="refresh">Reload saved values</button>
      <button id="openSettings">Open Settings</button>
    </div>
    <div class="buttons" style="margin-top: 8px;">
      <button id="openLogs" type="button">Open logs</button>
      <button id="openRemoteSshLog" type="button">Remote-SSH log</button>
    </div>
  </details>

  <div class="action-bar">
    <div class="action-main">
      <button id="connectToggle">Connect</button>
      <button id="cancelJob" type="button" class="button-secondary hidden">Cancel job (disconnects)</button>
      <div id="openInNewWindowWrap" class="action-options checkbox">
        <input id="openInNewWindow" type="checkbox" />
        <label for="openInNewWindow">Open in new window</label>
      </div>
      <div class="action-status">
        <div id="connectStatus" class="hint"></div>
        <div id="clusterUpdatedAt" class="hint"></div>
      </div>
    </div>
  </div>

  <script nonce="${nonce}">
    const vscode = acquireVsCodeApi();
    const FREE_RESOURCE_STALE_MS = 10 * 60 * 1000;
    let clusterInfo = null;
    let clusterInfoFetchedAt = null;
    let lastValues = {};
    let connectionState = 'idle';
    let availableModules = [];
    let moduleDisplayMap = new Map();
    let availableSessions = [];
    let profileSummaries = {};
    let selectedSessionKey = '';
    let selectedModules = [];
    let customModuleCommand = '';
    let uiDefaults = null;
    let connectedSessionMode = '';
    let sshHosts = [];
    let sshHostSource = '';
    let lastResolvedSshHost = '';
    let sshHostHintText = '';
    let sshHostHintIsError = false;
    let showSshHostHint = false;
    let sshHostMenuIndex = -1;
    let sshHostMenuItems = [];
    let pendingSshResolveHost = '';
    let pendingSshResolveForce = false;
    let suppressSshHostChangeResolve = false;
    const autoFilledValues = {
      loginHosts: '',
      user: '',
      identityFile: ''
    };
    const sshHostField = {
      input: 'loginHosts',
      menu: 'loginHostMenu',
      picker: 'loginHostPicker',
      options: []
    };
    let autoSaveTimer = 0;
    let suppressAutoSave = true;

    const resourceFields = {
      defaultPartition: {
        input: 'defaultPartitionInput',
        menu: 'defaultPartitionMenu',
        picker: 'defaultPartitionPicker',
        options: []
      },
      defaultNodes: {
        input: 'defaultNodesInput',
        menu: 'defaultNodesMenu',
        picker: 'defaultNodesPicker',
        options: []
      },
      defaultTasksPerNode: {
        input: 'defaultTasksPerNode',
        options: []
      },
      defaultCpusPerTask: {
        input: 'defaultCpusPerTaskInput',
        menu: 'defaultCpusPerTaskMenu',
        picker: 'defaultCpusPerTaskPicker',
        options: []
      },
      defaultMemoryMb: {
        input: 'defaultMemoryMbInput',
        menu: 'defaultMemoryMbMenu',
        picker: 'defaultMemoryMbPicker',
        options: []
      },
      defaultGpuType: {
        input: 'defaultGpuTypeInput',
        menu: 'defaultGpuTypeMenu',
        picker: 'defaultGpuTypePicker',
        options: []
      },
      defaultGpuCount: {
        input: 'defaultGpuCountInput',
        menu: 'defaultGpuCountMenu',
        picker: 'defaultGpuCountPicker',
        options: []
      },
      defaultTime: {
        input: 'defaultTime',
        options: []
      }
    };


    function setStatus(text, isError) {
      const el = document.getElementById('clusterStatus');
      if (!el) return;
      el.textContent = text || '';
      el.style.color = isError ? '#b00020' : '#555';
    }

    function setFreeResourceWarning(text) {
      const el = document.getElementById('freeResourceWarning');
      if (!el) return;
      el.textContent = text || '';
    }

    function setResourceWarning(text) {
      const el = document.getElementById('resourceWarning');
      if (!el) return;
      el.textContent = text || '';
    }

    function setResourceSummaryWarning(text) {
      const el = document.getElementById('resourceSummaryWarning');
      if (!el) return;
      el.textContent = text || '';
    }

    function setFieldHint(id, text) {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = text || '';
    }

    function updateClusterUpdatedAt() {
      const el = document.getElementById('clusterUpdatedAt');
      if (!el) return;
      if (!clusterInfoFetchedAt) {
        el.textContent = '';
        return;
      }
      const stamp = new Date(clusterInfoFetchedAt);
      el.textContent = 'Cluster info: ' + stamp.toLocaleString();
    }

    function setClusterInfoLoading(loading) {
      const getButton = document.getElementById('getClusterInfo');
      const clearButton = document.getElementById('clearClusterInfo');
      const spinner = document.getElementById('clusterInfoSpinner');
      if (getButton) getButton.disabled = loading;
      if (clearButton) clearButton.disabled = loading;
      if (spinner) spinner.classList.toggle('hidden', !loading);
    }

    initSectionState();

    function setProfileStatus(text, isError) {
      const el = document.getElementById('profileStatus');
      if (!el) return;
      el.textContent = text || '';
      el.style.color = isError ? '#b00020' : '#555';
    }

    function setAgentStatus(text, isError) {
      const el = document.getElementById('agentStatus');
      if (!el) return;
      if (!isError || !text) {
        el.textContent = '';
        return;
      }
      el.textContent = text;
      el.style.color = '#b00020';
    }

    function setSshHostHint(text, isError) {
      sshHostHintText = text || '';
      sshHostHintIsError = Boolean(isError);
      renderSshHostHint();
    }

    function renderSshHostHint() {
      const el = document.getElementById('sshHostHint');
      if (!el) return;
      const shouldShow = sshHostHintIsError || showSshHostHint;
      el.textContent = shouldShow ? sshHostHintText : '';
      el.style.color = sshHostHintIsError ? '#b00020' : '#555';
    }

    function resetAdvancedToDefaults() {
      if (!uiDefaults) {
        return;
      }
      const defaults = uiDefaults;
      const set = (id, value) => setValue(id, value ?? '');
      set('loginHostsCommand', defaults.loginHostsCommand);
      set('loginHostsQueryHost', defaults.loginHostsQueryHost);
      set('partitionInfoCommand', defaults.partitionInfoCommand);
      set('partitionCommand', defaults.partitionCommand);
      set('qosCommand', defaults.qosCommand);
      set('accountCommand', defaults.accountCommand);
      set('extraSallocArgs', defaults.extraSallocArgs);
      set('promptForExtraSallocArgs', defaults.promptForExtraSallocArgs);
      set('sessionMode', defaults.sessionMode);
      set('sessionKey', defaults.sessionKey);
      set('sessionIdleTimeoutSeconds', defaults.sessionIdleTimeoutSeconds);
      set('preSshCommand', defaults.preSshCommand);
      set('preSshCheckCommand', defaults.preSshCheckCommand);
      set('additionalSshOptions', defaults.additionalSshOptions);
      set('localProxyEnabled', defaults.localProxyEnabled);
      set('forwardAgent', defaults.forwardAgent);
      set('requestTTY', defaults.requestTTY);
      scheduleAutoSave();
    }

    function setClusterInfoFetchedAt(value) {
      if (!value) {
        clusterInfoFetchedAt = null;
        updateClusterUpdatedAt();
        return;
      }
      const parsed = value instanceof Date ? value : new Date(value);
      const time = parsed.getTime();
      clusterInfoFetchedAt = Number.isFinite(time) ? time : null;
      updateClusterUpdatedAt();
    }

    function formatAgeLabel(ageMs) {
      const totalMinutes = Math.floor(ageMs / 60000);
      if (totalMinutes < 1) {
        return 'moments';
      }
      if (totalMinutes < 60) {
        return totalMinutes + ' minute' + (totalMinutes === 1 ? '' : 's');
      }
      const totalHours = Math.floor(totalMinutes / 60);
      if (totalHours < 24) {
        return totalHours + ' hour' + (totalHours === 1 ? '' : 's');
      }
      const totalDays = Math.floor(totalHours / 24);
      return totalDays + ' day' + (totalDays === 1 ? '' : 's');
    }

    function updateFreeResourceWarning() {
      if (!clusterInfo || !clusterInfo.partitions) {
        setFreeResourceWarning('');
        return;
      }
      if (!Boolean(getValue('filterFreeResources'))) {
        setFreeResourceWarning('');
        return;
      }
      const hasFreeData = clusterInfo.partitions.some((partition) => hasFreeResourceData(partition));
      if (!hasFreeData || !clusterInfoFetchedAt) {
        setFreeResourceWarning('');
        return;
      }
      const ageMs = Date.now() - clusterInfoFetchedAt;
      if (!Number.isFinite(ageMs) || ageMs < FREE_RESOURCE_STALE_MS) {
        setFreeResourceWarning('');
        return;
      }
      const ageLabel = formatAgeLabel(ageMs);
      setFreeResourceWarning('Free-resource data is ' + ageLabel + ' old. Click "Get cluster info" to refresh.');
    }

    function parseIntInput(value, allowZero) {
      const trimmed = String(value || '').trim();
      if (!trimmed) {
        return undefined;
      }
      const parsed = Number(trimmed);
      if (!Number.isFinite(parsed)) {
        return undefined;
      }
      const integer = Math.floor(parsed);
      if (allowZero) {
        return integer < 0 ? undefined : integer;
      }
      return integer < 1 ? undefined : integer;
    }

    function getSelectedPartitionInfo() {
      if (!clusterInfo || !clusterInfo.partitions) {
        return { selected: '', partition: undefined };
      }
      const selected = String(getValue('defaultPartition') || '').trim();
      let partition = selected ? clusterInfo.partitions.find((item) => item.name === selected) : undefined;
      if (!partition && !selected) {
        const defaultName = getClusterDefaultPartitionName();
        if (defaultName) {
          partition = clusterInfo.partitions.find((item) => item.name === defaultName);
        }
      }
      return { selected, partition };
    }

    function normalizeGpuKey(value) {
      return String(value || '').trim().toLowerCase();
    }

    function buildNormalizedKeyMap(values) {
      const map = {};
      values.forEach((value) => {
        map[normalizeGpuKey(value)] = value;
      });
      return map;
    }

    function getDigitGroups(value) {
      const trimmed = String(value || '').trim();
      if (!trimmed) {
        return [];
      }
      const groups = [];
      let current = '';
      for (let i = 0; i < trimmed.length; i += 1) {
        const ch = trimmed[i];
        if (ch >= '0' && ch <= '9') {
          current += ch;
        } else if (current) {
          groups.push(current);
          current = '';
        }
      }
      if (current) {
        groups.push(current);
      }
      return groups;
    }

    function isValidTimeInput(value) {
      const trimmed = String(value || '').trim();
      if (!trimmed) {
        return true;
      }
      const parts = getDigitGroups(trimmed);
      if (parts.length === 3) {
        return parts[1].length === 2 && parts[2].length === 2;
      }
      if (parts.length === 4) {
        return parts[2].length === 2 && parts[3].length === 2;
      }
      return false;
    }

    function updateResourceWarning() {
      const warnings = [];
      const timeValue = getValue('defaultTime');
      let timeHint = '';
      let nodesHint = '';
      let tasksHint = '';
      let cpusHint = '';
      let memoryHint = '';
      let gpuTypeHint = '';
      let gpuCountHint = '';
      if (!isValidTimeInput(timeValue)) {
        warnings.push('Wall time must be HH:MM:SS or D-HH:MM:SS.');
        timeHint = 'Use HH:MM:SS or D-HH:MM:SS.';
      }

      if (!clusterInfo || !clusterInfo.partitions || clusterInfo.partitions.length === 0) {
        setResourceWarning(warnings.join(' | '));
        setResourceSummaryWarning(warnings.join(' | '));
        setFieldHint('defaultTimeHint', timeHint);
        setFieldHint('defaultNodesHint', nodesHint);
        setFieldHint('defaultTasksPerNodeHint', tasksHint);
        setFieldHint('defaultCpusPerTaskHint', cpusHint);
        setFieldHint('defaultMemoryMbHint', memoryHint);
        setFieldHint('defaultGpuTypeHint', gpuTypeHint);
        setFieldHint('defaultGpuCountHint', gpuCountHint);
        updateResourceSummary();
        return;
      }

      const { selected, partition } = getSelectedPartitionInfo();
      if (!partition) {
        if (selected) {
          warnings.push('Selected partition not found in cluster info; refresh to validate limits.');
        }
        setResourceWarning(warnings.join(' | '));
        setResourceSummaryWarning(warnings.join(' | '));
        setFieldHint('defaultTimeHint', timeHint);
        setFieldHint('defaultNodesHint', nodesHint);
        setFieldHint('defaultTasksPerNodeHint', tasksHint);
        setFieldHint('defaultCpusPerTaskHint', cpusHint);
        setFieldHint('defaultMemoryMbHint', memoryHint);
        setFieldHint('defaultGpuTypeHint', gpuTypeHint);
        setFieldHint('defaultGpuCountHint', gpuCountHint);
        updateResourceSummary();
        return;
      }

      const nodes = parseIntInput(getValue('defaultNodes'), false);
      const tasksPerNode = parseIntInput(getValue('defaultTasksPerNode'), false);
      const cpusPerTask = parseIntInput(getValue('defaultCpusPerTask'), false);
      const memoryMb = parseIntInput(getValue('defaultMemoryMb'), true);
      const gpuCount = parseIntInput(getValue('defaultGpuCount'), true);
      const gpuType = String(getValue('defaultGpuType') || '').trim();

      const cpusPerNode = partition.cpus || 0;
      if (cpusPerTask && cpusPerNode && cpusPerTask > cpusPerNode) {
        warnings.push('CPUs per task (' + cpusPerTask + ') exceeds CPUs per node (' + cpusPerNode + ').');
        if (!cpusHint) {
          cpusHint = 'Max CPUs per node: ' + cpusPerNode + '.';
        }
      } else if (tasksPerNode && cpusPerTask && cpusPerNode && tasksPerNode * cpusPerTask > cpusPerNode) {
        warnings.push(
          'Tasks per node x CPUs per task (' +
            tasksPerNode * cpusPerTask +
            ') exceeds CPUs per node (' +
            cpusPerNode +
            ').'
        );
        if (!tasksHint) {
          tasksHint = 'Tasks × CPUs per task exceeds CPUs per node.';
        }
      } else if (tasksPerNode && cpusPerNode && tasksPerNode > cpusPerNode) {
        warnings.push('Tasks per node (' + tasksPerNode + ') exceeds CPUs per node (' + cpusPerNode + ').');
        if (!tasksHint) {
          tasksHint = 'Max tasks per node: ' + cpusPerNode + '.';
        }
      }

      if (nodes && partition.nodes && nodes > partition.nodes) {
        warnings.push('Nodes (' + nodes + ') exceeds partition total nodes (' + partition.nodes + ').');
        if (!nodesHint) {
          nodesHint = 'Max nodes: ' + partition.nodes + '.';
        }
      }

      if (memoryMb && partition.memMb && memoryMb > partition.memMb) {
        warnings.push(
          'Memory per node (' + memoryMb + ' MB) exceeds partition max (' + partition.memMb + ' MB).'
        );
        if (!memoryHint) {
          memoryHint = 'Max memory per node: ' + formatMem(partition.memMb) + '.';
        }
      }

      if (gpuType) {
        const typeMap = partition.gpuTypes || {};
        const normalized = buildNormalizedKeyMap(Object.keys(typeMap));
        if (!normalized[normalizeGpuKey(gpuType)]) {
          warnings.push('GPU type "' + gpuType + '" is not available in this partition.');
          gpuTypeHint = 'GPU type not available in this partition.';
        }
      }

      if (gpuCount && gpuCount > 0) {
        if (gpuType) {
          const typeMap = partition.gpuTypes || {};
          const normalized = buildNormalizedKeyMap(Object.keys(typeMap));
          const matchedKey = normalized[normalizeGpuKey(gpuType)];
          const maxForType = matchedKey ? typeMap[matchedKey] : 0;
          if (maxForType && gpuCount > maxForType) {
            warnings.push(
              'GPU count (' + gpuCount + ') exceeds partition max for ' + gpuType + ' (' + maxForType + ').'
            );
            gpuCountHint = 'Max ' + gpuType + ' per node: ' + maxForType + '.';
          }
        } else if (partition.gpuMax && gpuCount > partition.gpuMax) {
          warnings.push('GPU count (' + gpuCount + ') exceeds partition max (' + partition.gpuMax + ').');
          gpuCountHint = 'Max GPUs per node: ' + partition.gpuMax + '.';
        }
      }

      if (Boolean(getValue('filterFreeResources')) && hasFreeResourceData(partition)) {
        if (!clusterInfoFetchedAt) {
          setResourceWarning(warnings.length ? warnings.join(' | ') : '');
          return;
        }
        const ageMs = Date.now() - clusterInfoFetchedAt;
        if (!Number.isFinite(ageMs) || ageMs >= FREE_RESOURCE_STALE_MS) {
          setResourceWarning(warnings.length ? warnings.join(' | ') : '');
          return;
        }
        if (nodes && typeof partition.freeNodes === 'number' && nodes > partition.freeNodes) {
          warnings.push('Nodes (' + nodes + ') exceeds currently free nodes (' + partition.freeNodes + ').');
          if (!nodesHint) {
            nodesHint = 'Free nodes: ' + partition.freeNodes + '.';
          }
        }
        const requestedCpus =
          tasksPerNode && cpusPerTask
            ? tasksPerNode * cpusPerTask
            : cpusPerTask
              ? cpusPerTask
              : tasksPerNode
                ? tasksPerNode
                : undefined;
        if (requestedCpus && typeof partition.freeCpusPerNode === 'number') {
          if (partition.freeCpusPerNode === 0) {
            warnings.push('No free CPUs currently reported for this partition.');
            if (!cpusHint) {
              cpusHint = 'No free CPUs currently reported.';
            }
          } else if (requestedCpus > partition.freeCpusPerNode) {
            warnings.push(
              'CPUs per node requested (' +
                requestedCpus +
                ') exceeds currently free CPUs per node (' +
                partition.freeCpusPerNode +
                ').'
            );
            if (!cpusHint) {
              cpusHint = 'Free CPUs per node: ' + partition.freeCpusPerNode + '.';
            }
          }
        }

        if (gpuCount && gpuCount > 0) {
          const freeTypeMap = partition.freeGpuTypes || {};
          if (gpuType) {
            const normalized = buildNormalizedKeyMap(Object.keys(freeTypeMap));
            const matchedKey = normalized[normalizeGpuKey(gpuType)];
            const maxForType = matchedKey ? freeTypeMap[matchedKey] : 0;
            if (matchedKey && maxForType === 0) {
              warnings.push('No free ' + gpuType + ' GPUs currently reported for this partition.');
              if (!gpuCountHint) {
                gpuCountHint = 'No free ' + gpuType + ' GPUs currently reported.';
              }
            } else if (maxForType && gpuCount > maxForType) {
              warnings.push(
                'GPU count (' + gpuCount + ') exceeds currently free ' + gpuType + ' GPUs (' + maxForType + ').'
              );
              if (!gpuCountHint) {
                gpuCountHint = 'Free ' + gpuType + ' GPUs: ' + maxForType + '.';
              }
            } else if (!matchedKey) {
              const partitionTypes = partition.gpuTypes || {};
              const partitionKeys = buildNormalizedKeyMap(Object.keys(partitionTypes));
              if (partitionKeys[normalizeGpuKey(gpuType)]) {
                warnings.push('No free ' + gpuType + ' GPUs currently reported for this partition.');
                if (!gpuCountHint) {
                  gpuCountHint = 'No free ' + gpuType + ' GPUs currently reported.';
                }
              }
            }
          } else if (typeof partition.freeGpuMax === 'number') {
            if (partition.freeGpuMax === 0) {
              warnings.push('No free GPUs currently reported for this partition.');
              if (!gpuCountHint) {
                gpuCountHint = 'No free GPUs currently reported.';
              }
            } else if (gpuCount > partition.freeGpuMax) {
              warnings.push(
                'GPU count (' + gpuCount + ') exceeds currently free GPUs (' + partition.freeGpuMax + ').'
              );
              if (!gpuCountHint) {
                gpuCountHint = 'Free GPUs: ' + partition.freeGpuMax + '.';
              }
            }
          }
        }
      }

      setResourceWarning(warnings.length ? warnings.join(' | ') : '');
      setResourceSummaryWarning(warnings.length ? warnings.join(' | ') : '');
      setFieldHint('defaultTimeHint', timeHint);
      setFieldHint('defaultNodesHint', nodesHint);
      setFieldHint('defaultTasksPerNodeHint', tasksHint);
      setFieldHint('defaultCpusPerTaskHint', cpusHint);
      setFieldHint('defaultMemoryMbHint', memoryHint);
      setFieldHint('defaultGpuTypeHint', gpuTypeHint);
      setFieldHint('defaultGpuCountHint', gpuCountHint);
      updateResourceSummary();
    }

    function initSectionState() {
      const sectionState = (vscode.getState && vscode.getState()) || {};
      const defaults = {
        connectionSection: true,
        resourceSection: true,
        profilesSection: false,
        advancedSection: false
      };
      const sections = [
        { id: 'connectionSection', key: 'connectionSection' },
        { id: 'resourceSection', key: 'resourceSection' },
        { id: 'profilesSection', key: 'profilesSection' },
        { id: 'advancedSection', key: 'advancedSection' }
      ];
      sections.forEach((section) => {
        const el = document.getElementById(section.id);
        if (!el) return;
        const stored =
          typeof sectionState[section.key] === 'boolean'
            ? sectionState[section.key]
            : defaults[section.key];
        el.open = stored;
        el.addEventListener('toggle', () => {
          const nextState = (vscode.getState && vscode.getState()) || {};
          nextState[section.key] = el.open;
          vscode.setState(nextState);
        });
      });
    }

    function setConnectState(state) {
      connectionState = state || 'idle';
      const button = document.getElementById('connectToggle');
      const cancelOnlyButton = document.getElementById('cancelJob');
      const status = document.getElementById('connectStatus');
      const openInNewWindowWrap = document.getElementById('openInNewWindowWrap');
      if (!button || !status) return;
      let label = 'Connect';
      let disabled = false;
      let statusText = '';
      if (connectionState === 'connecting') {
        label = 'Cancel';
        statusText = 'Connecting...';
      } else if (connectionState === 'connected') {
        label = 'Disconnect';
        statusText = 'Connected.';
      } else if (connectionState === 'disconnecting') {
        label = 'Disconnecting...';
        disabled = true;
        statusText = 'Disconnecting...';
      }
      button.textContent = label;
      button.disabled = disabled;
      status.textContent = statusText;
      if (openInNewWindowWrap) {
        openInNewWindowWrap.style.display = connectionState === 'idle' ? 'flex' : 'none';
      }
      if (cancelOnlyButton) {
        const canCancelOnly = connectionState === 'connected' && connectedSessionMode === 'persistent';
        cancelOnlyButton.classList.toggle('hidden', !canCancelOnly);
        cancelOnlyButton.disabled = !canCancelOnly || disabled;
      }
      updateActionBarSpacer();
    }

    function updateActionBarSpacer() {
      const bar = document.querySelector('.action-bar');
      if (!bar) return;
      const height = Math.ceil(bar.getBoundingClientRect().height);
      const spacer = Math.max(96, height + 24);
      document.body.style.paddingBottom = spacer + 'px';
    }


    function scheduleAutoSave() {
      if (suppressAutoSave) return;
      const saveTarget = document.getElementById('saveTarget');
      if (!saveTarget) return;
      if (autoSaveTimer) {
        clearTimeout(autoSaveTimer);
      }
      autoSaveTimer = setTimeout(() => {
        vscode.postMessage({
          command: 'saveSettings',
          values: gather(),
          target: saveTarget.value
        });
      }, 500);
    }

    function initializeAutoSave() {
      const fields = document.querySelectorAll('input, textarea, select');
      fields.forEach((field) => {
        field.addEventListener('change', scheduleAutoSave);
      });
    }

    function updateProfileList(list, activeName) {
      const select = document.getElementById('profileSelect');
      if (!select) return;
      select.innerHTML = '';
      const items = Array.isArray(list) ? list : [];
      if (items.length === 0) {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = 'No profiles saved';
        select.appendChild(option);
        select.value = '';
        updateProfileSummary('');
        return;
      }
      items.forEach((profile) => {
        const option = document.createElement('option');
        option.value = profile.name;
        option.textContent = profile.name;
        select.appendChild(option);
      });
      if (activeName && items.some((profile) => profile.name === activeName)) {
        select.value = activeName;
      } else {
        select.value = items[0].name;
      }
      updateProfileSummary(select.value);
    }

    function getSelectedProfileName() {
      const select = document.getElementById('profileSelect');
      if (!select) return '';
      return select.value || '';
    }

    function formatMem(mb) {
      if (!mb || mb <= 0) return 'unknown';
      if (mb >= 1024) {
        const gb = mb / 1024;
        const fixed = Math.round(gb * 10) / 10;
        return fixed + ' GB';
      }
      return mb + ' MB';
    }

    function resolveFirstLoginHost() {
      const raw = String(getValue('loginHosts') || '').trim();
      if (!raw) return '';
      const tokens = raw.split(/[\\s,]+/).filter(Boolean);
      return tokens.length > 0 ? tokens[0] : '';
    }

    function updateResourceSummary() {
      const host = getSingleLoginHostValue() || resolveFirstLoginHost();
      const partition = String(getValue('defaultPartition') || '').trim();
      const nodes = parseIntInput(getValue('defaultNodes'), false);
      const tasksPerNode = parseIntInput(getValue('defaultTasksPerNode'), false);
      const cpusPerTask = parseIntInput(getValue('defaultCpusPerTask'), false);
      const memoryMb = parseIntInput(getValue('defaultMemoryMb'), true);
      const gpuCount = parseIntInput(getValue('defaultGpuCount'), true);
      const gpuType = String(getValue('defaultGpuType') || '').trim();
      const timeValue = String(getValue('defaultTime') || '').trim();

      const setSummaryValue = (id, value) => {
        const el = document.getElementById(id);
        if (el) el.textContent = value || '—';
      };

      setSummaryValue('summaryHost', host || '—');
      setSummaryValue('summaryPartition', partition || '—');
      setSummaryValue('summaryNodes', nodes ? String(nodes) : '—');

      let cpuLabel = '—';
      if (cpusPerTask && tasksPerNode) {
        cpuLabel = cpusPerTask + ' × ' + tasksPerNode + ' / node';
      } else if (cpusPerTask) {
        cpuLabel = cpusPerTask + ' / task';
      } else if (tasksPerNode) {
        cpuLabel = tasksPerNode + ' tasks / node';
      }
      setSummaryValue('summaryCpus', cpuLabel);

      let gpuLabel = '—';
      if (gpuCount !== undefined) {
        if (gpuCount === 0 && !gpuType) {
          gpuLabel = '0';
        } else if (gpuCount === 0 && gpuType) {
          gpuLabel = '0 ' + gpuType;
        } else if (gpuCount > 0) {
          gpuLabel = gpuCount + ' ' + (gpuType || 'GPU') + ' / node';
        }
      } else if (gpuType) {
        gpuLabel = gpuType;
      }
      setSummaryValue('summaryGpus', gpuLabel);

      const memLabel = memoryMb && memoryMb > 0 ? formatMem(memoryMb) + ' / node' : '—';
      setSummaryValue('summaryMemory', memLabel);
      setSummaryValue('summaryTime', timeValue || '—');

      let totalCpu;
      if (nodes && (tasksPerNode || cpusPerTask)) {
        totalCpu = nodes * (tasksPerNode || 1) * (cpusPerTask || 1);
      }
      let totalMem;
      if (nodes && memoryMb && memoryMb > 0) {
        totalMem = nodes * memoryMb;
      }
      let totalGpu;
      if (nodes && gpuCount !== undefined) {
        totalGpu = nodes * gpuCount;
      }
      const totals = [];
      totals.push('Total CPU: ' + (totalCpu !== undefined ? totalCpu : '—'));
      totals.push('Total RAM: ' + (totalMem !== undefined ? formatMem(totalMem) : '—'));
      if (totalGpu !== undefined) {
        totals.push('Total GPU: ' + totalGpu + (gpuType ? ' ' + gpuType : ''));
      } else {
        totals.push('Total GPU: —');
      }
      const totalsEl = document.getElementById('resourceTotals');
      if (totalsEl) {
        totalsEl.textContent = totals.join(' • ');
      }
    }

    function updateProfileSummary(name) {
      const summary = name ? profileSummaries[name] : undefined;
      const setSummaryValue = (id, value) => {
        const el = document.getElementById(id);
        if (el) el.textContent = value || '—';
      };
      const overridesDetails = document.getElementById('profileOverrides');
      const overridesSummary = document.getElementById('profileOverridesSummary');
      const overridesList = document.getElementById('profileOverrideList');
      if (!summary) {
        setSummaryValue('profileSummaryHost', '—');
        setSummaryValue('profileSummaryPartition', '—');
        setSummaryValue('profileSummaryNodes', '—');
        setSummaryValue('profileSummaryCpus', '—');
        setSummaryValue('profileSummaryGpus', '—');
        setSummaryValue('profileSummaryTime', '—');
        setSummaryValue('profileSummaryMemory', '—');
        const totalsEl = document.getElementById('profileSummaryTotals');
        if (totalsEl) totalsEl.textContent = '';
        if (overridesList) overridesList.innerHTML = '';
        if (overridesDetails) {
          overridesDetails.classList.add('hidden');
          overridesDetails.open = false;
        }
        return;
      }

      setSummaryValue('profileSummaryHost', summary.host || '—');
      setSummaryValue('profileSummaryPartition', summary.partition || '—');
      setSummaryValue('profileSummaryNodes', summary.nodes !== undefined ? String(summary.nodes) : '—');

      let cpuLabel = '—';
      if (summary.cpusPerTask && summary.tasksPerNode) {
        cpuLabel = summary.cpusPerTask + ' × ' + summary.tasksPerNode + ' / node';
      } else if (summary.cpusPerTask) {
        cpuLabel = summary.cpusPerTask + ' / task';
      } else if (summary.tasksPerNode) {
        cpuLabel = summary.tasksPerNode + ' tasks / node';
      }
      setSummaryValue('profileSummaryCpus', cpuLabel);

      let gpuLabel = '—';
      if (summary.gpuCount !== undefined) {
        if (summary.gpuCount === 0 && !summary.gpuType) {
          gpuLabel = '0';
        } else if (summary.gpuCount === 0 && summary.gpuType) {
          gpuLabel = '0 ' + summary.gpuType;
        } else if (summary.gpuCount > 0) {
          gpuLabel = summary.gpuCount + ' ' + (summary.gpuType || 'GPU') + ' / node';
        }
      } else if (summary.gpuType) {
        gpuLabel = summary.gpuType;
      }
      setSummaryValue('profileSummaryGpus', gpuLabel);

      const memLabel =
        summary.memoryMb && summary.memoryMb > 0 ? formatMem(summary.memoryMb) + ' / node' : '—';
      setSummaryValue('profileSummaryMemory', memLabel);
      setSummaryValue('profileSummaryTime', summary.time || '—');

      const totals = [];
      totals.push('Total CPU: ' + (summary.totalCpu !== undefined ? summary.totalCpu : '—'));
      totals.push('Total RAM: ' + (summary.totalMemoryMb !== undefined ? formatMem(summary.totalMemoryMb) : '—'));
      if (summary.totalGpu !== undefined) {
        totals.push('Total GPU: ' + summary.totalGpu + (summary.gpuType ? ' ' + summary.gpuType : ''));
      } else {
        totals.push('Total GPU: —');
      }
      const totalsEl = document.getElementById('profileSummaryTotals');
      if (totalsEl) totalsEl.textContent = totals.join(' • ');

      if (overridesList) {
        overridesList.innerHTML = '';
      }
      const overrides = Array.isArray(summary.overrides) ? summary.overrides : [];
      if (overridesDetails && overridesList && overridesSummary) {
        if (overrides.length === 0) {
          overridesDetails.classList.add('hidden');
          overridesDetails.open = false;
        } else {
          overridesDetails.classList.remove('hidden');
          overridesDetails.open = false;
          overridesSummary.textContent = 'Other saved settings (' + overrides.length + ')';
          overrides.forEach((entry) => {
            const row = document.createElement('div');
            row.className = 'override-row';
            const key = document.createElement('span');
            key.className = 'override-key';
            key.textContent = entry.label;
            const value = document.createElement('span');
            value.className = 'override-value';
            value.textContent = entry.value;
            row.appendChild(key);
            row.appendChild(value);
            overridesList.appendChild(row);
          });
        }
      }
    }

    function normalizeOptions(options) {
      const items = Array.isArray(options) ? options : [];
      return items.map((opt) => {
        const value = opt && opt.value !== undefined && opt.value !== null ? String(opt.value) : '';
        const label = opt && opt.label !== undefined ? String(opt.label) : String(value);
        return { value, label };
      });
    }

    function setSshHostOptions(hosts, source, error) {
      const list = Array.isArray(hosts) ? hosts.map((host) => String(host).trim()).filter(Boolean) : [];
      sshHosts = list;
      sshHostSource = typeof source === 'string' ? source : '';
      lastResolvedSshHost = '';
      sshHostField.options = normalizeOptions(list.map((host) => ({ value: host, label: host })));
      const picker = document.getElementById(sshHostField.picker);
      if (picker) {
        if (sshHostField.options.length === 0) {
          picker.classList.add('no-options');
        } else {
          picker.classList.remove('no-options');
        }
      }
      const menu = document.getElementById(sshHostField.menu);
      if (menu && menu.classList.contains('visible')) {
        updateSshHostMenuFromInput();
      }

      if (error) {
        const normalized = String(error);
        const missing = normalized.toLowerCase().includes('ssh config not found');
        const hint = missing
          ? 'SSH config not found. Enter a login host manually.'
          : 'SSH config hosts not loaded: ' + error;
        setSshHostHint(hint, !missing);
        return;
      }
      if (sshHostField.options.length === 0) {
        const sourceLabel = sshHostSource ? ' in ' + sshHostSource : '';
        setSshHostHint('No SSH hosts found' + sourceLabel + '. Enter a login host manually.', false);
        return;
      }
      const sourceLabel = sshHostSource ? ' from ' + sshHostSource : '';
      setSshHostHint(
        'Loaded ' + sshHostField.options.length + ' SSH host(s)' + sourceLabel + '. Start typing to see matches.',
        false
      );
      maybeResolveTypedHost();
    }

    function filterSshHostOptions(filterText) {
      const text = String(filterText || '').trim().toLowerCase();
      if (!text) {
        return sshHostField.options.slice();
      }
      return sshHostField.options.filter((opt) =>
        opt.label.toLowerCase().includes(text) || opt.value.toLowerCase().includes(text)
      );
    }

    function renderSshHostMenu(options) {
      const menu = document.getElementById(sshHostField.menu);
      if (!menu) return;
      menu.innerHTML = '';
      const items = Array.isArray(options) ? options : [];
      sshHostMenuItems = items;
      if (sshHostMenuIndex >= items.length) {
        sshHostMenuIndex = -1;
      }
      if (items.length === 0) {
        const empty = document.createElement('div');
        empty.className = 'dropdown-option';
        empty.textContent = sshHostField.options.length ? 'No matches' : 'No SSH hosts';
        menu.appendChild(empty);
        return;
      }
      items.forEach((opt, index) => {
        const option = document.createElement('div');
        option.className = index === sshHostMenuIndex ? 'dropdown-option active' : 'dropdown-option';
        option.textContent = opt.label;
        option.addEventListener('mousedown', (event) => {
          event.preventDefault();
          chooseSshHostOption(opt.value, true);
        });
        menu.appendChild(option);
      });
    }

    function showSshHostMenu(showAll, closeOthers = true) {
      if (sshHostField.options.length === 0) {
        return;
      }
      const menu = document.getElementById(sshHostField.menu);
      const input = document.getElementById(sshHostField.input);
      if (!menu || !input) return;
      if (closeOthers) {
        closeAllMenus();
      }
      const options = showAll ? sshHostField.options : filterSshHostOptions(input.value);
      renderSshHostMenu(options);
      menu.classList.add('visible');
    }

    function hideSshHostMenu() {
      const menu = document.getElementById(sshHostField.menu);
      if (menu) {
        menu.classList.remove('visible');
      }
      sshHostMenuIndex = -1;
      sshHostMenuItems = [];
    }

    function updateSshHostMenuFromInput() {
      sshHostMenuIndex = -1;
      ensureSshHostMenuVisible();
    }

    function ensureSshHostMenuVisible() {
      const input = document.getElementById(sshHostField.input);
      if (!input) return;
      const text = String(input.value || '').trim();
      if (!text) {
        hideSshHostMenu();
        return;
      }
      const matches = filterSshHostOptions(text);
      if (matches.length === 0) {
        hideSshHostMenu();
        return;
      }
      const menu = document.getElementById(sshHostField.menu);
      if (!menu) return;
      closeAllMenus();
      renderSshHostMenu(matches);
      menu.classList.add('visible');
    }

    function moveSshHostMenuSelection(delta) {
      const menu = document.getElementById(sshHostField.menu);
      if (!menu || !menu.classList.contains('visible')) {
        ensureSshHostMenuVisible();
      }
      if (sshHostMenuItems.length === 0) {
        return;
      }
      if (sshHostMenuIndex < 0) {
        sshHostMenuIndex = delta > 0 ? 0 : sshHostMenuItems.length - 1;
      } else {
        const next = sshHostMenuIndex + delta;
        if (next < 0) {
          sshHostMenuIndex = sshHostMenuItems.length - 1;
        } else if (next >= sshHostMenuItems.length) {
          sshHostMenuIndex = 0;
        } else {
          sshHostMenuIndex = next;
        }
      }
      renderSshHostMenu(sshHostMenuItems);
    }

    function chooseSshHostOption(value, forceOverwrite) {
      const input = document.getElementById(sshHostField.input);
      if (input) {
        suppressSshHostChangeResolve = true;
        input.value = value;
        input.dispatchEvent(new Event('change', { bubbles: true }));
        suppressSshHostChangeResolve = false;
      }
      hideSshHostMenu();
      resolveSshHostSelection(value, { forceOverwrite });
    }

    function resolveSshHostSelection(host, options) {
      const trimmed = String(host || '').trim();
      if (!trimmed || trimmed === lastResolvedSshHost) {
        return;
      }
      const forceOverwrite = Boolean(options && options.forceOverwrite);
      lastResolvedSshHost = trimmed;
      pendingSshResolveHost = trimmed;
      pendingSshResolveForce = forceOverwrite;
      vscode.postMessage({
        command: 'resolveSshHost',
        host: trimmed,
        values: gather()
      });
    }

    function getSingleLoginHostValue() {
      const raw = String(getValue('loginHosts') || '').trim();
      const tokens = raw.split(/[\\s,]+/).filter(Boolean);
      return tokens.length === 1 ? tokens[0] : '';
    }

    function maybeResolveTypedHost() {
      const host = getSingleLoginHostValue();
      if (!host || sshHosts.length === 0) {
        return;
      }
      if (sshHosts.includes(host)) {
        resolveSshHostSelection(host);
      }
    }

    function setFieldOptions(fieldKey, options, selectedValue) {
      const field = resourceFields[fieldKey];
      if (!field) return;
      const normalized = normalizeOptions(options);
      field.options = normalized;
      const input = document.getElementById(field.input);
      if (input) {
        const current = input.value;
        if (selectedValue !== undefined && selectedValue !== null && selectedValue !== '') {
          input.value = String(selectedValue);
        }
      }
      if (field.picker) {
        const picker = document.getElementById(field.picker);
        if (picker) {
          if (normalized.length === 0) {
            picker.classList.add('no-options');
          } else {
            picker.classList.remove('no-options');
          }
        }
      }
      if (field.menu) {
        const menu = document.getElementById(field.menu);
        if (menu && menu.classList.contains('visible')) {
          showFieldMenu(fieldKey, false, false);
        }
      }
    }

    function filterFieldOptions(fieldKey, filterText) {
      const field = resourceFields[fieldKey];
      if (!field) return [];
      const text = String(filterText || '').trim().toLowerCase();
      if (!text) {
        return field.options.slice();
      }
      return field.options.filter((opt) =>
        opt.label.toLowerCase().includes(text) || opt.value.toLowerCase().includes(text)
      );
    }

    function renderFieldMenu(fieldKey, options) {
      const field = resourceFields[fieldKey];
      if (!field) return;
      const menu = document.getElementById(field.menu);
      if (!menu) return;
      menu.innerHTML = '';
      const items = Array.isArray(options) ? options : [];
      if (items.length === 0) {
        const empty = document.createElement('div');
        empty.className = 'dropdown-option';
        empty.textContent = field.options.length ? 'No matches' : 'No options';
        menu.appendChild(empty);
        return;
      }
      items.forEach((opt) => {
        const option = document.createElement('div');
        option.className = 'dropdown-option';
        option.textContent = opt.label;
        option.addEventListener('mousedown', (event) => {
          event.preventDefault();
          const input = document.getElementById(field.input);
          if (input) {
            input.value = opt.value;
            input.dispatchEvent(new Event('change', { bubbles: true }));
          }
          hideFieldMenu(fieldKey);
        });
        menu.appendChild(option);
      });
    }

    function showFieldMenu(fieldKey, showAll, closeOthers = true) {
      const field = resourceFields[fieldKey];
      if (!field || field.options.length === 0) return;
      const menu = document.getElementById(field.menu);
      const input = document.getElementById(field.input);
      if (!menu || !input) return;
      if (closeOthers) {
        closeAllMenus();
      }
      const options = showAll ? field.options : filterFieldOptions(fieldKey, input.value);
      renderFieldMenu(fieldKey, options);
      menu.classList.add('visible');
    }

    function hideFieldMenu(fieldKey) {
      const field = resourceFields[fieldKey];
      if (!field) return;
      if (field.menu) {
        const menu = document.getElementById(field.menu);
        if (menu) {
          menu.classList.remove('visible');
        }
      }
    }

    function closeAllMenus() {
      hideModuleMenu();
      hideSshHostMenu();
      Object.keys(resourceFields).forEach((key) => hideFieldMenu(key));
    }

    function clearResourceOptions() {
      Object.keys(resourceFields).forEach((key) => {
        const field = resourceFields[key];
        field.options = [];
        const picker = document.getElementById(field.picker);
        if (picker) {
          picker.classList.add('no-options');
        }
        hideFieldMenu(key);
      });
    }

    function isModuleHeaderEntry(value) {
      const trimmed = String(value || '').trim();
      return trimmed.startsWith('/') && trimmed.endsWith(':');
    }

    function formatModuleHeaderLabel(value) {
      const trimmed = String(value || '').trim();
      return trimmed.endsWith(':') ? trimmed.slice(0, -1) : trimmed;
    }

    function stripModuleTag(value) {
      const trimmed = String(value || '').trim();
      if (!trimmed) {
        return '';
      }
      const defaultMatch = trimmed.match(/^(.*?)(?:\\s*[<(]\\s*(?:default|d)\\s*[>)]\\s*)$/i);
      if (defaultMatch) {
        return defaultMatch[1].trim();
      }
      const shortTagMatch = trimmed.match(/^(.*?)(?:\\s*[<(]\\s*[a-zA-Z]{1,3}\\s*[>)]\\s*)$/);
      if (shortTagMatch) {
        return shortTagMatch[1].trim();
      }
      return trimmed;
    }

    function isDefaultTagged(value) {
      return /[<(]\\s*(?:default|d)\\s*[>)]\\s*$/i.test(String(value || '').trim());
    }

    function getModuleDisplayLabel(value) {
      const canonical = stripModuleTag(value);
      return moduleDisplayMap.get(canonical) || value;
    }

    function getSelectableModuleCount() {
      return availableModules.filter((entry) => !isModuleHeaderEntry(entry)).length;
    }

    function normalizeModuleName(value) {
      return stripModuleTag(value).replace(/\\s+/g, '');
    }

    function coalesceModuleTokens(tokens) {
      if (!Array.isArray(tokens) || tokens.length < 2 || availableModules.length === 0) {
        return tokens;
      }
      const normalizedMap = new Map();
      availableModules.forEach((name) => {
        if (isModuleHeaderEntry(name)) {
          return;
        }
        normalizedMap.set(normalizeModuleName(name), name);
      });
      const result = [];
      for (let i = 0; i < tokens.length; i += 1) {
        let combined = tokens[i];
        let matched = normalizedMap.get(normalizeModuleName(combined));
        if (matched) {
          result.push(matched);
          continue;
        }
        let found = false;
        for (let j = i + 1; j < tokens.length; j += 1) {
          combined += tokens[j];
          matched = normalizedMap.get(normalizeModuleName(combined));
          if (matched) {
            result.push(matched);
            i = j;
            found = true;
            break;
          }
        }
        if (!found) {
          result.push(tokens[i]);
        }
      }
      return result;
    }

    function parseModuleList(input) {
      const normalized = normalizeModuleInput(input);
      const tokens = normalized
        .split(/[\\s,]+/)
        .map((value) => value.trim())
        .filter(Boolean);
      return coalesceModuleTokens(tokens)
        .map((entry) => stripModuleTag(entry))
        .filter((entry) => entry.length > 0 && !isModuleHeaderEntry(entry));
    }

    function normalizeModuleInput(input) {
      const text = String(input || '');
      const lines = text.split(/\\r?\\n/);
      const parts = [];
      lines.forEach((line) => {
        const trimmed = line.trim();
        if (!trimmed) return;
        const moduleMatch = trimmed.match(/^module\\s+load\\s+(.+)$/i);
        const mlMatch = trimmed.match(/^ml\\s+(.+)$/i);
        if (moduleMatch) {
          parts.push(moduleMatch[1]);
        } else if (mlMatch) {
          parts.push(mlMatch[1]);
        } else {
          parts.push(trimmed);
        }
      });
      return parts.join(' ');
    }

    function updateModuleHint() {
      const hint = document.getElementById('moduleHint');
      if (!hint) return;
      if (customModuleCommand) {
        hint.textContent = 'Custom module command set; adding modules will replace it.';
        return;
      }
      const moduleCount = getSelectableModuleCount();
      if (!moduleCount) {
        hint.textContent = 'No module list available. Type or paste module names below.';
        return;
      }
      hint.textContent = '';
    }

    function syncModuleLoadValue() {
      const command = selectedModules.length > 0
        ? 'module load ' + selectedModules.join(' ')
        : customModuleCommand;
      setValue('moduleLoad', command || '');
    }

    function renderModuleList() {
      const container = document.getElementById('moduleList');
      if (!container) return;
      container.innerHTML = '';
      selectedModules.forEach((moduleName) => {
        const chip = document.createElement('div');
        chip.className = 'module-chip';
        const label = document.createElement('span');
        label.textContent = getModuleDisplayLabel(moduleName);
        chip.appendChild(label);
        const remove = document.createElement('button');
        remove.type = 'button';
        remove.textContent = '×';
        remove.addEventListener('click', () => {
          selectedModules = selectedModules.filter((entry) => entry !== moduleName);
          renderModuleList();
        });
        chip.appendChild(remove);
        container.appendChild(chip);
      });
      const clearAll = document.getElementById('moduleClearAll');
      if (clearAll) {
        clearAll.disabled = selectedModules.length === 0 && !customModuleCommand;
      }
      syncModuleLoadValue();
      updateModuleHint();
      scheduleAutoSave();
    }

    function applyModuleLoad(value) {
      const trimmed = String(value || '').trim();
      selectedModules = [];
      customModuleCommand = '';
      if (!trimmed) {
        renderModuleList();
        return;
      }
      const moduleMatch = trimmed.match(/^module\\s+load\\s+(.+)$/i);
      const mlMatch = trimmed.match(/^ml\\s+(.+)$/i);
      const match = moduleMatch || mlMatch;
      if (match) {
        selectedModules = parseModuleList(match[1]);
      } else {
        customModuleCommand = trimmed;
      }
      renderModuleList();
    }

    function applyModuleState(values) {
      const selections = Array.isArray(values.moduleSelections)
        ? values.moduleSelections.filter((entry) => typeof entry === 'string' && entry.trim().length > 0)
        : [];
      const custom = typeof values.moduleCustomCommand === 'string' ? values.moduleCustomCommand.trim() : '';
      if (selections.length > 0) {
        selectedModules = selections.map((entry) => stripModuleTag(entry)).filter(Boolean);
        customModuleCommand = '';
        renderModuleList();
        return;
      }
      if (custom) {
        selectedModules = [];
        customModuleCommand = custom;
        renderModuleList();
        return;
      }
      applyModuleLoad(values.moduleLoad);
    }

    function setModuleOptions(modules) {
      const list = Array.isArray(modules) ? modules : [];
      availableModules = list.slice();
      moduleDisplayMap = new Map();
      availableModules.forEach((entry) => {
        if (isModuleHeaderEntry(entry)) {
          return;
        }
        const canonical = stripModuleTag(entry);
        if (!canonical) {
          return;
        }
        const existing = moduleDisplayMap.get(canonical);
        if (!existing || (isDefaultTagged(entry) && !isDefaultTagged(existing))) {
          moduleDisplayMap.set(canonical, entry);
        }
      });
      const picker = document.getElementById('modulePicker');
      if (getSelectableModuleCount() === 0) {
        hideModuleMenu();
        if (picker) picker.classList.add('no-options');
      } else if (picker) {
        picker.classList.remove('no-options');
      }
      updateModuleHint();
    }

    function filterModules(filterText) {
      const text = String(filterText || '').trim().toLowerCase();
      if (!text) {
        return availableModules.slice();
      }
      const results = [];
      const seen = new Set();
      const push = (value) => {
        if (!seen.has(value)) {
          seen.add(value);
          results.push(value);
        }
      };
      let currentHeader = null;
      let headerAdded = false;
      let includeAllInSection = false;
      availableModules.forEach((name) => {
        if (isModuleHeaderEntry(name)) {
          currentHeader = name;
          headerAdded = false;
          includeAllInSection = false;
          const headerLabel = formatModuleHeaderLabel(name).toLowerCase();
          const headerMatches = name.toLowerCase().includes(text) || headerLabel.includes(text);
          if (headerMatches) {
            includeAllInSection = true;
            push(name);
            headerAdded = true;
          }
          return;
        }
        if (includeAllInSection) {
          push(name);
          return;
        }
        if (name.toLowerCase().includes(text)) {
          if (currentHeader && !headerAdded) {
            push(currentHeader);
            headerAdded = true;
          }
          push(name);
        }
      });
      return results;
    }

    function renderModuleMenu(options) {
      const menu = document.getElementById('moduleMenu');
      if (!menu) return;
      menu.innerHTML = '';
      const items = Array.isArray(options) ? options : [];
      if (items.length === 0) {
        const empty = document.createElement('div');
        empty.className = 'dropdown-option';
        empty.textContent = availableModules.length ? 'No matches' : 'No modules available';
        menu.appendChild(empty);
        return;
      }
      items.forEach((moduleName) => {
        if (isModuleHeaderEntry(moduleName)) {
          const header = document.createElement('div');
          header.className = 'dropdown-option module-header';
          header.textContent = formatModuleHeaderLabel(moduleName);
          header.setAttribute('aria-disabled', 'true');
          menu.appendChild(header);
          return;
        }
        const option = document.createElement('div');
        option.className = 'dropdown-option';
        option.textContent = moduleName;
        option.addEventListener('mousedown', (event) => {
          event.preventDefault();
          const input = document.getElementById('moduleInput');
          if (input) {
            input.value = moduleName;
            input.focus();
          }
          hideModuleMenu();
        });
        menu.appendChild(option);
      });
    }

    function showModuleMenu(showAll, closeOthers = true) {
      const menu = document.getElementById('moduleMenu');
      const input = document.getElementById('moduleInput');
      if (!menu || !input || getSelectableModuleCount() === 0) return;
      if (closeOthers) {
        closeAllMenus();
      }
      const options = showAll ? availableModules.slice() : filterModules(input.value);
      renderModuleMenu(options);
      menu.classList.add('visible');
    }

    function hideModuleMenu() {
      const menu = document.getElementById('moduleMenu');
      if (menu) {
        menu.classList.remove('visible');
      }
    }

    function addModulesFromInput(raw) {
      const input = document.getElementById('moduleInput');
      const text = typeof raw === 'string' ? raw : input ? input.value : '';
      const entries = parseModuleList(text);
      if (entries.length === 0) {
        return;
      }
      let changed = false;
      entries.forEach((entry) => {
        if (isModuleHeaderEntry(entry)) {
          return;
        }
        if (!selectedModules.includes(entry)) {
          selectedModules.push(entry);
          changed = true;
        }
      });
      if (changed) {
        customModuleCommand = '';
        renderModuleList();
      }
      if (input && typeof raw !== 'string') input.value = '';
    }

    function setFieldDisabled(fieldKey, disabled) {
      const field = resourceFields[fieldKey];
      if (!field) return;
      const input = document.getElementById(field.input);
      if (input) input.disabled = disabled;
      if (field.picker) {
        const picker = document.getElementById(field.picker);
        if (picker) {
          if (disabled || field.options.length === 0) {
            picker.classList.add('no-options');
          } else {
            picker.classList.remove('no-options');
          }
          picker.classList.toggle('disabled', disabled);
        }
      }
      if (disabled) {
        hideFieldMenu(fieldKey);
      }
    }

    function setResourceDisabled(disabled) {
      Object.keys(resourceFields).forEach((key) => setFieldDisabled(key, disabled));
      if (disabled) {
        const meta = document.getElementById('partitionMeta');
        if (meta) meta.textContent = '';
      }
    }

    function formatSessionLabel(session) {
      const name = session.jobName ? String(session.jobName).trim() : '';
      const state = session.state ? String(session.state).trim() : '';
      const clients = Number.isFinite(Number(session.clients)) ? Number(session.clients) : null;
      const idleTimeout = Number(session.idleTimeoutSeconds);
      const remaining = Number(session.idleRemainingSeconds);
      const pieces = [];
      if (name) pieces.push(name);
      pieces.push('job ' + session.jobId);
      if (state) pieces.push(state);
      if (clients !== null) pieces.push('clients ' + clients);
      if (Number.isFinite(idleTimeout)) {
        if (idleTimeout > 0) {
          if (clients === 0) {
            if (Number.isFinite(remaining)) {
              pieces.push('idle in ' + formatDurationSeconds(remaining));
            } else {
              pieces.push('idle timeout ' + formatDurationSeconds(idleTimeout));
            }
          } else {
            pieces.push('idle after disconnect ' + formatDurationSeconds(idleTimeout));
          }
        } else {
          pieces.push('idle timeout never');
        }
      }
      return session.sessionKey + ' (' + pieces.join(', ') + ')';
    }

    function getSelectedSessionDetails() {
      if (!selectedSessionKey) return null;
      return availableSessions.find((session) => session.sessionKey === selectedSessionKey) || null;
    }

    function buildSessionHint(session) {
      if (!session) return '';
      const parts = [];
      if (session.partition) parts.push('Partition: ' + session.partition);
      if (session.nodes) parts.push('Nodes: ' + session.nodes);
      if (session.cpus) parts.push('CPUs: ' + session.cpus);
      if (session.timeLimit) parts.push('Time limit: ' + session.timeLimit);
      if (Number.isFinite(Number(session.clients))) {
        const clients = Number(session.clients);
        parts.push('Clients: ' + clients);
      }
      const idleTimeout = Number(session.idleTimeoutSeconds);
      const remaining = Number(session.idleRemainingSeconds);
      if (Number.isFinite(idleTimeout)) {
        if (idleTimeout > 0) {
          if (Number.isFinite(Number(session.clients)) && Number(session.clients) === 0) {
            if (Number.isFinite(remaining)) {
              parts.push('Idle cancel in: ' + formatDurationSeconds(remaining));
            } else {
              parts.push('Idle timeout: ' + formatDurationSeconds(idleTimeout));
            }
          } else {
            parts.push('Idle timeout (after disconnect): ' + formatDurationSeconds(idleTimeout));
          }
        } else {
          parts.push('Idle timeout: never');
        }
      }
      parts.push('Job: ' + session.jobId);
      if (session.state) parts.push('State: ' + session.state);
      return parts.join(' • ');
    }

    function formatDurationSeconds(value) {
      const total = Math.max(0, Math.floor(Number(value) || 0));
      const hours = Math.floor(total / 3600);
      const minutes = Math.floor((total % 3600) / 60);
      const seconds = total % 60;
      if (hours > 0) {
        return hours + 'h ' + minutes + 'm';
      }
      if (minutes > 0) {
        return minutes + 'm ' + seconds + 's';
      }
      return seconds + 's';
    }

    function setSessionSelection(value) {
      selectedSessionKey = String(value || '').trim();
      const list = document.getElementById('sessionSelectionList');
      if (list) {
        const items = list.querySelectorAll('.session-option');
        items.forEach((item) => {
          const key = item.getAttribute('data-key') || '';
          const radio = item.querySelector('input[type="radio"]');
          const isSelected = key === selectedSessionKey;
          item.classList.toggle('selected', isSelected);
          if (radio) {
            radio.checked = isSelected;
          }
        });
      }
      updateSessionLockState();
      updateResourceSummary();
    }

    function updateSessionLockState() {
      const locked = Boolean(selectedSessionKey);
      setResourceDisabled(locked);
      const fillButton = document.getElementById('fillPartitionDefaults');
      if (fillButton) fillButton.disabled = locked;
      const clearButton = document.getElementById('clearResources');
      if (clearButton) clearButton.disabled = locked;
      const filterToggle = document.getElementById('filterFreeResources');
      if (filterToggle) filterToggle.disabled = locked;
      const hint = document.getElementById('sessionSelectionHint');
      if (hint) {
        if (locked) {
          const details = buildSessionHint(getSelectedSessionDetails());
          hint.textContent = details
            ? 'Connecting will attach to this allocation. ' + details
            : 'Connecting will attach to this allocation; resource fields are disabled.';
        } else {
          hint.textContent = '';
        }
      }
      const resourceSection = document.getElementById('resourceSection');
      if (resourceSection && locked && resourceSection.open) {
        resourceSection.open = false;
      }
      if (resourceSection && !locked && !resourceSection.open) {
        resourceSection.open = true;
      }
      const moduleSection = document.getElementById('moduleSection');
      if (moduleSection) {
        moduleSection.classList.toggle('disabled', locked);
      }
      if (locked) {
        setFreeResourceWarning('');
        setResourceWarning('');
      }
    }

    function setSessionOptions(sessions) {
      const list = Array.isArray(sessions) ? sessions : [];
      availableSessions = list.slice();
      const container = document.getElementById('sessionSelector');
      const listEl = document.getElementById('sessionSelectionList');
      if (!container || !listEl) return;
      if (list.length === 0) {
        container.classList.add('hidden');
        listEl.innerHTML = '';
        setSessionSelection('');
        return;
      }
      container.classList.remove('hidden');
      listEl.innerHTML = '';
      const entries = [{ sessionKey: '', label: 'New allocation' }]
        .concat(
          list.map((session) => ({
            sessionKey: session.sessionKey,
            label: formatSessionLabel(session)
          }))
        );
      entries.forEach((entry) => {
        const row = document.createElement('label');
        row.className = 'session-option';
        row.setAttribute('data-key', entry.sessionKey);
        const radio = document.createElement('input');
        radio.type = 'radio';
        radio.name = 'sessionSelection';
        radio.value = entry.sessionKey;
        const text = document.createElement('span');
        text.textContent = entry.label;
        row.appendChild(radio);
        row.appendChild(text);
        row.addEventListener('click', () => {
          setSessionSelection(entry.sessionKey);
        });
        listEl.appendChild(row);
      });
      const stillExists = list.some((session) => session.sessionKey === selectedSessionKey);
      setSessionSelection(stillExists ? selectedSessionKey : '');
    }

    function getSelectedSessionKey() {
      return selectedSessionKey;
    }

    function clearClusterInfoUi() {
      const preserved = {};
      Object.keys(resourceFields).forEach((key) => {
        preserved[key] = getValue(key);
      });
      clusterInfo = null;
      setClusterInfoFetchedAt(null);
      setFreeResourceWarning('');
      setResourceWarning('');
      setResourceDisabled(false);
      setSessionOptions([]);
      clearResourceOptions();
      setModuleOptions([]);
      Object.keys(preserved).forEach((key) => {
        if (preserved[key] !== undefined && preserved[key] !== null) {
          setValue(key, preserved[key]);
        }
      });
      const meta = document.getElementById('partitionMeta');
      if (meta) meta.textContent = '';
      updateResourceSummary();
      setResourceSummaryWarning('');
    }

    function clearResourceFields() {
      const resourceKeys = [
        'defaultPartition',
        'defaultNodes',
        'defaultTasksPerNode',
        'defaultCpusPerTask',
        'defaultTime',
        'defaultMemoryMb',
        'defaultGpuType',
        'defaultGpuCount'
      ];
      resourceKeys.forEach((key) => {
        setValue(key, '');
        lastValues[key] = '';
      });
      selectedModules = [];
      customModuleCommand = '';
      renderModuleList();
      const moduleInput = document.getElementById('moduleInput');
      if (moduleInput) moduleInput.value = '';
      const meta = document.getElementById('partitionMeta');
      if (meta) meta.textContent = '';
      updateResourceWarning();
      updatePartitionDetails();
      const saveTarget = document.getElementById('saveTarget');
      if (saveTarget) {
        vscode.postMessage({
          command: 'saveSettings',
          values: gather(),
          target: saveTarget.value
        });
      }
    }

    function buildRangeOptions(maxValue) {
      const options = [];
      const max = Number(maxValue) || 0;
      if (!max) return options;
      const limit = Math.min(max, 128);
      for (let i = 1; i <= limit; i += 1) {
        options.push({ value: String(i), label: String(i) });
      }
      if (max > limit) {
        options.push({ value: String(max), label: String(max) + ' (max)' });
      }
      return options;
    }

    function buildMemoryOptions(maxMb) {
      const options = [{ value: '', label: 'Use default' }];
      const max = Number(maxMb) || 0;
      if (!max) return options;
      const maxOptions = 32;
      let step = 1024;
      const steps = Math.floor(max / step);
      if (steps > maxOptions) {
        step = Math.ceil(max / maxOptions / 1024) * 1024;
      }
      let added = false;
      for (let mb = step; mb <= max; mb += step) {
        options.push({ value: String(mb), label: formatMem(mb) });
        added = true;
      }
      if (!added) {
        options.push({ value: String(max), label: formatMem(max) });
      } else if (options[options.length - 1].value !== String(max)) {
        options.push({ value: String(max), label: formatMem(max) + ' (max)' });
      }
      return options;
    }

    function getClusterDefaultPartitionName() {
      if (!clusterInfo || !clusterInfo.partitions) return '';
      if (clusterInfo.defaultPartition) return clusterInfo.defaultPartition;
      const flagged = clusterInfo.partitions.find((partition) => partition.isDefault);
      return flagged ? flagged.name : '';
    }

    function hasFreeResourceData(partition) {
      if (!partition) return false;
      return (
        typeof partition.freeCpuTotal === 'number' ||
        typeof partition.freeGpuTotal === 'number' ||
        typeof partition.freeNodes === 'number'
      );
    }

    function partitionHasFreeResources(partition) {
      if (!partition) return false;
      const freeCpu = typeof partition.freeCpuTotal === 'number' ? partition.freeCpuTotal : 0;
      const freeGpu = typeof partition.freeGpuTotal === 'number' ? partition.freeGpuTotal : 0;
      const freeNodes = typeof partition.freeNodes === 'number' ? partition.freeNodes : 0;
      return freeCpu > 0 || freeGpu > 0 || freeNodes > 0;
    }

    function resolvePartitionOptions(info) {
      const partitions = info && Array.isArray(info.partitions) ? info.partitions : [];
      const filterFree = Boolean(getValue('filterFreeResources'));
      if (!filterFree) {
        return { partitions, filtered: false };
      }
      const hasFreeData = partitions.some((partition) => hasFreeResourceData(partition));
      if (!hasFreeData) {
        return { partitions, filtered: false };
      }
      const filtered = partitions.filter((partition) => partitionHasFreeResources(partition));
      return { partitions: filtered, filtered: true };
    }

    function buildClusterInfoSummary(info) {
      if (!info || !Array.isArray(info.partitions) || info.partitions.length === 0) {
        return { text: '', hasFreeData: false };
      }
      const totalCount = info.partitions.length;
      const hasFreeData = info.partitions.some((partition) => hasFreeResourceData(partition));
      if (!hasFreeData) {
        return {
          text: 'Cluster info loaded: ' + totalCount + ' partitions; free-resource data unavailable.',
          hasFreeData: false
        };
      }
      const freeCount = info.partitions.filter((partition) => partitionHasFreeResources(partition)).length;
      return {
        text: 'Cluster info loaded: ' + totalCount + ' partitions; ' + freeCount + ' have free resources.',
        hasFreeData: true
      };
    }

    function updatePartitionDetails() {
      if (!clusterInfo || !clusterInfo.partitions) {
        updateResourceWarning();
        return;
      }
      const selected = getValue('defaultPartition').trim();
      let chosen = selected ? clusterInfo.partitions.find((p) => p.name === selected) : undefined;
      if (!chosen && !selected) {
        const fallback = getClusterDefaultPartitionName();
        if (fallback) {
          chosen = clusterInfo.partitions.find((p) => p.name === fallback);
        }
      }
      if (!chosen) {
        const meta = document.getElementById('partitionMeta');
        if (meta) meta.textContent = '';
        setFieldOptions('defaultNodes', [], '');
        setFieldOptions('defaultCpusPerTask', [], '');
        setFieldOptions('defaultMemoryMb', [], '');
        setFieldOptions('defaultGpuType', [], '');
        setFieldOptions('defaultGpuCount', [], '');
        updateResourceWarning();
        return;
      }

      const meta = document.getElementById('partitionMeta');
      if (meta) {
        const gpuSummary = (() => {
          if (!chosen.gpuMax || !chosen.gpuTypes || Object.keys(chosen.gpuTypes).length === 0) {
            return 'GPU: none';
          }
          const parts = Object.keys(chosen.gpuTypes)
            .sort()
            .map((key) => {
              const label = key ? key : 'gpu';
              return label + 'x' + chosen.gpuTypes[key];
            });
          return 'GPU: ' + parts.join(', ');
        })();
        meta.textContent =
          'Nodes: ' +
          chosen.nodes +
          ' | CPUs/node: ' +
          chosen.cpus +
          ' | Mem/node: ' +
          formatMem(chosen.memMb) +
          ' | ' +
          gpuSummary;
      }

      const preferredNodes = getValue('defaultNodes') || lastValues.defaultNodes;
      const preferredCpus = getValue('defaultCpusPerTask') || lastValues.defaultCpusPerTask;
      const preferredMem = getValue('defaultMemoryMb') || lastValues.defaultMemoryMb;
      const filterFree = Boolean(getValue('filterFreeResources'));
      const nodesLimit =
        filterFree && typeof chosen.freeNodes === 'number' ? chosen.freeNodes : chosen.nodes;
      const cpuLimit =
        filterFree && typeof chosen.freeCpusPerNode === 'number' ? chosen.freeCpusPerNode : chosen.cpus;

      setFieldOptions('defaultNodes', buildRangeOptions(nodesLimit), preferredNodes);
      setFieldOptions('defaultCpusPerTask', buildRangeOptions(cpuLimit), preferredCpus);
      setFieldOptions('defaultMemoryMb', buildMemoryOptions(chosen.memMb), preferredMem);

      const gpuTypes = chosen.gpuTypes || {};
      const hasFreeGpuData = filterFree && chosen.freeGpuTypes !== undefined;
      const effectiveGpuTypes = hasFreeGpuData ? chosen.freeGpuTypes : gpuTypes;
      const gpuTypeKeys = Object.keys(effectiveGpuTypes || {});
      const preferredGpuType = getValue('defaultGpuType') || lastValues.defaultGpuType;
      const preferredGpuCount = getValue('defaultGpuCount') || lastValues.defaultGpuCount;

      if (gpuTypeKeys.length === 0) {
        setFieldOptions('defaultGpuType', [{ value: '', label: 'None' }], '');
        setFieldOptions('defaultGpuCount', [{ value: '0', label: '0' }], preferredGpuCount);
        setFieldDisabled('defaultGpuType', true);
        setFieldDisabled('defaultGpuCount', true);
      } else {
        const typeOptions = [{ value: '', label: 'Any' }];
        gpuTypeKeys.sort().forEach((key) => {
          const label = key ? key : 'Generic';
          typeOptions.push({ value: key, label });
        });
        setFieldOptions('defaultGpuType', typeOptions, preferredGpuType);
        const selectedType = getValue('defaultGpuType');
        const maxGpu = selectedType
          ? (effectiveGpuTypes || {})[selectedType]
          : filterFree && typeof chosen.freeGpuMax === 'number'
            ? chosen.freeGpuMax
            : chosen.gpuMax;
        const countOptions = [{ value: '0', label: '0' }, ...buildRangeOptions(maxGpu)];
        setFieldOptions('defaultGpuCount', countOptions, preferredGpuCount);
        setFieldDisabled('defaultGpuType', false);
        setFieldDisabled('defaultGpuCount', false);
      }
      updateResourceWarning();
    }

    function applyPartitionDefaults() {
      if (!clusterInfo || !clusterInfo.partitions) {
        setStatus('Load cluster info to apply partition defaults.', true);
        return;
      }
      const selected = getValue('defaultPartition').trim();
      let chosen = selected ? clusterInfo.partitions.find((p) => p.name === selected) : undefined;
      if (!chosen && !selected) {
        const fallback = getClusterDefaultPartitionName();
        if (fallback) {
          chosen = clusterInfo.partitions.find((p) => p.name === fallback);
        }
      }
      if (!chosen) {
        setStatus('Select a partition before applying defaults.', true);
        return;
      }

      const setFieldValue = (key, value) => {
        setValue(key, value);
        lastValues[key] = value;
      };
      const applyNumberDefault = (key, value) => {
        if (typeof value === 'number' && value > 0) {
          setFieldValue(key, String(value));
        }
      };
      const applyStringDefault = (key, value) => {
        if (typeof value === 'string' && value.trim().length > 0) {
          setFieldValue(key, value.trim());
        }
      };

      applyNumberDefault('defaultNodes', chosen.defaultNodes);
      applyNumberDefault('defaultTasksPerNode', chosen.defaultTasksPerNode);
      applyNumberDefault('defaultCpusPerTask', chosen.defaultCpusPerTask);
      applyNumberDefault('defaultMemoryMb', chosen.defaultMemoryMb);

      applyStringDefault('defaultGpuType', chosen.defaultGpuType);
      applyNumberDefault('defaultGpuCount', chosen.defaultGpuCount);

      if (typeof chosen.defaultTime === 'string' && chosen.defaultTime.trim().length > 0) {
        setFieldValue('defaultTime', chosen.defaultTime.trim());
      } else {
        setFieldValue('defaultTime', '24:00:00');
      }

      updatePartitionDetails();
      scheduleAutoSave();
    }

    function applyClusterInfo(info, options = {}) {
      const manualPartition = getValue('defaultPartition');
      clusterInfo = info;
      setModuleOptions(info && Array.isArray(info.modules) ? info.modules : []);
      updateFreeResourceWarning();
      updateResourceWarning();
      if (!info || !info.partitions || info.partitions.length === 0) {
        setStatus('No partitions found.', true);
        clearResourceOptions();
        setResourceDisabled(false);
        return;
      }
      setStatus('', false);
      setResourceDisabled(false);
      const resolved = resolvePartitionOptions(info);
      const partitionOptions = resolved.partitions.map((partition) => ({
        value: partition.name,
        label: partition.isDefault ? partition.name + ' (default)' : partition.name
      }));
      const resolvedDefault = getClusterDefaultPartitionName();
      const preferredPartition = manualPartition || lastValues.defaultPartition || resolvedDefault;
      setFieldOptions('defaultPartition', partitionOptions, preferredPartition);
      if (!manualPartition && !lastValues.defaultPartition && resolvedDefault) {
        lastValues.defaultPartition = resolvedDefault;
      }
      updatePartitionDetails();
      const shouldAnnounce = options.announce !== false;
      if (shouldAnnounce) {
        // Intentionally avoid extra success hints; action bar shows timestamp.
      }
    }

    function setValue(id, value) {
      if (resourceFields[id]) {
        const field = resourceFields[id];
        const input = document.getElementById(field.input);
        if (input) input.value = value ?? '';
        return;
      }
      const el = document.getElementById(id);
      if (!el) return;
      if (el.type === 'checkbox') {
        el.checked = Boolean(value);
      } else {
        el.value = value ?? '';
      }
    }

    function getValue(id) {
      if (resourceFields[id]) {
        const field = resourceFields[id];
        const el = document.getElementById(field.input);
        return el ? el.value || '' : '';
      }
      const el = document.getElementById(id);
      if (!el) return '';
      if (el.type === 'checkbox') {
        return el.checked;
      }
      return el.value || '';
    }

    function gather() {
      syncModuleLoadValue();
      return {
        loginHosts: getValue('loginHosts'),
        loginHostsCommand: getValue('loginHostsCommand'),
        loginHostsQueryHost: getValue('loginHostsQueryHost'),
        partitionCommand: getValue('partitionCommand'),
        partitionInfoCommand: getValue('partitionInfoCommand'),
        filterFreeResources: getValue('filterFreeResources'),
        qosCommand: getValue('qosCommand'),
        accountCommand: getValue('accountCommand'),
        user: getValue('user'),
        identityFile: getValue('identityFile'),
        preSshCommand: getValue('preSshCommand'),
        preSshCheckCommand: getValue('preSshCheckCommand'),
        additionalSshOptions: getValue('additionalSshOptions'),
        moduleLoad: getValue('moduleLoad'),
        moduleSelections: selectedModules.slice(),
        moduleCustomCommand: customModuleCommand,
        localProxyEnabled: getValue('localProxyEnabled'),
        extraSallocArgs: getValue('extraSallocArgs'),
        promptForExtraSallocArgs: getValue('promptForExtraSallocArgs'),
        sessionMode: getValue('sessionMode'),
        sessionKey: getValue('sessionKey'),
        sessionIdleTimeoutSeconds: getValue('sessionIdleTimeoutSeconds'),
        defaultPartition: getValue('defaultPartition'),
        defaultNodes: getValue('defaultNodes'),
        defaultTasksPerNode: getValue('defaultTasksPerNode'),
        defaultCpusPerTask: getValue('defaultCpusPerTask'),
        defaultTime: getValue('defaultTime'),
        defaultMemoryMb: getValue('defaultMemoryMb'),
        defaultGpuType: getValue('defaultGpuType'),
        defaultGpuCount: getValue('defaultGpuCount'),
        forwardAgent: getValue('forwardAgent'),
        requestTTY: getValue('requestTTY'),
        openInNewWindow: getValue('openInNewWindow'),
        remoteWorkspacePath: getValue('remoteWorkspacePath')
      };
    }

    function applyResolvedSshHost(message) {
      const host = String(message.host || '').trim();
      if (message.error) {
        const label = host ? '"' + host + '"' : 'host';
        setSshHostHint('Failed to resolve SSH ' + label + ': ' + message.error, true);
        return;
      }

      const currentHost = getSingleLoginHostValue();
      if (currentHost && host && currentHost !== host) {
        return;
      }

      const forceOverwrite = pendingSshResolveForce && pendingSshResolveHost === host;
      if (pendingSshResolveHost === host) {
        pendingSshResolveHost = '';
        pendingSshResolveForce = false;
      }

      let changed = false;
      let needsAgentRefresh = false;
      const loginValue = String(getValue('loginHosts') || '').trim();
      const loginTokens = loginValue.split(/[\\s,]+/).filter(Boolean);
      const isSingleLogin = loginTokens.length === 1 ? loginTokens[0] : '';
      const canOverwrite = (fieldKey, currentValue) => {
        if (forceOverwrite) {
          return true;
        }
        if (!currentValue) {
          return true;
        }
        const previousAuto = autoFilledValues[fieldKey] || '';
        return currentValue === previousAuto;
      };
      const canOverwriteLoginHost =
        forceOverwrite || !loginValue || loginValue === autoFilledValues.loginHosts || isSingleLogin === host;
      const resolvedHost = String(message.hostname || '').trim();
      if (resolvedHost && canOverwriteLoginHost) {
        setValue('loginHosts', resolvedHost);
        lastValues.loginHosts = resolvedHost;
        autoFilledValues.loginHosts = resolvedHost;
        changed = true;
      }

      const resolvedUser = String(message.user || '').trim();
      const currentUser = String(getValue('user') || '').trim();
      if (resolvedUser) {
        if (canOverwrite('user', currentUser)) {
          setValue('user', resolvedUser);
          lastValues.user = resolvedUser;
          autoFilledValues.user = resolvedUser;
          changed = true;
        }
      } else if (forceOverwrite && message.hasExplicitHost) {
        setValue('user', '');
        lastValues.user = '';
        autoFilledValues.user = '';
        changed = true;
      }

      const resolvedIdentity = String(message.identityFile || '').trim();
      const currentIdentity = String(getValue('identityFile') || '').trim();
      if (resolvedIdentity) {
        if (canOverwrite('identityFile', currentIdentity)) {
          setValue('identityFile', resolvedIdentity);
          lastValues.identityFile = resolvedIdentity;
          autoFilledValues.identityFile = resolvedIdentity;
          changed = true;
          needsAgentRefresh = true;
        }
      } else if (forceOverwrite && message.hasExplicitHost) {
        setValue('identityFile', '');
        lastValues.identityFile = '';
        autoFilledValues.identityFile = '';
        changed = true;
        needsAgentRefresh = true;
      }

      const warnings = [];
      if (message.hasProxyJump || message.hasProxyCommand) {
        warnings.push('Host uses ProxyJump/ProxyCommand; not imported.');
      }
      const hints = [];
      if (host) {
        hints.push('Resolved from SSH host "' + host + '".');
      }
      if (resolvedHost && resolvedHost !== host) {
        hints.push('Using HostName ' + resolvedHost + '.');
      }
      if (warnings.length > 0) {
        hints.push(warnings.join(' '));
      }
      if (hints.length > 0) {
        setSshHostHint(hints.join(' '), warnings.length > 0);
      }

      if (changed) {
        scheduleAutoSave();
        updateResourceSummary();
      }
      if (needsAgentRefresh) {
        vscode.postMessage({
          command: 'refreshAgentStatus',
          identityFile: resolvedIdentity
        });
      }
    }

    function applyMessageState(message) {
      if (message.profiles) {
        updateProfileList(message.profiles, message.activeProfile);
      }
      if (message.connectionState) {
        setConnectState(message.connectionState);
      }
      if (message.profileStatus) {
        setProfileStatus(message.profileStatus, Boolean(message.profileError));
      }
      if (message.agentStatus) {
        setAgentStatus(message.agentStatus, Boolean(message.agentStatusError));
      }
    }

    window.addEventListener('message', (event) => {
      const message = event.data;
      if (message.command === 'load') {
        setClusterInfoLoading(false);
        suppressAutoSave = true;
        const values = message.values || {};
        lastValues = values;
        if (message.defaults) {
          uiDefaults = message.defaults;
        }
        connectedSessionMode = message.connectionSessionMode || '';
        if (message.profileSummaries) {
          profileSummaries = message.profileSummaries;
        }
        Object.keys(values).forEach((key) => setValue(key, values[key]));
        const saveTarget = document.getElementById('saveTarget');
        if (saveTarget && message.saveTarget) {
          saveTarget.value = message.saveTarget;
        }
        applyModuleState(values);
        setSessionOptions(Array.isArray(message.sessions) ? message.sessions : []);
        if (message.clusterInfo) {
          setClusterInfoFetchedAt(message.clusterInfoCachedAt);
          applyClusterInfo(message.clusterInfo);
          if (message.clusterInfoCachedAt) {
            // Keep action bar timestamp only; avoid extra hint text here.
          }
        } else {
          clusterInfo = null;
          setClusterInfoFetchedAt(null);
          clearResourceOptions();
          const meta = document.getElementById('partitionMeta');
          if (meta) meta.textContent = '';
          setModuleOptions([]);
          setResourceDisabled(false);
          updateResourceWarning();
        }
        applyMessageState(message);
        updateProfileSummary(getSelectedProfileName());
        suppressAutoSave = false;
        updateActionBarSpacer();
      } else if (message.command === 'clusterInfo') {
        setClusterInfoLoading(false);
        setClusterInfoFetchedAt(message.fetchedAt || new Date());
        applyClusterInfo(message.info);
        setSessionOptions(Array.isArray(message.sessions) ? message.sessions : []);
        applyMessageState(message);
      } else if (message.command === 'clusterInfoError') {
        setClusterInfoLoading(false);
        setStatus(message.message || 'Failed to load cluster info.', true);
        setResourceDisabled(false);
        setSessionOptions(Array.isArray(message.sessions) ? message.sessions : []);
        applyMessageState(message);
      } else if (message.command === 'sshHosts') {
        setSshHostOptions(message.hosts, message.source, message.error);
      } else if (message.command === 'sshHostResolved') {
        applyResolvedSshHost(message);
      } else if (message.command === 'agentStatus') {
        applyMessageState(message);
      } else if (message.command === 'pickedPath') {
        const field = String(message.field || '').trim();
        if (field) {
          setValue(field, message.value || '');
          lastValues[field] = message.value || '';
          scheduleAutoSave();
          if (field === 'identityFile') {
            vscode.postMessage({
              command: 'refreshAgentStatus',
              identityFile: message.value || ''
            });
          }
        }
      } else if (message.command === 'profiles') {
        if (message.profileSummaries) {
          profileSummaries = message.profileSummaries;
        }
        applyMessageState(message);
        updateProfileSummary(getSelectedProfileName());
      } else if (message.command === 'connectionState') {
        connectedSessionMode = message.sessionMode || '';
        setConnectState(message.state || 'idle');
      }
    });

    window.addEventListener('resize', () => {
      updateActionBarSpacer();
    });

    const pickIdentityButton = document.getElementById('pickIdentityFile');
    if (pickIdentityButton) {
      pickIdentityButton.addEventListener('click', () => {
        vscode.postMessage({ command: 'pickIdentityFile' });
      });
    }

    document.getElementById('getClusterInfo').addEventListener('click', () => {
      setStatus('', false);
      setClusterInfoLoading(true);
      vscode.postMessage({
        command: 'getClusterInfo',
        values: gather()
      });
    });

    document.getElementById('clearClusterInfo').addEventListener('click', () => {
      setClusterInfoLoading(false);
      clearClusterInfoUi();
    });

    document.getElementById('fillPartitionDefaults').addEventListener('click', () => {
      applyPartitionDefaults();
    });

    document.getElementById('clearResources').addEventListener('click', () => {
      clearResourceFields();
    });

    const moduleAddButton = document.getElementById('moduleAdd');
    const moduleClearButton = document.getElementById('moduleClearAll');
    const moduleInput = document.getElementById('moduleInput');
    const moduleMenu = document.getElementById('moduleMenu');
    const modulePicker = document.getElementById('modulePicker');
    const ignoreNextInputClicks = new Set();

    function shouldIgnoreInputClick(inputId) {
      if (ignoreNextInputClicks.has(inputId)) {
        ignoreNextInputClicks.delete(inputId);
        return true;
      }
      return false;
    }

    function isInsideAnyPicker(target) {
      if (!(target instanceof Node)) {
        return false;
      }
      if (modulePicker && modulePicker.contains(target)) {
        return true;
      }
      const loginHostPicker = document.getElementById(sshHostField.picker);
      if (loginHostPicker && loginHostPicker.contains(target)) {
        return true;
      }
      return Object.values(resourceFields).some((field) => {
        const picker = document.getElementById(field.picker);
        return picker ? picker.contains(target) : false;
      });
    }
    if (moduleAddButton) {
      moduleAddButton.addEventListener('click', () => {
        addModulesFromInput();
      });
    }
    if (moduleClearButton) {
      moduleClearButton.addEventListener('click', () => {
        selectedModules = [];
        customModuleCommand = '';
        renderModuleList();
      });
    }
    if (moduleInput) {
      moduleInput.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
          hideModuleMenu();
          return;
        }
        if (event.key === 'ArrowDown') {
          showModuleMenu(true);
          return;
        }
        if (event.key === 'Enter') {
          event.preventDefault();
          addModulesFromInput();
        }
      });
      moduleInput.addEventListener('paste', (event) => {
        const text = event.clipboardData ? event.clipboardData.getData('text') : '';
        if (!text) return;
        event.preventDefault();
        addModulesFromInput(text);
      });
      moduleInput.addEventListener('click', () => {
        if (shouldIgnoreInputClick('moduleInput')) {
          return;
        }
        const isOpen = moduleMenu && moduleMenu.classList.contains('visible');
        if (isOpen) {
          hideModuleMenu();
        } else {
          showModuleMenu(true);
        }
      });
      moduleInput.addEventListener('input', () => {
        showModuleMenu(false);
      });
      moduleInput.addEventListener('blur', () => {
        setTimeout(() => {
          hideModuleMenu();
        }, 150);
      });
    }
    if (moduleMenu) {
      moduleMenu.addEventListener('mousedown', (event) => {
        event.preventDefault();
      });
    }

    const loginHostInput = document.getElementById(sshHostField.input);
    const loginHostMenu = document.getElementById(sshHostField.menu);
    if (loginHostInput) {
      loginHostInput.addEventListener('focus', () => {
        showSshHostHint = true;
        renderSshHostHint();
      });
      loginHostInput.addEventListener('blur', () => {
        showSshHostHint = false;
        renderSshHostHint();
      });
      loginHostInput.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
          hideSshHostMenu();
          return;
        }
        if (event.key === 'ArrowDown') {
          event.preventDefault();
          moveSshHostMenuSelection(1);
          return;
        }
        if (event.key === 'ArrowUp') {
          event.preventDefault();
          moveSshHostMenuSelection(-1);
          return;
        }
        if (event.key === 'Enter') {
          const menuVisible = loginHostMenu && loginHostMenu.classList.contains('visible');
          if (menuVisible && sshHostMenuItems.length > 0 && sshHostMenuIndex >= 0) {
            event.preventDefault();
            const selected = sshHostMenuItems[sshHostMenuIndex];
            if (selected) {
              chooseSshHostOption(selected.value, true);
              return;
            }
          }
          hideSshHostMenu();
          maybeResolveTypedHost();
        }
      });
      loginHostInput.addEventListener('input', () => {
        lastResolvedSshHost = '';
        updateSshHostMenuFromInput();
        updateResourceSummary();
      });
      loginHostInput.addEventListener('blur', () => {
        setTimeout(() => {
          hideSshHostMenu();
        }, 150);
        maybeResolveTypedHost();
      });
      loginHostInput.addEventListener('change', () => {
        if (suppressSshHostChangeResolve) {
          return;
        }
        maybeResolveTypedHost();
        updateResourceSummary();
      });
    }
    if (loginHostMenu) {
      loginHostMenu.addEventListener('mousedown', (event) => {
        event.preventDefault();
      });
    }

    Object.keys(resourceFields).forEach((fieldKey) => {
      const field = resourceFields[fieldKey];
      const input = document.getElementById(field.input);
      const menu = document.getElementById(field.menu);
      if (!input) return;
      input.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
          hideFieldMenu(fieldKey);
          return;
        }
        if (event.key === 'ArrowDown') {
          showFieldMenu(fieldKey, true);
          return;
        }
        if (event.key === 'Enter') {
          hideFieldMenu(fieldKey);
        }
      });
      input.addEventListener('click', () => {
        if (shouldIgnoreInputClick(field.input)) {
          return;
        }
        const isOpen = menu && menu.classList.contains('visible');
        if (isOpen) {
          hideFieldMenu(fieldKey);
        } else {
          showFieldMenu(fieldKey, true);
        }
      });
      input.addEventListener('input', () => {
        showFieldMenu(fieldKey, false);
      });
      input.addEventListener('blur', () => {
        setTimeout(() => {
          hideFieldMenu(fieldKey);
        }, 150);
      });
      if (menu) {
        menu.addEventListener('mousedown', (event) => {
          event.preventDefault();
        });
      }
    });
    document.addEventListener('mousedown', (event) => {
      const target = event.target;
      if (target instanceof Element) {
        const label = target.closest('label');
        if (label && label.htmlFor) {
          ignoreNextInputClicks.add(label.htmlFor);
        }
      }
      if (isInsideAnyPicker(target)) {
        return;
      }
      closeAllMenus();
    });

    function addFieldListener(fieldKey, handler) {
      const field = resourceFields[fieldKey];
      if (!field) return;
      const input = document.getElementById(field.input);
      if (input) {
        input.addEventListener('change', handler);
        input.addEventListener('input', handler);
      }
    }

    addFieldListener('defaultPartition', updatePartitionDetails);
    addFieldListener('defaultGpuType', updatePartitionDetails);
    addFieldListener('defaultNodes', updateResourceWarning);
    addFieldListener('defaultCpusPerTask', updateResourceWarning);
    addFieldListener('defaultMemoryMb', updateResourceWarning);
    addFieldListener('defaultGpuCount', updateResourceWarning);

    const tasksPerNodeInput = document.getElementById('defaultTasksPerNode');
    if (tasksPerNodeInput) {
      tasksPerNodeInput.addEventListener('change', updateResourceWarning);
      tasksPerNodeInput.addEventListener('input', updateResourceWarning);
    }
    const defaultTimeInput = document.getElementById('defaultTime');
    if (defaultTimeInput) {
      defaultTimeInput.addEventListener('change', updateResourceWarning);
      defaultTimeInput.addEventListener('input', updateResourceWarning);
    }

    const filterToggle = document.getElementById('filterFreeResources');
    if (filterToggle) {
      filterToggle.addEventListener('change', () => {
        if (clusterInfo) {
          applyClusterInfo(clusterInfo, { announce: false });
        }
      });
    }

    setInterval(() => {
      updateFreeResourceWarning();
    }, 60000);

    document.getElementById('profileSelect').addEventListener('change', () => {
      updateProfileSummary(getSelectedProfileName());
    });

    document.getElementById('profileSave').addEventListener('click', () => {
      setProfileStatus('Saving profile...', false);
      vscode.postMessage({
        command: 'saveProfile',
        suggestedName: getSelectedProfileName(),
        values: gather()
      });
    });

    document.getElementById('profileLoad').addEventListener('click', () => {
      const name = getSelectedProfileName();
      if (!name) {
        setProfileStatus('Select a profile to load.', true);
        return;
      }
      setProfileStatus('Loading profile...', false);
      vscode.postMessage({
        command: 'loadProfile',
        name
      });
    });

    document.getElementById('profileDelete').addEventListener('click', () => {
      const name = getSelectedProfileName();
      if (!name) {
        setProfileStatus('Select a profile to delete.', true);
        return;
      }
      setProfileStatus('Deleting profile...', false);
      vscode.postMessage({
        command: 'deleteProfile',
        name
      });
    });

    document.getElementById('connectToggle').addEventListener('click', () => {
      if (connectionState === 'connecting') {
        setConnectState('idle');
        vscode.postMessage({ command: 'cancelConnect' });
        return;
      }
      if (connectionState === 'connected') {
        setConnectState('disconnecting');
        vscode.postMessage({ command: 'disconnect' });
        return;
      }
      setConnectState('connecting');
      vscode.postMessage({
        command: 'connect',
        values: gather(),
        sessionSelection: getSelectedSessionKey(),
        target: document.getElementById('saveTarget').value
      });
    });

    const cancelOnlyButton = document.getElementById('cancelJob');
    if (cancelOnlyButton) {
      cancelOnlyButton.addEventListener('click', () => {
        if (connectionState !== 'connected') {
          return;
        }
        vscode.postMessage({ command: 'cancelSession' });
      });
    }

    document.getElementById('refresh').addEventListener('click', () => {
      vscode.postMessage({ command: 'ready' });
    });

    const resetAdvancedButton = document.getElementById('resetAdvancedDefaults');
    if (resetAdvancedButton) {
      resetAdvancedButton.addEventListener('click', () => {
        resetAdvancedToDefaults();
      });
    }
    document.getElementById('openSettings').addEventListener('click', () => {
      vscode.postMessage({ command: 'openSettings' });
    });

    document.getElementById('openLogs').addEventListener('click', () => {
      vscode.postMessage({ command: 'openLogs' });
    });

    document.getElementById('openRemoteSshLog').addEventListener('click', () => {
      vscode.postMessage({ command: 'openRemoteSshLog' });
    });

    initializeAutoSave();
    setConnectState('idle');
    vscode.postMessage({ command: 'ready' });
    //# sourceURL=slurm-connect-webview.js
  </script>
</body>
</html>`;
}

function formatError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}
