import * as vscode from 'vscode';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs/promises';
import * as crypto from 'crypto';
import * as zlib from 'zlib';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { expandHome } from './utils/sshConfig';
import { ClusterInfo } from './utils/clusterInfo';
import { quoteShellArg, splitShellArgs } from './utils/shellArgs';
import { normalizeProxyPort as normalizeProxyPortUtil } from './utils/localProxy';
import type {
  LocalProxyTunnelMode,
  ResolvedSshHostInfo,
  SessionMode,
  SlurmConnectConfig,
  SshHostKeyCheckingMode
} from './config/types';
import {
  cancelActiveConnectToken as cancelActiveConnectTokenState,
  createConnectToken as createConnectTokenState,
  finalizeConnectToken as finalizeConnectTokenState,
  isConnectCancelled,
  type ConnectToken,
  type ConnectTokenState,
  type ConnectionState
} from './state/connectionState';
import type {
  ClusterUiCache,
  ProfileResourceSummary,
  ProfileStore,
  ProfileSummary,
  ProfileValues,
  UiValues
} from './state/types';
import {
  buildProfileSummaryMap as buildProfileSummaryMapState,
  getActiveProfileName as getActiveProfileNameState,
  getProfileStore as getProfileStoreState,
  getProfileSummaries as getProfileSummariesState,
  getProfileValues as getProfileValuesState,
  setActiveProfileName as setActiveProfileNameState,
  updateProfileStore as updateProfileStoreState
} from './state/profiles';
import {
  buildWebviewValues as buildWebviewValuesState,
  filterProfileValues as filterProfileValuesState,
  getClusterUiCache as getClusterUiCacheState,
  getMergedClusterUiCache as getMergedClusterUiCacheState,
  getUiValuesFromConfig as getUiValuesFromConfigState,
  mergeUiValuesWithDefaults as mergeUiValuesWithDefaultsState,
  pickProfileValues as pickProfileValuesState,
  resolvePreferredSaveTarget as resolvePreferredSaveTargetState,
  updateClusterUiCache as updateClusterUiCacheState
} from './state/uiCache';
import {
  buildClusterInfoWebviewMessage,
  queryPartitions as queryPartitionsConnect,
  querySimpleList as querySimpleListConnect,
  resolveDefaultPartitionForHost as resolveDefaultPartitionForHostConnect,
  resolvePartitionDefaultTimeForHost as resolvePartitionDefaultTimeForHostConnect,
  validateGpuRequestAgainstCachedClusterInfo as validateGpuRequestAgainstCachedClusterInfoConnect,
  type ClusterInfoMessageRuntime,
  type ClusterQueryRuntime
} from './connect/clusterQueries';
import { runConnectFlow, type ConnectFlowRuntime } from './connect/flow';
import type { LocalProxyPlan } from './connect/types';
import {
  createSshCommandRuntime,
  type SshCommandRuntime
} from './ssh/commands';
import {
  collectSshConfigHosts as collectSshConfigHostsSsh,
  ensureSshIncludeInstalled as ensureSshIncludeInstalledSsh,
  normalizeRemoteConfigPath as normalizeRemoteConfigPathSsh,
  resolveRemoteConfigContext as resolveRemoteConfigContextSsh,
  resolveSlurmConnectIncludeFilePath as resolveSlurmConnectIncludeFilePathSsh,
  resolveSshHostFromConfig as resolveSshHostFromConfigSsh,
  resolveSshHostsConfigPath as resolveSshHostsConfigPathSsh,
  writeSlurmConnectIncludeFile as writeSlurmConnectIncludeFileSsh
} from './ssh/config';
import {
  connectToHost as connectToHostRemoteSsh,
  disconnectFromHost as disconnectFromHostRemoteSsh,
  ensureRemoteSshSettings as ensureRemoteSshSettingsRemoteSsh,
  migrateStaleRemoteSshConfigIfNeeded as migrateStaleRemoteSshConfigIfNeededRemoteSsh,
  openRemoteSshLog as openRemoteSshLogRemoteSsh,
  refreshRemoteSshHosts as refreshRemoteSshHostsRemoteSsh,
  waitForRemoteConnectionOrTimeout as waitForRemoteConnectionOrTimeoutRemoteSsh
} from './ssh/remoteSsh';
import {
  applyStoredConnectionStateIfMissing as applyStoredConnectionStateIfMissingLocalProxyState,
  getCachedRemoteHome as getCachedRemoteHomeLocalProxyState,
  getStoredConnectionState as getStoredConnectionStateLocalProxyState,
  resolveSessionKey as resolveSessionKeyLocalProxyState,
  storeConnectionState as storeConnectionStateLocalProxyState,
  storeRemoteHome as storeRemoteHomeLocalProxyState
} from './localProxy/state';
import {
  createLocalProxyRuntime,
  type LocalProxyRuntime
} from './localProxy/runtime';
import { SlurmConnectViewProvider } from './webview/provider';

const execFileAsync = promisify(execFile);

export interface TestConnectInvocation {
  alias: string;
  openInNewWindow: boolean;
  remoteWorkspacePath?: string;
}

export interface TestConnectResult {
  didConnect: boolean;
  alias?: string;
  sessionKey?: string;
  loginHost?: string;
  includeFilePath: string;
  logFilePath: string;
  connectInvocations: TestConnectInvocation[];
}

let outputChannel: vscode.OutputChannel | undefined;
let logFilePath: string | undefined;
let extensionStoragePath: string | undefined;
let extensionRootPath: string | undefined;
let extensionGlobalState: vscode.Memento | undefined;
let extensionWorkspaceState: vscode.Memento | undefined;
let viewProvider: SlurmConnectViewProvider | undefined;
let connectionState: ConnectionState = 'idle';
let lastConnectionAlias: string | undefined;
let lastConnectionSessionKey: string | undefined;
let lastConnectionSessionMode: SessionMode | undefined;
let lastConnectionLoginHost: string | undefined;
const connectTokenState: ConnectTokenState = { connectTokenCounter: 0 };
let preSshCommandInFlight: Promise<void> | undefined;
let logWriteQueue: Promise<void> = Promise.resolve();
let testConnectToHostOverride:
  | ((alias: string, openInNewWindow: boolean, remoteWorkspacePath?: string) => Promise<boolean>)
  | undefined;
let testEnsureRemoteSshSettingsOverride: ((cfg: SlurmConnectConfig) => Promise<void>) | undefined;
let testRemoteSshConfigPathOverride: string | undefined;
let sessionE2EStarted = false;
let sshCommandRuntime: SshCommandRuntime | undefined;
let localProxyRuntime: LocalProxyRuntime | undefined;

const LOG_MAX_BYTES = 5 * 1024 * 1024;
const LOG_TRUNCATE_KEEP_BYTES = 4 * 1024 * 1024;
let clusterInfoRequestInFlight = false;
const SETTINGS_SECTION = 'slurmConnect';
const LEGACY_SETTINGS_SECTION = 'sciamaSlurm';
const PROFILE_STORE_KEY = 'slurmConnect.profiles';
const ACTIVE_PROFILE_KEY = 'slurmConnect.activeProfile';
const CLUSTER_INFO_CACHE_KEY = 'slurmConnect.clusterInfoCache';
const CLUSTER_UI_CACHE_KEY = 'slurmConnect.clusterUiCache';
const LEGACY_PROFILE_STORE_KEY = 'sciamaSlurm.profiles';
const LEGACY_ACTIVE_PROFILE_KEY = 'sciamaSlurm.activeProfile';
const LEGACY_CLUSTER_INFO_CACHE_KEY = 'sciamaSlurm.clusterInfoCache';
const DEFAULT_PROXY_SCRIPT_INSTALL_PATH = '~/.slurm-connect/vscode-proxy.py';
const PROXY_OVERRIDE_RESET_KEY = 'slurmConnect.proxyOverrideReset';
const SSH_AGENT_ENV_KEY = 'slurmConnect.sshAgentEnv';
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
  'startupCommand',
  'proxyCommand',
  'proxyArgs',
  'proxyDebugLogging',
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

function getSshCommandRuntime(): SshCommandRuntime {
  if (!sshCommandRuntime) {
    sshCommandRuntime = createSshCommandRuntime({
      getOutputChannel,
      fileExists,
      getStoredSshAgentEnv,
      storeSshAgentEnv,
      formatError,
      refreshAgentStatus,
      openSlurmConnectView,
      ensurePreSshCommand,
      runSshCommandInTerminal,
      runSshAddInTerminal,
      runSshKeygenPublicKeyInTerminal
    });
  }
  return sshCommandRuntime;
}

function getSshConfigRuntime() {
  const sshRuntime = getSshCommandRuntime();
  return {
    fileExists,
    getOutputChannel,
    resolveSshToolPath: (tool: 'ssh') => sshRuntime.resolveSshToolPath(tool),
    normalizeSshErrorText: sshRuntime.normalizeSshErrorText,
    pickSshErrorSummary: sshRuntime.pickSshErrorSummary
  };
}

function getLocalProxyRuntime(): LocalProxyRuntime {
  if (!localProxyRuntime) {
    const sshRuntime = getSshCommandRuntime();
    localProxyRuntime = createLocalProxyRuntime({
      getOutputChannel,
      formatError,
      getGlobalState: () => extensionGlobalState,
      getConfig,
      getConnectionState: () => connectionState,
      getStoredConnectionState,
      resolveRemoteSshAlias,
      hasSessionE2EContext,
      resolveSshToolPath: (tool) => sshRuntime.resolveSshToolPath(tool),
      normalizeSshErrorText: sshRuntime.normalizeSshErrorText,
      pickSshErrorSummary: sshRuntime.pickSshErrorSummary,
      runSshCommand: (host, cfg, command) => sshRuntime.runSshCommand(host, cfg, command),
      runPreSshCommandInTerminal
    });
  }
  return localProxyRuntime;
}

async function runStartupMigrations(): Promise<void> {
  await migrateLegacyState();
  await migrateLegacySettings();
  await migrateLegacyModuleCommands();
  await resetProxyOverridesToDefaults();
}

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  extensionStoragePath = context.globalStorageUri.fsPath;
  extensionRootPath = context.extensionUri.fsPath;
  extensionGlobalState = context.globalState;
  extensionWorkspaceState = context.workspaceState;
  await runStartupMigrations();
  syncConnectionStateFromEnvironment();
  void resumeLocalProxyForRemoteSession();
  const disposable = vscode.commands.registerCommand('slurmConnect.connect', async () => connectCommand());
  context.subscriptions.push(disposable);

  void migrateStaleRemoteSshConfigIfNeeded();

  viewProvider = new SlurmConnectViewProvider(context, {
    getWebviewHtml,
    syncConnectionStateFromEnvironment,
    getReadyPayload,
    getActiveProfileName,
    getProfileStore,
    getProfileSummaries,
    buildProfileSummaryMap,
    getUiGlobalDefaults,
    getProfileValues,
    resolvePreferredSaveTarget,
    firstLoginHostFromInput,
    getCachedClusterInfo,
    pickProfileValues,
    setActiveProfileName,
    updateProfileStore,
    buildOverridesFromUi,
    updateConfigFromUi,
    resolveSshHostFromConfig,
    getConfigWithOverrides,
    buildAgentStatusMessage,
    handleConnectMessage,
    handleCancelConnectMessage,
    handleDisconnectMessage,
    handleCancelSessionMessage,
    handleClusterInfoRequest,
    loadSshHosts,
    openLogs: openLogFile,
    openRemoteSshLog,
    openSettings: () => {
      void vscode.commands.executeCommand('workbench.action.openSettings', SETTINGS_SECTION);
    },
    formatError
  });
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider('slurmConnect.connectView', viewProvider)
  );
  maybeRunSessionE2E();
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
  if (extensionGlobalState && !extensionGlobalState.get<boolean>(PROXY_OVERRIDE_RESET_KEY)) {
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
    const current = getClusterUiCache(extensionWorkspaceState);
    const { next, changed } = stripProxyOverridesFromCache(current);
    if (changed) {
      await extensionWorkspaceState.update(CLUSTER_UI_CACHE_KEY, next);
    }
    await extensionWorkspaceState.update(PROXY_OVERRIDE_RESET_KEY, true);
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
  await openRemoteSshLogRemoteSsh();
}

async function loadSshHosts(overrides?: Partial<SlurmConnectConfig>): Promise<{
  hosts: string[];
  source?: string;
  error?: string;
}> {
  try {
    const cfg = getConfigWithOverrides(overrides);
    const configPath = await resolveSshHostsConfigPathSsh(cfg, getSshConfigRuntime());
    if (!configPath) {
      return {
        hosts: [],
        error: 'SSH config not found.'
      };
    }
    return {
      hosts: await collectSshConfigHostsSsh(configPath, getSshConfigRuntime()),
      source: configPath
    };
  } catch (error) {
    return {
      hosts: [],
      error: formatError(error)
    };
  }
}

async function openSlurmConnectView(): Promise<void> {
  await viewProvider?.openView();
}

async function refreshAgentStatus(identityFile: string): Promise<void> {
  await viewProvider?.refreshAgentStatus(identityFile);
}

function setConnectionState(state: ConnectionState): void {
  connectionState = state;
  void viewProvider?.setConnectionState(state, lastConnectionSessionMode);
}

function createConnectToken(): ConnectToken {
  return createConnectTokenState(connectTokenState);
}

function cancelActiveConnectToken(): void {
  cancelActiveConnectTokenState(connectTokenState);
}

function finalizeConnectToken(token?: ConnectToken): void {
  finalizeConnectTokenState(connectTokenState, token);
}

function resolveRemoteAuthority(): string | undefined {
  const folders = vscode.workspace.workspaceFolders;
  const folderAuthority = folders && folders.length > 0 ? folders[0].uri.authority : undefined;
  const envAuthority = process.env.VSCODE_REMOTE_AUTHORITY;
  return folderAuthority || envAuthority;
}

function resolveRemoteSshAlias(): string | undefined {
  if (vscode.env.remoteName !== 'ssh-remote') {
    return undefined;
  }
  const authority = resolveRemoteAuthority();
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

function parseSessionE2EOverrides(): Partial<SlurmConnectConfig> {
  const raw = process.env.SLURM_CONNECT_SESSION_E2E_CONNECT_OVERRIDES;
  if (!raw) {
    return {};
  }
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('SLURM_CONNECT_SESSION_E2E_CONNECT_OVERRIDES must be a JSON object.');
  }
  return parsed as Partial<SlurmConnectConfig>;
}

function hasSessionE2EContext(): boolean {
  return (
    Boolean(process.env.SLURM_CONNECT_SESSION_E2E_MODE) ||
    Boolean(process.env.SLURM_CONNECT_SESSION_E2E_REMOTE_MARKER) ||
    Boolean(process.env.SLURM_CONNECT_SESSION_E2E_STATUS_MARKER)
  );
}

async function writeSessionE2EJson(filePath: string | undefined, payload: Record<string, unknown>): Promise<void> {
  if (!filePath) {
    return;
  }
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(payload, null, 2), 'utf8');
}

async function writeSessionE2EStatus(phase: string, details: Record<string, unknown> = {}): Promise<void> {
  await writeSessionE2EJson(process.env.SLURM_CONNECT_SESSION_E2E_STATUS_MARKER, {
    phase,
    remoteName: vscode.env.remoteName || '',
    authority: resolveRemoteAuthority() || '',
    alias: resolveRemoteSshAlias() || '',
    ...details
  });
}

function buildSessionE2ERemotePayload(authority: string | undefined): Record<string, unknown> {
  return {
    remoteName: vscode.env.remoteName || '',
    authority: authority || '',
    alias: resolveRemoteSshAlias() || '',
    hostname: os.hostname(),
    slurmJobId: process.env.SLURM_JOB_ID || process.env.SLURM_JOBID || '',
    timestamp: new Date().toISOString()
  };
}

async function maybeWriteSessionE2ERemoteFsMarker(authority: string | undefined): Promise<void> {
  const remoteMarkerPath = process.env.SLURM_CONNECT_SESSION_E2E_REMOTE_FS_MARKER;
  if (!remoteMarkerPath || !authority) {
    return;
  }
  const uri = vscode.Uri.from({
    scheme: 'vscode-remote',
    authority,
    path: remoteMarkerPath
  });
  const contents = Buffer.from(
    JSON.stringify(buildSessionE2ERemotePayload(authority), null, 2),
    'utf8'
  );
  let lastError: unknown;
  for (let attempt = 0; attempt < 30; attempt += 1) {
    try {
      await vscode.workspace.fs.writeFile(uri, contents);
      return;
    } catch (error) {
      lastError = error;
      if (attempt === 29) {
        break;
      }
      await delay(1000);
    }
  }
  throw lastError;
}

async function handleSessionE2ERemoteWindow(): Promise<void> {
  const authority = resolveRemoteAuthority();
  const alias = resolveRemoteSshAlias() || '';
  const expectedAlias = process.env.SLURM_CONNECT_SESSION_E2E_EXPECTED_ALIAS || '';
  const payload: Record<string, unknown> = {
    ...buildSessionE2ERemotePayload(authority),
    alias,
    expectedAlias
  };
  try {
    await maybeWriteSessionE2ERemoteFsMarker(authority);
    if (expectedAlias && alias !== expectedAlias) {
      payload.error = `Expected alias ${expectedAlias}, got ${alias}`;
    }
    await writeSessionE2EJson(process.env.SLURM_CONNECT_SESSION_E2E_REMOTE_MARKER, payload);
    await writeSessionE2EStatus(payload.error ? 'remote-marker-error' : 'remote-marker-written', payload);
  } catch (error) {
    payload.error = formatError(error);
    await writeSessionE2EJson(process.env.SLURM_CONNECT_SESSION_E2E_REMOTE_MARKER, payload);
    await writeSessionE2EStatus('remote-marker-error', payload);
  }
}

async function handleSessionE2ELocalWindow(): Promise<void> {
  await writeSessionE2EStatus('local-start');
  try {
    await delay(1000);
    await writeSessionE2EStatus('local-command-ready');
    const mode = process.env.SLURM_CONNECT_SESSION_E2E_MODE || 'noninteractive';
    const result =
      mode === 'command'
        ? await vscode.commands.executeCommand<boolean>('slurmConnect.connect')
        : await connectCommand(parseSessionE2EOverrides(), { interactive: false });
    const didConnect = result === undefined ? true : Boolean(result);
    await writeSessionE2EStatus(didConnect ? 'local-command-complete' : 'local-command-failed', {
      result: didConnect
    });
  } catch (error) {
    await writeSessionE2EStatus('local-command-error', { error: formatError(error) });
  }
}

function maybeRunSessionE2E(): void {
  if (sessionE2EStarted) {
    return;
  }
  if (!hasSessionE2EContext()) {
    return;
  }
  sessionE2EStarted = true;
  if (vscode.env.remoteName === 'ssh-remote') {
    void handleSessionE2ERemoteWindow();
    return;
  }
  if (vscode.env.remoteName !== 'ssh-remote') {
    void handleSessionE2ELocalWindow();
  }
}

function syncConnectionStateFromEnvironment(): void {
  applyStoredConnectionStateIfMissing();
  const remoteActive = vscode.env.remoteName === 'ssh-remote';
  if (remoteActive) {
    if (connectionState === 'idle') {
      connectionState = 'connected';
    }
    const alias = resolveRemoteSshAlias();
    if (alias) {
      lastConnectionAlias = alias;
      storeConnectionState({
        alias,
        sessionKey: lastConnectionSessionKey,
        sessionMode: lastConnectionSessionMode,
        loginHost: lastConnectionLoginHost
      });
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
    storeConnectionState({
      alias: lastConnectionAlias,
      sessionKey: lastConnectionSessionKey,
      sessionMode: lastConnectionSessionMode,
      loginHost: lastConnectionLoginHost
    });
    return;
  }
  if (connectionState === 'connected') {
    connectionState = 'idle';
  }
}

function mergeUiValuesWithDefaults(values?: Partial<UiValues>, defaults?: UiValues): UiValues {
  return mergeUiValuesWithDefaultsState(values, defaults ?? getUiValuesFromStorage());
}

function pickProfileValues(values: Partial<UiValues>): ProfileValues {
  return pickProfileValuesState(values);
}

function buildWebviewValues(values: UiValues): Partial<UiValues> {
  return buildWebviewValuesState(values);
}

function filterProfileValues(values: Partial<UiValues>): { next: ProfileValues; changed: boolean } {
  return filterProfileValuesState(values);
}

function getClusterUiCache(state?: vscode.Memento): ClusterUiCache | undefined {
  return getClusterUiCacheState(state, CLUSTER_UI_CACHE_KEY);
}

function getMergedClusterUiCache(): ClusterUiCache | undefined {
  return getMergedClusterUiCacheState({
    globalState: extensionGlobalState,
    workspaceState: extensionWorkspaceState,
    hasWorkspace: Boolean(vscode.workspace.workspaceFolders && vscode.workspace.workspaceFolders.length > 0),
    cacheKey: CLUSTER_UI_CACHE_KEY
  });
}

function resolvePreferredSaveTarget(): 'global' | 'workspace' {
  return resolvePreferredSaveTargetState({
    workspaceFolders: vscode.workspace.workspaceFolders,
    workspaceState: extensionWorkspaceState,
    cacheKey: CLUSTER_UI_CACHE_KEY
  });
}

async function updateClusterUiCache(values: ProfileValues, target: vscode.ConfigurationTarget): Promise<void> {
  await updateClusterUiCacheState({
    values,
    target,
    globalState: extensionGlobalState,
    workspaceState: extensionWorkspaceState,
    workspaceFolders: vscode.workspace.workspaceFolders,
    cacheKey: CLUSTER_UI_CACHE_KEY
  });
}

function getProfileStore(): ProfileStore {
  return getProfileStoreState(extensionGlobalState, PROFILE_STORE_KEY);
}

async function updateProfileStore(store: ProfileStore): Promise<void> {
  await updateProfileStoreState(extensionGlobalState, PROFILE_STORE_KEY, store);
}

function getActiveProfileName(): string | undefined {
  return getActiveProfileNameState(extensionGlobalState, ACTIVE_PROFILE_KEY);
}

async function setActiveProfileName(name?: string): Promise<void> {
  await setActiveProfileNameState(extensionGlobalState, ACTIVE_PROFILE_KEY, name);
}

function getProfileSummaries(store: ProfileStore): ProfileSummary[] {
  return getProfileSummariesState(store);
}

function getProfileValues(name: string): UiValues | undefined {
  return getProfileValuesState(
    name,
    getProfileStore(),
    mergeUiValuesWithDefaults,
    pickProfileValues,
    getUiGlobalDefaults()
  );
}

function buildProfileSummaryMap(
  store: ProfileStore,
  defaults: UiValues
): Record<string, ProfileResourceSummary> {
  return buildProfileSummaryMapState(store, defaults, mergeUiValuesWithDefaults, pickProfileValues, {
    firstLoginHostFromInput,
    parsePositiveNumberInput,
    parseNonNegativeNumberInput
  });
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
    startupCommand: (cfg.get<string>('startupCommand') || '').trim(),
    proxyCommand: (cfg.get<string>('proxyCommand') || '').trim(),
    proxyArgs: cfg.get<string[]>('proxyArgs', []),
    proxyDebugLogging: cfg.get<boolean>('proxyDebugLogging', false),
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
    startupCommand: String(getDefault<string>('startupCommand', '') || '').trim(),
    proxyCommand: String(getDefault<string>('proxyCommand', '') || '').trim(),
    proxyArgs: getDefault<string[]>('proxyArgs', []),
    proxyDebugLogging: Boolean(getDefault<boolean>('proxyDebugLogging', false)),
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

async function getReadyPayload(): Promise<{
  values: Partial<UiValues>;
  defaults: Partial<UiValues>;
  profiles: ProfileSummary[];
  profileSummaries: Record<string, ProfileResourceSummary>;
  activeProfile?: string;
  connectionState: ConnectionState;
  connectionSessionMode?: SessionMode;
  remoteActive: boolean;
  saveTarget: 'global' | 'workspace';
}> {
  const defaults = getUiValuesFromStorage();
  return {
    values: buildWebviewValues(defaults),
    defaults: buildWebviewValues(getUiDefaultsFromConfig()),
    profiles: getProfileSummaries(getProfileStore()),
    profileSummaries: buildProfileSummaryMap(getProfileStore(), getUiGlobalDefaults()),
    activeProfile: getActiveProfileName(),
    connectionState,
    connectionSessionMode: lastConnectionSessionMode,
    remoteActive: vscode.env.remoteName === 'ssh-remote',
    saveTarget: resolvePreferredSaveTarget()
  };
}

async function handleConnectMessage(
  uiValues: Partial<UiValues>,
  target: vscode.ConfigurationTarget,
  sessionSelection?: string
): Promise<void> {
  const normalizedSelection = sessionSelection?.trim() || '';
  await updateConfigFromUi(uiValues, target);
  if (
    connectionState === 'connecting' &&
    connectTokenState.activeConnectToken &&
    !connectTokenState.activeConnectToken.cancelled
  ) {
    return;
  }
  const token = createConnectToken();
  setConnectionState('connecting');
  const overrides = buildOverridesFromUi(uiValues);
  if (normalizedSelection) {
    overrides.sessionMode = 'persistent';
    overrides.sessionKey = normalizedSelection;
  }
  let connected = false;
  try {
    connected = await connectCommand(overrides, {
      interactive: false,
      aliasOverride: normalizedSelection,
      token
    });
  } catch (error) {
    getOutputChannel().appendLine(`Connect failed: ${formatError(error)}`);
  }
  if (!isConnectCancelled(token)) {
    const expectedAlias = (lastConnectionAlias || '').trim();
    const currentRemoteAlias = (resolveRemoteSshAlias() || '').trim();
    const aliasMatchesCurrentWindow =
      !expectedAlias || (currentRemoteAlias.length > 0 && currentRemoteAlias === expectedAlias);
    if (connected && vscode.env.remoteName === 'ssh-remote' && !aliasMatchesCurrentWindow) {
      getOutputChannel().appendLine(
        `Connect state check mismatch: expected alias "${expectedAlias || '(none)'}", current remote alias "${currentRemoteAlias || '(none)'}".`
      );
    }
    const connectedInCurrentWindow = connected && vscode.env.remoteName === 'ssh-remote' && aliasMatchesCurrentWindow;
    setConnectionState(connectedInCurrentWindow ? 'connected' : 'idle');
  }
  finalizeConnectToken(token);
}

function handleCancelConnectMessage(): void {
  cancelActiveConnectToken();
  stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: false });
  setConnectionState('idle');
}

async function handleDisconnectMessage(): Promise<void> {
  setConnectionState('disconnecting');
  const disconnected = await disconnectFromHost(lastConnectionAlias);
  if (disconnected) {
    lastConnectionSessionKey = undefined;
    lastConnectionSessionMode = undefined;
    lastConnectionLoginHost = undefined;
  }
  setConnectionState(disconnected ? 'idle' : 'connected');
}

async function handleCancelSessionMessage(): Promise<void> {
  if (lastConnectionSessionMode !== 'persistent') {
    void vscode.window.showWarningMessage('No persistent Slurm session is active to cancel.');
    return;
  }
  getOutputChannel().appendLine(
    `Cancel job requested. remoteName=${vscode.env.remoteName || 'local'}, alias=${lastConnectionAlias || '(none)'}, sessionKey=${lastConnectionSessionKey || '(none)'}, loginHost=${lastConnectionLoginHost || '(none)'}`
  );
  const label = lastConnectionSessionKey || lastConnectionAlias || 'current session';
  const choice = await vscode.window.showWarningMessage(
    `Cancel job "${label}"? This will end the allocation and close the remote connection. Close this dialog to keep it running.`,
    { modal: true },
    'Cancel job'
  );
  if (choice !== 'Cancel job') {
    return;
  }
  const cfg = getConfig();
  const aliasFromRemote = resolveRemoteSshAlias();
  const stored = getStoredConnectionState();
  const aliasForSession = lastConnectionAlias || stored?.alias || aliasFromRemote || '';
  const sessionKey =
    lastConnectionSessionKey ||
    stored?.sessionKey ||
    (aliasForSession ? resolveSessionKey(cfg, aliasForSession) : '');
  let loginHost =
    lastConnectionLoginHost ||
    stored?.loginHost ||
    (cfg.loginHosts.length > 0 ? cfg.loginHosts[0] : undefined);
  if (!loginHost && aliasForSession) {
    try {
      const resolved = await resolveSshHostFromConfig(aliasForSession, cfg);
      loginHost = resolved.hostname || resolved.host || loginHost;
    } catch {
      // Ignore; we'll fall back to the existing value.
    }
  }
  const isRemoteSession = vscode.env.remoteName === 'ssh-remote';
  getOutputChannel().appendLine(
    `Cancel job resolved. alias=${aliasForSession || '(none)'}, sessionKey=${sessionKey || '(none)'}, loginHost=${loginHost || '(none)'}, remote=${isRemoteSession}`
  );
  if (!sessionKey) {
    getOutputChannel().appendLine(
      'Cancel job: session key unavailable; will rely on SLURM_JOB_ID in the remote terminal.'
    );
  }
  let cancelSent = false;
  if (loginHost || isRemoteSession) {
    cancelSent = await cancelPersistentSessionJob(loginHost || aliasForSession || 'remote', sessionKey, cfg, {
      useTerminal: true
    });
  } else {
    getOutputChannel().appendLine(
      'Cancel job failed to resolve login host or session key; cannot proceed.'
    );
    void vscode.window.showWarningMessage(
      'Unable to resolve the login host or session key to cancel the Slurm job.'
    );
  }
  if (!cancelSent) {
    return;
  }
  setConnectionState('disconnecting');
  const disconnected = await disconnectFromHost(aliasForSession || lastConnectionAlias);
  if (disconnected) {
    lastConnectionSessionKey = undefined;
    lastConnectionSessionMode = undefined;
    lastConnectionLoginHost = undefined;
    setConnectionState('idle');
  } else {
    setConnectionState('connected');
    void vscode.window.showWarningMessage(
      'Job cancel command was sent, but the Remote-SSH window stayed connected. Disconnect manually if needed.'
    );
  }
}

async function connectCommand(
  overrides?: Partial<SlurmConnectConfig>,
  options?: { interactive?: boolean; aliasOverride?: string; token?: ConnectToken }
): Promise<boolean> {
  const cfg = getConfigWithOverrides(overrides);
  const result = await runConnectFlow(createConnectFlowRuntime(), cfg, options);
  const alias = result.alias?.trim() || undefined;
  if (alias) {
    lastConnectionAlias = alias;
  }
  if (result.didConnect) {
    lastConnectionSessionKey = result.sessionKey;
    lastConnectionSessionMode = result.sessionMode;
    lastConnectionLoginHost = result.loginHost;
    storeConnectionState({
      alias: lastConnectionAlias,
      sessionKey: lastConnectionSessionKey,
      sessionMode: lastConnectionSessionMode,
      loginHost: lastConnectionLoginHost
    });
    if (result.loginHost) {
      const cachedHome = getCachedRemoteHome(result.loginHost);
      if (!cachedHome) {
        void fetchRemoteHomeNoPrompt(result.loginHost, cfg).then((home) => {
          if (home) {
            storeRemoteHome(result.loginHost || '', home);
          }
        });
      }
    }
  } else if (result.clearSessionState) {
    lastConnectionSessionKey = undefined;
    lastConnectionSessionMode = undefined;
    lastConnectionLoginHost = undefined;
  }
  return result.didConnect;
}

function createClusterQueryRuntime(): ClusterQueryRuntime {
  const sshRuntime = getSshCommandRuntime();
  return {
    getOutputChannel,
    runSshCommand: (loginHost, cfg, command) => sshRuntime.runSshCommand(loginHost, cfg, command),
    runSshCommandWithInput: (loginHost, cfg, command, input) =>
      sshRuntime.runSshCommandWithInput(loginHost, cfg, command, input),
    formatError,
    showWarningMessage: (message) => {
      void vscode.window.showWarningMessage(message);
    },
    getCachedClusterInfo,
    cacheClusterInfo,
    wrapPythonScriptCommand
  };
}

function createClusterInfoMessageRuntime(): ClusterInfoMessageRuntime {
  const sshRuntime = getSshCommandRuntime();
  return {
    ...createClusterQueryRuntime(),
    buildOverridesFromUi,
    getConfigWithOverrides,
    parseListInput,
    maybePromptForSshAuthOnConnect: (cfg, loginHost) =>
      sshRuntime.maybePromptForSshAuthOnConnect(cfg, loginHost),
    buildAgentStatusMessage: (identityFile) => sshRuntime.buildAgentStatusMessage(identityFile),
    showErrorMessage: (message) => {
      void vscode.window.showErrorMessage(message);
    }
  };
}

function createConnectFlowRuntime(): ConnectFlowRuntime {
  const sshRuntime = getSshCommandRuntime();
  return {
    getOutputChannel,
    formatError,
    showErrorMessage: (message) => {
      void vscode.window.showErrorMessage(message);
    },
    showWarningMessage: (message, ...items) => vscode.window.showWarningMessage(message, ...items),
    showInputBox: (options) => vscode.window.showInputBox(options),
    showQuickPick: (items, options) => vscode.window.showQuickPick(items, options),
    withProgress: async (options, task) => await vscode.window.withProgress(options, () => task()),
    runSshCommand: (loginHost, cfg, command) => sshRuntime.runSshCommand(loginHost, cfg, command),
    maybePromptForSshAuthOnConnect: (cfg, loginHost) =>
      sshRuntime.maybePromptForSshAuthOnConnect(cfg, loginHost),
    queryPartitions: (loginHost, cfg) => queryPartitionsConnect(createClusterQueryRuntime(), loginHost, cfg),
    querySimpleList: (loginHost, cfg, command) =>
      querySimpleListConnect(createClusterQueryRuntime(), loginHost, cfg, command),
    resolveDefaultPartitionForHost: (loginHost, cfg) =>
      resolveDefaultPartitionForHostConnect(createClusterQueryRuntime(), loginHost, cfg),
    resolvePartitionDefaultTimeForHost: (loginHost, partition, cfg) =>
      resolvePartitionDefaultTimeForHostConnect(createClusterQueryRuntime(), loginHost, partition, cfg),
    validateGpuRequestAgainstCachedClusterInfo: (loginHost, partition, gpuCount, gpuType) =>
      validateGpuRequestAgainstCachedClusterInfoConnect(createClusterQueryRuntime(), loginHost, partition, gpuCount, gpuType),
    resolveSessionKey,
    getProxyInstallSnippet,
    normalizeLocalProxyTunnelMode,
    ensureRemoteSshSettings: (flowCfg) =>
      testEnsureRemoteSshSettingsOverride
        ? testEnsureRemoteSshSettingsOverride(flowCfg)
        : ensureRemoteSshSettingsRemoteSsh(flowCfg, sshRuntime, fileExists, { formatError }),
    migrateStaleRemoteSshConfigIfNeeded: () =>
      migrateStaleRemoteSshConfigIfNeededRemoteSsh({ getOutputChannel }),
    resolveRemoteConfigContext: (flowCfg) =>
      resolveRemoteConfigContextSsh(
        flowCfg,
        { fileExists, getOutputChannel },
        testRemoteSshConfigPathOverride ||
          normalizeRemoteConfigPathSsh(vscode.workspace.getConfiguration('remote.SSH').get<string>('configFile'))
      ),
    ensureLocalProxyPlan,
    buildRemoteCommand,
    ensureSshAgentEnvForCurrentSsh: () => sshRuntime.ensureSshAgentEnvForCurrentSsh(),
    resolveSshToolPath: (tool) => sshRuntime.resolveSshToolPath(tool),
    hasSshOption: (options, key) => sshRuntime.hasSshOption(options, key),
    redactRemoteCommandForLog,
    writeSlurmConnectIncludeFile: (entry, includePath) =>
      writeSlurmConnectIncludeFileSsh(entry, includePath),
    ensureSshIncludeInstalled: (baseConfigPath, includePath) =>
      ensureSshIncludeInstalledSsh(baseConfigPath, includePath, { getOutputChannel }),
    refreshRemoteSshHosts: () => refreshRemoteSshHostsRemoteSsh(),
    ensurePreSshCommand,
    connectToHost: (alias, openInNewWindow, remoteWorkspacePath) =>
      testConnectToHostOverride
        ? testConnectToHostOverride(alias, openInNewWindow, remoteWorkspacePath)
        : connectToHostRemoteSsh(alias, openInNewWindow, remoteWorkspacePath, sshRuntime, {
            getOutputChannel,
            formatError
          }),
    delay,
    waitForRemoteConnectionOrTimeout: (timeoutMs, pollMs) =>
      waitForRemoteConnectionOrTimeoutRemoteSsh(timeoutMs, pollMs, { delay }),
    resolveSshIdentityAgentOption: (sshPath) => sshRuntime.resolveSshIdentityAgentOption(sshPath),
    getCurrentLocalProxyPort: () => getLocalProxyRuntime().getCurrentLocalProxyPort(),
    showOutput: () => {
      getOutputChannel().show(true);
    },
    isConnectCancelled,
    stopLocalProxyServer: () => {
      getLocalProxyRuntime().stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: false });
    }
  };
}

async function handleClusterInfoRequest(values: Partial<UiValues>, webview: vscode.Webview): Promise<void> {
  if (clusterInfoRequestInFlight) {
    return;
  }
  clusterInfoRequestInFlight = true;
  try {
    const message = await buildClusterInfoWebviewMessage(createClusterInfoMessageRuntime(), values);
    await webview.postMessage(message);
  } finally {
    clusterInfoRequestInFlight = false;
  }
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

interface StoredConnectionState {
  alias?: string;
  sessionKey?: string;
  sessionMode?: SessionMode;
  loginHost?: string;
  updatedAt?: number;
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

function getStoredConnectionState(): StoredConnectionState | undefined {
  return getStoredConnectionStateLocalProxyState(extensionGlobalState);
}

function storeConnectionState(state: StoredConnectionState): void {
  storeConnectionStateLocalProxyState(extensionGlobalState, state);
}

function getCachedRemoteHome(loginHost: string): string | undefined {
  return getCachedRemoteHomeLocalProxyState(extensionGlobalState, loginHost);
}

function storeRemoteHome(loginHost: string, homeDir: string): void {
  storeRemoteHomeLocalProxyState(extensionGlobalState, loginHost, homeDir);
}

function applyStoredConnectionStateIfMissing(): void {
  const merged = applyStoredConnectionStateIfMissingLocalProxyState(
    {
      alias: lastConnectionAlias,
      sessionKey: lastConnectionSessionKey,
      sessionMode: lastConnectionSessionMode,
      loginHost: lastConnectionLoginHost
    },
    getStoredConnectionState()
  );
  lastConnectionAlias = merged.alias;
  lastConnectionSessionKey = merged.sessionKey;
  lastConnectionSessionMode = merged.sessionMode;
  lastConnectionLoginHost = merged.loginHost;
}

type SshToolName = 'ssh' | 'ssh-add' | 'ssh-keygen';

async function ensureSshAgentEnvForCurrentSsh(identityPathRaw?: string): Promise<void> {
  await getSshCommandRuntime().ensureSshAgentEnvForCurrentSsh(identityPathRaw);
}

async function resolveSshToolPath(tool: SshToolName): Promise<string> {
  return await getSshCommandRuntime().resolveSshToolPath(tool as 'ssh' | 'ssh-add' | 'ssh-keygen');
}

function appendSshHostKeyCheckingArgs(args: string[], cfg: SlurmConnectConfig): void {
  const mode = normalizeSshHostKeyChecking(cfg.sshHostKeyChecking);
  args.push('-o', `StrictHostKeyChecking=${mode}`);
}

async function fetchRemoteHomeNoPrompt(loginHost: string, cfg: SlurmConnectConfig): Promise<string | undefined> {
  if (!loginHost) {
    return undefined;
  }
  const sshPath = await resolveSshToolPath('ssh');
  const args = buildSshArgs(loginHost, cfg, 'printf %s "$HOME"', { batchMode: true });
  try {
    const { stdout } = await execFileAsync(sshPath, args, { timeout: 5000 });
    const trimmed = stdout.trim();
    if (trimmed.startsWith('/') && !trimmed.includes('\u0000')) {
      return trimmed;
    }
  } catch {
    // Ignore failures; we'll fall back to a safe path.
  }
  return undefined;
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
  const hasRemoteAuthority = Boolean(vscode.env.remoteName || process.env.VSCODE_REMOTE_AUTHORITY);
  if (hasRemoteAuthority) {
    if (!vscode.workspace.workspaceFolders || vscode.workspace.workspaceFolders.length === 0) {
      const cfg = getConfig();
      const stored = getStoredConnectionState();
      let loginHost = stored?.loginHost || lastConnectionLoginHost || '';
      let resolvedUser = cfg.user?.trim() || '';
      const alias = stored?.alias || lastConnectionAlias || resolveRemoteSshAlias() || '';
      if ((!loginHost || !resolvedUser) && alias) {
        try {
          const resolved = await resolveSshHostFromConfig(alias, cfg);
          loginHost = loginHost || resolved.hostname || resolved.host || '';
          resolvedUser = resolvedUser || (resolved.user || '').trim();
        } catch {
          // Ignore resolve failures; we'll fall back.
        }
      }
      const cachedHome = loginHost ? getCachedRemoteHome(loginHost) : undefined;
      if (cachedHome) {
        return cachedHome;
      }
      const fetched = loginHost ? await fetchRemoteHomeNoPrompt(loginHost, cfg) : undefined;
      if (fetched) {
        storeRemoteHome(loginHost, fetched);
        return fetched;
      }
      if (resolvedUser) {
        return `/home/${resolvedUser}`;
      }
      return '/tmp';
    }
    return undefined;
  }
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
  command: string,
  input?: string
): Promise<string> {
  const normalizedCommand =
    process.platform === 'win32' ? normalizeRemoteCommandForWindowsTerminal(command) : command;
  const args = buildSshArgs(host, cfg, normalizedCommand, { batchMode: false });
  const sshPath = await resolveSshToolPath('ssh');
  const { dirPath, stdoutPath, statusPath } = await createTerminalSshRunFiles();
  let stdinPath: string | undefined;
  if (input !== undefined) {
    stdinPath = path.join(dirPath, 'stdin.txt');
    const normalizedInput = input.endsWith('\n') ? input : `${input}\n`;
    await fs.writeFile(stdinPath, normalizedInput, 'utf8');
  }
  const isWindows = process.platform === 'win32';
  const shellCommand = isWindows
    ? (() => {
        const psArgs = args.map((arg) => quotePowerShellString(arg)).join(', ');
        const psScript = [
          "$ErrorActionPreference = 'Continue'",
          `$sshPath = ${quotePowerShellString(sshPath)}`,
          stdinPath ? `$stdinPath = ${quotePowerShellString(stdinPath)}` : '',
          `$stdoutPath = ${quotePowerShellString(stdoutPath)}`,
          `$statusPath = ${quotePowerShellString(statusPath)}`,
          psArgs ? `$sshArgs = @(${psArgs})` : '$sshArgs = @()',
          '$exitCode = 1',
          'try {',
          stdinPath
            ? [
                '  $inputText = Get-Content -Raw $stdinPath',
                '  $inputText = $inputText.Replace("`r", "")',
                '  $stdinNormalizedPath = Join-Path ([System.IO.Path]::GetDirectoryName($stdinPath)) "stdin-normalized.txt"',
                '  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)',
                '  [System.IO.File]::WriteAllText($stdinNormalizedPath, $inputText, $utf8NoBom)',
                '  $proc = Start-Process -FilePath $sshPath -ArgumentList $sshArgs -NoNewWindow -Wait -PassThru -RedirectStandardInput $stdinNormalizedPath -RedirectStandardOutput $stdoutPath',
                '  $exitCode = if ($null -ne $proc) { $proc.ExitCode } else { $LASTEXITCODE }'
              ].join('; ')
            : [
                '  $proc = Start-Process -FilePath $sshPath -ArgumentList $sshArgs -NoNewWindow -Wait -PassThru -RedirectStandardOutput $stdoutPath',
                '  $exitCode = if ($null -ne $proc) { $proc.ExitCode } else { $LASTEXITCODE }'
              ].join('; '),
          '} catch {',
          '  $exitCode = 1',
          '}',
          '$exitCode | Out-File -FilePath $statusPath -Encoding ascii -NoNewline'
        ].filter(Boolean).join('; ');
        const encoded = Buffer.from(psScript, 'utf16le').toString('base64');
        return `powershell -NoProfile -EncodedCommand ${encoded}`;
      })()
    : (() => {
        const sshCommand = [sshPath, ...args].map(quoteShellArg).join(' ');
        if (stdinPath) {
          const inputCommand = `cat ${quoteShellArg(stdinPath)} | ${sshCommand}`;
          return `${inputCommand} > ${quoteShellArg(stdoutPath)}; printf "%s" $? > ${quoteShellArg(statusPath)}`;
        }
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

async function runPreSshCommandInTerminal(
  command: string,
  options?: {
    terminalName?: string;
    promptMessage?: string;
    failureMessage?: string;
    wrapInBash?: boolean;
  }
): Promise<void> {
  const { dirPath, statusPath } = await createTerminalSshRunFiles();
  const isWindows = process.platform === 'win32';
  const wrappedCommand =
    !isWindows && options?.wrapInBash ? `bash -lc ${quoteShellArg(command)}` : command;
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
    : `${wrappedCommand}; printf "%s" $? > ${quoteShellArg(statusPath)}`;

  const terminal = await createLocalTerminal(options?.terminalName || 'Slurm Connect Auth');
  terminal.show(true);
  terminal.sendText(shellCommand, true);
  const promptMessage = options?.promptMessage ?? 'Complete the pre-SSH authentication in the terminal.';
  if (promptMessage.trim().length > 0) {
    void vscode.window.showInformationMessage(promptMessage);
  }

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
    throw new Error(
      options?.failureMessage || 'Pre-SSH command failed in the terminal. Check the terminal output for details.'
    );
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

function getPublicKeyPath(identityPath: string): string {
  return identityPath.endsWith('.pub') ? identityPath : `${identityPath}.pub`;
}

async function buildAgentStatusMessage(identityPathRaw: string): Promise<{ text: string; isError: boolean }> {
  return await getSshCommandRuntime().buildAgentStatusMessage(identityPathRaw);
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
        const psEnvLines: string[] = [];
        if (agentEnv.sock) {
          psEnvLines.push(`$env:SSH_AUTH_SOCK = ${quotePowerShellString(agentEnv.sock)}`);
        }
        if (agentEnv.pid) {
          psEnvLines.push(`$env:SSH_AGENT_PID = ${quotePowerShellString(agentEnv.pid)}`);
        }
        const psScript = [
          "$ErrorActionPreference = 'Continue'",
          `$stdoutPath = ${quotePowerShellString(stdoutPath)}`,
          `$statusPath = ${quotePowerShellString(statusPath)}`,
          `$cmd = ${quotePowerShellString(sshAdd)}`,
          ...psEnvLines,
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
        const sshCommand = [quoteShellArg(sshAdd), ...args.map(quoteShellArg)].join(' ');
        const envPrefix = agentEnv.sock
          ? `SSH_AUTH_SOCK=${quoteShellArg(agentEnv.sock)}${agentEnv.pid ? ` SSH_AGENT_PID=${quoteShellArg(agentEnv.pid)}` : ''} `
          : '';
        return `${envPrefix}${sshCommand} > ${quoteShellArg(stdoutPath)}; printf "%s" $? > ${quoteShellArg(statusPath)}`;
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

function resolveSessionKey(cfg: SlurmConnectConfig, alias: string): string {
  return resolveSessionKeyLocalProxyState(cfg, alias);
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

async function cancelPersistentSessionJob(
  loginHost: string,
  sessionKey: string | undefined,
  cfg: SlurmConnectConfig,
  options?: { useTerminal?: boolean }
): Promise<boolean> {
  return await getLocalProxyRuntime().cancelPersistentSessionJob(loginHost, sessionKey, cfg, options);
}

async function ensureLocalProxyPlan(
  cfg: SlurmConnectConfig,
  loginHost: string,
  options?: { remotePortOverride?: number }
): Promise<LocalProxyPlan> {
  return await getLocalProxyRuntime().ensureLocalProxyPlan(cfg, loginHost, options);
}

function buildRemoteCommand(
  cfg: SlurmConnectConfig,
  sallocArgs: string[],
  sessionKey?: string,
  clientId?: string,
  installSnippet?: string,
  localProxyEnv?: { proxyUrl: string; noProxy?: string },
  localProxyPlan?: LocalProxyPlan
): string {
  return getLocalProxyRuntime().buildRemoteCommand(
    cfg,
    sallocArgs,
    sessionKey,
    clientId,
    installSnippet,
    localProxyEnv,
    localProxyPlan
  );
}

function stopLocalProxyServer(options?: { stopTunnel?: boolean; clearRuntimeState?: boolean }): void {
  getLocalProxyRuntime().stopLocalProxyServer(options);
}

async function resumeLocalProxyForRemoteSession(): Promise<void> {
  await getLocalProxyRuntime().resumeLocalProxyForRemoteSession();
}

async function resolveSshHostFromConfig(
  host: string,
  cfg: SlurmConnectConfig
): Promise<ResolvedSshHostInfo> {
  return await resolveSshHostFromConfigSsh(host, cfg, getSshConfigRuntime());
}

async function migrateStaleRemoteSshConfigIfNeeded(): Promise<void> {
  await migrateStaleRemoteSshConfigIfNeededRemoteSsh({ getOutputChannel });
}

export async function __resetForTests(): Promise<void> {
  cancelActiveConnectToken();
  finalizeConnectToken(connectTokenState.activeConnectToken);
  connectionState = 'idle';
  lastConnectionAlias = undefined;
  lastConnectionSessionKey = undefined;
  lastConnectionSessionMode = undefined;
  lastConnectionLoginHost = undefined;
  sshCommandRuntime?.resetState();
  testConnectToHostOverride = undefined;
  testEnsureRemoteSshSettingsOverride = undefined;
  testRemoteSshConfigPathOverride = undefined;
  stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: true });
  localProxyRuntime?.resetState();
  localProxyRuntime = undefined;
  if (extensionGlobalState) {
    try {
      await extensionGlobalState.update(CLUSTER_INFO_CACHE_KEY, undefined);
      await extensionGlobalState.update(CLUSTER_UI_CACHE_KEY, undefined);
      await extensionGlobalState.update(PROXY_OVERRIDE_RESET_KEY, undefined);
    } catch {
      // Ignore cleanup failures during tests.
    }
  }
  if (extensionWorkspaceState) {
    try {
      await extensionWorkspaceState.update(CLUSTER_UI_CACHE_KEY, undefined);
      await extensionWorkspaceState.update(PROXY_OVERRIDE_RESET_KEY, undefined);
    } catch {
      // Ignore cleanup failures during tests.
    }
  }
  try {
    await fs.unlink(resolveLogFilePath());
  } catch {
    // Ignore missing test log files.
  }
}

export async function __runStartupMigrationsForTests(): Promise<void> {
  await runStartupMigrations();
}

export async function __runProxyOverrideCleanupForTests(): Promise<void> {
  if (extensionGlobalState) {
    await extensionGlobalState.update(PROXY_OVERRIDE_RESET_KEY, undefined);
  }
  if (extensionWorkspaceState) {
    await extensionWorkspaceState.update(PROXY_OVERRIDE_RESET_KEY, undefined);
  }
  await resetProxyOverridesToDefaults();
}

export async function __runNonInteractiveConnectForTests(
  overrides: Partial<SlurmConnectConfig> = {},
  options?: {
    aliasOverride?: string;
    connectResult?: boolean;
    bypassRemoteSshSettings?: boolean;
    remoteSshConfigPath?: string;
  }
): Promise<TestConnectResult> {
  const finalOverrides: Partial<SlurmConnectConfig> = {
    ...overrides,
    openInNewWindow: true
  };
  const connectInvocations: TestConnectInvocation[] = [];
  const previousConnectToHostOverride = testConnectToHostOverride;
  const previousEnsureRemoteSshSettingsOverride = testEnsureRemoteSshSettingsOverride;
  const previousRemoteSshConfigPathOverride = testRemoteSshConfigPathOverride;
  testConnectToHostOverride = async (
    alias: string,
    openInNewWindow: boolean,
    remoteWorkspacePath?: string
  ): Promise<boolean> => {
    connectInvocations.push({
      alias,
      openInNewWindow,
      remoteWorkspacePath: remoteWorkspacePath?.trim() || undefined
    });
    return options?.connectResult ?? true;
  };
  testEnsureRemoteSshSettingsOverride = options?.bypassRemoteSshSettings
    ? async () => undefined
    : previousEnsureRemoteSshSettingsOverride;
  testRemoteSshConfigPathOverride = options?.remoteSshConfigPath || previousRemoteSshConfigPathOverride;

  try {
    const didConnect = await connectCommand(finalOverrides, {
      interactive: false,
      aliasOverride: options?.aliasOverride
    });
    const cfg = getConfigWithOverrides(finalOverrides);
    return {
      didConnect,
      alias: lastConnectionAlias,
      sessionKey: lastConnectionSessionKey,
      loginHost: lastConnectionLoginHost,
      includeFilePath: resolveSlurmConnectIncludeFilePathSsh(cfg),
      logFilePath: resolveLogFilePath(),
      connectInvocations
    };
  } finally {
    testConnectToHostOverride = previousConnectToHostOverride;
    testEnsureRemoteSshSettingsOverride = previousEnsureRemoteSshSettingsOverride;
    testRemoteSshConfigPathOverride = previousRemoteSshConfigPathOverride;
  }
}

async function disconnectFromHost(alias?: string): Promise<boolean> {
  return await disconnectFromHostRemoteSsh(alias, {
    getOutputChannel,
    formatError,
    delay,
    stopLocalProxyServer: () => {
      stopLocalProxyServer({ stopTunnel: true, clearRuntimeState: true });
    }
  });
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
  return getUiValuesFromConfigState({
    cfg,
    cache,
    hasConfigValue,
    formatAdditionalSshOptions
  });
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
  if (has('proxyDebugLogging')) {
    updates.push(cfg.update('proxyDebugLogging', Boolean(values.proxyDebugLogging), target));
  }
  if (has('localProxyNoProxy')) {
    const list = parseListInput(String(values.localProxyNoProxy ?? ''));
    updates.push(cfg.update('localProxyNoProxy', list, target));
  }
  if (has('localProxyPort')) {
    const raw = String(values.localProxyPort ?? '').trim();
    const parsed = raw ? Number(raw) : 0;
    const port = normalizeProxyPortUtil(parsed);
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
  if (has('startupCommand')) overrides.startupCommand = String(values.startupCommand ?? '').trim();
  if (has('extraSallocArgs')) overrides.extraSallocArgs = splitShellArgs(String(values.extraSallocArgs ?? ''));
  if (has('promptForExtraSallocArgs')) {
    overrides.promptForExtraSallocArgs = Boolean(values.promptForExtraSallocArgs);
  }
  if (has('sessionMode')) overrides.sessionMode = normalizeSessionMode(String(values.sessionMode ?? ''));
  if (has('sessionKey')) overrides.sessionKey = String(values.sessionKey ?? '').trim();
  if (has('sessionIdleTimeoutSeconds')) {
    const parsed = parseOptionalNonNegativeNumberInput(String(values.sessionIdleTimeoutSeconds ?? ''));
    if (parsed !== undefined) {
      overrides.sessionIdleTimeoutSeconds = parsed;
    }
  }
  if (has('defaultPartition')) overrides.defaultPartition = String(values.defaultPartition ?? '').trim();
  if (has('defaultNodes')) {
    const parsed = parseOptionalPositiveNumberInput(String(values.defaultNodes ?? ''));
    if (parsed !== undefined) {
      overrides.defaultNodes = parsed;
    }
  }
  if (has('defaultTasksPerNode')) {
    const parsed = parseOptionalPositiveNumberInput(String(values.defaultTasksPerNode ?? ''));
    if (parsed !== undefined) {
      overrides.defaultTasksPerNode = parsed;
    }
  }
  if (has('defaultCpusPerTask')) {
    const parsed = parseOptionalPositiveNumberInput(String(values.defaultCpusPerTask ?? ''));
    if (parsed !== undefined) {
      overrides.defaultCpusPerTask = parsed;
    }
  }
  if (has('defaultTime')) overrides.defaultTime = String(values.defaultTime ?? '').trim();
  if (has('defaultMemoryMb')) {
    const parsed = parseOptionalNonNegativeNumberInput(String(values.defaultMemoryMb ?? ''));
    if (parsed !== undefined) {
      overrides.defaultMemoryMb = parsed;
    }
  }
  if (has('defaultGpuType')) overrides.defaultGpuType = String(values.defaultGpuType ?? '').trim();
  if (has('defaultGpuCount')) {
    const parsed = parseOptionalNonNegativeNumberInput(String(values.defaultGpuCount ?? ''));
    if (parsed !== undefined) {
      overrides.defaultGpuCount = parsed;
    }
  }
  if (has('forwardAgent')) overrides.forwardAgent = Boolean(values.forwardAgent);
  if (has('requestTTY')) overrides.requestTTY = Boolean(values.requestTTY);
  if (has('openInNewWindow')) overrides.openInNewWindow = Boolean(values.openInNewWindow);
  if (has('remoteWorkspacePath')) overrides.remoteWorkspacePath = String(values.remoteWorkspacePath ?? '').trim();

  return overrides;
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

interface BundledProxyFile {
  path: string;
  mode: number;
  base64: string;
}

interface BundledProxyBundle {
  payloadBase64: string;
  hashManifestBase64: string;
  sha256: string;
}

interface ProxyInstallPlan extends BundledProxyBundle {
  installPath: string;
}

let bundledProxyBundleCache: BundledProxyBundle | undefined;

async function collectBundledProxyFiles(baseDir: string, relativeDir: string): Promise<BundledProxyFile[]> {
  const dirPath = path.join(baseDir, relativeDir);
  const entries = await fs.readdir(dirPath, { withFileTypes: true });
  const files: BundledProxyFile[] = [];
  for (const entry of entries) {
    const entryRelativePath = path.posix.join(relativeDir, entry.name);
    const entryPath = path.join(baseDir, entryRelativePath);
    if (entry.isDirectory()) {
      if (entry.name === '__pycache__') {
        continue;
      }
      files.push(...(await collectBundledProxyFiles(baseDir, entryRelativePath)));
      continue;
    }
    if (!entry.isFile() || path.extname(entry.name) !== '.py') {
      continue;
    }
    const [data, stats] = await Promise.all([fs.readFile(entryPath), fs.stat(entryPath)]);
    files.push({
      path: entryRelativePath,
      mode: stats.mode & 0o777,
      base64: data.toString('base64')
    });
  }
  return files.sort((left, right) => left.path.localeCompare(right.path));
}

async function readBundledProxyFile(baseDir: string, relativePath: string): Promise<BundledProxyFile> {
  const filePath = path.join(baseDir, relativePath);
  const [data, stats] = await Promise.all([fs.readFile(filePath), fs.stat(filePath)]);
  return {
    path: relativePath,
    mode: stats.mode & 0o777,
    base64: data.toString('base64')
  };
}

async function getBundledProxyBundle(): Promise<BundledProxyBundle | undefined> {
  if (bundledProxyBundleCache) {
    return bundledProxyBundleCache;
  }
  if (!extensionRootPath) {
    return undefined;
  }
  try {
    const mediaRoot = path.join(extensionRootPath, 'media');
    const proxyFiles = [
      await readBundledProxyFile(mediaRoot, 'vscode-proxy.py'),
      ...(await collectBundledProxyFiles(mediaRoot, 'vscode_proxy')),
    ].sort((left, right) => left.path.localeCompare(right.path));
    const payload = Buffer.from(JSON.stringify({ files: proxyFiles }), 'utf8');
    const hashManifest = Buffer.from(
      JSON.stringify({
        files: proxyFiles.map((file) => ({
          path: file.path,
          mode: file.mode
        }))
      }),
      'utf8'
    );
    const hash = crypto.createHash('sha256');
    for (const file of proxyFiles) {
      hash.update(file.path, 'utf8');
      hash.update('\0', 'utf8');
      hash.update(String(file.mode), 'utf8');
      hash.update('\0', 'utf8');
      hash.update(Buffer.from(file.base64, 'base64'));
      hash.update('\0', 'utf8');
    }
    const sha256 = hash.digest('hex');
    const compressed = zlib.gzipSync(payload, { level: 9 });
    bundledProxyBundleCache = {
      payloadBase64: compressed.toString('base64'),
      hashManifestBase64: hashManifest.toString('base64'),
      sha256
    };
    return bundledProxyBundleCache;
  } catch (error) {
    getOutputChannel().appendLine(`Failed to read bundled proxy bundle: ${formatError(error)}`);
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
  const hashManifest = plan.hashManifestBase64;
  const expected = plan.sha256;
  const chunks: string[] = [];
  const chunkSize = 1024;
  for (let i = 0; i < plan.payloadBase64.length; i += chunkSize) {
    chunks.push(plan.payloadBase64.slice(i, i + chunkSize));
  }
  const hashManifestChunks: string[] = [];
  for (let i = 0; i < hashManifest.length; i += chunkSize) {
    hashManifestChunks.push(hashManifest.slice(i, i + chunkSize));
  }
  const hashSnippet = [
    'import base64,hashlib,json,os,sys',
    'root=sys.argv[1]',
    'entries=json.loads(base64.b64decode("".join([',
    ...hashManifestChunks.map((chunk) => `    "${chunk}",`),
    '])).decode("utf-8")).get("files", [])',
    'h=hashlib.sha256()',
    'for entry in entries:',
    '    rel=entry["path"]',
    '    path=os.path.join(root, rel)',
    '    try:',
    '        data=open(path,"rb").read()',
    '    except Exception:',
    '        sys.exit(1)',
    '    h.update(rel.encode("utf-8"))',
    '    h.update(b"\\0")',
    '    h.update(str(int(entry.get("mode",0))).encode("ascii"))',
    '    h.update(b"\\0")',
    '    h.update(data)',
    '    h.update(b"\\0")',
    'print(h.hexdigest())'
  ].join('\n');
  const installSnippet = [
    `target="${targetEscaped}"`,
    'root="$(dirname "$target")"',
    `expected="${expected}"`,
    'PYTHON=$(command -v python3 || command -v python || true)',
    'if [ -z "$PYTHON" ]; then',
    '  echo "Slurm Connect: python not found; cannot install proxy." >&2',
    '  exit 1',
    'fi',
    'current=""',
    'if [ -f "$target" ]; then',
    '  current=$("$PYTHON" - "$root" 2>/dev/null <<\'PY\'',
    hashSnippet,
    'PY',
    '  )',
    'fi',
    'if [ "$current" != "$expected" ]; then',
    '  umask 077',
    '  mkdir -p "$root"',
    '  "$PYTHON" - "$root" <<\'PY\'',
    'import base64,gzip,io,json,os,sys',
    'root=sys.argv[1]',
    'data=base64.b64decode("".join([',
    ...chunks.map((chunk) => `    "${chunk}",`),
    ']))',
    'try:',
    '    payload=gzip.decompress(data)',
    'except AttributeError:',
    '    payload=gzip.GzipFile(fileobj=io.BytesIO(data)).read()',
    'bundle=json.loads(payload.decode("utf-8"))',
    'for entry in bundle.get("files", []):',
    '    rel=entry["path"]',
    '    path=os.path.join(root, rel)',
    '    os.makedirs(os.path.dirname(path), exist_ok=True)',
    '    tmp=path + ".tmp"',
    '    with open(tmp,"wb") as f:',
    '        f.write(base64.b64decode(entry["base64"]))',
    '    os.chmod(tmp,int(entry.get("mode",0o600)))',
    '    os.replace(tmp,path)',
    'PY',
    '  current=$("$PYTHON" - "$root" 2>/dev/null <<\'PY\'',
    hashSnippet,
    'PY',
    '  )',
    '  if [ "$current" != "$expected" ]; then',
    '    echo "Slurm Connect: proxy bundle hash mismatch after install" >&2',
    '  fi',
    'fi'
  ];
  return installSnippet.join('\n');
}

async function getProxyInstallSnippet(cfg: SlurmConnectConfig): Promise<string | undefined> {
  if (!cfg.autoInstallProxyScriptOnClusterInfo) {
    return undefined;
  }
  const bundled = await getBundledProxyBundle();
  if (!bundled) {
    return undefined;
  }
  return buildProxyInstallSnippet({
    ...bundled,
    installPath: DEFAULT_PROXY_SCRIPT_INSTALL_PATH
  });
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

function parseOptionalPositiveNumberInput(input: string): number | undefined {
  const trimmed = input.trim();
  if (!trimmed) {
    return undefined;
  }
  const parsed = parsePositiveNumberInput(trimmed);
  return parsed > 0 ? parsed : undefined;
}

function parseOptionalNonNegativeNumberInput(input: string): number | undefined {
  const trimmed = input.trim();
  if (!trimmed) {
    return undefined;
  }
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return undefined;
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
  const overrides: Partial<SlurmConnectConfig> = {
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
    startupCommand: String(values.startupCommand ?? '').trim(),
    extraSallocArgs: splitShellArgs(String(values.extraSallocArgs ?? '')),
    promptForExtraSallocArgs: Boolean(values.promptForExtraSallocArgs),
    sessionMode: normalizeSessionMode(String(values.sessionMode ?? '')),
    sessionKey: String(values.sessionKey ?? '').trim(),
    defaultPartition: String(values.defaultPartition ?? '').trim(),
    defaultTime: String(values.defaultTime ?? '').trim(),
    defaultGpuType: String(values.defaultGpuType ?? '').trim(),
    forwardAgent: Boolean(values.forwardAgent),
    requestTTY: Boolean(values.requestTTY),
    openInNewWindow: Boolean(values.openInNewWindow),
    remoteWorkspacePath: String(values.remoteWorkspacePath ?? '').trim()
  };
  const sessionIdleTimeoutSeconds = parseOptionalNonNegativeNumberInput(String(values.sessionIdleTimeoutSeconds ?? ''));
  if (sessionIdleTimeoutSeconds !== undefined) {
    overrides.sessionIdleTimeoutSeconds = sessionIdleTimeoutSeconds;
  }
  const defaultNodes = parseOptionalPositiveNumberInput(String(values.defaultNodes ?? ''));
  if (defaultNodes !== undefined) {
    overrides.defaultNodes = defaultNodes;
  }
  const defaultTasksPerNode = parseOptionalPositiveNumberInput(String(values.defaultTasksPerNode ?? ''));
  if (defaultTasksPerNode !== undefined) {
    overrides.defaultTasksPerNode = defaultTasksPerNode;
  }
  const defaultCpusPerTask = parseOptionalPositiveNumberInput(String(values.defaultCpusPerTask ?? ''));
  if (defaultCpusPerTask !== undefined) {
    overrides.defaultCpusPerTask = defaultCpusPerTask;
  }
  const defaultMemoryMb = parseOptionalNonNegativeNumberInput(String(values.defaultMemoryMb ?? ''));
  if (defaultMemoryMb !== undefined) {
    overrides.defaultMemoryMb = defaultMemoryMb;
  }
  const defaultGpuCount = parseOptionalNonNegativeNumberInput(String(values.defaultGpuCount ?? ''));
  if (defaultGpuCount !== undefined) {
    overrides.defaultGpuCount = defaultGpuCount;
  }
  return overrides;
}

function getWebviewHtml(webview: vscode.Webview, nonceOverride?: string): string {
  const nonce = nonceOverride ?? String(Date.now());
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
    .advanced-label-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 6px;
      margin-bottom: 4px;
    }
    .advanced-label-row > label {
      margin-bottom: 0;
      flex: 1;
    }
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
    .checkbox > label { margin-bottom: 0; }
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
    .cluster-actions { align-items: center; flex-wrap: wrap; }
    .cluster-actions button { flex: 1 1 140px; }
    .cluster-actions .spinner { flex: 0 0 auto; }
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
    .field-reset-button {
      width: auto;
      min-width: 22px;
      padding: 0 6px;
      line-height: 18px;
      font-size: 12px;
      flex: 0 0 auto;
    }
    .checkbox .field-reset-button {
      margin-left: auto;
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
    <label for="startupCommand">Startup command (optional)</label>
    <input id="startupCommand" type="text" placeholder="e.g. . ~/envfile.sh" />
    <label for="additionalSshOptions">Additional SSH options (one per line)</label>
    <textarea id="additionalSshOptions" rows="3" placeholder=""></textarea>
    <div class="checkbox">
      <input id="localProxyEnabled" type="checkbox" />
      <label for="localProxyEnabled">Route remote HTTP(S) through local proxy</label>
    </div>
    <div class="hint">Advanced local proxy settings live in VS Code Settings.</div>
    <div class="checkbox">
      <input id="proxyDebugLogging" type="checkbox" />
      <label for="proxyDebugLogging">Enable proxy debug logging</label>
    </div>
    <div class="hint">Writes proxy logs to <code>~/.slurm-connect/vscode-proxy-[PID].log</code>.</div>
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
    const vscode =
      typeof acquireVsCodeApi === 'function'
        ? acquireVsCodeApi()
        : { postMessage() {}, setState() {}, getState() { return {}; } };
    const FREE_RESOURCE_STALE_MS = 10 * 60 * 1000;
    let clusterInfo = null;
    let clusterInfoFetchedAt = null;
    let lastValues = {};
    let rememberedGpuSelection = { type: '', count: '' };
    let gpuSelectionSuppressedForCpuPartition = false;
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

    const advancedResetFieldIds = [
      'loginHostsCommand',
      'loginHostsQueryHost',
      'partitionInfoCommand',
      'partitionCommand',
      'qosCommand',
      'accountCommand',
      'extraSallocArgs',
      'promptForExtraSallocArgs',
      'sessionMode',
      'sessionKey',
      'sessionIdleTimeoutSeconds',
      'preSshCommand',
      'preSshCheckCommand',
      'startupCommand',
      'additionalSshOptions',
      'localProxyEnabled',
      'proxyDebugLogging',
      'forwardAgent',
      'requestTTY'
    ];

    function resetAdvancedFieldToDefault(fieldId) {
      if (!uiDefaults || !Object.prototype.hasOwnProperty.call(uiDefaults, fieldId)) {
        return false;
      }
      setValue(fieldId, uiDefaults[fieldId] ?? '');
      return true;
    }

    function formatAdvancedDefaultValue(value) {
      if (typeof value === 'boolean') {
        return value ? 'enabled' : 'disabled';
      }
      const text = String(value ?? '').trim();
      if (!text) {
        return 'empty';
      }
      if (text.length > 42) {
        return text.slice(0, 39) + '...';
      }
      return text;
    }

    function updateAdvancedResetButtonTooltip(button, fieldId) {
      if (!button) {
        return;
      }
      if (!uiDefaults || !Object.prototype.hasOwnProperty.call(uiDefaults, fieldId)) {
        button.title = 'Reset this field to the default value';
        return;
      }
      button.title =
        'Reset this field to default (' + formatAdvancedDefaultValue(uiDefaults[fieldId]) + ')';
    }

    function updateAdvancedResetButtonTooltips() {
      advancedResetFieldIds.forEach((fieldId) => {
        const button = document.querySelector('.field-reset-button[data-field-id="' + fieldId + '"]');
        updateAdvancedResetButtonTooltip(button, fieldId);
      });
    }

    function attachAdvancedFieldResetButtons() {
      advancedResetFieldIds.forEach((fieldId) => {
        const label = document.querySelector('label[for="' + fieldId + '"]');
        if (!label) {
          return;
        }
        let button = document.querySelector('.field-reset-button[data-field-id="' + fieldId + '"]');
        if (!button) {
          button = document.createElement('button');
          button.type = 'button';
          button.className = 'field-reset-button button-secondary';
          button.dataset.fieldId = fieldId;
          button.textContent = '↩';
          const labelText = (label.textContent || fieldId).trim();
          button.setAttribute('aria-label', 'Reset "' + labelText + '" to default');
          button.addEventListener('click', (event) => {
            event.preventDefault();
            event.stopPropagation();
            if (resetAdvancedFieldToDefault(fieldId)) {
              scheduleAutoSave();
            }
          });
          const checkboxRow = label.closest('.checkbox');
          if (checkboxRow) {
            checkboxRow.appendChild(button);
          } else {
            const parent = label.parentElement;
            if (!parent) {
              return;
            }
            let row = label.closest('.advanced-label-row');
            if (!row) {
              row = document.createElement('div');
              row.className = 'advanced-label-row';
              parent.insertBefore(row, label);
              row.appendChild(label);
            }
            row.appendChild(button);
          }
        }
        updateAdvancedResetButtonTooltip(button, fieldId);
      });
    }

    function resetAdvancedToDefaults() {
      if (!uiDefaults) {
        return;
      }
      advancedResetFieldIds.forEach((fieldId) => {
        resetAdvancedFieldToDefault(fieldId);
      });
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
        } else if (typeof partition.gpuMax === 'number' && gpuCount > partition.gpuMax) {
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
      if (document.body && document.body.getAttribute('data-slurm-connect-snapshot') === '1') {
        document.body.style.paddingBottom = '12px';
        return;
      }
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
        const allowed = new Set(normalized.map((opt) => opt.value));
        const fallback = normalized.length > 0 ? normalized[0].value : '';
        const candidates = [];
        if (selectedValue !== undefined && selectedValue !== null) {
          candidates.push(String(selectedValue));
        }
        candidates.push(String(input.value || ''), fallback);
        const next =
          normalized.length === 0
            ? ''
            : candidates.find((candidate) => allowed.has(candidate)) || fallback;
        input.value = next;
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
      setRememberedGpuSelection('', '');
      gpuSelectionSuppressedForCpuPartition = false;
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

      const filterFree = Boolean(getValue('filterFreeResources'));
      const hasFreeNodesData = filterFree && typeof chosen.freeNodes === 'number';
      const hasFreeCpuData = filterFree && typeof chosen.freeCpusPerNode === 'number';
      const gpuTypes = chosen.gpuTypes || {};
      const hasFreeGpuData = filterFree && chosen.freeGpuTypes !== undefined;
      const effectiveGpuTypes = hasFreeGpuData ? chosen.freeGpuTypes : gpuTypes;

      const meta = document.getElementById('partitionMeta');
      if (meta) {
        const hasAnyFreeSummaryData =
          filterFree &&
          (hasFreeNodesData ||
            hasFreeCpuData ||
            hasFreeGpuData ||
            typeof chosen.freeCpuTotal === 'number' ||
            typeof chosen.freeGpuTotal === 'number' ||
            typeof chosen.freeGpuMax === 'number');
        if (hasAnyFreeSummaryData) {
          const summaryParts = ['Nodes total: ' + chosen.nodes];

          if (hasFreeCpuData || typeof chosen.freeCpuTotal === 'number') {
            const cpuTotal = typeof chosen.freeCpuTotal === 'number' ? chosen.freeCpuTotal : 0;
            const cpuPerNode = typeof chosen.freeCpusPerNode === 'number' ? chosen.freeCpusPerNode : 0;
            let cpuSummary = 'CPU free: ' + cpuTotal + ' total';
            if (hasFreeNodesData) {
              cpuSummary += ' across ' + chosen.freeNodes + ' nodes';
            }
            cpuSummary += ' (max ' + cpuPerNode + ' on one node)';
            summaryParts.push(cpuSummary);
          } else {
            summaryParts.push('CPUs/node: ' + chosen.cpus);
          }

          summaryParts.push('Mem/node: ' + formatMem(chosen.memMb));

          const partitionHasGpuCapacity =
            (typeof chosen.gpuMax === 'number' && chosen.gpuMax > 0) || Object.keys(gpuTypes || {}).length > 0;
          if (partitionHasGpuCapacity) {
            if (hasFreeGpuData || typeof chosen.freeGpuTotal === 'number' || typeof chosen.freeGpuMax === 'number') {
              const gpuTotal = typeof chosen.freeGpuTotal === 'number' ? chosen.freeGpuTotal : 0;
              const gpuPerNode = typeof chosen.freeGpuMax === 'number' ? chosen.freeGpuMax : 0;
              const gpuTypeKeys = Object.keys(effectiveGpuTypes || {});
              let gpuSummary = 'GPU free: ' + gpuTotal + ' total (max ' + gpuPerNode + ' on one node)';
              if (gpuTypeKeys.length > 0) {
                const typed = gpuTypeKeys
                  .sort()
                  .map((key) => {
                    const label = key ? key : 'gpu';
                    return label + 'x' + effectiveGpuTypes[key];
                  });
                gpuSummary += ', per-node max by type: ' + typed.join(', ');
              }
              summaryParts.push(gpuSummary);
            } else {
              summaryParts.push('GPU: none');
            }
          }

          meta.textContent = summaryParts.join(' | ');
        } else {
          const totalGpuKeys = Object.keys(gpuTypes || {});
          const gpuSummary =
            totalGpuKeys.length === 0
              ? 'GPU: none'
              : 'GPU: ' +
                totalGpuKeys
                  .sort()
                  .map((key) => {
                    const label = key ? key : 'gpu';
                    return label + 'x' + gpuTypes[key];
                  })
                  .join(', ');
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
      }

      const preferredNodes = getValue('defaultNodes') || lastValues.defaultNodes;
      const preferredCpus = getValue('defaultCpusPerTask') || lastValues.defaultCpusPerTask;
      const preferredMem = getValue('defaultMemoryMb') || lastValues.defaultMemoryMb;
      const nodesLimit =
        filterFree && typeof chosen.freeNodes === 'number' ? chosen.freeNodes : chosen.nodes;
      const cpuLimit =
        filterFree && typeof chosen.freeCpusPerNode === 'number' ? chosen.freeCpusPerNode : chosen.cpus;

      setFieldOptions('defaultNodes', buildRangeOptions(nodesLimit), preferredNodes);
      setFieldOptions('defaultCpusPerTask', buildRangeOptions(cpuLimit), preferredCpus);
      setFieldOptions('defaultMemoryMb', buildMemoryOptions(chosen.memMb), preferredMem);

      const gpuTypeKeys = Object.keys(effectiveGpuTypes || {});
      const currentGpuType = String(getValue('defaultGpuType') || '').trim();
      const currentGpuCount = String(getValue('defaultGpuCount') || '').trim();
      const preferredGpuType = gpuSelectionSuppressedForCpuPartition
        ? rememberedGpuSelection.type || lastValues.defaultGpuType
        : currentGpuType || lastValues.defaultGpuType;
      const preferredGpuCount = gpuSelectionSuppressedForCpuPartition
        ? rememberedGpuSelection.count || lastValues.defaultGpuCount
        : currentGpuCount || lastValues.defaultGpuCount;

      if (gpuTypeKeys.length === 0) {
        if (!gpuSelectionSuppressedForCpuPartition) {
          rememberGpuSelectionFromInputs();
        }
        setFieldOptions('defaultGpuType', [{ value: '', label: 'None' }], '');
        setFieldOptions('defaultGpuCount', [{ value: '0', label: '0' }], '0');
        setValue('defaultGpuType', '');
        setValue('defaultGpuCount', '0');
        gpuSelectionSuppressedForCpuPartition = true;
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
        gpuSelectionSuppressedForCpuPartition = false;
        rememberGpuSelectionFromInputs();
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

    function setRememberedGpuSelection(typeValue, countValue) {
      rememberedGpuSelection = {
        type: String(typeValue || '').trim(),
        count: String(countValue || '').trim()
      };
    }

    function rememberGpuSelectionFromInputs() {
      setRememberedGpuSelection(getValue('defaultGpuType'), getValue('defaultGpuCount'));
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
        startupCommand: getValue('startupCommand'),
        additionalSshOptions: getValue('additionalSshOptions'),
        moduleLoad: getValue('moduleLoad'),
        moduleSelections: selectedModules.slice(),
        moduleCustomCommand: customModuleCommand,
        proxyDebugLogging: getValue('proxyDebugLogging'),
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
          updateAdvancedResetButtonTooltips();
        }
        connectedSessionMode = message.connectionSessionMode || '';
        if (message.profileSummaries) {
          profileSummaries = message.profileSummaries;
        }
        Object.keys(values).forEach((key) => setValue(key, values[key]));
        rememberGpuSelectionFromInputs();
        gpuSelectionSuppressedForCpuPartition = false;
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
    addFieldListener('defaultGpuType', () => {
      if (!gpuSelectionSuppressedForCpuPartition) {
        rememberGpuSelectionFromInputs();
      }
      updatePartitionDetails();
    });
    addFieldListener('defaultNodes', updateResourceWarning);
    addFieldListener('defaultCpusPerTask', updateResourceWarning);
    addFieldListener('defaultMemoryMb', updateResourceWarning);
    addFieldListener('defaultGpuCount', () => {
      if (!gpuSelectionSuppressedForCpuPartition) {
        rememberGpuSelectionFromInputs();
      }
      updateResourceWarning();
    });

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
    attachAdvancedFieldResetButtons();
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

function getSnapshotThemeVarsCss(): string {
  return `<style id="slurm-connect-snapshot-theme-vars">
  :root {
    --vscode-font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    --vscode-foreground: #24292f;
    --vscode-descriptionForeground: #57606a;
    --vscode-editor-background: #ffffff;
    --vscode-sideBar-background: #ffffff;
    --vscode-panel-border: #d0d7de;
    --vscode-input-background: #ffffff;
    --vscode-input-foreground: #24292f;
    --vscode-input-border: #c6cbd1;
    --vscode-widget-border: #c6cbd1;
    --vscode-focusBorder: #0969da;
    --vscode-button-background: #0969da;
    --vscode-button-foreground: #ffffff;
    --vscode-button-hoverBackground: #0860ca;
    --vscode-button-secondaryBackground: #eaeef2;
    --vscode-button-secondaryForeground: #24292f;
    --vscode-button-secondaryHoverBackground: #d0d7de;
    --vscode-checkbox-background: #ffffff;
    --vscode-checkbox-border: #8c959f;
    --vscode-checkbox-foreground: #ffffff;
    --vscode-list-hoverBackground: #f3f4f6;
    --vscode-list-activeSelectionBackground: #ddf4ff;
    --vscode-list-activeSelectionForeground: #0b3066;
    --vscode-disabledForeground: #8c959f;
  }
  body[data-slurm-connect-snapshot="1"] {
    padding-bottom: 12px !important;
  }
  body[data-slurm-connect-snapshot="1"] .action-bar {
    position: static;
    left: auto;
    right: auto;
    bottom: auto;
    margin-top: 12px;
    padding: 8px 0 0;
    background: transparent;
  }
  body[data-slurm-connect-snapshot="1"] .action-main {
    width: 100%;
  }
  </style>`;
}

export function renderWebviewHtmlForSnapshot(options?: {
  cspSource?: string;
  nonce?: string;
  injectThemeVars?: boolean;
  openProfilesSection?: boolean;
}): string {
  const cspSource = options?.cspSource || 'https://snapshot.invalid';
  const nonce = options?.nonce || 'slurm-connect-snapshot';
  let html = getWebviewHtml({ cspSource } as vscode.Webview, nonce);
  html = html.replace('<body>', '<body data-slurm-connect-snapshot="1">');
  if (options?.injectThemeVars !== false) {
    html = html.replace('</head>', `${getSnapshotThemeVarsCss()}\n</head>`);
  }
  if (options?.openProfilesSection !== false) {
    html = html.replace(
      '<details class="section" id="profilesSection">',
      '<details class="section" id="profilesSection" open>'
    );
    html = html.replace('profilesSection: false,', 'profilesSection: true,');
  }
  return html;
}

function formatError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}
