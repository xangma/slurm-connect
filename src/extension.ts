import * as vscode from 'vscode';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs/promises';
import * as jsonc from 'jsonc-parser';
import { execFile } from 'child_process';
import { promisify } from 'util';
import {
  buildHostEntry,
  buildSlurmConnectIncludeBlock,
  buildSlurmConnectIncludeContent,
  buildTemporarySshConfigContent,
  expandHome,
  SLURM_CONNECT_INCLUDE_END,
  SLURM_CONNECT_INCLUDE_START
} from './utils/sshConfig';
import {
  ClusterInfo,
  getMaxFieldCount,
  hasMeaningfulClusterInfo,
  parsePartitionInfoOutput
} from './utils/clusterInfo';

const execFileAsync = promisify(execFile);

interface SlurmConnectConfig {
  loginHosts: string[];
  loginHostsCommand: string;
  loginHostsQueryHost: string;
  partitionCommand: string;
  partitionInfoCommand: string;
  qosCommand: string;
  accountCommand: string;
  user: string;
  identityFile: string;
  forwardAgent: boolean;
  requestTTY: boolean;
  moduleLoad: string;
  proxyCommand: string;
  proxyArgs: string[];
  extraSallocArgs: string[];
  promptForExtraSallocArgs: boolean;
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
  restoreSshConfigAfterConnect: boolean;
  additionalSshOptions: Record<string, string>;
  sshQueryConfigPath: string;
  sshConnectTimeoutSeconds: number;
}

interface PartitionResult {
  partitions: string[];
  defaultPartition?: string;
}

type ConnectionState = 'idle' | 'connecting' | 'connected' | 'disconnecting';
type SshAuthMode = 'agent' | 'terminal';

let outputChannel: vscode.OutputChannel | undefined;
let logFilePath: string | undefined;
let extensionStoragePath: string | undefined;
let extensionGlobalState: vscode.Memento | undefined;
let extensionWorkspaceState: vscode.Memento | undefined;
let activeWebview: vscode.Webview | undefined;
let connectionState: ConnectionState = 'idle';
let lastConnectionAlias: string | undefined;
let previousRemoteSshConfigPath: string | undefined;
let activeTempSshConfigPath: string | undefined;
let lastSshAuthPrompt: { identityPath: string; timestamp: number; mode: SshAuthMode } | undefined;
let clusterInfoRequestInFlight = false;
const SETTINGS_SECTION = 'slurmConnect';
const LEGACY_SETTINGS_SECTION = 'sciamaSlurm';
const PROFILE_STORE_KEY = 'slurmConnect.profiles';
const ACTIVE_PROFILE_KEY = 'slurmConnect.activeProfile';
const PENDING_RESTORE_KEY = 'slurmConnect.pendingRestore';
const CLUSTER_INFO_CACHE_KEY = 'slurmConnect.clusterInfoCache';
const CLUSTER_UI_CACHE_KEY = 'slurmConnect.clusterUiCache';
const LEGACY_PROFILE_STORE_KEY = 'sciamaSlurm.profiles';
const LEGACY_ACTIVE_PROFILE_KEY = 'sciamaSlurm.activeProfile';
const LEGACY_PENDING_RESTORE_KEY = 'sciamaSlurm.pendingRestore';
const LEGACY_CLUSTER_INFO_CACHE_KEY = 'sciamaSlurm.clusterInfoCache';
const CONFIG_KEYS = [
  'loginHosts',
  'loginHostsCommand',
  'loginHostsQueryHost',
  'partitionCommand',
  'partitionInfoCommand',
  'qosCommand',
  'accountCommand',
  'user',
  'identityFile',
  'forwardAgent',
  'requestTTY',
  'moduleLoad',
  'proxyCommand',
  'proxyArgs',
  'extraSallocArgs',
  'promptForExtraSallocArgs',
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
  'restoreSshConfigAfterConnect',
  'additionalSshOptions',
  'sshQueryConfigPath',
  'sshConnectTimeoutSeconds'
];
const CLUSTER_SETTING_KEYS = [
  'loginHosts',
  'loginHostsCommand',
  'loginHostsQueryHost',
  'partitionCommand',
  'partitionInfoCommand',
  'qosCommand',
  'accountCommand',
  'user',
  'identityFile',
  'moduleLoad',
  'proxyCommand',
  'proxyArgs',
  'extraSallocArgs',
  'defaultPartition',
  'defaultNodes',
  'defaultTasksPerNode',
  'defaultCpusPerTask',
  'defaultTime',
  'defaultMemoryMb',
  'defaultGpuType',
  'defaultGpuCount'
] as const;

type ClusterSettingKey = typeof CLUSTER_SETTING_KEYS[number];

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  extensionStoragePath = context.globalStorageUri.fsPath;
  extensionGlobalState = context.globalState;
  extensionWorkspaceState = context.workspaceState;
  await migrateLegacyState();
  await migrateLegacySettings();
  await migrateLegacyModuleCommands();
  await migrateClusterSettingsToCache();
  syncConnectionStateFromEnvironment();
  const disposable = vscode.commands.registerCommand('slurmConnect.connect', () => {
    void connectCommand();
  });
  context.subscriptions.push(disposable);

  void maybeRestorePendingOnStartup();

  const viewProvider = new SlurmConnectViewProvider(context);
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider('slurmConnect.connectView', viewProvider)
  );
}

export function deactivate(): void {
  // No-op
}

async function migrateLegacyState(): Promise<void> {
  if (!extensionGlobalState) {
    return;
  }
  const migrations: Array<[string, string]> = [
    [LEGACY_PROFILE_STORE_KEY, PROFILE_STORE_KEY],
    [LEGACY_ACTIVE_PROFILE_KEY, ACTIVE_PROFILE_KEY],
    [LEGACY_PENDING_RESTORE_KEY, PENDING_RESTORE_KEY],
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

function isModuleLoadCommand(value: string): boolean {
  return /^module\s+load\s+.+/i.test(value) || /^ml\s+.+/i.test(value);
}

function sanitizeModuleCache(cache?: ClusterUiCache): { next: ClusterUiCache; changed: boolean } {
  if (!cache) {
    return { next: {}, changed: false };
  }
  const next: ClusterUiCache = { ...cache };
  let changed = false;
  const hasSelectionsKey = Object.prototype.hasOwnProperty.call(cache, 'moduleSelections');
  const hasCustomKey = Object.prototype.hasOwnProperty.call(cache, 'moduleCustomCommand');
  const hasLegacyModuleLoadOnly = !hasSelectionsKey && !hasCustomKey;

  const custom = typeof next.moduleCustomCommand === 'string' ? next.moduleCustomCommand.trim() : '';
  if (custom) {
    delete next.moduleCustomCommand;
    changed = true;
  }

  const selections = Array.isArray(next.moduleSelections)
    ? next.moduleSelections.filter((entry) => typeof entry === 'string' && entry.trim().length > 0)
    : [];
  if (selections.length === 0) {
    delete next.moduleSelections;
    changed = changed || Array.isArray(cache.moduleSelections);
    const moduleLoad = typeof next.moduleLoad === 'string' ? next.moduleLoad.trim() : '';
    if (moduleLoad && (hasLegacyModuleLoadOnly || !isModuleLoadCommand(moduleLoad))) {
      delete next.moduleLoad;
      changed = true;
    }
  }

  return { next, changed };
}

function sanitizeProfileModuleCommands(values: UiValues): { next: UiValues; changed: boolean } {
  let changed = false;
  const next = { ...values };
  const hasSelectionsKey = Object.prototype.hasOwnProperty.call(values, 'moduleSelections');
  const hasCustomKey = Object.prototype.hasOwnProperty.call(values, 'moduleCustomCommand');
  const hasLegacyModuleLoadOnly = !hasSelectionsKey && !hasCustomKey;
  const custom = typeof next.moduleCustomCommand === 'string' ? next.moduleCustomCommand.trim() : '';
  if (custom) {
    next.moduleCustomCommand = '';
    changed = true;
  }
  const selections = Array.isArray(next.moduleSelections)
    ? next.moduleSelections.filter((entry) => typeof entry === 'string' && entry.trim().length > 0)
    : [];
  if (selections.length === 0) {
    next.moduleSelections = [];
    const moduleLoad = typeof next.moduleLoad === 'string' ? next.moduleLoad.trim() : '';
    if (moduleLoad && (hasLegacyModuleLoadOnly || !isModuleLoadCommand(moduleLoad))) {
      next.moduleLoad = '';
      changed = true;
    }
  }
  return { next, changed };
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
      const sanitized = sanitizeProfileModuleCommands(profile.values);
      if (sanitized.changed) {
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

function mapConfigValueToUi(key: ClusterSettingKey, value: unknown): string {
  switch (key) {
    case 'loginHosts':
    case 'proxyArgs':
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
        nextGlobalCache[key] = mapConfigValueToUi(key, inspect.globalValue);
        updatedGlobal = true;
      }
      globalKeysToClear.add(key);
    }
    if (inspect?.workspaceValue !== undefined) {
      if (!Object.prototype.hasOwnProperty.call(nextWorkspaceCache, key)) {
        nextWorkspaceCache[key] = mapConfigValueToUi(key, inspect.workspaceValue);
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
              nextWorkspaceCache[key] = mapConfigValueToUi(key, folderValues[0].value);
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

function appendLogFile(text: string): void {
  void (async () => {
    const filePath = await ensureLogFile();
    await fs.appendFile(filePath, text, 'utf8');
  })().catch(() => {
    // Ignore logging failures to avoid breaking the main flow.
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
  qosCommand: string;
  accountCommand: string;
  user: string;
  identityFile: string;
  moduleLoad: string;
  moduleSelections?: string[];
  moduleCustomCommand?: string;
  proxyCommand: string;
  proxyArgs: string;
  extraSallocArgs: string;
  promptForExtraSallocArgs: boolean;
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
  restoreSshConfigAfterConnect: boolean;
  sshQueryConfigPath: string;
  openInNewWindow: boolean;
  remoteWorkspacePath: string;
}

type ClusterUiCache = Partial<UiValues>;

const CLUSTER_UI_KEYS = new Set<keyof UiValues>([
  'loginHosts',
  'loginHostsCommand',
  'loginHostsQueryHost',
  'partitionCommand',
  'partitionInfoCommand',
  'qosCommand',
  'accountCommand',
  'user',
  'identityFile',
  'moduleLoad',
  'moduleSelections',
  'moduleCustomCommand',
  'proxyCommand',
  'proxyArgs',
  'extraSallocArgs',
  'defaultPartition',
  'defaultNodes',
  'defaultTasksPerNode',
  'defaultCpusPerTask',
  'defaultTime',
  'defaultMemoryMb',
  'defaultGpuType',
  'defaultGpuCount'
]);

interface ProfileEntry {
  name: string;
  values: UiValues;
  createdAt: string;
  updatedAt: string;
}

interface ProfileSummary {
  name: string;
  updatedAt: string;
}

type ProfileStore = Record<string, ProfileEntry>;

interface PendingRestoreState {
  tempConfigPath: string;
  previousConfigPath?: string;
  alias: string;
  openInNewWindow: boolean;
  createdAt: string;
}

function postToWebview(message: unknown): void {
  if (!activeWebview) {
    return;
  }
  void activeWebview.postMessage(message);
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
    agentStatus: agentStatus.text,
    agentStatusError: agentStatus.isError
  });
}

function setConnectionState(state: ConnectionState): void {
  connectionState = state;
  postToWebview({ command: 'connectionState', state });
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
    return;
  }
  if (connectionState === 'connected') {
    connectionState = 'idle';
  }
}

function mergeUiValuesWithDefaults(values?: Partial<UiValues>): UiValues {
  const defaults = getUiValuesFromStorage();
  if (!values) {
    return defaults;
  }
  return {
    ...defaults,
    ...values
  } as UiValues;
}

function pickClusterUiValues(values: UiValues): ClusterUiCache {
  const picked: ClusterUiCache = {};
  for (const key of CLUSTER_UI_KEYS) {
    const value = values[key];
    if (value !== undefined) {
      (picked as Record<keyof UiValues, UiValues[keyof UiValues]>)[key] = value;
    }
  }
  return picked;
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

async function updateClusterUiCache(values: UiValues, target: vscode.ConfigurationTarget): Promise<void> {
  const hasWorkspace = Boolean(vscode.workspace.workspaceFolders && vscode.workspace.workspaceFolders.length > 0);
  const useWorkspace =
    hasWorkspace &&
    (target === vscode.ConfigurationTarget.Workspace || target === vscode.ConfigurationTarget.WorkspaceFolder);
  const state = useWorkspace ? extensionWorkspaceState : extensionGlobalState;
  if (!state) {
    return;
  }
  await state.update(CLUSTER_UI_CACHE_KEY, pickClusterUiValues(values));
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
  return store[name]?.values;
}

function getPendingRestore(): PendingRestoreState | undefined {
  if (!extensionGlobalState) {
    return undefined;
  }
  return extensionGlobalState.get<PendingRestoreState>(PENDING_RESTORE_KEY);
}

async function setPendingRestore(state?: PendingRestoreState): Promise<void> {
  if (!extensionGlobalState) {
    return;
  }
  await extensionGlobalState.update(PENDING_RESTORE_KEY, state);
}

async function clearPendingRestore(): Promise<void> {
  await setPendingRestore(undefined);
}

async function maybeRestorePendingOnStartup(): Promise<void> {
  const pending = getPendingRestore();
  if (!pending) {
    await migrateStaleRemoteSshConfigIfNeeded();
    return;
  }
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const current = normalizeRemoteConfigPath(remoteCfg.get<string>('configFile'));
  if (!current || current !== pending.tempConfigPath) {
    await clearPendingRestore();
    await migrateStaleRemoteSshConfigIfNeeded();
    return;
  }

  if (pending.openInNewWindow) {
    await waitForRemoteConnectionOrTimeout(30_000, 500);
  } else {
    await delay(500);
  }
  await migrateStaleRemoteSshConfigIfNeeded();
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
    qosCommand: (cfg.get<string>('qosCommand') || '').trim(),
    accountCommand: (cfg.get<string>('accountCommand') || '').trim(),
    user,
    identityFile: (cfg.get<string>('identityFile') || '').trim(),
    forwardAgent: cfg.get<boolean>('forwardAgent', true),
    requestTTY: cfg.get<boolean>('requestTTY', true),
    moduleLoad: (cfg.get<string>('moduleLoad') || '').trim(),
    proxyCommand: (cfg.get<string>('proxyCommand') || '').trim(),
    proxyArgs: cfg.get<string[]>('proxyArgs', []),
    extraSallocArgs: cfg.get<string[]>('extraSallocArgs', []),
    promptForExtraSallocArgs: cfg.get<boolean>('promptForExtraSallocArgs', false),
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
    restoreSshConfigAfterConnect: cfg.get<boolean>('restoreSshConfigAfterConnect', true),
    additionalSshOptions: cfg.get<Record<string, string>>('additionalSshOptions', {}),
    sshQueryConfigPath: (cfg.get<string>('sshQueryConfigPath') || '').trim(),
    sshConnectTimeoutSeconds: cfg.get<number>('sshConnectTimeoutSeconds', 15)
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
          let values = defaults;
          if (activeProfile) {
            const stored = getProfileValues(activeProfile);
            if (stored) {
              values = { ...defaults, ...stored } as UiValues;
            } else {
              activeProfile = undefined;
            }
          }
          const agentStatus = await buildAgentStatusMessage(values.identityFile);
          const host = firstLoginHostFromInput(values.loginHosts);
          const cached = host ? getCachedClusterInfo(host) : undefined;
          webview.postMessage({
            command: 'load',
            values,
            clusterInfo: cached?.info,
            clusterInfoCachedAt: cached?.fetchedAt,
            profiles: getProfileSummaries(getProfileStore()),
            activeProfile,
            connectionState,
            agentStatus: agentStatus.text,
            agentStatusError: agentStatus.isError,
            remoteActive: vscode.env.remoteName === 'ssh-remote',
            saveTarget: resolvePreferredSaveTarget()
          });
          break;
        }
        case 'connect': {
          const target = message.target === 'workspace'
            ? vscode.ConfigurationTarget.Workspace
            : vscode.ConfigurationTarget.Global;
          const uiValues = message.values as UiValues;
          await updateConfigFromUi(uiValues, target);
          setConnectionState('connecting');
          const overrides = buildOverridesFromUi(uiValues);
          const connected = await connectCommand(overrides, { interactive: false });
          setConnectionState(connected ? 'connected' : 'idle');
          break;
        }
        case 'saveSettings': {
          const target = message.target === 'workspace'
            ? vscode.ConfigurationTarget.Workspace
            : vscode.ConfigurationTarget.Global;
          const uiValues = message.values as UiValues;
          await updateConfigFromUi(uiValues, target);
          break;
        }
        case 'disconnect': {
          setConnectionState('disconnecting');
          const disconnected = await disconnectFromHost(lastConnectionAlias);
          if (disconnected) {
            await restoreRemoteSshConfigIfNeeded();
          }
          setConnectionState(disconnected ? 'idle' : 'connected');
          break;
        }
        case 'saveProfile': {
          const uiValues = message.values as UiValues;
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
            values: uiValues,
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
              activeProfile: getActiveProfileName(),
              profileStatus: `Profile "${name}" not found.`,
              profileError: true
            });
            break;
          }
          await setActiveProfileName(name);
          syncConnectionStateFromEnvironment();
          const host = firstLoginHostFromInput(profile.values.loginHosts);
          const cached = host ? getCachedClusterInfo(host) : undefined;
          webview.postMessage({
            command: 'load',
            values: profile.values,
            clusterInfo: cached?.info,
            clusterInfoCachedAt: cached?.fetchedAt,
            profiles: getProfileSummaries(store),
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
            activeProfile,
            profileStatus: `Profile "${name}" deleted.`
          });
          break;
        }
        case 'getClusterInfo': {
          const uiValues = message.values as UiValues;
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
  options?: { interactive?: boolean }
): Promise<boolean> {
  const cfg = getConfigWithOverrides(overrides);
  const interactive = options?.interactive !== false;
  const log = getOutputChannel();
  log.clear();
  log.appendLine('Slurm Connect started.');

  let didConnect = false;

  const loginHosts = await resolveLoginHosts(cfg, { interactive });
  log.appendLine(`Login hosts resolved: ${loginHosts.join(', ') || '(none)'}`);
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

  const proceed = await maybePromptForSshAuthOnConnect(cfg, loginHost);
  if (!proceed) {
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

    qos = await pickOptionalValue('Select QoS (optional)', await querySimpleList(loginHost, cfg, cfg.qosCommand));
    account = await pickOptionalValue(
      'Select account (optional)',
      await querySimpleList(loginHost, cfg, cfg.accountCommand)
    );
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

    const tasksInput = await promptNumber('Tasks per node', cfg.defaultTasksPerNode, 1);
    if (tasksInput === undefined) {
      return false;
    }
    tasksPerNode = tasksInput;

    const cpusInput = await promptNumber('CPUs per task', cfg.defaultCpusPerTask, 1);
    if (cpusInput === undefined) {
      return false;
    }
    cpusPerTask = cpusInput;

    const timeInput = await promptTime('Wall time', cfg.defaultTime || '24:00:00');
    if (timeInput === undefined) {
      return false;
    }
    time = timeInput;
  }

  if (!partition || partition.trim().length === 0) {
    const resolvedPartition = await resolveDefaultPartitionForHost(loginHost, cfg);
    if (resolvedPartition) {
      partition = resolvedPartition;
      log.appendLine(`Using default partition: ${partition}`);
    }
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
  const remoteCommand = buildRemoteCommand(cfg, [...sallocArgs, ...cfg.extraSallocArgs, ...extraArgs]);
  if (!remoteCommand) {
    void vscode.window.showErrorMessage('RemoteCommand is empty. Check slurmConnect.proxyCommand.');
    return false;
  }

  const defaultAlias = buildDefaultAlias(cfg.sshHostPrefix || 'slurm', loginHost, partition, nodes, cpusPerTask);
  let alias = defaultAlias;
  if (interactive) {
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

  lastConnectionAlias = alias.trim();

  const hostEntry = buildHostEntry(alias.trim(), loginHost, cfg, remoteCommand);
  log.appendLine('Generated SSH host entry:');
  log.appendLine(hostEntry);

  let tempConfigPath: string | undefined;
  let usedLegacyConfig = false;

  await ensureRemoteSshSettings();
  await migrateStaleRemoteSshConfigIfNeeded();
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const currentRemoteConfig = normalizeRemoteConfigPath(remoteCfg.get<string>('configFile'));
  const baseConfigPath = normalizeSshPath(currentRemoteConfig || defaultSshConfigPath());

  try {
    const includePath = resolveSlurmConnectIncludePath(cfg);
    if (normalizeSshPath(includePath) === normalizeSshPath(baseConfigPath)) {
      throw new Error('Slurm Connect include file path must not be the same as the SSH config path.');
    }
    await writeSlurmConnectIncludeFile(hostEntry, includePath);
    const status = await ensureSshIncludeInstalled(baseConfigPath, includePath);
    log.appendLine(`SSH config include ${status} in ${baseConfigPath}.`);
  } catch (error) {
    usedLegacyConfig = true;
    log.appendLine(`Failed to install SSH Include: ${formatError(error)}`);
    log.appendLine('Falling back to temporary Remote.SSH configFile override.');

    const fallbackRemoteConfig = normalizeRemoteConfigPath(remoteCfg.get<string>('configFile'));
    const baseRemoteConfig = resolveBaseRemoteConfig(fallbackRemoteConfig);
    const includePaths = resolveSshConfigIncludes(baseRemoteConfig);
    previousRemoteSshConfigPath = baseRemoteConfig;
    if (includePaths.length > 0) {
      log.appendLine(`Temporary SSH config includes: ${includePaths.join(', ')}`);
    }

    try {
      tempConfigPath = await writeTemporarySshConfig(hostEntry, includePaths);
    } catch (error) {
      void vscode.window.showErrorMessage(`Failed to write temporary SSH config: ${formatError(error)}`);
      return false;
    }

    activeTempSshConfigPath = tempConfigPath;
    await remoteCfg.update('configFile', tempConfigPath, vscode.ConfigurationTarget.Global);
    await setPendingRestore({
      tempConfigPath,
      previousConfigPath: baseRemoteConfig || undefined,
      alias: alias.trim(),
      openInNewWindow: cfg.openInNewWindow,
      createdAt: new Date().toISOString()
    });
  }
  await delay(300);
  await refreshRemoteSshHosts();

  const connected = await connectToHost(
    alias.trim(),
    cfg.openInNewWindow,
    cfg.remoteWorkspacePath
  );
  didConnect = connected;
  if (!connected) {
    void vscode.window.showWarningMessage(
      `SSH host "${alias.trim()}" created, but auto-connect failed. Use Remote-SSH to connect.`,
      'Show Output'
    ).then((selection) => {
      if (selection === 'Show Output') {
        getOutputChannel().show(true);
      }
    });
    if (usedLegacyConfig) {
      await restoreRemoteSshConfigIfNeeded();
    }
    return didConnect;
  }
  if (cfg.openInNewWindow) {
    if (usedLegacyConfig) {
      log.appendLine('Deferring SSH config restore until the remote window is ready.');
    }
    return didConnect;
  }
  await waitForRemoteConnectionOrTimeout(30_000, 500);
  if (usedLegacyConfig) {
    await restoreRemoteSshConfigIfNeeded();
  }

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
      const lowered = output.toLowerCase();
      if (lowered.includes('command not found') || lowered.includes('module: not found')) {
        continue;
      }
      const modules = parseSimpleList(output);
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

async function handleClusterInfoRequest(values: UiValues, webview: vscode.Webview): Promise<void> {
  if (clusterInfoRequestInFlight) {
    return;
  }
  clusterInfoRequestInFlight = true;
  const overrides = buildOverridesFromUi(values);
  const cfg = getConfigWithOverrides(overrides);
  const loginHosts = parseListInput(values.loginHosts);
  const log = getOutputChannel();

  try {
    if (loginHosts.length === 0) {
      const message = 'Enter a login host before fetching cluster info.';
      void vscode.window.showErrorMessage(message);
      const agentStatus = await buildAgentStatusMessage(values.identityFile);
      webview.postMessage({
        command: 'clusterInfoError',
        message,
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
      const agentStatus = await buildAgentStatusMessage(values.identityFile);
      webview.postMessage({
        command: 'clusterInfoError',
        message,
        agentStatus: agentStatus.text,
        agentStatusError: agentStatus.isError
      });
      return;
    }

    const info = await fetchClusterInfo(loginHost, cfg);
    cacheClusterInfo(loginHost, info);
    const agentStatus = await buildAgentStatusMessage(values.identityFile);
    webview.postMessage({
      command: 'clusterInfo',
      info,
      agentStatus: agentStatus.text,
      agentStatusError: agentStatus.isError
    });
  } catch (error) {
    const message = formatError(error);
    void vscode.window.showErrorMessage(`Failed to fetch cluster info: ${message}`);
    const agentStatus = await buildAgentStatusMessage(values.identityFile);
    webview.postMessage({
      command: 'clusterInfoError',
      message,
      agentStatus: agentStatus.text,
      agentStatusError: agentStatus.isError
    });
  } finally {
    clusterInfoRequestInFlight = false;
  }
}

async function fetchClusterInfo(loginHost: string, cfg: SlurmConnectConfig): Promise<ClusterInfo> {
  const commands = [
    cfg.partitionInfoCommand,
    'sinfo -h -N -o "%P|%n|%c|%m|%G"',
    'sinfo -h -o "%P|%D|%c|%m|%G"',
    'scontrol show partition -o'
  ].filter(Boolean);

  const log = getOutputChannel();
  let info: ClusterInfo | undefined;
  try {
    info = await fetchClusterInfoSingleCall(loginHost, cfg, commands);
  } catch (error) {
    log.appendLine(`Single-call cluster info failed, falling back. ${formatError(error)}`);
  }

  if (!info) {
    let lastInfo: ClusterInfo = { partitions: [] };
    let bestInfo: ClusterInfo | undefined;
    let partitionDefaults: Record<string, PartitionDefaults> = {};
    let defaultPartitionFromScontrol: string | undefined;
    for (const command of commands) {
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
  }

  return info;
}

const CLUSTER_CMD_START = '__SC_CMD_START__';
const CLUSTER_CMD_END = '__SC_CMD_END__';
const CLUSTER_MODULES_START = '__SC_MODULES_START__';
const CLUSTER_MODULES_END = '__SC_MODULES_END__';

function buildCombinedClusterInfoCommand(commands: string[]): string {
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
): { commandOutputs: string[]; modulesOutput: string } {
  const commandOutputs: string[] = Array.from({ length: commandCount }, () => '');
  let currentIndex: number | null = null;
  let inModules = false;
  const moduleLines: string[] = [];
  const lines = output.split(/\r?\n/);

  for (const line of lines) {
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
    if (currentIndex !== null && currentIndex >= 0 && currentIndex < commandOutputs.length) {
      commandOutputs[currentIndex] += line + '\n';
    }
  }

  return {
    commandOutputs: commandOutputs.map((text) => text.trim()),
    modulesOutput: moduleLines.join('\n').trim()
  };
}

function parseModulesOutput(output: string): string[] {
  if (!output) {
    return [];
  }
  const lowered = output.toLowerCase();
  if (lowered.includes('command not found') || lowered.includes('module: not found')) {
    return [];
  }
  return parseSimpleList(output);
}

async function fetchClusterInfoSingleCall(
  loginHost: string,
  cfg: SlurmConnectConfig,
  commands: string[]
): Promise<ClusterInfo> {
  const log = getOutputChannel();
  const combinedCommand = buildCombinedClusterInfoCommand(commands);
  log.appendLine(`Cluster info single-call command: ${combinedCommand}`);
  const output = await runSshCommand(loginHost, cfg, combinedCommand);
  const { commandOutputs, modulesOutput } = parseCombinedClusterInfoOutput(output, commands.length);

  let lastInfo: ClusterInfo = { partitions: [] };
  let bestInfo: ClusterInfo | undefined;
  let partitionDefaults: Record<string, PartitionDefaults> = {};
  let defaultPartitionFromScontrol: string | undefined;
  for (let i = 0; i < commandOutputs.length; i += 1) {
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
  return info;
}


function normalizeSshErrorText(error: unknown): string {
  if (error && typeof error === 'object') {
    const err = error as { stderr?: string; stdout?: string; message?: string };
    return [err.message, err.stderr, err.stdout].filter(Boolean).join('\n');
  }
  return String(error);
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
  if (cfg.identityFile) {
    args.push('-i', expandHome(cfg.identityFile));
  }
  const target = cfg.user ? `${cfg.user}@${host}` : host;
  args.push(target, command);
  return args;
}

function quoteShellArg(value: string): string {
  if (value === '') {
    return "''";
  }
  if (/^[A-Za-z0-9_./:@%+=-]+$/.test(value)) {
    return value;
  }
  return "'" + value.replace(/'/g, "'\\''") + "'";
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

async function waitForFile(filePath: string, timeoutMs: number, pollMs: number): Promise<boolean> {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    try {
      await fs.access(filePath);
      return true;
    } catch {
      // Keep waiting.
    }
    await delay(pollMs);
  }
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
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
  const { dirPath, stdoutPath, statusPath } = await createTerminalSshRunFiles();
  const isWindows = process.platform === 'win32';
  const shellCommand = isWindows
    ? (() => {
        const psArgs = args.map((arg) => quotePowerShellString(arg)).join(', ');
        const psScript = [
          "$ErrorActionPreference = 'Continue'",
          `$stdoutPath = ${quotePowerShellString(stdoutPath)}`,
          `$statusPath = ${quotePowerShellString(statusPath)}`,
          '$exitCode = 1',
          'try {',
          `  & ssh @(${psArgs}) | Out-File -FilePath $stdoutPath -Encoding utf8`,
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
        const sshCommand = ['ssh', ...args].map(quoteShellArg).join(' ');
        return `${sshCommand} > ${quoteShellArg(stdoutPath)}; printf "%s" $? > ${quoteShellArg(statusPath)}`;
      })();
  const terminal = vscode.window.createTerminal({ name: 'Slurm Connect SSH' });
  terminal.show(true);
  terminal.sendText(shellCommand, true);
  void vscode.window.showInformationMessage('Enter your SSH key passphrase in the terminal.');

  let stdout = '';
  let exitCode = NaN;
  try {
    const completed = await waitForFile(statusPath, cfg.sshConnectTimeoutSeconds * 1000, 500);
    if (!completed) {
      throw new Error('Timed out waiting for SSH to finish in the terminal.');
    }
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

async function getPublicKeyFingerprint(identityPath: string): Promise<string | undefined> {
  if (!identityPath) {
    return undefined;
  }
  const pubPath = identityPath.endsWith('.pub') ? identityPath : `${identityPath}.pub`;
  try {
    await fs.access(pubPath);
  } catch {
    return undefined;
  }
  try {
    const { stdout } = await execFileAsync('ssh-keygen', ['-lf', pubPath, '-E', 'sha256']);
    const match = stdout.match(/SHA256:[A-Za-z0-9+/=]+/);
    return match ? match[0] : undefined;
  } catch {
    return undefined;
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

async function getSshAgentInfo(): Promise<SshAgentInfo> {
  try {
    const result = await execFileAsync('ssh-add', ['-l']);
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
  return false;
}

async function buildAgentStatusMessage(identityPathRaw: string): Promise<{ text: string; isError: boolean }> {
  const agentInfo = await getSshAgentInfo();
  const agentBase = agentInfo.status === 'available'
    ? 'SSH agent running'
    : agentInfo.status === 'empty'
      ? 'SSH agent running (no keys loaded)'
      : 'SSH agent unavailable';
  if (!identityPathRaw) {
    const isError = agentInfo.status !== 'available';
    return { text: `${agentBase}.`, isError };
  }
  const identityPath = expandHome(identityPathRaw);
  const identityExists = await fileExists(identityPath);
  if (!identityExists) {
    return { text: `${agentBase}. Identity file not found.`, isError: true };
  }
  if (agentInfo.status === 'unavailable') {
    return {
      text: `SSH agent unavailable. Passphrase required for ${identityPath}.`,
      isError: true
    };
  }
  if (agentInfo.status === 'empty') {
    return { text: 'SSH agent running, no keys loaded.', isError: true };
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
  const agentInfo = await getSshAgentInfo();
  if (agentInfo.status !== 'available') {
    return false;
  }
  return await isSshKeyListedInAgentOutput(identityPath, agentInfo.output);
}


function buildTerminalSshAddCommand(identityPath: string): string {
  const trimmed = identityPath.trim();
  if (!trimmed) {
    return 'ssh-add';
  }
  if (!/[\s"]/g.test(trimmed)) {
    return `ssh-add ${trimmed}`;
  }
  const escaped = trimmed.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
  return `ssh-add "${escaped}"`;
}

function openSshAddTerminal(identityPath: string): vscode.Terminal {
  const terminal = vscode.window.createTerminal({ name: 'Slurm Connect SSH Add' });
  terminal.show(true);
  const command = buildTerminalSshAddCommand(identityPath);
  terminal.sendText(command, true);
  void vscode.window.showInformationMessage('Follow the terminal prompt to add your SSH key with ssh-add.');
  return terminal;
}

async function waitForSshKeyLoaded(
  identityPath: string,
  timeoutMs: number,
  pollMs: number
): Promise<boolean> {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (await isSshKeyLoaded(identityPath)) {
      return true;
    }
    await delay(pollMs);
  }
  return await isSshKeyLoaded(identityPath);
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
  const canTerminalPassphrase = Boolean(identityPath && identityExists);
  const agentInfo = identityPath && identityExists ? await getSshAgentInfo() : undefined;
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
  if (!identityPath || !identityExists) {
    actions.push('Open Slurm Connect');
  }
  actions.push('Dismiss');
  const message = identityPath && !identityExists
    ? `SSH authentication failed while querying the cluster. The identity file ${identityPath} was not found. Update it in the Slurm Connect view.`
    : identityPath
      ? agentStatus === 'unavailable'
        ? `SSH authentication failed while querying the cluster. SSH agent is unavailable, enter the passphrase for ${identityPath} in the terminal.`
        : `SSH authentication failed while querying the cluster. Add ${identityPath} to your ssh-agent or enter its passphrase in a terminal.`
      : 'SSH authentication failed while querying the cluster. Add your key to the ssh-agent (ssh-add <key>) or set your identity file in the Slurm Connect view.';
  const choice = await vscode.window.showWarningMessage(message, { modal: true }, ...actions);
  const defaultMode: SshAuthMode = canAddToAgent ? 'agent' : 'terminal';
  const mode: SshAuthMode = choice === 'Enter Passphrase in Terminal'
    ? 'terminal'
    : choice === 'Add to Agent'
      ? 'agent'
      : defaultMode;
  lastSshAuthPrompt = { identityPath, timestamp: now, mode };
  if (choice === 'Enter Passphrase in Terminal' && identityPath && identityExists && canTerminalPassphrase) {
    return { kind: 'terminal' };
  }
  if (choice === 'Add to Agent' && identityPath && canAddToAgent) {
    const terminal = openSshAddTerminal(identityPath);
    const loaded = await vscode.window.withProgress(
      {
        location: vscode.ProgressLocation.Notification,
        title: 'Waiting for ssh-add to load the key',
        cancellable: false
      },
      async () => await waitForSshKeyLoaded(identityPath, 180_000, 1500)
    );
    if (loaded) {
      terminal.dispose();
      await refreshAgentStatus(cfg.identityFile);
      return { kind: 'agent' };
    }
    void vscode.window.showInformationMessage(
      'SSH key not detected in agent yet. Re-run Get cluster info after ssh-add completes.'
    );
    return undefined;
  }
  if (choice === 'Open Slurm Connect') {
    await openSlurmConnectView();
  }
  return undefined;
}

async function maybePromptForSshAuthOnConnect(cfg: SlurmConnectConfig, loginHost: string): Promise<boolean> {
  const identityPath = cfg.identityFile ? expandHome(cfg.identityFile) : '';
  if (!identityPath) {
    const choice = await vscode.window.showWarningMessage(
      'SSH identity file is not set. Configure it in the Slurm Connect view.',
      { modal: true },
      'Open Slurm Connect',
      'Dismiss'
    );
    if (choice === 'Open Slurm Connect') {
      await openSlurmConnectView();
      return false;
    }
    return true;
  }
  const identityExists = await fileExists(identityPath);
  const agentInfo = identityExists ? await getSshAgentInfo() : undefined;
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
  if (!identityExists) {
    actions.push('Open Slurm Connect');
  }
  actions.push('Dismiss');

  const message = !identityExists
    ? `SSH identity file ${identityPath} was not found. Update it in the Slurm Connect view.`
    : `SSH key is not loaded in the agent. Add ${identityPath} to avoid extra prompts.`;

  const choice = await vscode.window.showWarningMessage(message, { modal: true }, ...actions);
  const mode: SshAuthMode = choice === 'Add to Agent' ? 'agent' : 'terminal';
  lastSshAuthPrompt = { identityPath, timestamp: now, mode };

  if (choice === 'Add to Agent' && canAddToAgent) {
    const terminal = openSshAddTerminal(identityPath);
    const loaded = await vscode.window.withProgress(
      {
        location: vscode.ProgressLocation.Notification,
        title: 'Waiting for ssh-add to load the key',
        cancellable: false
      },
      async () => await waitForSshKeyLoaded(identityPath, 180_000, 1500)
    );
    if (loaded) {
      terminal.dispose();
      await refreshAgentStatus(cfg.identityFile);
    } else {
      void vscode.window.showInformationMessage(
        'SSH key not detected in agent yet. Continue once ssh-add completes.'
      );
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
  const args = buildSshArgs(host, cfg, command, { batchMode: true });

  try {
    const { stdout } = await execFileAsync('ssh', args, { timeout: cfg.sshConnectTimeoutSeconds * 1000 });
    return stdout.trim();
  } catch (error) {
    const errorText = normalizeSshErrorText(error);
    const retry = await maybePromptForSshAuth(cfg, errorText);
    if (retry?.kind === 'agent') {
      try {
        const { stdout } = await execFileAsync('ssh', args, { timeout: cfg.sshConnectTimeoutSeconds * 1000 });
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


function buildRemoteCommand(cfg: SlurmConnectConfig, sallocArgs: string[]): string {
  const proxyParts = [cfg.proxyCommand, ...cfg.proxyArgs.filter(Boolean)];
  const proxyCommand = proxyParts.filter(Boolean).join(' ').trim();
  if (!proxyCommand) {
    return '';
  }
  const sallocFlags = sallocArgs.map((arg) => `--salloc-arg=${arg}`);
  const fullProxyCommand = [proxyCommand, ...sallocFlags].join(' ').trim();
  const baseCommand = cfg.moduleLoad
    ? `${cfg.moduleLoad} && ${fullProxyCommand}`.trim()
    : fullProxyCommand;
  if (!baseCommand) {
    return '';
  }
  return baseCommand;
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
  if (trimmed) {
    return normalizeSshPath(trimmed);
  }
  return path.join(os.homedir(), '.ssh', 'slurm-connect.conf');
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

function ensureTrailingNewline(value: string, lineEnding: string): string {
  if (!value) {
    return value;
  }
  return value.endsWith(lineEnding) ? value : `${value}${lineEnding}`;
}

async function ensureSshIncludeInstalled(baseConfigPath: string, includePath: string): Promise<'added' | 'updated' | 'already'> {
  const resolvedBase = normalizeSshPath(baseConfigPath);
  const resolvedInclude = normalizeSshPath(includePath);
  if (resolvedBase === resolvedInclude) {
    throw new Error('Slurm Connect include path must not be the same as the SSH config path.');
  }
  let content = '';
  try {
    content = await fs.readFile(resolvedBase, 'utf8');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
      throw error;
    }
  }
  const lineEnding = detectLineEnding(content);
  const block = buildSlurmConnectIncludeBlock(includePath).split('\n').join(lineEnding);
  const blockRegex = new RegExp(
    `${SLURM_CONNECT_INCLUDE_START.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}[\\s\\S]*?${SLURM_CONNECT_INCLUDE_END.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`,
    'm'
  );
  let next = content;
  let status: 'added' | 'updated' | 'already' = 'already';
  if (blockRegex.test(content)) {
    next = content.replace(blockRegex, block);
    status = next === content ? 'already' : 'updated';
  } else if (sshConfigHasIncludePath(content, includePath)) {
    next = replaceIncludeLineWithBlock(content, includePath, block);
    status = next === content ? 'already' : 'updated';
  } else {
    let prefix = content;
    if (prefix && !prefix.endsWith(lineEnding)) {
      prefix = `${prefix}${lineEnding}`;
    }
    if (prefix && !prefix.endsWith(`${lineEnding}${lineEnding}`)) {
      prefix = `${prefix}${lineEnding}`;
    }
    next = `${prefix}${block}${lineEnding}`;
    status = 'added';
  }
  if (next !== content) {
    await fs.mkdir(path.dirname(resolvedBase), { recursive: true });
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

async function writeTemporarySshConfig(entry: string, includePaths: string[]): Promise<string> {
  const basePath = path.join(extensionStoragePath || os.tmpdir(), 'slurm-connect-ssh-config');
  const dir = path.dirname(basePath);
  await fs.mkdir(dir, { recursive: true });
  const resolvedBase = path.resolve(basePath);
  const includes = uniqueList(includePaths)
    .map((value) => value.trim())
    .filter(Boolean)
    .map((value) => expandHome(value))
    .filter((value) => value && path.resolve(value) !== resolvedBase);
  const content = buildTemporarySshConfigContent(entry, includes);
  await fs.writeFile(basePath, content, 'utf8');
  return basePath;
}

function normalizeRemoteConfigPath(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function resolveBaseRemoteConfig(currentConfig?: string): string | undefined {
  if (currentConfig && activeTempSshConfigPath && currentConfig === activeTempSshConfigPath && previousRemoteSshConfigPath) {
    return previousRemoteSshConfigPath;
  }
  return currentConfig;
}

function resolveSshConfigIncludes(baseConfig?: string): string[] {
  const includes: string[] = [];
  if (baseConfig) {
    includes.push(baseConfig);
  }
  includes.push('~/.ssh/config');
  return uniqueList(includes);
}

async function restoreRemoteSshConfigIfNeeded(): Promise<boolean> {
  const cfg = getConfig();
  if (!cfg.restoreSshConfigAfterConnect) {
    await clearPendingRestore();
    return false;
  }
  const pending = getPendingRestore();
  const tempPath = activeTempSshConfigPath || pending?.tempConfigPath;
  if (!tempPath) {
    await clearPendingRestore();
    return false;
  }
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const current = normalizeRemoteConfigPath(remoteCfg.get<string>('configFile'));
  if (current && current !== tempPath) {
    if (pending && current === normalizeRemoteConfigPath(pending.previousConfigPath)) {
      await clearPendingRestore();
    }
    return false;
  }
  const restored = previousRemoteSshConfigPath ?? pending?.previousConfigPath;
  await remoteCfg.update('configFile', restored, vscode.ConfigurationTarget.Global);
  const log = getOutputChannel();
  log.appendLine(`Restored Remote.SSH configFile ${restored ? `to ${restored}` : 'to default'}.`);
  previousRemoteSshConfigPath = undefined;
  activeTempSshConfigPath = undefined;
  await clearPendingRestore();
  return true;
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
  await restoreRemoteSshConfigIfNeeded();
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const current = normalizeRemoteConfigPath(remoteCfg.get<string>('configFile'));
  if (!current) {
    await clearPendingRestore();
    return;
  }
  if (!(await isSlurmConnectTempConfigFile(current))) {
    return;
  }
  const pending = getPendingRestore();
  const restored = pending?.previousConfigPath || await guessPreviousConfigFromTempConfig(current);
  await remoteCfg.update('configFile', restored, vscode.ConfigurationTarget.Global);
  const log = getOutputChannel();
  log.appendLine(`Detected stale Slurm Connect temporary SSH config. Restored Remote.SSH configFile ${restored ? `to ${restored}` : 'to default'}.`);
  previousRemoteSshConfigPath = undefined;
  activeTempSshConfigPath = undefined;
  await clearPendingRestore();
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
    if (useLocalServer) {
      return;
    }
    try {
      const hasKey = await userSettingsHasKey(settingsPath, 'remote.SSH.useLocalServer');
      if (!hasKey) {
        const action = await vscode.window.showInformationMessage(
          `Slurm Connect detected ${platform} (${osVersion}). To work around a Remote-SSH issue, it can add "remote.SSH.useLocalServer": true to ${settingsPath}.`,
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


async function ensureRemoteSshSettings(): Promise<void> {
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
  await remoteExtension.activate();
  const availableCommands = await vscode.commands.getCommands(true);
  const sshCommands = availableCommands.filter((command) => /ssh|openssh/i.test(command));

  const trimmedPath = remoteWorkspacePath?.trim();
  if (trimmedPath) {
    const normalizedPath = trimmedPath.startsWith('/') ? trimmedPath : `/${trimmedPath}`;
    try {
      const remoteUri = vscode.Uri.parse(
        `vscode-remote://ssh-remote+${encodeURIComponent(alias)}${normalizedPath}`
      );
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
        return true;
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
      return true;
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
    qosCommand: fromCache('qosCommand', cfg.qosCommand || ''),
    accountCommand: fromCache('accountCommand', cfg.accountCommand || ''),
    user: fromCache('user', cfg.user || ''),
    identityFile: fromCache('identityFile', cfg.identityFile || ''),
    moduleLoad: fromCache('moduleLoad', cfg.moduleLoad || ''),
    moduleSelections: getCachedUiValue(cache, 'moduleSelections'),
    moduleCustomCommand: getCachedUiValue(cache, 'moduleCustomCommand'),
    proxyCommand: fromCache('proxyCommand', cfg.proxyCommand || ''),
    proxyArgs: fromCache('proxyArgs', cfg.proxyArgs.join('\n')),
    extraSallocArgs: fromCache('extraSallocArgs', cfg.extraSallocArgs.join('\n')),
    promptForExtraSallocArgs: cfg.promptForExtraSallocArgs,
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
    forwardAgent: cfg.forwardAgent,
    requestTTY: cfg.requestTTY,
    openInNewWindow: cfg.openInNewWindow,
    remoteWorkspacePath: cfg.remoteWorkspacePath || '',
    temporarySshConfigPath: cfg.temporarySshConfigPath || '',
    restoreSshConfigAfterConnect: cfg.restoreSshConfigAfterConnect,
    sshQueryConfigPath: cfg.sshQueryConfigPath || ''
  };
}

async function updateConfigFromUi(values: UiValues, target: vscode.ConfigurationTarget): Promise<void> {
  const cfg = vscode.workspace.getConfiguration(SETTINGS_SECTION);

  const updates: Array<[string, unknown]> = [
    ['promptForExtraSallocArgs', Boolean(values.promptForExtraSallocArgs)],
    ['sshHostPrefix', values.sshHostPrefix.trim()],
    ['forwardAgent', Boolean(values.forwardAgent)],
    ['requestTTY', Boolean(values.requestTTY)],
    ['openInNewWindow', Boolean(values.openInNewWindow)],
    ['remoteWorkspacePath', values.remoteWorkspacePath.trim()],
    ['temporarySshConfigPath', values.temporarySshConfigPath.trim()],
    ['restoreSshConfigAfterConnect', Boolean(values.restoreSshConfigAfterConnect)],
    ['sshQueryConfigPath', values.sshQueryConfigPath.trim()]
  ];

  for (const [key, value] of updates) {
    await cfg.update(key, value, target);
  }

  await updateClusterUiCache(values, target);
}

function buildClusterOverridesFromUi(values: ClusterUiCache): Partial<SlurmConnectConfig> {
  const overrides: Partial<SlurmConnectConfig> = {};
  const has = (key: keyof ClusterUiCache): boolean => Object.prototype.hasOwnProperty.call(values, key);

  if (has('loginHosts')) overrides.loginHosts = parseListInput(String(values.loginHosts ?? ''));
  if (has('loginHostsCommand')) overrides.loginHostsCommand = String(values.loginHostsCommand ?? '').trim();
  if (has('loginHostsQueryHost')) overrides.loginHostsQueryHost = String(values.loginHostsQueryHost ?? '').trim();
  if (has('partitionCommand')) overrides.partitionCommand = String(values.partitionCommand ?? '').trim();
  if (has('partitionInfoCommand')) overrides.partitionInfoCommand = String(values.partitionInfoCommand ?? '').trim();
  if (has('qosCommand')) overrides.qosCommand = String(values.qosCommand ?? '').trim();
  if (has('accountCommand')) overrides.accountCommand = String(values.accountCommand ?? '').trim();
  if (has('user')) overrides.user = String(values.user ?? '').trim();
  if (has('identityFile')) overrides.identityFile = String(values.identityFile ?? '').trim();
  if (has('moduleLoad')) overrides.moduleLoad = String(values.moduleLoad ?? '').trim();
  if (has('proxyCommand')) overrides.proxyCommand = String(values.proxyCommand ?? '').trim();
  if (has('proxyArgs')) overrides.proxyArgs = parseListInput(String(values.proxyArgs ?? ''));
  if (has('extraSallocArgs')) overrides.extraSallocArgs = parseListInput(String(values.extraSallocArgs ?? ''));
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
  return input.split(/\s+/).filter(Boolean);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

function buildOverridesFromUi(values: UiValues): Partial<SlurmConnectConfig> {
  return {
    loginHosts: parseListInput(values.loginHosts),
    loginHostsCommand: values.loginHostsCommand.trim(),
    loginHostsQueryHost: values.loginHostsQueryHost.trim(),
    partitionCommand: values.partitionCommand.trim(),
    partitionInfoCommand: values.partitionInfoCommand.trim(),
    qosCommand: values.qosCommand.trim(),
    accountCommand: values.accountCommand.trim(),
    user: values.user.trim(),
    identityFile: values.identityFile.trim(),
    moduleLoad: values.moduleLoad.trim(),
    proxyCommand: values.proxyCommand.trim(),
    proxyArgs: parseListInput(values.proxyArgs),
    extraSallocArgs: parseListInput(values.extraSallocArgs),
    promptForExtraSallocArgs: Boolean(values.promptForExtraSallocArgs),
    defaultPartition: values.defaultPartition.trim(),
    defaultNodes: parsePositiveNumberInput(values.defaultNodes),
    defaultTasksPerNode: parsePositiveNumberInput(values.defaultTasksPerNode),
    defaultCpusPerTask: parsePositiveNumberInput(values.defaultCpusPerTask),
    defaultTime: values.defaultTime.trim(),
    defaultMemoryMb: parseNonNegativeNumberInput(values.defaultMemoryMb),
    defaultGpuType: values.defaultGpuType.trim(),
    defaultGpuCount: parseNonNegativeNumberInput(values.defaultGpuCount),
    sshHostPrefix: values.sshHostPrefix.trim(),
    forwardAgent: Boolean(values.forwardAgent),
    requestTTY: Boolean(values.requestTTY),
    openInNewWindow: Boolean(values.openInNewWindow),
    remoteWorkspacePath: values.remoteWorkspacePath.trim(),
    temporarySshConfigPath: values.temporarySshConfigPath.trim(),
    restoreSshConfigAfterConnect: Boolean(values.restoreSshConfigAfterConnect),
    sshQueryConfigPath: values.sshQueryConfigPath.trim()
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
      padding: 12px;
      color: var(--vscode-foreground);
      background: var(--vscode-editor-background);
    }
    h2 { font-size: 16px; margin: 0 0 12px 0; }
    .section { margin-bottom: 16px; }
    label { font-size: 12px; display: block; margin-bottom: 4px; color: var(--vscode-foreground); }
    input, textarea, select, button { width: 100%; box-sizing: border-box; font-size: 12px; padding: 6px; }
    input:not([type="checkbox"]), textarea, select {
      background: var(--vscode-input-background);
      color: var(--vscode-input-foreground);
      border: 1px solid var(--vscode-input-border);
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
    .checkbox input { width: auto; }
    .buttons { display: flex; gap: 8px; }
    .buttons button { flex: 1; }
    .hidden { display: none; }
    .hint { font-size: 11px; color: var(--vscode-descriptionForeground); margin-top: 4px; }
    details summary { cursor: pointer; margin-bottom: 6px; color: var(--vscode-foreground); }
    .module-row { align-items: center; }
    .row > .module-actions { flex: 0 0 auto; }
    .row > .combo-picker { flex: 1; }
    .module-actions button { width: auto; min-width: 32px; }
    .module-list { margin-top: 6px; display: flex; flex-direction: column; gap: 4px; }
    .combo-picker { position: relative; }
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
    .combo-picker.no-options::after { display: none; }
    .combo-picker input { padding-right: 22px; }
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
      z-index: 5;
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
    .module-item {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      padding: 4px 6px;
      border: 1px solid var(--vscode-input-border);
      border-radius: 4px;
      background: var(--vscode-input-background);
    }
    .module-item button { width: auto; min-width: 28px; padding: 2px 6px; }
  </style>
</head>
<body>
  <h2>Slurm Connect</h2>

  <div class="section">
    <label for="loginHosts">Login host</label>
    <input id="loginHosts" type="text" placeholder="hostname1.com" />
    <label for="user">SSH user</label>
    <input id="user" type="text" />
    <label for="identityFile">Identity file</label>
    <input id="identityFile" type="text" />
    <div id="agentStatus" class="hint"></div>
    <div class="buttons" style="margin-top: 8px;">
      <button id="getClusterInfo">Get cluster info</button>
      <button id="clearClusterInfo">Clear cluster info</button>
    </div>
    <div id="clusterStatus" class="hint"></div>
  </div>

  <details class="section">
    <summary>Profiles</summary>
    <label for="profileSelect">Saved profiles</label>
    <select id="profileSelect"></select>
    <div class="buttons" style="margin-top: 8px;">
      <button id="profileLoad">Load</button>
      <button id="profileSave">Save profile</button>
      <button id="profileDelete">Delete</button>
    </div>
    <div id="profileStatus" class="hint"></div>
  </details>

  <div class="section">
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
      </div>
      <div>
        <label for="defaultTasksPerNode">Tasks per node</label>
        <input id="defaultTasksPerNode" type="number" min="1" />
      </div>
    </div>
    <div class="row">
      <div>
        <label for="defaultCpusPerTaskInput">CPUs per task</label>
        <div class="combo-picker" id="defaultCpusPerTaskPicker">
          <input id="defaultCpusPerTaskInput" type="number" min="1" />
          <div id="defaultCpusPerTaskMenu" class="dropdown-menu"></div>
        </div>
      </div>
      <div>
        <label for="defaultMemoryMbInput">Memory per node</label>
        <div class="combo-picker" id="defaultMemoryMbPicker">
          <input id="defaultMemoryMbInput" type="number" min="0" placeholder="MB (optional)" />
          <div id="defaultMemoryMbMenu" class="dropdown-menu"></div>
        </div>
      </div>
    </div>
    <div class="row">
      <div>
        <label for="defaultGpuTypeInput">GPU type</label>
        <div class="combo-picker" id="defaultGpuTypePicker">
          <input id="defaultGpuTypeInput" type="text" placeholder="Any" />
          <div id="defaultGpuTypeMenu" class="dropdown-menu"></div>
        </div>
      </div>
      <div>
        <label for="defaultGpuCountInput">GPU count</label>
        <div class="combo-picker" id="defaultGpuCountPicker">
          <input id="defaultGpuCountInput" type="number" min="0" />
          <div id="defaultGpuCountMenu" class="dropdown-menu"></div>
        </div>
      </div>
    </div>
    <label for="defaultTime">Wall time</label>
    <input id="defaultTime" type="text" placeholder="HH:MM:SS" />
    <div class="buttons" style="margin-top: 8px;">
      <button id="fillPartitionDefaults" type="button">Use partition defaults</button>
      <button id="clearResources" type="button">Clear resources</button>
    </div>
  </div>

  <div class="section">
    <label for="moduleInput">Modules</label>
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

  <div class="section">
    <div class="checkbox">
      <input id="openInNewWindow" type="checkbox" />
      <label for="openInNewWindow">Open in new window</label>
    </div>
    <label for="remoteWorkspacePath">Remote folder to open (optional)</label>
    <input id="remoteWorkspacePath" type="text" placeholder="/home/user/project" />
  </div>

  <details class="section">
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
    <label for="proxyCommand">Proxy command</label>
    <input id="proxyCommand" type="text" />
    <label for="proxyArgs">Proxy args (space or newline separated)</label>
    <textarea id="proxyArgs" rows="2"></textarea>
    <label for="extraSallocArgs">Extra salloc args</label>
    <textarea id="extraSallocArgs" rows="2"></textarea>
    <div class="checkbox">
      <input id="promptForExtraSallocArgs" type="checkbox" />
      <label for="promptForExtraSallocArgs">Prompt for extra salloc args</label>
    </div>
    <label for="sshHostPrefix">SSH host prefix</label>
    <input id="sshHostPrefix" type="text" />
    <label for="temporarySshConfigPath">Slurm Connect include file path</label>
    <input id="temporarySshConfigPath" type="text" />
    <div class="checkbox">
      <input id="restoreSshConfigAfterConnect" type="checkbox" />
      <label for="restoreSshConfigAfterConnect">Restore Remote.SSH config after legacy fallback</label>
    </div>
    <label for="sshQueryConfigPath">SSH query config path</label>
    <input id="sshQueryConfigPath" type="text" />
    <div class="checkbox">
      <input id="forwardAgent" type="checkbox" />
      <label for="forwardAgent">Forward agent</label>
    </div>
    <div class="checkbox">
      <input id="requestTTY" type="checkbox" />
      <label for="requestTTY">Request TTY</label>
    </div>
    <label for="saveTarget">Save settings to</label>
    <select id="saveTarget">
      <option value="global">User settings</option>
      <option value="workspace">Workspace settings</option>
    </select>
    <div class="buttons" style="margin-top: 8px;">
      <button id="refresh">Reload saved values</button>
      <button id="openSettings">Open Settings</button>
    </div>
    <div class="buttons" style="margin-top: 8px;">
      <button id="openLogs" type="button">Open logs</button>
      <button id="openRemoteSshLog" type="button">Remote-SSH log</button>
    </div>
  </details>

  <div class="section">
    <div class="buttons" style="margin-top: 8px;">
      <button id="connectToggle">Connect</button>
    </div>
    <div id="connectStatus" class="hint"></div>
  <div class="hint">Connections add a small Slurm Connect Include block to your SSH config and update the Slurm Connect include file.</div>
  </div>

  <script nonce="${nonce}">
    const vscode = acquireVsCodeApi();
    let clusterInfo = null;
    let lastValues = {};
    let connectionState = 'idle';
    let availableModules = [];
    let selectedModules = [];
    let customModuleCommand = '';
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
      }
    };

    function setStatus(text, isError) {
      const el = document.getElementById('clusterStatus');
      if (!el) return;
      el.textContent = text || '';
      el.style.color = isError ? '#b00020' : '#555';
    }

    function setProfileStatus(text, isError) {
      const el = document.getElementById('profileStatus');
      if (!el) return;
      el.textContent = text || '';
      el.style.color = isError ? '#b00020' : '#555';
    }

    function setAgentStatus(text, isError) {
      const el = document.getElementById('agentStatus');
      if (!el) return;
      el.textContent = text || '';
      el.style.color = isError ? '#b00020' : '#555';
    }

    function setConnectState(state) {
      connectionState = state || 'idle';
      const button = document.getElementById('connectToggle');
      const status = document.getElementById('connectStatus');
      if (!button || !status) return;
      let label = 'Connect';
      let disabled = false;
      let statusText = '';
      if (connectionState === 'connecting') {
        label = 'Connecting...';
        disabled = true;
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

    function normalizeOptions(options) {
      const items = Array.isArray(options) ? options : [];
      return items.map((opt) => {
        const value = opt && opt.value !== undefined && opt.value !== null ? String(opt.value) : '';
        const label = opt && opt.label !== undefined ? String(opt.label) : String(value);
        return { value, label };
      });
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
      const picker = document.getElementById(field.picker);
      if (picker) {
        if (normalized.length === 0) {
          picker.classList.add('no-options');
        } else {
          picker.classList.remove('no-options');
        }
      }
      const menu = document.getElementById(field.menu);
      if (menu && menu.classList.contains('visible')) {
        showFieldMenu(fieldKey, false, false);
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
      const menu = document.getElementById(field.menu);
      if (menu) {
        menu.classList.remove('visible');
      }
    }

    function closeAllMenus() {
      hideModuleMenu();
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

    function normalizeModuleName(value) {
      return String(value || '').replace(/\s+/g, '');
    }

    function coalesceModuleTokens(tokens) {
      if (!Array.isArray(tokens) || tokens.length < 2 || availableModules.length === 0) {
        return tokens;
      }
      const normalizedMap = new Map();
      availableModules.forEach((name) => {
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
      const tokens = String(input || '')
        .split(/[\s,]+/)
        .map((value) => value.trim())
        .filter(Boolean);
      return coalesceModuleTokens(tokens);
    }

    function updateModuleHint() {
      const hint = document.getElementById('moduleHint');
      if (!hint) return;
      if (customModuleCommand) {
        hint.textContent = 'Custom module command set; adding modules will replace it.';
        return;
      }
      if (!availableModules.length) {
        hint.textContent = 'No module list available. Type module names below.';
        return;
      }
      hint.textContent = availableModules.length + ' modules available. Click the field or type to filter.';
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
        const row = document.createElement('div');
        row.className = 'module-item';
        const label = document.createElement('span');
        label.textContent = moduleName;
        row.appendChild(label);
        const remove = document.createElement('button');
        remove.type = 'button';
        remove.textContent = '-';
        remove.addEventListener('click', () => {
          selectedModules = selectedModules.filter((entry) => entry !== moduleName);
          renderModuleList();
        });
        row.appendChild(remove);
        container.appendChild(row);
      });
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
      const moduleMatch = trimmed.match(/^module\s+load\s+(.+)$/i);
      const mlMatch = trimmed.match(/^ml\s+(.+)$/i);
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
        selectedModules = selections.map((entry) => entry.trim()).filter(Boolean);
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
      const picker = document.getElementById('modulePicker');
      if (availableModules.length === 0) {
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
      return availableModules.filter((name) => name.toLowerCase().includes(text));
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
      if (!menu || !input || availableModules.length === 0) return;
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

    function addModulesFromInput() {
      const input = document.getElementById('moduleInput');
      const entries = parseModuleList(input ? input.value : '');
      if (entries.length === 0) {
        return;
      }
      let changed = false;
      entries.forEach((entry) => {
        if (!selectedModules.includes(entry)) {
          selectedModules.push(entry);
          changed = true;
        }
      });
      if (changed) {
        customModuleCommand = '';
        renderModuleList();
      }
      if (input) input.value = '';
    }

    function setFieldDisabled(fieldKey, disabled) {
      const field = resourceFields[fieldKey];
      if (!field) return;
      const input = document.getElementById(field.input);
      if (input) input.disabled = disabled;
      const picker = document.getElementById(field.picker);
      if (picker) {
        if (disabled || field.options.length === 0) {
          picker.classList.add('no-options');
        } else {
          picker.classList.remove('no-options');
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

    function clearClusterInfoUi() {
      const preserved = {};
      Object.keys(resourceFields).forEach((key) => {
        preserved[key] = getValue(key);
      });
      clusterInfo = null;
      setResourceDisabled(false);
      clearResourceOptions();
      setModuleOptions([]);
      Object.keys(preserved).forEach((key) => {
        if (preserved[key] !== undefined && preserved[key] !== null) {
          setValue(key, preserved[key]);
        }
      });
      const meta = document.getElementById('partitionMeta');
      if (meta) meta.textContent = '';
      setStatus('Cluster info cleared. Enter values manually or click \"Get cluster info\".', false);
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

    function updatePartitionDetails() {
      if (!clusterInfo || !clusterInfo.partitions) return;
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

      setFieldOptions('defaultNodes', buildRangeOptions(chosen.nodes), preferredNodes);
      setFieldOptions('defaultCpusPerTask', buildRangeOptions(chosen.cpus), preferredCpus);
      setFieldOptions('defaultMemoryMb', buildMemoryOptions(chosen.memMb), preferredMem);

      const gpuTypes = chosen.gpuTypes || {};
      const gpuTypeKeys = Object.keys(gpuTypes);
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
        const maxGpu = selectedType ? gpuTypes[selectedType] : chosen.gpuMax;
        const countOptions = [{ value: '0', label: '0' }, ...buildRangeOptions(maxGpu)];
        setFieldOptions('defaultGpuCount', countOptions, preferredGpuCount);
        setFieldDisabled('defaultGpuType', false);
        setFieldDisabled('defaultGpuCount', false);
      }
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

    function applyClusterInfo(info) {
      const manualPartition = getValue('defaultPartition');
      clusterInfo = info;
      setModuleOptions(info && Array.isArray(info.modules) ? info.modules : []);
      if (!info || !info.partitions || info.partitions.length === 0) {
        setStatus('No partitions found.', true);
        clearResourceOptions();
        setResourceDisabled(false);
        return;
      }
      setResourceDisabled(false);
      const options = info.partitions.map((partition) => ({
        value: partition.name,
        label: partition.isDefault ? partition.name + ' (default)' : partition.name
      }));
      const resolvedDefault = getClusterDefaultPartitionName();
      const preferredPartition = manualPartition || lastValues.defaultPartition || resolvedDefault;
      setFieldOptions('defaultPartition', options, preferredPartition);
      if (!manualPartition && !lastValues.defaultPartition && resolvedDefault) {
        lastValues.defaultPartition = resolvedDefault;
      }
      updatePartitionDetails();
      setStatus('Loaded ' + info.partitions.length + ' partitions.', false);
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
        qosCommand: getValue('qosCommand'),
        accountCommand: getValue('accountCommand'),
        user: getValue('user'),
        identityFile: getValue('identityFile'),
        moduleLoad: getValue('moduleLoad'),
        moduleSelections: selectedModules.slice(),
        moduleCustomCommand: customModuleCommand,
        proxyCommand: getValue('proxyCommand'),
        proxyArgs: getValue('proxyArgs'),
        extraSallocArgs: getValue('extraSallocArgs'),
        promptForExtraSallocArgs: getValue('promptForExtraSallocArgs'),
        defaultPartition: getValue('defaultPartition'),
        defaultNodes: getValue('defaultNodes'),
        defaultTasksPerNode: getValue('defaultTasksPerNode'),
        defaultCpusPerTask: getValue('defaultCpusPerTask'),
        defaultTime: getValue('defaultTime'),
        defaultMemoryMb: getValue('defaultMemoryMb'),
        defaultGpuType: getValue('defaultGpuType'),
        defaultGpuCount: getValue('defaultGpuCount'),
        sshHostPrefix: getValue('sshHostPrefix'),
        temporarySshConfigPath: getValue('temporarySshConfigPath'),
        restoreSshConfigAfterConnect: getValue('restoreSshConfigAfterConnect'),
        sshQueryConfigPath: getValue('sshQueryConfigPath'),
        forwardAgent: getValue('forwardAgent'),
        requestTTY: getValue('requestTTY'),
        openInNewWindow: getValue('openInNewWindow'),
        remoteWorkspacePath: getValue('remoteWorkspacePath')
      };
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
        suppressAutoSave = true;
        const values = message.values || {};
        lastValues = values;
        Object.keys(values).forEach((key) => setValue(key, values[key]));
        const saveTarget = document.getElementById('saveTarget');
        if (saveTarget && message.saveTarget) {
          saveTarget.value = message.saveTarget;
        }
        applyModuleState(values);
        if (message.clusterInfo) {
          applyClusterInfo(message.clusterInfo);
          if (message.clusterInfoCachedAt) {
            const cachedAt = new Date(message.clusterInfoCachedAt).toLocaleString();
            setStatus('Loaded cached cluster info (' + cachedAt + ').', false);
          }
        } else {
          clusterInfo = null;
          clearResourceOptions();
          const meta = document.getElementById('partitionMeta');
          if (meta) meta.textContent = '';
          setModuleOptions([]);
          setResourceDisabled(false);
          setStatus('Enter values manually or click "Get cluster info" to populate suggestions.', false);
        }
        applyMessageState(message);
        suppressAutoSave = false;
      } else if (message.command === 'clusterInfo') {
        applyClusterInfo(message.info);
        applyMessageState(message);
      } else if (message.command === 'clusterInfoError') {
        setStatus(message.message || 'Failed to load cluster info.', true);
        setResourceDisabled(false);
        applyMessageState(message);
      } else if (message.command === 'profiles') {
        applyMessageState(message);
      } else if (message.command === 'connectionState') {
        setConnectState(message.state || 'idle');
      }
    });

    document.getElementById('getClusterInfo').addEventListener('click', () => {
      setStatus('Fetching cluster info...', false);
      vscode.postMessage({
        command: 'getClusterInfo',
        values: gather()
      });
    });

    document.getElementById('clearClusterInfo').addEventListener('click', () => {
      clearClusterInfoUi();
    });

    document.getElementById('fillPartitionDefaults').addEventListener('click', () => {
      applyPartitionDefaults();
    });

    document.getElementById('clearResources').addEventListener('click', () => {
      clearResourceFields();
    });

    const moduleAddButton = document.getElementById('moduleAdd');
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

    document.getElementById('profileSelect').addEventListener('change', () => {
      // no-op, selection already reflects current profile
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
      if (connectionState === 'connected') {
        setConnectState('disconnecting');
        vscode.postMessage({ command: 'disconnect' });
        return;
      }
      setConnectState('connecting');
      vscode.postMessage({
        command: 'connect',
        values: gather(),
        target: document.getElementById('saveTarget').value
      });
    });

    document.getElementById('refresh').addEventListener('click', () => {
      vscode.postMessage({ command: 'ready' });
    });
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
