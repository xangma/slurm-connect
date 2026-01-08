import * as vscode from 'vscode';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs/promises';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { buildHostEntry, buildTemporarySshConfigContent, expandHome } from './utils/sshConfig';
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

let outputChannel: vscode.OutputChannel | undefined;
let logFilePath: string | undefined;
let extensionStoragePath: string | undefined;
let extensionGlobalState: vscode.Memento | undefined;
let activeWebview: vscode.Webview | undefined;
let connectionState: ConnectionState = 'idle';
let lastConnectionAlias: string | undefined;
let previousRemoteSshConfigPath: string | undefined;
let activeTempSshConfigPath: string | undefined;
let lastSshAuthPrompt: { identityPath: string; timestamp: number } | undefined;
let clusterInfoRequestInFlight = false;
const SETTINGS_SECTION = 'slurmConnect';
const LEGACY_SETTINGS_SECTION = 'sciamaSlurm';
const PROFILE_STORE_KEY = 'slurmConnect.profiles';
const ACTIVE_PROFILE_KEY = 'slurmConnect.activeProfile';
const PENDING_RESTORE_KEY = 'slurmConnect.pendingRestore';
const CLUSTER_INFO_CACHE_KEY = 'slurmConnect.clusterInfoCache';
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

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  extensionStoragePath = context.globalStorageUri.fsPath;
  extensionGlobalState = context.globalState;
  await migrateLegacyState();
  await migrateLegacySettings();
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
  const defaults = getUiValuesFromConfig(getConfig());
  if (!values) {
    return defaults;
  }
  return {
    ...defaults,
    ...values
  } as UiValues;
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
    return;
  }
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const current = normalizeRemoteConfigPath(remoteCfg.get<string>('configFile'));
  if (!current || current !== pending.tempConfigPath) {
    await clearPendingRestore();
    return;
  }

  if (pending.openInNewWindow) {
    await waitForRemoteConnectionOrTimeout(30_000, 500);
  } else {
    await delay(500);
  }
  await restoreRemoteSshConfigIfNeeded();
}

function getConfig(): SlurmConnectConfig {
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
          const defaults = getUiValuesFromConfig(getConfig());
          let values = defaults;
          if (activeProfile) {
            const stored = getProfileValues(activeProfile);
            if (stored) {
              values = { ...defaults, ...stored } as UiValues;
            } else {
              activeProfile = undefined;
            }
          }
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
            remoteActive: vscode.env.remoteName === 'ssh-remote'
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
            profileStatus: `Loaded profile "${name}".`
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
    if (!nodesInput) {
      return false;
    }
    nodes = nodesInput;

    const tasksInput = await promptNumber('Tasks per node', cfg.defaultTasksPerNode, 1);
    if (!tasksInput) {
      return false;
    }
    tasksPerNode = tasksInput;

    const cpusInput = await promptNumber('CPUs per task', cfg.defaultCpusPerTask, 1);
    if (!cpusInput) {
      return false;
    }
    cpusPerTask = cpusInput;

    const timeInput = await promptTime('Wall time', cfg.defaultTime || '01:00:00');
    if (!timeInput) {
      return false;
    }
    time = timeInput;
  } else {
    if (!nodes || nodes < 1) {
      void vscode.window.showErrorMessage('Default nodes is not set. Fill it in the side panel or settings.');
      return false;
    }
    if (!tasksPerNode || tasksPerNode < 1) {
      void vscode.window.showErrorMessage('Default tasks per node is not set. Fill it in the side panel or settings.');
      return false;
    }
    if (!cpusPerTask || cpusPerTask < 1) {
      void vscode.window.showErrorMessage('Default CPUs per task is not set. Fill it in the side panel or settings.');
      return false;
    }
    if (!time) {
      void vscode.window.showErrorMessage('Default wall time is not set. Fill it in the side panel or settings.');
      return false;
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

  await ensureRemoteSshSettings();
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const currentRemoteConfig = normalizeRemoteConfigPath(remoteCfg.get<string>('configFile'));
  const baseRemoteConfig = resolveBaseRemoteConfig(currentRemoteConfig);
  const includePaths = resolveSshConfigIncludes(baseRemoteConfig);
  previousRemoteSshConfigPath = baseRemoteConfig;
  if (includePaths.length > 0) {
    log.appendLine(`Temporary SSH config includes: ${includePaths.join(', ')}`);
  }

  try {
    tempConfigPath = await writeTemporarySshConfig(hostEntry, cfg, includePaths);
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
  await delay(300);
  await refreshRemoteSshHosts();

  const connected = await connectToHost(
    alias.trim(),
    cfg.openInNewWindow,
    cfg.remoteWorkspacePath
  );
  didConnect = connected;
  if (connected && !cfg.openInNewWindow) {
    await delay(500);
    await restoreRemoteSshConfigIfNeeded();
  } else if (connected && cfg.openInNewWindow) {
    log.appendLine('Deferring SSH config restore until the remote window is ready.');
  }
  if (!connected) {
    void vscode.window.showWarningMessage(
      `SSH host "${alias.trim()}" created, but auto-connect failed. Use Remote-SSH to connect.`,
      'Show Output'
    ).then((selection) => {
      if (selection === 'Show Output') {
        getOutputChannel().show(true);
      }
    });
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
      webview.postMessage({ command: 'clusterInfoError', message });
      return;
    }

    const loginHost = loginHosts[0];
    log.appendLine(`Fetching cluster info from ${loginHost}...`);

    const info = await fetchClusterInfo(loginHost, cfg);
    cacheClusterInfo(loginHost, info);
    webview.postMessage({ command: 'clusterInfo', info });
  } catch (error) {
    const message = formatError(error);
    void vscode.window.showErrorMessage(`Failed to fetch cluster info: ${message}`);
    webview.postMessage({ command: 'clusterInfoError', message });
  } finally {
    clusterInfoRequestInFlight = false;
  }
}

async function fetchClusterInfo(loginHost: string, cfg: SlurmConnectConfig): Promise<ClusterInfo> {
  const commands = [
    cfg.partitionInfoCommand,
    'sinfo -h -N -o "%P|%n|%c|%m|%G"',
    'sinfo -h -o "%P|%D|%c|%m|%G"'
  ].filter(Boolean);

  let lastInfo: ClusterInfo = { partitions: [] };
  let bestInfo: ClusterInfo | undefined;
  const log = getOutputChannel();
  for (const command of commands) {
    log.appendLine(`Cluster info command: ${command}`);
    const output = await runSshCommand(loginHost, cfg, command);
    const info = parsePartitionInfoOutput(output);
    lastInfo = info;
    const maxFields = getMaxFieldCount(output);
    const outputHasGpu = output.includes('gpu:');
    const hasGpu = info.partitions.some((partition) => partition.gpuMax > 0);
    log.appendLine(`Cluster info fields: ${maxFields}, partitions: ${info.partitions.length}, outputHasGpu: ${outputHasGpu}, hasGpu: ${hasGpu}`);
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
  const modules = await queryAvailableModules(loginHost, cfg);
  info.modules = modules;
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
    /sign_and_send_pubkey/i,
    /identity file .* not accessible/i
  ].some((pattern) => pattern.test(text));
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

async function isSshKeyLoaded(identityPath: string): Promise<boolean> {
  if (!identityPath) {
    return false;
  }
  let output = '';
  try {
    const result = await execFileAsync('ssh-add', ['-l']);
    output = [result.stdout, result.stderr].filter(Boolean).join('\n');
  } catch (error) {
    const errorText = normalizeSshErrorText(error);
    if (/no identities/i.test(errorText)) {
      return false;
    }
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
  void vscode.window.showInformationMessage('Follow the terminal prompt to add your SSH key.');
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

async function maybePromptForSshAuth(cfg: SlurmConnectConfig, errorText: string): Promise<boolean> {
  if (!looksLikeAuthFailure(errorText)) {
    return false;
  }
  const identityPath = cfg.identityFile ? expandHome(cfg.identityFile) : '';
  const identityExists = identityPath ? await fileExists(identityPath) : false;
  if (identityPath && identityExists && await isSshKeyLoaded(identityPath)) {
    return false;
  }
  const now = Date.now();
  if (lastSshAuthPrompt) {
    const sameIdentity = lastSshAuthPrompt.identityPath === identityPath;
    const recentlyPrompted = now - lastSshAuthPrompt.timestamp < 60_000;
    if (sameIdentity && recentlyPrompted) {
      return false;
    }
  }
  lastSshAuthPrompt = { identityPath, timestamp: now };

  const actions: string[] = [];
  if (identityPath && identityExists) {
    actions.push('Open Terminal');
  }
  if (!identityPath || !identityExists) {
    actions.push('Open Settings');
  }
  actions.push('Dismiss');
  const message = identityPath && !identityExists
    ? `SSH authentication failed while querying the cluster. The identity file ${identityPath} was not found. Update slurmConnect.identityFile to a valid key.`
    : identityPath
      ? `SSH authentication failed while querying the cluster. Add ${identityPath} to your ssh-agent.`
      : 'SSH authentication failed while querying the cluster. Add your key to the ssh-agent (ssh-add <key>) or set slurmConnect.identityFile.';
  const choice = await vscode.window.showWarningMessage(message, { modal: true }, ...actions);
  if (choice === 'Open Terminal' && identityPath) {
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
      return true;
    }
    void vscode.window.showInformationMessage(
      'SSH key not detected in agent yet. Re-run Get cluster info after ssh-add completes.'
    );
    return false;
  }
  if (choice === 'Open Settings') {
    void vscode.commands.executeCommand('workbench.action.openSettings', `${SETTINGS_SECTION}.identityFile`);
  }
  return false;
}

async function runSshCommand(host: string, cfg: SlurmConnectConfig, command: string): Promise<string> {
  const args: string[] = [];
  if (cfg.sshQueryConfigPath) {
    args.push('-F', expandHome(cfg.sshQueryConfigPath));
  }
  args.push('-T', '-o', 'BatchMode=yes', '-o', `ConnectTimeout=${cfg.sshConnectTimeoutSeconds}`);
  if (cfg.identityFile) {
    args.push('-i', expandHome(cfg.identityFile));
  }
  const target = cfg.user ? `${cfg.user}@${host}` : host;
  args.push(target, command);

  try {
    const { stdout } = await execFileAsync('ssh', args, { timeout: cfg.sshConnectTimeoutSeconds * 1000 });
    return stdout.trim();
  } catch (error) {
    const errorText = normalizeSshErrorText(error);
    const shouldRetry = await maybePromptForSshAuth(cfg, errorText);
    if (shouldRetry) {
      try {
        const { stdout } = await execFileAsync('ssh', args, { timeout: cfg.sshConnectTimeoutSeconds * 1000 });
        return stdout.trim();
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
      title: 'Partition (optional)',
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
    return undefined;
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
    validateInput: (input) => {
      const parsed = Number(input);
      if (!Number.isInteger(parsed) || parsed < minValue) {
        return `Enter an integer >= ${minValue}.`;
      }
      return undefined;
    }
  });
  if (!value) {
    return undefined;
  }
  return Number(value);
}

async function promptTime(title: string, defaultValue: string): Promise<string | undefined> {
  const timePattern = /^(\d+-)?\d{1,2}:\d{2}:\d{2}$/;
  const value = await vscode.window.showInputBox({
    title,
    value: defaultValue,
    prompt: 'HH:MM:SS or D-HH:MM:SS',
    validateInput: (input) => (timePattern.test(input) ? undefined : 'Invalid time format.')
  });
  return value?.trim() || undefined;
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
  args.push(`--nodes=${params.nodes}`);
  args.push(`--ntasks-per-node=${params.tasksPerNode}`);
  args.push(`--cpus-per-task=${params.cpusPerTask}`);
  args.push(`--time=${params.time}`);
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
  nodes: number,
  cpusPerTask: number
): string {
  const hostShort = loginHost.split('.')[0];
  const pieces = [prefix || 'slurm', hostShort];
  if (partition) {
    pieces.push(partition);
  }
  pieces.push(`${nodes}n`, `${cpusPerTask}c`);
  return pieces.join('-').replace(/[^a-zA-Z0-9_-]/g, '-');
}

async function writeTemporarySshConfig(
  entry: string,
  cfg: SlurmConnectConfig,
  includePaths: string[]
): Promise<string> {
  const basePath = cfg.temporarySshConfigPath
    ? expandHome(cfg.temporarySshConfigPath)
    : path.join(extensionStoragePath || os.tmpdir(), 'slurm-connect-ssh-config');
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


async function ensureRemoteSshSettings(): Promise<void> {
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const enableRemoteCommand = remoteCfg.get<boolean>('enableRemoteCommand', false);
  if (!enableRemoteCommand) {
    const enable = await vscode.window.showWarningMessage(
      'Remote.SSH: Enable Remote Command is disabled. This is required for Slurm proxying.',
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
  const sshCommands = availableCommands.filter((command) =>
    /ssh|openssh/i.test(command)
  );
  log.appendLine(`Available SSH commands: ${sshCommands.join(', ') || '(none)'}`);

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
  log.appendLine(`Available SSH commands: ${sshCommands.join(', ') || '(none)'}`);

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
  return false;
}

function getUiValuesFromConfig(cfg: SlurmConnectConfig): UiValues {
  const config = vscode.workspace.getConfiguration(SETTINGS_SECTION);
  const hasValue = (key: string): boolean => {
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
  const hasDefaultPartition = hasValue('defaultPartition');
  const hasDefaultNodes = hasValue('defaultNodes');
  const hasDefaultTasksPerNode = hasValue('defaultTasksPerNode');
  const hasDefaultCpusPerTask = hasValue('defaultCpusPerTask');
  const hasDefaultTime = hasValue('defaultTime');
  const hasDefaultMemoryMb = hasValue('defaultMemoryMb');
  const hasDefaultGpuType = hasValue('defaultGpuType');
  const hasDefaultGpuCount = hasValue('defaultGpuCount');
  return {
    loginHosts: cfg.loginHosts.join('\n'),
    loginHostsCommand: cfg.loginHostsCommand || '',
    loginHostsQueryHost: cfg.loginHostsQueryHost || '',
    partitionCommand: cfg.partitionCommand || '',
    partitionInfoCommand: cfg.partitionInfoCommand || '',
    qosCommand: cfg.qosCommand || '',
    accountCommand: cfg.accountCommand || '',
    user: cfg.user || '',
    identityFile: cfg.identityFile || '',
    moduleLoad: cfg.moduleLoad || '',
    proxyCommand: cfg.proxyCommand || '',
    proxyArgs: cfg.proxyArgs.join('\n'),
    extraSallocArgs: cfg.extraSallocArgs.join('\n'),
    promptForExtraSallocArgs: cfg.promptForExtraSallocArgs,
    defaultPartition: hasDefaultPartition ? (cfg.defaultPartition || '') : '',
    defaultNodes: hasDefaultNodes && cfg.defaultNodes ? String(cfg.defaultNodes) : '',
    defaultTasksPerNode: hasDefaultTasksPerNode && cfg.defaultTasksPerNode ? String(cfg.defaultTasksPerNode) : '',
    defaultCpusPerTask: hasDefaultCpusPerTask && cfg.defaultCpusPerTask ? String(cfg.defaultCpusPerTask) : '',
    defaultTime: hasDefaultTime ? (cfg.defaultTime || '') : '',
    defaultMemoryMb: hasDefaultMemoryMb && cfg.defaultMemoryMb ? String(cfg.defaultMemoryMb) : '',
    defaultGpuType: hasDefaultGpuType ? (cfg.defaultGpuType || '') : '',
    defaultGpuCount: hasDefaultGpuCount && cfg.defaultGpuCount ? String(cfg.defaultGpuCount) : '',
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
    ['loginHosts', parseListInput(values.loginHosts)],
    ['loginHostsCommand', values.loginHostsCommand.trim()],
    ['loginHostsQueryHost', values.loginHostsQueryHost.trim()],
    ['partitionCommand', values.partitionCommand.trim()],
    ['partitionInfoCommand', values.partitionInfoCommand.trim()],
    ['qosCommand', values.qosCommand.trim()],
    ['accountCommand', values.accountCommand.trim()],
    ['user', values.user.trim()],
    ['identityFile', values.identityFile.trim()],
    ['moduleLoad', values.moduleLoad.trim()],
    ['proxyCommand', values.proxyCommand.trim()],
    ['proxyArgs', parseListInput(values.proxyArgs)],
    ['extraSallocArgs', parseListInput(values.extraSallocArgs)],
    ['promptForExtraSallocArgs', Boolean(values.promptForExtraSallocArgs)],
    ['defaultPartition', values.defaultPartition.trim()],
    ['defaultNodes', parseNumberValue(values.defaultNodes, 1)],
    ['defaultTasksPerNode', parseNumberValue(values.defaultTasksPerNode, 1)],
    ['defaultCpusPerTask', parseNumberValue(values.defaultCpusPerTask, 1)],
    ['defaultTime', values.defaultTime.trim()],
    ['defaultMemoryMb', parseNumberValue(values.defaultMemoryMb, 0)],
    ['defaultGpuType', values.defaultGpuType.trim()],
    ['defaultGpuCount', parseNumberValue(values.defaultGpuCount, 0)],
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

function parseNumberValue(input: string, fallback: number): number {
  const parsed = Number(input);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
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
    defaultNodes: parseNumberValue(values.defaultNodes, 1),
    defaultTasksPerNode: parseNumberValue(values.defaultTasksPerNode, 1),
    defaultCpusPerTask: parseNumberValue(values.defaultCpusPerTask, 1),
    defaultTime: values.defaultTime.trim(),
    defaultMemoryMb: parseNumberValue(values.defaultMemoryMb, 0),
    defaultGpuType: values.defaultGpuType.trim(),
    defaultGpuCount: parseNumberValue(values.defaultGpuCount, 0),
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
      <input id="defaultPartitionInput" type="text" placeholder="(optional)" />
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
    <input id="defaultTime" type="text" />
    <div class="buttons" style="margin-top: 8px;">
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
    <label for="temporarySshConfigPath">Temporary SSH config path</label>
    <input id="temporarySshConfigPath" type="text" />
    <div class="checkbox">
      <input id="restoreSshConfigAfterConnect" type="checkbox" />
      <label for="restoreSshConfigAfterConnect">Restore Remote.SSH config after connect</label>
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
    <div class="hint">Defaults save on connect (change in Advanced).</div>
    <div class="hint">Connections use a temporary SSH config; your main SSH config is not modified.</div>
  </div>

  <script nonce="${nonce}">
    const vscode = acquireVsCodeApi();
    let clusterInfo = null;
    let lastValues = {};
    let connectionState = 'idle';
    let availableModules = [];
    let selectedModules = [];
    let customModuleCommand = '';

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

    function updatePartitionDetails() {
      if (!clusterInfo || !clusterInfo.partitions) return;
      const selected = getValue('defaultPartition').trim();
      let chosen = selected ? clusterInfo.partitions.find((p) => p.name === selected) : undefined;
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
      const preferredPartition = manualPartition || lastValues.defaultPartition;
      setFieldOptions('defaultPartition', options, preferredPartition);
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
    }

    window.addEventListener('message', (event) => {
      const message = event.data;
      if (message.command === 'load') {
        const values = message.values || {};
        lastValues = values;
        Object.keys(values).forEach((key) => setValue(key, values[key]));
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
