import * as vscode from 'vscode';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs/promises';
import { execFile } from 'child_process';
import { promisify } from 'util';

const execFileAsync = promisify(execFile);

interface SciamaConfig {
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

interface PartitionInfo {
  name: string;
  nodes: number;
  cpus: number;
  memMb: number;
  gpuMax: number;
  gpuTypes: Record<string, number>;
  isDefault: boolean;
}

interface ClusterInfo {
  partitions: PartitionInfo[];
  defaultPartition?: string;
  modules?: string[];
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
const PROFILE_STORE_KEY = 'sciamaSlurm.profiles';
const ACTIVE_PROFILE_KEY = 'sciamaSlurm.activeProfile';
const PENDING_RESTORE_KEY = 'sciamaSlurm.pendingRestore';

export function activate(context: vscode.ExtensionContext): void {
  extensionStoragePath = context.globalStorageUri.fsPath;
  extensionGlobalState = context.globalState;
  syncConnectionStateFromEnvironment();
  const disposable = vscode.commands.registerCommand('sciamaSlurm.connect', () => {
    void connectCommand();
  });
  context.subscriptions.push(disposable);

  void maybeRestorePendingOnStartup();

  const viewProvider = new SlurmConnectViewProvider(context);
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider('sciamaSlurm.connectView', viewProvider)
  );
}

export function deactivate(): void {
  // No-op
}

function getOutputChannel(): vscode.OutputChannel {
  if (!outputChannel) {
    const baseChannel = vscode.window.createOutputChannel('Sciama Slurm');
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
  logFilePath = path.join(baseDir, 'sciama-slurm.log');
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
  await delay(500);
  await restoreRemoteSshConfigIfNeeded();
}

function getConfig(): SciamaConfig {
  const cfg = vscode.workspace.getConfiguration('sciamaSlurm');
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

function getConfigWithOverrides(overrides?: Partial<SciamaConfig>): SciamaConfig {
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
          const rawName = typeof message.name === 'string' ? message.name : '';
          const name = rawName.trim();
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
          const uiValues = message.values as UiValues;
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
          void vscode.commands.executeCommand('workbench.action.openSettings', 'sciamaSlurm');
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
  overrides?: Partial<SciamaConfig>,
  options?: { interactive?: boolean }
): Promise<boolean> {
  const cfg = getConfigWithOverrides(overrides);
  const interactive = options?.interactive !== false;
  const log = getOutputChannel();
  log.clear();
  log.appendLine('Sciama Slurm connect started.');

  let didConnect = false;

  const loginHosts = await resolveLoginHosts(cfg);
  log.appendLine(`Login hosts resolved: ${loginHosts.join(', ') || '(none)'}`);
  if (loginHosts.length === 0) {
    void vscode.window.showErrorMessage('No login hosts available. Configure sciamaSlurm.loginHosts or loginHostsCommand.');
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
    void vscode.window.showErrorMessage('RemoteCommand is empty. Check sciamaSlurm.proxyCommand.');
    return false;
  }

  const defaultAlias = buildDefaultAlias(cfg.sshHostPrefix || 'sciama', loginHost, partition, nodes, cpusPerTask);
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
  if (connected) {
    await delay(500);
    await restoreRemoteSshConfigIfNeeded();
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

async function resolveLoginHosts(cfg: SciamaConfig): Promise<string[]> {
  let hosts = cfg.loginHosts.slice();
  if (cfg.loginHostsCommand) {
    let queryHost: string | undefined = cfg.loginHostsQueryHost || hosts[0];
    if (!queryHost) {
      queryHost = await vscode.window.showInputBox({
        title: 'Login host for discovery',
        prompt: 'Enter a login host to run the loginHostsCommand on.'
      });
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
  if (hosts.length === 0) {
    const manual = await vscode.window.showInputBox({
      title: 'Login host',
      prompt: 'Enter a login host'
    });
    if (manual) {
      hosts = [manual.trim()];
    }
  }
  return hosts;
}

async function queryPartitions(loginHost: string, cfg: SciamaConfig): Promise<PartitionResult> {
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

async function querySimpleList(loginHost: string, cfg: SciamaConfig, command: string): Promise<string[]> {
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

async function queryAvailableModules(loginHost: string, cfg: SciamaConfig): Promise<string[]> {
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
  const overrides = buildOverridesFromUi(values);
  const cfg = getConfigWithOverrides(overrides);
  const loginHosts = parseListInput(values.loginHosts);
  const log = getOutputChannel();

  if (loginHosts.length === 0) {
    const message = 'Enter a login host before fetching cluster info.';
    void vscode.window.showErrorMessage(message);
    webview.postMessage({ command: 'clusterInfoError', message });
    return;
  }

  const loginHost = loginHosts[0];
  log.appendLine(`Fetching cluster info from ${loginHost}...`);

  try {
    const info = await fetchClusterInfo(loginHost, cfg);
    cacheClusterInfo(loginHost, info);
    webview.postMessage({ command: 'clusterInfo', info });
  } catch (error) {
    const message = formatError(error);
    void vscode.window.showErrorMessage(`Failed to fetch cluster info: ${message}`);
    webview.postMessage({ command: 'clusterInfoError', message });
  }
}

async function fetchClusterInfo(loginHost: string, cfg: SciamaConfig): Promise<ClusterInfo> {
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

function hasMeaningfulClusterInfo(info: ClusterInfo): boolean {
  if (!info.partitions || info.partitions.length === 0) {
    return false;
  }
  return info.partitions.some((partition) =>
    partition.cpus > 0 || partition.memMb > 0 || partition.gpuMax > 0
  );
}

function getMaxFieldCount(output: string): number {
  const lines = output.split(/\r?\n/).filter((line) => line.trim().length > 0);
  let max = 0;
  for (const line of lines) {
    const count = line.split('|').length;
    if (count > max) {
      max = count;
    }
  }
  return max;
}

function parsePartitionInfoOutput(output: string): ClusterInfo {
  const lines = output.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  const partitions = new Map<string, PartitionInfo>();
  const nodeSets = new Map<string, Set<string>>();
  let defaultPartition: string | undefined;

  for (const line of lines) {
    const fields = line.split('|').map((value) => value.trim());
    if (fields.length < 3) {
      continue;
    }
    const rawName = fields[0];
    const field1 = fields[1] || '';
    const field2 = fields[2] || '';
    const field3 = fields[3] || '';
    const field4 = fields[4] || '';
    if (!rawName) {
      continue;
    }

    const isField1Numeric = /^\d/.test(field1);
    const nodeName = !isField1Numeric && field1 ? field1 : undefined;
    const nodesCount = isField1Numeric ? parseNumericField(field1) : 0;
    const cpusRaw = field2;
    const memRaw = field3;
    const gresRaw = field4;
    const cpus = parseNumericField(cpusRaw.includes('/') ? cpusRaw.split('/').pop() || '' : cpusRaw);
    const memMb = parseNumericField(memRaw);
    const gresInfo = parseGresInfo(gresRaw || '');

    const partitionNames = rawName.split(',').map((part) => part.trim()).filter(Boolean);
    for (const partitionName of partitionNames) {
      const isDefault = partitionName.includes('*');
      const name = partitionName.replace(/\*/g, '');
      if (isDefault && !defaultPartition) {
        defaultPartition = name;
      }

      const existing = partitions.get(name) || {
        name,
        nodes: 0,
        cpus: 0,
        memMb: 0,
        gpuMax: 0,
        gpuTypes: {},
        isDefault
      };

      if (nodeName) {
        if (!nodeSets.has(name)) {
          nodeSets.set(name, new Set());
        }
        nodeSets.get(name)?.add(nodeName);
      } else if (nodesCount) {
        existing.nodes = Math.max(existing.nodes, nodesCount);
      }

      existing.cpus = Math.max(existing.cpus, cpus);
      existing.memMb = Math.max(existing.memMb, memMb);
      existing.gpuMax = Math.max(existing.gpuMax, gresInfo.gpuMax);
      for (const [type, count] of Object.entries(gresInfo.gpuTypes)) {
        const current = existing.gpuTypes[type] || 0;
        existing.gpuTypes[type] = Math.max(current, count);
      }
      existing.isDefault = existing.isDefault || isDefault;

      partitions.set(name, existing);
    }
  }

  for (const [name, info] of partitions) {
    const set = nodeSets.get(name);
    if (set && set.size > 0) {
      info.nodes = set.size;
    }
  }

  const list = Array.from(partitions.values()).sort((a, b) => a.name.localeCompare(b.name));
  return { partitions: list, defaultPartition };
}

function parseGresInfo(raw: string): { gpuMax: number; gpuTypes: Record<string, number> } {
  const result: { gpuMax: number; gpuTypes: Record<string, number> } = {
    gpuMax: 0,
    gpuTypes: {}
  };
  if (!raw) {
    return result;
  }
  const tokens = raw.split(',').map((token) => token.trim()).filter(Boolean);
  for (const token of tokens) {
    if (!token.includes('gpu')) {
      continue;
    }
    const cleaned = token.replace(/\(.*?\)/g, '');
    const parts = cleaned.split(':').map((part) => part.trim()).filter(Boolean);
    if (parts.length === 0 || parts[0] !== 'gpu') {
      continue;
    }
    let type = '';
    let count = 0;
    if (parts.length === 2) {
      if (/^\d+$/.test(parts[1])) {
        count = Number(parts[1]);
      } else {
        type = parts[1];
      }
    } else if (parts.length >= 3) {
      type = parts[1];
      if (/^\d+$/.test(parts[2])) {
        count = Number(parts[2]);
      }
    }
    if (count <= 0) {
      continue;
    }
    result.gpuMax = Math.max(result.gpuMax, count);
    const key = type || '';
    const existing = result.gpuTypes[key] || 0;
    result.gpuTypes[key] = Math.max(existing, count);
  }
  return result;
}

function parseNumericField(value: string): number {
  const match = value.match(/\d+/);
  if (!match) {
    return 0;
  }
  return Number(match[0]) || 0;
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


async function ensureAskpassScript(): Promise<string> {
  const baseDir = extensionStoragePath || os.tmpdir();
  const scriptPath = path.join(baseDir, 'sciama-ssh-askpass.sh');
  try {
    await fs.access(scriptPath);
    return scriptPath;
  } catch {
    // continue to (re)create
  }
  const script = ['#!/bin/sh', 'printf "%s\n" "${SCIAMA_SSH_PASSPHRASE:-}"', ''].join('\n');
  await fs.mkdir(path.dirname(scriptPath), { recursive: true });
  await fs.writeFile(scriptPath, script, { mode: 0o700 });
  await fs.chmod(scriptPath, 0o700);
  return scriptPath;
}

async function addKeyToAgent(identityPath: string): Promise<boolean> {
  const passphrase = await vscode.window.showInputBox({
    title: 'SSH key passphrase',
    prompt: `Enter the passphrase for ${identityPath} (leave blank if none)`,
    password: true,
    ignoreFocusOut: true
  });
  if (passphrase === undefined) {
    return false;
  }
  const askpassPath = await ensureAskpassScript();
  const env = {
    ...process.env,
    SCIAMA_SSH_PASSPHRASE: passphrase,
    SSH_ASKPASS: askpassPath,
    SSH_ASKPASS_REQUIRE: 'force',
    DISPLAY: process.env.DISPLAY || '1'
  };
  const log = getOutputChannel();
  try {
    log.appendLine(`Running ssh-add for ${identityPath}.`);
    const { stderr } = await execFileAsync('ssh-add', [identityPath], { env, timeout: 15000 });
    if (stderr && stderr.trim().length > 0) {
      log.appendLine(stderr.trim());
    }
    void vscode.window.showInformationMessage('SSH key added to agent.');
    return true;
  } catch (error) {
    const errorText = normalizeSshErrorText(error);
    const summary = pickSshErrorSummary(errorText) || 'SSH key add failed.';
    log.appendLine(`ssh-add failed: ${summary}`);
    void vscode.window.showWarningMessage(`Failed to add SSH key: ${summary}`);
    return false;
  }
  return false;
}

async function maybePromptForSshAuth(cfg: SciamaConfig, errorText: string): Promise<boolean> {
  if (!looksLikeAuthFailure(errorText)) {
    return false;
  }
  const identityPath = cfg.identityFile ? expandHome(cfg.identityFile) : '';
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
  if (identityPath) {
    actions.push('Add key to agent');
  }
  actions.push('Open Settings', 'Dismiss');
  const message = identityPath
    ? `SSH authentication failed while querying the cluster. Add ${identityPath} to your ssh-agent or update sciamaSlurm.identityFile.`
    : 'SSH authentication failed while querying the cluster. Add your key to the ssh-agent (ssh-add <key>) or set sciamaSlurm.identityFile.';
  const choice = await vscode.window.showWarningMessage(message, ...actions);
  if (choice === 'Add key to agent' && identityPath) {
    return await addKeyToAgent(identityPath);
  }
  if (choice === 'Open Settings') {
    void vscode.commands.executeCommand('workbench.action.openSettings', 'sciamaSlurm.identityFile');
  }
  return false;
}

async function runSshCommand(host: string, cfg: SciamaConfig, command: string): Promise<string> {
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


function buildRemoteCommand(cfg: SciamaConfig, sallocArgs: string[]): string {
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
  const pieces = [prefix || 'sciama', hostShort];
  if (partition) {
    pieces.push(partition);
  }
  pieces.push(`${nodes}n`, `${cpusPerTask}c`);
  return pieces.join('-').replace(/[^a-zA-Z0-9_-]/g, '-');
}

function buildHostEntry(alias: string, loginHost: string, cfg: SciamaConfig, remoteCommand: string): string {
  const lines: string[] = [];
  lines.push(`# Generated by Sciama Slurm Connect on ${new Date().toISOString()}`);
  lines.push(`Host ${alias}`);
  lines.push(`  HostName ${loginHost}`);
  if (cfg.user) {
    lines.push(`  User ${cfg.user}`);
  }
  if (cfg.requestTTY) {
    lines.push('  RequestTTY yes');
  }
  if (cfg.forwardAgent) {
    lines.push('  ForwardAgent yes');
  }
  if (cfg.identityFile) {
    lines.push(`  IdentityFile ${cfg.identityFile}`);
  }
  lines.push(`  RemoteCommand ${remoteCommand}`);

  const extraKeys = Object.keys(cfg.additionalSshOptions || {}).sort();
  for (const key of extraKeys) {
    const value = cfg.additionalSshOptions[key];
    if (value !== undefined && value !== null && String(value).length > 0) {
      lines.push(`  ${key} ${value}`);
    }
  }

  return `${lines.join('\n')}\n`;
}

async function writeTemporarySshConfig(
  entry: string,
  cfg: SciamaConfig,
  includePaths: string[]
): Promise<string> {
  const basePath = cfg.temporarySshConfigPath
    ? expandHome(cfg.temporarySshConfigPath)
    : path.join(extensionStoragePath || os.tmpdir(), 'sciama-ssh-config');
  const dir = path.dirname(basePath);
  await fs.mkdir(dir, { recursive: true });
  const resolvedBase = path.resolve(basePath);
  const includes = uniqueList(includePaths)
    .map((value) => value.trim())
    .filter(Boolean)
    .map((value) => expandHome(value))
    .filter((value) => value && path.resolve(value) !== resolvedBase);
  const includeLines = includes.map((value) => `Include ${value}`);
  const contentLines = ['# Temporary SSH config generated by Sciama Slurm Connect', entry.trimEnd()];
  if (includeLines.length > 0) {
    contentLines.push('', ...includeLines);
  }
  const content = `${contentLines.join('\n')}\n`;
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

function getUiValuesFromConfig(cfg: SciamaConfig): UiValues {
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
    defaultPartition: cfg.defaultPartition || '',
    defaultNodes: String(cfg.defaultNodes),
    defaultTasksPerNode: String(cfg.defaultTasksPerNode),
    defaultCpusPerTask: String(cfg.defaultCpusPerTask),
    defaultTime: cfg.defaultTime || '',
    defaultMemoryMb: cfg.defaultMemoryMb ? String(cfg.defaultMemoryMb) : '',
    defaultGpuType: cfg.defaultGpuType || '',
    defaultGpuCount: cfg.defaultGpuCount ? String(cfg.defaultGpuCount) : '',
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
  const cfg = vscode.workspace.getConfiguration('sciamaSlurm');

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

function expandHome(input: string): string {
  if (!input) {
    return input;
  }
  if (input.startsWith('~')) {
    return path.join(os.homedir(), input.slice(1));
  }
  return input;
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
    'sciamaSlurm.clusterInfoCache'
  );
  return cache ? cache[host] : undefined;
}

function cacheClusterInfo(host: string, info: ClusterInfo): void {
  if (!extensionGlobalState) {
    return;
  }
  const cache =
    extensionGlobalState.get<Record<string, { info: ClusterInfo; fetchedAt: string }>>(
      'sciamaSlurm.clusterInfoCache'
    ) || {};
  cache[host] = { info, fetchedAt: new Date().toISOString() };
  void extensionGlobalState.update('sciamaSlurm.clusterInfoCache', cache);
}

function parseNumberValue(input: string, fallback: number): number {
  const parsed = Number(input);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.floor(parsed);
}

function buildOverridesFromUi(values: UiValues): Partial<SciamaConfig> {
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
  <title>Sciama Slurm</title>
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
    .row > .module-picker { flex: 1; }
    .module-actions button { width: auto; min-width: 32px; }
    .module-list { margin-top: 6px; display: flex; flex-direction: column; gap: 4px; }
    .module-picker { position: relative; }
    .module-toggle { width: auto; min-width: 28px; padding: 0 6px; }
    .module-menu {
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
    .module-menu.visible { display: block; }
    .module-option {
      padding: 4px 6px;
      cursor: pointer;
    }
    .module-option:hover {
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
  <h2>Sciama Slurm</h2>

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
    <label for="profileName">Profile name</label>
    <input id="profileName" type="text" placeholder="e.g. gpu-a100" />
    <div class="buttons" style="margin-top: 8px;">
      <button id="profileLoad">Load</button>
      <button id="profileSave">Save profile</button>
      <button id="profileDelete">Delete</button>
    </div>
    <div id="profileStatus" class="hint"></div>
  </details>

  <div class="section">
    <label for="defaultPartitionInput">Partition</label>
    <select id="defaultPartitionSelect" class="hidden"></select>
    <input id="defaultPartitionInput" type="text" list="partitionOptions" placeholder="(optional)" />
    <datalist id="partitionOptions"></datalist>
    <div id="partitionMeta" class="hint"></div>
    <div class="row">
      <div>
        <label for="defaultNodesInput">Nodes</label>
        <select id="defaultNodesSelect" class="hidden"></select>
        <input id="defaultNodesInput" type="number" min="1" list="nodesOptions" />
        <datalist id="nodesOptions"></datalist>
      </div>
      <div>
        <label for="defaultTasksPerNode">Tasks per node</label>
        <input id="defaultTasksPerNode" type="number" min="1" />
      </div>
    </div>
    <div class="row">
      <div>
        <label for="defaultCpusPerTaskInput">CPUs per task</label>
        <select id="defaultCpusPerTaskSelect" class="hidden"></select>
        <input id="defaultCpusPerTaskInput" type="number" min="1" list="cpusOptions" />
        <datalist id="cpusOptions"></datalist>
      </div>
      <div>
        <label for="defaultMemoryMbInput">Memory per node</label>
        <select id="defaultMemoryMbSelect" class="hidden"></select>
        <input id="defaultMemoryMbInput" type="number" min="0" list="memoryOptions" placeholder="MB (optional)" />
        <datalist id="memoryOptions"></datalist>
      </div>
    </div>
    <div class="row">
      <div>
        <label for="defaultGpuTypeInput">GPU type</label>
        <select id="defaultGpuTypeSelect" class="hidden"></select>
        <input id="defaultGpuTypeInput" type="text" list="gpuTypeOptions" placeholder="Any" />
        <datalist id="gpuTypeOptions"></datalist>
      </div>
      <div>
        <label for="defaultGpuCountInput">GPU count</label>
        <select id="defaultGpuCountSelect" class="hidden"></select>
        <input id="defaultGpuCountInput" type="number" min="0" list="gpuCountOptions" />
        <datalist id="gpuCountOptions"></datalist>
      </div>
    </div>
    <label for="defaultTime">Wall time</label>
    <input id="defaultTime" type="text" />
  </div>

  <div class="section">
    <label for="moduleInput">Modules</label>
    <div class="row module-row">
      <div class="module-picker">
        <input id="moduleInput" type="text" placeholder="Type module(s) or pick from the list" />
        <div id="moduleMenu" class="module-menu"></div>
      </div>
      <div class="module-actions">
        <button id="moduleToggle" class="module-toggle" type="button" aria-label="Show modules">v</button>
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
    let clusterMode = false;
    let lastValues = {};
    let connectionState = 'idle';
    let availableModules = [];
    let selectedModules = [];
    let customModuleCommand = '';

    const resourceFields = {
      defaultPartition: { select: 'defaultPartitionSelect', input: 'defaultPartitionInput', list: 'partitionOptions' },
      defaultNodes: { select: 'defaultNodesSelect', input: 'defaultNodesInput', list: 'nodesOptions' },
      defaultCpusPerTask: { select: 'defaultCpusPerTaskSelect', input: 'defaultCpusPerTaskInput', list: 'cpusOptions' },
      defaultMemoryMb: { select: 'defaultMemoryMbSelect', input: 'defaultMemoryMbInput', list: 'memoryOptions' },
      defaultGpuType: { select: 'defaultGpuTypeSelect', input: 'defaultGpuTypeInput', list: 'gpuTypeOptions' },
      defaultGpuCount: { select: 'defaultGpuCountSelect', input: 'defaultGpuCountInput', list: 'gpuCountOptions' }
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
      const nameInput = document.getElementById('profileName');
      if (nameInput && select.value) {
        nameInput.value = select.value;
      }
    }

    function getProfileNameInput() {
      const input = document.getElementById('profileName');
      if (!input) return '';
      return input.value.trim();
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

    function setSelectOptions(selectId, options, selectedValue) {
      const select = document.getElementById(selectId);
      if (!select) return;
      select.innerHTML = '';
      options.forEach((opt) => {
        const option = document.createElement('option');
        option.value = String(opt.value);
        option.textContent = opt.label;
        select.appendChild(option);
      });
      if (selectedValue !== undefined && selectedValue !== null && selectedValue !== '') {
        const desired = String(selectedValue);
        if (Array.from(select.options).some((opt) => opt.value === desired)) {
          select.value = desired;
        }
      }
      if (!select.value && select.options.length > 0) {
        select.value = select.options[0].value;
      }
    }

    function setInputOptions(inputId, listId, options, selectedValue) {
      const input = document.getElementById(inputId);
      const list = document.getElementById(listId);
      if (!input || !list) return;
      list.innerHTML = '';
      options.forEach((opt) => {
        const option = document.createElement('option');
        const value = opt.value === undefined || opt.value === null ? '' : opt.value;
        option.value = String(value);
        option.textContent = opt.label ?? String(value);
        list.appendChild(option);
      });
      const current = input.value;
      if (selectedValue !== undefined && selectedValue !== null && selectedValue !== '') {
        input.value = String(selectedValue);
      } else if (!current && options.length > 0) {
        input.value = String(options[0].value ?? '');
      }
    }

    function setFieldOptions(fieldKey, options, selectedValue) {
      const field = resourceFields[fieldKey];
      if (!field) return;
      setSelectOptions(field.select, options, selectedValue);
      setInputOptions(field.input, field.list, options, selectedValue);
    }

    function clearDatalist(listId) {
      const list = document.getElementById(listId);
      if (list) list.innerHTML = '';
    }

    function clearResourceLists() {
      Object.values(resourceFields).forEach((field) => {
        if (field.list) {
          clearDatalist(field.list);
        }
      });
    }

    function parseModuleList(input) {
      return String(input || '')
        .split(/[\s,]+/)
        .map((value) => value.trim())
        .filter(Boolean);
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
      hint.textContent = availableModules.length + ' modules available. Click the dropdown button or type to filter.';
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

    function setModuleOptions(modules) {
      const list = Array.isArray(modules) ? modules : [];
      availableModules = list.slice();
      const toggle = document.getElementById('moduleToggle');
      if (toggle) {
        toggle.disabled = availableModules.length === 0;
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
        empty.className = 'module-option';
        empty.textContent = availableModules.length ? 'No matches' : 'No modules available';
        menu.appendChild(empty);
        return;
      }
      items.forEach((moduleName) => {
        const option = document.createElement('div');
        option.className = 'module-option';
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

    function showModuleMenu(showAll) {
      const menu = document.getElementById('moduleMenu');
      const input = document.getElementById('moduleInput');
      if (!menu || !input || availableModules.length === 0) return;
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

    function setClusterMode(enabled) {
      clusterMode = enabled;
      Object.values(resourceFields).forEach((field) => {
        const select = document.getElementById(field.select);
        const input = document.getElementById(field.input);
        if (!select || !input) return;
        if (enabled) {
          if (input.value) {
            select.value = input.value;
          }
          select.classList.remove('hidden');
          input.classList.add('hidden');
        } else {
          if (select.value) {
            input.value = select.value;
          }
          input.classList.remove('hidden');
          select.classList.add('hidden');
        }
      });
    }

    function setFieldDisabled(fieldKey, disabled) {
      const field = resourceFields[fieldKey];
      if (!field) return;
      const select = document.getElementById(field.select);
      const input = document.getElementById(field.input);
      if (select) select.disabled = disabled;
      if (input) input.disabled = disabled;
    }

    function setResourceDisabled(disabled) {
      Object.keys(resourceFields).forEach((key) => setFieldDisabled(key, disabled));
      if (disabled) {
        clearResourceLists();
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
      setClusterMode(false);
      setResourceDisabled(false);
      clearResourceLists();
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
      if (!chosen && !selected) {
        chosen = clusterInfo.partitions[0];
      }
      if (!chosen) {
        const meta = document.getElementById('partitionMeta');
        if (meta) meta.textContent = '';
        clearDatalist('nodesOptions');
        clearDatalist('cpusOptions');
        clearDatalist('memoryOptions');
        clearDatalist('gpuTypeOptions');
        clearDatalist('gpuCountOptions');
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
        clearResourceLists();
        setClusterMode(false);
        setResourceDisabled(false);
        return;
      }
      setResourceDisabled(false);
      setClusterMode(true);
      const options = info.partitions.map((partition) => ({
        value: partition.name,
        label: partition.isDefault ? partition.name + ' (default)' : partition.name
      }));
      const preferredPartition = manualPartition || lastValues.defaultPartition || info.defaultPartition;
      setFieldOptions('defaultPartition', options, preferredPartition);
      updatePartitionDetails();
      setStatus('Loaded ' + info.partitions.length + ' partitions.', false);
    }

    function setValue(id, value) {
      if (resourceFields[id]) {
        const field = resourceFields[id];
        const select = document.getElementById(field.select);
        const input = document.getElementById(field.input);
        if (select) select.value = value ?? '';
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
        const el = document.getElementById(clusterMode ? field.select : field.input);
        if (el) {
          return el.value || '';
        }
        const fallback = document.getElementById(field.input);
        return fallback ? fallback.value || '' : '';
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
        applyModuleLoad(values.moduleLoad);
        if (message.clusterInfo) {
          applyClusterInfo(message.clusterInfo);
          if (message.clusterInfoCachedAt) {
            const cachedAt = new Date(message.clusterInfoCachedAt).toLocaleString();
            setStatus('Loaded cached cluster info (' + cachedAt + ').', false);
          }
        } else {
          clusterInfo = null;
          clearResourceLists();
          const meta = document.getElementById('partitionMeta');
          if (meta) meta.textContent = '';
          setModuleOptions([]);
          setClusterMode(false);
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

    const moduleAddButton = document.getElementById('moduleAdd');
    const moduleInput = document.getElementById('moduleInput');
    const moduleToggle = document.getElementById('moduleToggle');
    const moduleMenu = document.getElementById('moduleMenu');
    if (moduleAddButton) {
      moduleAddButton.addEventListener('click', () => {
        addModulesFromInput();
      });
    }
    if (moduleInput) {
      moduleInput.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
          event.preventDefault();
          addModulesFromInput();
        }
      });
      moduleInput.addEventListener('input', () => {
        showModuleMenu(false);
      });
      moduleInput.addEventListener('focus', () => {
        showModuleMenu(true);
      });
      moduleInput.addEventListener('blur', () => {
        setTimeout(() => {
          hideModuleMenu();
        }, 150);
      });
    }
    if (moduleToggle) {
      moduleToggle.addEventListener('click', () => {
        const isOpen = moduleMenu && moduleMenu.classList.contains('visible');
        if (isOpen) {
          hideModuleMenu();
        } else {
          showModuleMenu(true);
        }
      });
    }
    if (moduleMenu) {
      moduleMenu.addEventListener('mousedown', (event) => {
        event.preventDefault();
      });
    }

    function addFieldListener(fieldKey, handler) {
      const field = resourceFields[fieldKey];
      if (!field) return;
      [field.select, field.input].forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('change', handler);
      });
    }

    addFieldListener('defaultPartition', updatePartitionDetails);
    addFieldListener('defaultGpuType', updatePartitionDetails);

    document.getElementById('profileSelect').addEventListener('change', () => {
      const name = getSelectedProfileName();
      const input = document.getElementById('profileName');
      if (input) input.value = name;
    });

    document.getElementById('profileSave').addEventListener('click', () => {
      const name = getProfileNameInput() || getSelectedProfileName();
      if (!name) {
        setProfileStatus('Enter a profile name to save.', true);
        return;
      }
      setProfileStatus('Saving profile...', false);
      vscode.postMessage({
        command: 'saveProfile',
        name,
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
      if (!confirm('Delete profile "' + name + '"?')) {
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
