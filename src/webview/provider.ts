import * as vscode from 'vscode';

import { ClusterInfo } from '../utils/clusterInfo';
import type { ResolvedSshHostInfo, SessionMode, SlurmConnectConfig } from '../config/types';
import type {
  ProfileResourceSummary,
  ProfileStore,
  ProfileSummary,
  ProfileValues,
  UiValues
} from '../state/types';
import type { ConnectionState } from '../state/connectionState';

type ReadyPayload = {
  values: Partial<UiValues>;
  defaults: Partial<UiValues>;
  activeProfile?: string;
  connectionState: ConnectionState;
  connectionSessionMode?: SessionMode;
  remoteActive: boolean;
  saveTarget: 'global' | 'workspace';
};

type ProfilePayload = {
  profiles: ProfileSummary[];
  profileSummaries: Record<string, ProfileResourceSummary>;
  activeProfile?: string;
};

export interface SlurmConnectWebviewDependencies {
  getWebviewHtml(webview: vscode.Webview): string;
  syncConnectionStateFromEnvironment(): void;
  getReadyPayload(): Promise<ReadyPayload & ProfilePayload>;
  getActiveProfileName(): string | undefined;
  getProfileStore(): ProfileStore;
  getProfileSummaries(store: ProfileStore): ProfileSummary[];
  buildProfileSummaryMap(store: ProfileStore, defaults: UiValues): Record<string, ProfileResourceSummary>;
  getUiGlobalDefaults(): UiValues;
  getProfileValues(name: string): UiValues | undefined;
  resolvePreferredSaveTarget(): 'global' | 'workspace';
  firstLoginHostFromInput(input: string): string | undefined;
  getCachedClusterInfo(host: string): { info: ClusterInfo; fetchedAt: string } | undefined;
  pickProfileValues(values: Partial<UiValues>): ProfileValues;
  setActiveProfileName(name?: string): Promise<void>;
  updateProfileStore(store: ProfileStore): Promise<void>;
  buildOverridesFromUi(values: Partial<UiValues>): Partial<SlurmConnectConfig>;
  updateConfigFromUi(values: Partial<UiValues>, target: vscode.ConfigurationTarget): Promise<void>;
  resolveSshHostFromConfig(host: string, cfg: SlurmConnectConfig): Promise<ResolvedSshHostInfo>;
  getConfigWithOverrides(overrides?: Partial<SlurmConnectConfig>): SlurmConnectConfig;
  buildAgentStatusMessage(identityFile: string): Promise<{ text: string; isError: boolean }>;
  handleConnectMessage(
    values: Partial<UiValues>,
    target: vscode.ConfigurationTarget,
    sessionSelection?: string
  ): Promise<void>;
  handleCancelConnectMessage(): void;
  handleDisconnectMessage(): Promise<void>;
  handleCancelSessionMessage(): Promise<void>;
  handleClusterInfoRequest(values: Partial<UiValues>, webview: vscode.Webview): Promise<void>;
  loadSshHosts(overrides?: Partial<SlurmConnectConfig>): Promise<{
    hosts: string[];
    source?: string;
    error?: string;
  }>;
  openLogs(): Promise<void>;
  openRemoteSshLog(): Promise<void>;
  openSettings(): void;
  formatError(error: unknown): string;
}

export class SlurmConnectViewProvider implements vscode.WebviewViewProvider {
  private view?: vscode.WebviewView;
  private sshHostsRequestInFlight = false;

  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly deps: SlurmConnectWebviewDependencies
  ) {}

  resolveWebviewView(view: vscode.WebviewView): void {
    this.view = view;
    const webview = view.webview;
    view.onDidDispose(() => {
      if (this.view === view) {
        this.view = undefined;
      }
    });
    webview.options = {
      enableScripts: true,
      localResourceRoots: [this.context.extensionUri]
    };

    webview.html = this.deps.getWebviewHtml(webview);
    webview.onDidReceiveMessage(async (message) => {
      switch (message.command) {
        case 'ready': {
          await this.handleReady(webview);
          break;
        }
        case 'connect': {
          await this.deps.handleConnectMessage(
            message.values as Partial<UiValues>,
            this.resolveTarget(message.target),
            typeof message.sessionSelection === 'string' ? message.sessionSelection.trim() : ''
          );
          break;
        }
        case 'cancelConnect': {
          this.deps.handleCancelConnectMessage();
          break;
        }
        case 'saveSettings': {
          await this.deps.updateConfigFromUi(
            message.values as Partial<UiValues>,
            this.resolveTarget(message.target)
          );
          break;
        }
        case 'resolveSshHost': {
          await this.handleResolveSshHost(webview, message);
          break;
        }
        case 'refreshAgentStatus': {
          await this.refreshAgentStatus(typeof message.identityFile === 'string' ? message.identityFile : '');
          break;
        }
        case 'pickIdentityFile': {
          await this.handlePickIdentityFile(webview);
          break;
        }
        case 'disconnect': {
          await this.deps.handleDisconnectMessage();
          break;
        }
        case 'cancelSession': {
          await this.deps.handleCancelSessionMessage();
          break;
        }
        case 'saveProfile': {
          await this.handleSaveProfile(webview, message);
          break;
        }
        case 'loadProfile': {
          await this.handleLoadProfile(webview, message);
          break;
        }
        case 'deleteProfile': {
          await this.handleDeleteProfile(webview, message);
          break;
        }
        case 'getClusterInfo': {
          await this.deps.handleClusterInfoRequest(message.values as Partial<UiValues>, webview);
          break;
        }
        case 'openSettings': {
          this.deps.openSettings();
          break;
        }
        case 'openLogs': {
          await this.deps.openLogs();
          break;
        }
        case 'openRemoteSshLog': {
          await this.deps.openRemoteSshLog();
          break;
        }
        default:
          break;
      }
    });
  }

  async openView(): Promise<void> {
    try {
      await vscode.commands.executeCommand('workbench.view.extension.slurmConnect');
    } catch {
      // Ignore if the view cannot be opened.
    }
  }

  async refreshAgentStatus(identityFile: string): Promise<void> {
    const agentStatus = await this.deps.buildAgentStatusMessage(identityFile);
    await this.postMessage({
      command: 'agentStatus',
      agentStatus: agentStatus.text,
      agentStatusError: agentStatus.isError
    });
  }

  async setConnectionState(state: ConnectionState, sessionMode?: SessionMode): Promise<void> {
    await this.postMessage({
      command: 'connectionState',
      state,
      sessionMode
    });
  }

  async loadSshHostsForWebview(overrides?: Partial<SlurmConnectConfig>): Promise<void> {
    if (!this.view || this.sshHostsRequestInFlight) {
      return;
    }
    this.sshHostsRequestInFlight = true;
    try {
      const result = await this.deps.loadSshHosts(overrides);
      await this.postMessage({
        command: 'sshHosts',
        hosts: result.hosts,
        source: result.source,
        error: result.error
      });
    } catch (error) {
      await this.postMessage({
        command: 'sshHosts',
        hosts: [],
        error: this.deps.formatError(error)
      });
    } finally {
      this.sshHostsRequestInFlight = false;
    }
  }

  private async handleReady(webview: vscode.Webview): Promise<void> {
    this.deps.syncConnectionStateFromEnvironment();
    const payload = await this.deps.getReadyPayload();
    let activeProfile = payload.activeProfile;
    if (activeProfile && !this.deps.getProfileValues(activeProfile)) {
      activeProfile = undefined;
    }
    const identityFile = typeof payload.values.identityFile === 'string' ? payload.values.identityFile : '';
    const loginHosts = typeof payload.values.loginHosts === 'string' ? payload.values.loginHosts : '';
    const agentStatus = await this.deps.buildAgentStatusMessage(identityFile);
    const host = this.deps.firstLoginHostFromInput(loginHosts);
    const cached = host ? this.deps.getCachedClusterInfo(host) : undefined;
    await webview.postMessage({
      command: 'load',
      values: payload.values,
      defaults: payload.defaults,
      clusterInfo: cached?.info,
      clusterInfoCachedAt: cached?.fetchedAt,
      sessions: [],
      profiles: payload.profiles,
      profileSummaries: payload.profileSummaries,
      activeProfile,
      connectionState: payload.connectionState,
      connectionSessionMode: payload.connectionSessionMode,
      agentStatus: agentStatus.text,
      agentStatusError: agentStatus.isError,
      remoteActive: payload.remoteActive,
      saveTarget: payload.saveTarget
    });
    void this.loadSshHostsForWebview(this.deps.buildOverridesFromUi(this.deps.pickProfileValues(payload.values)));
  }

  private async handleResolveSshHost(webview: vscode.Webview, message: { host?: string; values?: Partial<UiValues> }): Promise<void> {
    const host = typeof message.host === 'string' ? message.host.trim() : '';
    if (!host) {
      return;
    }
    const uiValues = message.values;
    const overrides = uiValues ? this.deps.buildOverridesFromUi(uiValues) : undefined;
    try {
      const cfg = this.deps.getConfigWithOverrides(overrides);
      const resolved = await this.deps.resolveSshHostFromConfig(host, cfg);
      await webview.postMessage({
        command: 'sshHostResolved',
        ...resolved
      });
    } catch (error) {
      await webview.postMessage({
        command: 'sshHostResolved',
        host,
        error: this.deps.formatError(error)
      });
    }
  }

  private async handlePickIdentityFile(webview: vscode.Webview): Promise<void> {
    const result = await vscode.window.showOpenDialog({
      title: 'Select identity file',
      canSelectFiles: true,
      canSelectFolders: false,
      canSelectMany: false
    });
    if (result && result[0]) {
      await webview.postMessage({
        command: 'pickedPath',
        field: 'identityFile',
        value: result[0].fsPath
      });
    }
  }

  private async handleSaveProfile(webview: vscode.Webview, message: { values?: Partial<UiValues>; suggestedName?: string }): Promise<void> {
    const uiValues = (message.values || {}) as Partial<UiValues>;
    const suggestedRaw = typeof message.suggestedName === 'string' ? message.suggestedName : '';
    const suggestedName = suggestedRaw.trim();
    const nameInput = await vscode.window.showInputBox({
      title: 'Profile name',
      prompt: 'Enter a profile name',
      value: suggestedName || undefined
    });
    if (nameInput === undefined) {
      await this.postProfilesMessage(webview, {
        profileStatus: 'Profile save cancelled.'
      });
      return;
    }
    const name = nameInput.trim();
    if (!name) {
      await this.postProfilesMessage(webview, {
        profileStatus: 'Enter a profile name before saving.',
        profileError: true
      });
      return;
    }

    const store = this.deps.getProfileStore();
    const existing = store[name];
    const now = new Date().toISOString();
    store[name] = {
      name,
      values: this.deps.pickProfileValues(uiValues),
      createdAt: existing?.createdAt ?? now,
      updatedAt: now
    };
    await this.deps.updateProfileStore(store);
    await this.deps.setActiveProfileName(name);
    await this.postProfilesMessage(webview, {
      store,
      activeProfile: name,
      profileStatus: existing ? `Profile "${name}" updated.` : `Profile "${name}" saved.`
    });
  }

  private async handleLoadProfile(webview: vscode.Webview, message: { name?: string }): Promise<void> {
    const rawName = typeof message.name === 'string' ? message.name : '';
    const name = rawName.trim();
    if (!name) {
      await this.postProfilesMessage(webview, {
        profileStatus: 'Select a profile to load.',
        profileError: true
      });
      return;
    }

    const store = this.deps.getProfileStore();
    if (!store[name]) {
      await this.postProfilesMessage(webview, {
        store,
        activeProfile: this.deps.getActiveProfileName(),
        profileStatus: `Profile "${name}" not found.`,
        profileError: true
      });
      return;
    }

    await this.deps.setActiveProfileName(name);
    this.deps.syncConnectionStateFromEnvironment();
    const resolved = this.deps.getProfileValues(name);
    if (!resolved) {
      await this.postProfilesMessage(webview, {
        store,
        profileStatus: `Profile "${name}" not found.`,
        profileError: true
      });
      return;
    }

    const host = this.deps.firstLoginHostFromInput(resolved.loginHosts);
    const cached = host ? this.deps.getCachedClusterInfo(host) : undefined;
    const defaults = this.deps.getUiGlobalDefaults();
    await webview.postMessage({
      command: 'load',
      values: this.deps.pickProfileValues(resolved),
      clusterInfo: cached?.info,
      clusterInfoCachedAt: cached?.fetchedAt,
      profiles: this.deps.getProfileSummaries(store),
      profileSummaries: this.deps.buildProfileSummaryMap(store, defaults),
      activeProfile: name,
      connectionState: (await this.deps.getReadyPayload()).connectionState,
      remoteActive: (await this.deps.getReadyPayload()).remoteActive,
      profileStatus: `Loaded profile "${name}".`,
      saveTarget: this.deps.resolvePreferredSaveTarget()
    });
  }

  private async handleDeleteProfile(webview: vscode.Webview, message: { name?: string }): Promise<void> {
    const rawName = typeof message.name === 'string' ? message.name : '';
    const name = rawName.trim();
    const store = this.deps.getProfileStore();
    if (!name || !store[name]) {
      await this.postProfilesMessage(webview, {
        store,
        profileStatus: 'Select a profile to delete.',
        profileError: true
      });
      return;
    }
    const confirmDelete = await vscode.window.showWarningMessage(
      `Delete profile "${name}"?`,
      { modal: true },
      'Delete'
    );
    if (confirmDelete !== 'Delete') {
      await this.postProfilesMessage(webview, {
        store,
        profileStatus: 'Profile deletion cancelled.'
      });
      return;
    }
    delete store[name];
    await this.deps.updateProfileStore(store);
    let activeProfile = undefined;
    const current = (await this.deps.getReadyPayload()).activeProfile;
    if (current && current !== name) {
      activeProfile = current;
    }
    if (current === name) {
      await this.deps.setActiveProfileName(undefined);
    }
    await this.postProfilesMessage(webview, {
      store,
      activeProfile,
      profileStatus: `Profile "${name}" deleted.`
    });
  }

  private async postProfilesMessage(
    webview: vscode.Webview,
    options: {
      store?: ProfileStore;
      activeProfile?: string;
      profileStatus: string;
      profileError?: boolean;
    }
  ): Promise<void> {
    const store = options.store || this.deps.getProfileStore();
    const defaults = this.deps.getUiGlobalDefaults();
    const activeProfile = options.activeProfile ?? (await this.deps.getReadyPayload()).activeProfile;
    await webview.postMessage({
      command: 'profiles',
      profiles: this.deps.getProfileSummaries(store),
      profileSummaries: this.deps.buildProfileSummaryMap(store, defaults),
      activeProfile,
      profileStatus: options.profileStatus,
      profileError: options.profileError
    });
  }

  private resolveTarget(target: unknown): vscode.ConfigurationTarget {
    return target === 'workspace' ? vscode.ConfigurationTarget.Workspace : vscode.ConfigurationTarget.Global;
  }

  private async postMessage(message: unknown): Promise<void> {
    if (!this.view) {
      return;
    }
    await this.view.webview.postMessage(message);
  }
}
