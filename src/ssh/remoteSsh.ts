import * as vscode from 'vscode';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs/promises';
import * as jsonc from 'jsonc-parser';

import type { SlurmConnectConfig } from '../config/types';
import type { SshCommandRuntime } from './commands';
import { defaultSshConfigPath, normalizeRemoteConfigPath, normalizeSshPath, splitSshConfigArgs } from './config';

const WINDOWS_OPENSSH_PATH = 'C:\\Windows\\System32\\OpenSSH\\ssh.exe';

export interface RemoteSshDependencies {
  getOutputChannel(): vscode.OutputChannel;
  formatError(error: unknown): string;
  delay(ms: number): Promise<void>;
  stopLocalProxyServer(): void;
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

async function ensureLocalServerSetting(deps: Pick<RemoteSshDependencies, 'formatError'>): Promise<void> {
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
            `Failed to update ${settingsPath} with remote.SSH.useLocalServer: ${deps.formatError(error)}`
          );
        }
        return;
      }
    } catch (error) {
      void vscode.window.showWarningMessage(
        `Slurm Connect could not read ${settingsPath}. Please add "remote.SSH.useLocalServer": true manually. (${deps.formatError(error)})`
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
        `Failed to update user settings with remote.SSH.useLocalServer: ${deps.formatError(error)}`
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
      `Failed to update user settings with remote.SSH.useLocalServer: ${deps.formatError(error)}`
    );
  }
}

async function ensureRemoteSshPathOnWindows(
  sshRuntime: Pick<SshCommandRuntime, 'resolveSshToolPath' | 'isGitSshPath'>,
  fileExists: (filePath: string) => Promise<boolean>,
  deps: Pick<RemoteSshDependencies, 'formatError'>
): Promise<void> {
  if (process.platform !== 'win32') {
    return;
  }
  const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
  const configured = String(remoteCfg.get<string>('path') || '').trim();
  if (configured) {
    return;
  }
  let paths: string[] = [];
  try {
    const whereOutput = await (await import('util')).promisify((await import('child_process')).execFile)('where', ['ssh']);
    paths = whereOutput.stdout
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);
  } catch {
    paths = [];
  }
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
      `Failed to update remote.SSH.path: ${deps.formatError(error)}`
    );
  }
}

export async function ensureRemoteSshSettings(
  cfg: SlurmConnectConfig,
  sshRuntime: Pick<SshCommandRuntime, 'ensureSshAgentEnvForCurrentSsh' | 'resolveSshToolPath' | 'isGitSshPath'>,
  fileExists: (filePath: string) => Promise<boolean>,
  deps: Pick<RemoteSshDependencies, 'formatError'>
): Promise<void> {
  await ensureRemoteSshPathOnWindows(sshRuntime, fileExists, deps);
  await sshRuntime.ensureSshAgentEnvForCurrentSsh();
  await ensureLocalServerSetting(deps);
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

  const recommendedConnectTimeout = 300;
  const connectTimeoutInspect = remoteCfg.inspect<number>('connectTimeout');
  const configuredConnectTimeout = Number(
    connectTimeoutInspect?.globalValue ??
      connectTimeoutInspect?.workspaceValue ??
      connectTimeoutInspect?.workspaceFolderValue ??
      connectTimeoutInspect?.defaultValue ??
      0
  );
  const effectiveConnectTimeout = Number.isFinite(configuredConnectTimeout) ? configuredConnectTimeout : 0;
  if (effectiveConnectTimeout > 0 && effectiveConnectTimeout < recommendedConnectTimeout) {
    const raise = await vscode.window.showWarningMessage(
      `Remote.SSH: Connect Timeout is ${effectiveConnectTimeout}s. First-time connections may need longer to download/start the VS Code server. Set it to ${recommendedConnectTimeout}s?`,
      `Set to ${recommendedConnectTimeout}s`,
      'Ignore'
    );
    if (raise === `Set to ${recommendedConnectTimeout}s`) {
      try {
        await remoteCfg.update('connectTimeout', recommendedConnectTimeout, vscode.ConfigurationTarget.Global);
      } catch (error) {
        void vscode.window.showWarningMessage(
          `Failed to update user settings with remote.SSH.connectTimeout: ${deps.formatError(error)}`
        );
      }
    }
  }

  const tunnelMode = cfg.localProxyTunnelMode;
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
}

export async function refreshRemoteSshHosts(): Promise<void> {
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

export async function connectToHost(
  alias: string,
  openInNewWindow: boolean,
  remoteWorkspacePath: string | undefined,
  sshRuntime: Pick<SshCommandRuntime, 'ensureSshAgentEnvForCurrentSsh' | 'resolveSshToolPath' | 'isGitSshPath'>,
  deps: Pick<RemoteSshDependencies, 'getOutputChannel' | 'formatError'>
): Promise<boolean> {
  const remoteExtension = vscode.extensions.getExtension('ms-vscode-remote.remote-ssh');
  if (!remoteExtension) {
    return false;
  }
  const log = deps.getOutputChannel();
  await sshRuntime.ensureSshAgentEnvForCurrentSsh();
  if (process.platform === 'win32') {
    const sshPath = await sshRuntime.resolveSshToolPath('ssh');
    if (sshRuntime.isGitSshPath(sshPath) && !process.env.SSH_AUTH_SOCK) {
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
      log.appendLine(`Failed to open folder via vscode.openFolder: ${deps.formatError(error)}`);
    }
  }

  const hostArg = { host: alias };
  const openEmptyOnly = !trimmedPath;

  const candidateSets: Array<Array<[string, unknown]>> = openInNewWindow
    ? [[
        ['opensshremotes.openEmptyWindow', hostArg],
        ['remote-ssh.openEmptyWindow', alias],
        ['opensshremotes.openEmptyWindowInCurrentWindow', hostArg],
        ['remote-ssh.openEmptyWindowInCurrentWindow', alias]
      ]]
    : [[
        ['opensshremotes.openEmptyWindowInCurrentWindow', hostArg],
        ['remote-ssh.openEmptyWindowInCurrentWindow', alias],
        ['opensshremotes.openEmptyWindow', hostArg],
        ['remote-ssh.openEmptyWindow', alias]
      ]];

  if (!openEmptyOnly) {
    candidateSets[0].push(
      ['remote-ssh.connectToHost', alias],
      ['opensshremotes.connectToHost', hostArg]
    );
  } else {
    log.appendLine('Remote folder not set; opening an empty remote window only.');
  }

  for (const [command, args] of candidateSets[0]) {
    try {
      log.appendLine(`Trying command: ${command}`);
      await vscode.commands.executeCommand(command, args);
      log.appendLine(`Command succeeded: ${command}`);
      return true;
    } catch (error) {
      log.appendLine(`Command failed: ${command} -> ${deps.formatError(error)}`);
    }
  }
  log.appendLine(`Available SSH commands: ${sshCommands.join(', ') || '(none)'}`);
  if (openEmptyOnly) {
    void vscode.window.showWarningMessage(
      'Could not open an empty Remote-SSH window. Configure a remote folder to open or update Remote-SSH.'
    );
  }
  return false;
}

export async function waitForRemoteConnectionOrTimeout(
  timeoutMs: number,
  pollMs: number,
  deps: Pick<RemoteSshDependencies, 'delay'>
): Promise<boolean> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (vscode.env.remoteName === 'ssh-remote') {
      return true;
    }
    await deps.delay(pollMs);
  }
  return vscode.env.remoteName === 'ssh-remote';
}

export async function disconnectFromHost(
  alias: string | undefined,
  deps: Pick<RemoteSshDependencies, 'getOutputChannel' | 'formatError' | 'delay' | 'stopLocalProxyServer'>
): Promise<boolean> {
  const remoteExtension = vscode.extensions.getExtension('ms-vscode-remote.remote-ssh');
  if (!remoteExtension) {
    return false;
  }
  const log = deps.getOutputChannel();
  await remoteExtension.activate();
  const availableCommands = await vscode.commands.getCommands(true);
  const sshCommands = availableCommands.filter((command) => /ssh|openssh|remote\.close/i.test(command));
  const ensureRemoteSessionClosed = async (): Promise<boolean> => {
    if (vscode.env.remoteName !== 'ssh-remote') {
      return true;
    }
    const timeoutMs = 5000;
    const pollMs = 100;
    const startedAt = Date.now();
    while (Date.now() - startedAt < timeoutMs) {
      if (vscode.env.remoteName !== 'ssh-remote') {
        return true;
      }
      await deps.delay(pollMs);
    }
    return vscode.env.remoteName !== 'ssh-remote';
  };
  const finalizeDisconnect = async (): Promise<boolean> => {
    const closed = await ensureRemoteSessionClosed();
    if (!closed) {
      log.appendLine('Disconnect command completed but remote session is still active.');
      return false;
    }
    deps.stopLocalProxyServer();
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
        const closed = await finalizeDisconnect();
        if (closed) {
          log.appendLine(`Disconnect command succeeded: ${command}`);
          return true;
        }
        log.appendLine(`Disconnect command did not close remote session: ${command}`);
      } catch (error) {
        log.appendLine(`Disconnect command failed: ${command} -> ${deps.formatError(error)}`);
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
      const closed = await finalizeDisconnect();
      if (closed) {
        log.appendLine(`Disconnect command succeeded: ${command}`);
        return true;
      }
      log.appendLine(`Disconnect command did not close remote session: ${command}`);
    } catch (error) {
      log.appendLine(`Disconnect command failed: ${command} -> ${deps.formatError(error)}`);
    }
  }
  log.appendLine(`Available SSH commands: ${sshCommands.join(', ') || '(none)'}`);
  return false;
}

export async function openRemoteSshLog(): Promise<void> {
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
    const includes = content
      .split(/\r?\n/)
      .flatMap((line) => {
        const trimmed = line.trim();
        const match = /^Include\s+(.+)$/i.exec(trimmed);
        return match ? splitSshConfigArgs(match[1]) : [];
      })
      .map((value) => normalizeSshPath(value));
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

export async function migrateStaleRemoteSshConfigIfNeeded(
  deps: Pick<RemoteSshDependencies, 'getOutputChannel'>
): Promise<void> {
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
  deps.getOutputChannel().appendLine(
    `Detected stale Slurm Connect temporary SSH config. Restored Remote.SSH configFile ${restored ? `to ${restored}` : 'to default'}.`
  );
}
