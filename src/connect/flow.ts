import * as vscode from 'vscode';

import { buildLocalProxyRemotePortCandidates as buildLocalProxyRemotePortCandidatesUtil } from '../utils/localProxy';
import { splitShellArgs } from '../utils/shellArgs';
import { buildHostEntry } from '../utils/sshConfig';
import type { ConnectToken } from '../state/connectionState';
import type {
  LocalProxyTunnelMode,
  SessionMode,
  SlurmConnectConfig
} from '../config/types';
import type {
  CachedGpuValidationResult,
  ConnectFlowResult,
  LocalProxyPlan,
  PartitionResult
} from './types';
import { parseSimpleList } from './clusterQueries';

export interface ConnectFlowRuntime {
  getOutputChannel(): vscode.OutputChannel;
  formatError(error: unknown): string;
  showErrorMessage(message: string): void;
  showWarningMessage(message: string, ...items: string[]): Thenable<string | undefined>;
  showInputBox(options: vscode.InputBoxOptions): Thenable<string | undefined>;
  showQuickPick(
    items: readonly vscode.QuickPickItem[],
    options: vscode.QuickPickOptions
  ): Thenable<vscode.QuickPickItem | undefined>;
  withProgress<T>(options: vscode.ProgressOptions, task: () => Promise<T>): Promise<T>;
  runSshCommand(loginHost: string, cfg: SlurmConnectConfig, command: string): Promise<string>;
  maybePromptForSshAuthOnConnect(cfg: SlurmConnectConfig, loginHost: string): Promise<boolean>;
  queryPartitions(loginHost: string, cfg: SlurmConnectConfig): Promise<PartitionResult>;
  querySimpleList(loginHost: string, cfg: SlurmConnectConfig, command: string): Promise<string[]>;
  resolveDefaultPartitionForHost(loginHost: string, cfg: SlurmConnectConfig): Promise<string | undefined>;
  resolvePartitionDefaultTimeForHost(
    loginHost: string,
    partition: string | undefined,
    cfg: SlurmConnectConfig
  ): Promise<string | undefined>;
  validateGpuRequestAgainstCachedClusterInfo(
    loginHost: string,
    partition: string | undefined,
    gpuCount: number,
    gpuType?: string
  ): CachedGpuValidationResult;
  resolveSessionKey(cfg: SlurmConnectConfig, alias: string): string;
  getProxyInstallSnippet(cfg: SlurmConnectConfig): Promise<string | undefined>;
  normalizeLocalProxyTunnelMode(value?: string): LocalProxyTunnelMode;
  ensureRemoteSshSettings(cfg: SlurmConnectConfig): Promise<void>;
  migrateStaleRemoteSshConfigIfNeeded(): Promise<void>;
  resolveRemoteConfigContext(cfg: SlurmConnectConfig): Promise<{
    baseConfigPath: string;
    includePath: string;
    includeFilePath: string;
  }>;
  ensureLocalProxyPlan(
    cfg: SlurmConnectConfig,
    loginHost: string,
    options?: { remotePortOverride?: number }
  ): Promise<LocalProxyPlan>;
  buildRemoteCommand(
    cfg: SlurmConnectConfig,
    sallocArgs: string[],
    sessionKey: string,
    clientId: string,
    installSnippet?: string,
    localProxyEnv?: { proxyUrl: string; noProxy?: string },
    localProxyPlan?: LocalProxyPlan
  ): string;
  ensureSshAgentEnvForCurrentSsh(): Promise<void>;
  resolveSshToolPath(name: 'ssh'): Promise<string>;
  hasSshOption(options: Record<string, string> | undefined, key: string): boolean;
  redactRemoteCommandForLog(entry: string): string;
  writeSlurmConnectIncludeFile(entry: string, includePath: string): Promise<string>;
  ensureSshIncludeInstalled(baseConfigPath: string, includePath: string): Promise<'added' | 'updated' | 'already'>;
  refreshRemoteSshHosts(): Promise<void>;
  ensurePreSshCommand(cfg: SlurmConnectConfig, reason: string): Promise<void>;
  connectToHost(alias: string, openInNewWindow: boolean, remoteWorkspacePath?: string): Promise<boolean>;
  delay(ms: number): Promise<void>;
  waitForRemoteConnectionOrTimeout(timeoutMs: number, pollMs: number): Promise<boolean>;
  resolveSshIdentityAgentOption(sshPath: string): string | undefined;
  getCurrentLocalProxyPort(): number;
  showOutput(): void;
  isConnectCancelled(token?: ConnectToken): boolean;
  stopLocalProxyServer(): void;
}

export async function runConnectFlow(
  runtime: ConnectFlowRuntime,
  cfg: SlurmConnectConfig,
  options?: { interactive?: boolean; aliasOverride?: string; token?: ConnectToken }
): Promise<ConnectFlowResult> {
  const interactive = options?.interactive !== false;
  const connectToken = options?.token;
  const log = runtime.getOutputChannel();
  log.clear();
  log.appendLine('Slurm Connect started.');

  let didConnect = false;
  const wasCancelled = (): boolean => runtime.isConnectCancelled(connectToken);
  const cancelAndReturn = (): boolean => {
    if (!wasCancelled()) {
      return false;
    }
    log.appendLine('Connect cancelled by user.');
    runtime.stopLocalProxyServer();
    return true;
  };

  if (wasCancelled()) {
    return { didConnect: false };
  }

  const loginHosts = await resolveLoginHosts(runtime, cfg, { interactive });
  log.appendLine(`Login hosts resolved: ${loginHosts.join(', ') || '(none)'}`);
  if (cancelAndReturn()) {
    return { didConnect: false };
  }
  if (loginHosts.length === 0) {
    runtime.showErrorMessage('No login hosts available. Configure slurmConnect.loginHosts or loginHostsCommand.');
    return { didConnect: false };
  }

  let loginHost: string | undefined;
  if (loginHosts.length === 1) {
    loginHost = loginHosts[0];
  } else if (interactive) {
    loginHost = await pickFromList(runtime, 'Select login host', loginHosts, true);
  } else {
    loginHost = loginHosts[0];
    log.appendLine(`Using first login host: ${loginHost}`);
  }
  if (!loginHost) {
    return { didConnect: false };
  }
  if (cancelAndReturn()) {
    return { didConnect: false };
  }

  const proceed = await runtime.maybePromptForSshAuthOnConnect(cfg, loginHost);
  if (!proceed) {
    return { didConnect: false };
  }
  if (cancelAndReturn()) {
    return { didConnect: false };
  }

  let partition: string | undefined;
  let qos: string | undefined;
  let account: string | undefined;

  if (interactive) {
    const { partitions, defaultPartition } = await runtime.withProgress(
      {
        location: vscode.ProgressLocation.Notification,
        title: 'Querying Slurm resources',
        cancellable: false
      },
      async () => runtime.queryPartitions(loginHost, cfg)
    );

    const partitionPick = await pickPartition(runtime, partitions, cfg.defaultPartition || defaultPartition);
    if (partitionPick === null) {
      return { didConnect: false };
    }
    partition = partitionPick;
    if (cancelAndReturn()) {
      return { didConnect: false };
    }

    qos = await pickOptionalValue(
      runtime,
      'Select QoS (optional)',
      await runtime.querySimpleList(loginHost, cfg, cfg.qosCommand)
    );
    account = await pickOptionalValue(
      runtime,
      'Select account (optional)',
      await runtime.querySimpleList(loginHost, cfg, cfg.accountCommand)
    );
    if (cancelAndReturn()) {
      return { didConnect: false };
    }
  } else {
    partition = cfg.defaultPartition || undefined;
  }

  let nodes = cfg.defaultNodes;
  let tasksPerNode = cfg.defaultTasksPerNode;
  let cpusPerTask = cfg.defaultCpusPerTask;
  let time = cfg.defaultTime;
  const memoryMb = cfg.defaultMemoryMb;
  const gpuType = cfg.defaultGpuType;
  const gpuCount = cfg.defaultGpuCount;

  if (interactive) {
    const nodesInput = await promptNumber(runtime, 'Nodes', cfg.defaultNodes, 1);
    if (nodesInput === undefined) {
      return { didConnect: false };
    }
    nodes = nodesInput;
    if (cancelAndReturn()) {
      return { didConnect: false };
    }

    const tasksInput = await promptNumber(runtime, 'Tasks per node', cfg.defaultTasksPerNode, 1);
    if (tasksInput === undefined) {
      return { didConnect: false };
    }
    tasksPerNode = tasksInput;
    if (cancelAndReturn()) {
      return { didConnect: false };
    }

    const cpusInput = await promptNumber(runtime, 'CPUs per task', cfg.defaultCpusPerTask, 1);
    if (cpusInput === undefined) {
      return { didConnect: false };
    }
    cpusPerTask = cpusInput;
    if (cancelAndReturn()) {
      return { didConnect: false };
    }

    const timeInput = await promptTime(runtime, 'Wall time', cfg.defaultTime || '24:00:00');
    if (timeInput === undefined) {
      return { didConnect: false };
    }
    time = timeInput;
    if (cancelAndReturn()) {
      return { didConnect: false };
    }
  }

  if (!partition || partition.trim().length === 0) {
    const resolvedPartition = await runtime.resolveDefaultPartitionForHost(loginHost, cfg);
    if (resolvedPartition) {
      partition = resolvedPartition;
      log.appendLine(`Using default partition: ${partition}`);
    }
  }
  if (cancelAndReturn()) {
    return { didConnect: false };
  }

  if (!time || time.trim().length === 0) {
    const resolvedTime = await runtime.resolvePartitionDefaultTimeForHost(loginHost, partition, cfg);
    if (resolvedTime) {
      time = resolvedTime;
      log.appendLine(`Using partition default time: ${time}`);
    } else {
      time = '24:00:00';
      log.appendLine(`No partition default time found; using fallback wall time: ${time}`);
    }
  }
  if (cancelAndReturn()) {
    return { didConnect: false };
  }

  let extraArgs: string[] = [];
  if (interactive && cfg.promptForExtraSallocArgs) {
    const extra = await runtime.showInputBox({
      title: 'Extra salloc args (optional)',
      prompt: 'Example: --gres=gpu:1 --mem=32G',
      placeHolder: '--gres=gpu:1'
    });
    if (extra && extra.trim().length > 0) {
      extraArgs = splitArgs(extra.trim());
    }
  }
  if (cancelAndReturn()) {
    return { didConnect: false };
  }

  const gpuValidation = runtime.validateGpuRequestAgainstCachedClusterInfo(loginHost, partition, gpuCount, gpuType);
  if (gpuValidation.note) {
    log.appendLine(gpuValidation.note);
  }
  if (gpuValidation.blocked) {
    const message = gpuValidation.message || 'GPU request is incompatible with the selected partition.';
    log.appendLine(message);
    runtime.showErrorMessage(message);
    return { didConnect: false };
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
    const aliasInput = await runtime.showInputBox({
      title: 'SSH host alias',
      value: defaultAlias,
      prompt: 'This name will appear in your SSH config and Remote-SSH hosts list.',
      validateInput: (value) => (value.trim().length === 0 ? 'Alias is required.' : undefined)
    });
    if (!aliasInput) {
      return { didConnect: false };
    }
    alias = aliasInput.trim();
  }
  if (cancelAndReturn()) {
    return { didConnect: false, alias: alias || defaultAlias };
  }
  if (!alias) {
    alias = defaultAlias;
  }
  const trimmedAlias = alias.trim();

  const sessionKey = runtime.resolveSessionKey(cfg, trimmedAlias);
  const clientId = vscode.env.sessionId || '';
  const installSnippet = await runtime.getProxyInstallSnippet(cfg);
  const tunnelMode = runtime.normalizeLocalProxyTunnelMode(cfg.localProxyTunnelMode || 'remoteSsh');
  const shouldRetryProxyPort = cfg.localProxyEnabled && tunnelMode === 'remoteSsh';
  const maxProxyPortAttempts = shouldRetryProxyPort ? 3 : 1;
  let proxyPortCandidates: number[] = [];
  let localProxyPlan: LocalProxyPlan = { enabled: false };

  await runtime.ensureRemoteSshSettings(cfg);
  if (cancelAndReturn()) {
    return { didConnect: false, alias: trimmedAlias };
  }
  await runtime.migrateStaleRemoteSshConfigIfNeeded();
  if (cancelAndReturn()) {
    return { didConnect: false, alias: trimmedAlias };
  }
  const { baseConfigPath, includePath, includeFilePath } = await runtime.resolveRemoteConfigContext(cfg);
  if (baseConfigPath === includeFilePath) {
    const message = 'Slurm Connect include file path must not be the same as the SSH config path.';
    log.appendLine(message);
    runtime.showErrorMessage(message);
    return { didConnect: false, alias: trimmedAlias };
  }
  if (cancelAndReturn()) {
    return { didConnect: false, alias: trimmedAlias };
  }

  let preSshReady = false;
  let connected = false;
  for (let attempt = 0; attempt < maxProxyPortAttempts; attempt += 1) {
    if (cancelAndReturn()) {
      return { didConnect: false, alias: trimmedAlias };
    }
    const overridePort =
      shouldRetryProxyPort && proxyPortCandidates.length > 0 ? proxyPortCandidates[attempt] : undefined;
    try {
      localProxyPlan = await runtime.ensureLocalProxyPlan(cfg, loginHost, { remotePortOverride: overridePort });
    } catch (error) {
      if (!wasCancelled()) {
        const message = `Failed to start local proxy: ${runtime.formatError(error)}`;
        log.appendLine(message);
        runtime.showErrorMessage(message);
      }
      return { didConnect: false, alias: trimmedAlias };
    }
    if (cancelAndReturn()) {
      return { didConnect: false, alias: trimmedAlias };
    }
    if (shouldRetryProxyPort && proxyPortCandidates.length === 0) {
      const basePort = runtime.getCurrentLocalProxyPort();
      proxyPortCandidates = buildLocalProxyRemotePortCandidates(basePort, maxProxyPortAttempts);
    }
    const localProxyEnv =
      localProxyPlan.enabled && localProxyPlan.proxyUrl && !localProxyPlan.computeTunnel
        ? { proxyUrl: localProxyPlan.proxyUrl, noProxy: localProxyPlan.noProxy }
        : undefined;
    const remoteCommand = runtime.buildRemoteCommand(
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
        runtime.showErrorMessage('RemoteCommand is empty. Check slurmConnect.proxyCommand.');
      }
      return { didConnect: false, alias: trimmedAlias };
    }
    if (cancelAndReturn()) {
      return { didConnect: false, alias: trimmedAlias };
    }

    await runtime.ensureSshAgentEnvForCurrentSsh();
    const additionalSshOptions: Record<string, string> = { ...(cfg.additionalSshOptions || {}) };
    if (process.platform === 'win32') {
      const sshPath = await runtime.resolveSshToolPath('ssh');
      const identityAgent = runtime.resolveSshIdentityAgentOption(sshPath);
      if (identityAgent && !runtime.hasSshOption(additionalSshOptions, 'IdentityAgent')) {
        additionalSshOptions.IdentityAgent = identityAgent;
      }
    }

    const hostEntry = buildHostEntry(
      trimmedAlias,
      loginHost,
      { ...cfg, additionalSshOptions, extraSshOptions: localProxyPlan.sshOptions },
      remoteCommand
    );
    log.appendLine('Generated SSH host entry:');
    log.appendLine(runtime.redactRemoteCommandForLog(hostEntry));

    try {
      await runtime.writeSlurmConnectIncludeFile(hostEntry, includeFilePath);
      const status = await runtime.ensureSshIncludeInstalled(baseConfigPath, includePath);
      log.appendLine(`SSH config include ${status} in ${baseConfigPath}.`);
    } catch (error) {
      if (!wasCancelled()) {
        const message = `Failed to install SSH Include block: ${runtime.formatError(error)}`;
        log.appendLine(message);
        runtime.showErrorMessage(message);
      }
      return { didConnect: false, alias: trimmedAlias };
    }
    if (cancelAndReturn()) {
      return { didConnect: false, alias: trimmedAlias };
    }
    await runtime.delay(300);
    await runtime.refreshRemoteSshHosts();
    if (cancelAndReturn()) {
      return { didConnect: false, alias: trimmedAlias };
    }

    if (!preSshReady) {
      try {
        await runtime.ensurePreSshCommand(cfg, `Remote-SSH connect to ${trimmedAlias}`);
      } catch (error) {
        if (!wasCancelled()) {
          const message = `Pre-SSH command failed: ${runtime.formatError(error)}`;
          log.appendLine(message);
          runtime.showErrorMessage(message);
        }
        return { didConnect: false, alias: trimmedAlias };
      }
      preSshReady = true;
    }
    if (cancelAndReturn()) {
      return { didConnect: false, alias: trimmedAlias };
    }

    connected = await runtime.connectToHost(trimmedAlias, cfg.openInNewWindow, cfg.remoteWorkspacePath);
    if (cancelAndReturn()) {
      return { didConnect: false, alias: trimmedAlias };
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
  if (!connected) {
    if (!wasCancelled()) {
      void runtime
        .showWarningMessage(
          `SSH host "${trimmedAlias}" created, but auto-connect failed. Use Remote-SSH to connect.`,
          'Show Output'
        )
        .then((selection) => {
          if (selection === 'Show Output') {
            runtime.showOutput();
          }
        });
    }
    return { didConnect, alias: trimmedAlias, clearSessionState: true };
  }

  if (!cfg.openInNewWindow) {
    await runtime.waitForRemoteConnectionOrTimeout(30_000, 500);
  }

  return {
    didConnect,
    alias: trimmedAlias,
    sessionKey: sessionKey || undefined,
    loginHost,
    sessionMode: cfg.sessionMode
  };
}

export function buildSallocArgs(params: {
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

export function buildDefaultAlias(
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

async function resolveLoginHosts(
  runtime: Pick<
    ConnectFlowRuntime,
    'getOutputChannel' | 'runSshCommand' | 'showInputBox' | 'showWarningMessage' | 'formatError'
  >,
  cfg: SlurmConnectConfig,
  options?: { interactive?: boolean }
): Promise<string[]> {
  const interactive = options?.interactive !== false;
  const log = runtime.getOutputChannel();
  let hosts = cfg.loginHosts.slice();
  if (cfg.loginHostsCommand) {
    let queryHost: string | undefined = cfg.loginHostsQueryHost || hosts[0];
    if (!queryHost && interactive) {
      queryHost = await runtime.showInputBox({
        title: 'Login host for discovery',
        prompt: 'Enter a login host to run the loginHostsCommand on.'
      });
    }
    if (!queryHost && !interactive) {
      log.appendLine('Login host discovery skipped (no query host and prompts disabled).');
    }
    if (queryHost) {
      try {
        const output = await runtime.runSshCommand(queryHost, cfg, cfg.loginHostsCommand);
        const discovered = parseSimpleList(output);
        if (discovered.length > 0) {
          hosts = discovered;
        }
      } catch (error) {
        runtime.showWarningMessage(`Failed to query login hosts: ${runtime.formatError(error)}`);
      }
    }
  }

  hosts = uniqueList(hosts);
  if (hosts.length === 0 && interactive) {
    const manual = await runtime.showInputBox({
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

async function pickFromList(
  runtime: Pick<ConnectFlowRuntime, 'showQuickPick' | 'showInputBox'>,
  title: string,
  items: string[],
  allowManual: boolean
): Promise<string | undefined> {
  const picks: vscode.QuickPickItem[] = items.map((item) => ({ label: item }));
  if (allowManual) {
    picks.unshift({ label: 'Enter manually' });
  }

  const picked = await runtime.showQuickPick(picks, {
    title,
    placeHolder: items.length ? 'Select an item' : 'Enter a value'
  });

  if (!picked) {
    return undefined;
  }

  if (allowManual && picked.label === 'Enter manually') {
    const manual = await runtime.showInputBox({ title, prompt: 'Enter value' });
    return manual?.trim() || undefined;
  }

  return picked.label;
}

async function pickPartition(
  runtime: Pick<ConnectFlowRuntime, 'showQuickPick' | 'showInputBox'>,
  partitions: string[],
  defaultPartition?: string
): Promise<string | undefined | null> {
  if (partitions.length === 0) {
    const manual = await runtime.showInputBox({
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

  const picked = await runtime.showQuickPick(picks, {
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

async function pickOptionalValue(
  runtime: Pick<ConnectFlowRuntime, 'showQuickPick'>,
  title: string,
  items: string[]
): Promise<string | undefined> {
  if (items.length === 0) {
    return undefined;
  }

  const picks: vscode.QuickPickItem[] = [{ label: 'None' }, ...items.map((item) => ({ label: item }))];
  const picked = await runtime.showQuickPick(picks, {
    title,
    placeHolder: 'Select a value'
  });
  if (!picked || picked.label === 'None') {
    return undefined;
  }
  return picked.label;
}

async function promptNumber(
  runtime: Pick<ConnectFlowRuntime, 'showInputBox'>,
  title: string,
  defaultValue: number,
  minValue: number
): Promise<number | undefined> {
  const value = await runtime.showInputBox({
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

async function promptTime(
  runtime: Pick<ConnectFlowRuntime, 'showInputBox'>,
  title: string,
  defaultValue: string
): Promise<string | undefined> {
  const timePattern = /^(\d+-)?\d{1,2}:\d{2}:\d{2}$/;
  const value = await runtime.showInputBox({
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

function splitArgs(input: string): string[] {
  return splitShellArgs(input);
}

function uniqueList(values: string[]): string[] {
  return Array.from(new Set(values.filter(Boolean)));
}

function buildLocalProxyRemotePortCandidates(basePort: number, attempts: number): number[] {
  return buildLocalProxyRemotePortCandidatesUtil(basePort, attempts);
}
