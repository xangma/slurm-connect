import type { ClusterInfo } from '../utils/clusterInfo';
import {
  getMaxFieldCount,
  hasMeaningfulClusterInfo,
  parsePartitionInfoOutput
} from '../utils/clusterInfo';
import { parseModulesOutput as parseModulesOutputUtil } from '../utils/moduleOutput';
import {
  applySessionIdleInfo as applySessionIdleInfoUtil,
  parseSessionListOutput as parseSessionListOutputUtil,
  type SessionSummary
} from '../utils/sessionInfo';
import {
  applyFreeResourceSummary as applyFreeResourceSummaryUtil,
  buildClusterInfoCommandSet as buildClusterInfoCommandSetUtil,
  computeFreeResourceSummary as computeFreeResourceSummaryUtil,
  type FreeResourceCommandIndexes,
  type PartitionFreeSummary
} from '../utils/slurmResources';
import type { SlurmConnectConfig } from '../config/types';
import type { UiValues } from '../state/types';
import type {
  CachedGpuValidationResult,
  PartitionDefaults,
  PartitionResult
} from './types';

const DEFAULT_SESSION_STATE_DIR = '~/.slurm-connect';
const CLUSTER_CMD_START = '__SC_CMD_START__';
const CLUSTER_CMD_END = '__SC_CMD_END__';
const CLUSTER_MODULES_START = '__SC_MODULES_START__';
const CLUSTER_MODULES_END = '__SC_MODULES_END__';
const CLUSTER_SESSIONS_START = '__SC_SESSIONS_START__';
const CLUSTER_SESSIONS_END = '__SC_SESSIONS_END__';

export interface ClusterQueryRuntime {
  getOutputChannel(): { appendLine(value: string): void };
  runSshCommand(loginHost: string, cfg: SlurmConnectConfig, command: string): Promise<string>;
  runSshCommandWithInput(
    loginHost: string,
    cfg: SlurmConnectConfig,
    command: string,
    input: string
  ): Promise<string>;
  formatError(error: unknown): string;
  showWarningMessage(message: string): void;
  getCachedClusterInfo(host: string): { info: ClusterInfo; fetchedAt: string } | undefined;
  cacheClusterInfo(host: string, info: ClusterInfo): void;
  wrapPythonScriptCommand(encodedScript: string, emptyFallback: string): string;
}

export interface ClusterInfoMessageRuntime extends ClusterQueryRuntime {
  buildOverridesFromUi(values: Partial<UiValues>): Partial<SlurmConnectConfig>;
  getConfigWithOverrides(overrides?: Partial<SlurmConnectConfig>): SlurmConnectConfig;
  parseListInput(input: string): string[];
  maybePromptForSshAuthOnConnect(cfg: SlurmConnectConfig, loginHost: string): Promise<boolean>;
  buildAgentStatusMessage(identityFile: string): Promise<{ text: string; isError: boolean }>;
  showErrorMessage(message: string): void;
}

export type ClusterInfoWebviewMessage =
  | {
      command: 'clusterInfo';
      info: ClusterInfo;
      sessions: SessionSummary[];
      fetchedAt?: string;
      agentStatus: string;
      agentStatusError: boolean;
    }
  | {
      command: 'clusterInfoError';
      message: string;
      sessions: SessionSummary[];
      agentStatus: string;
      agentStatusError: boolean;
    };

export function parsePartitionOutput(output: string): PartitionResult {
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

export function parseSimpleList(output: string): string[] {
  return uniqueList(
    output
      .split(/\s+/)
      .map((value) => value.trim())
      .filter(Boolean)
  );
}

export function parsePartitionDefaultTimesOutput(
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

export async function queryPartitions(
  runtime: ClusterQueryRuntime,
  loginHost: string,
  cfg: SlurmConnectConfig
): Promise<PartitionResult> {
  if (!cfg.partitionCommand) {
    return { partitions: [] };
  }
  try {
    const output = await runtime.runSshCommand(loginHost, cfg, cfg.partitionCommand);
    return parsePartitionOutput(output);
  } catch (error) {
    runtime.showWarningMessage(`Failed to query partitions: ${runtime.formatError(error)}`);
    return { partitions: [] };
  }
}

export async function querySimpleList(
  runtime: ClusterQueryRuntime,
  loginHost: string,
  cfg: SlurmConnectConfig,
  command: string
): Promise<string[]> {
  if (!command) {
    return [];
  }
  try {
    const output = await runtime.runSshCommand(loginHost, cfg, command);
    return parseSimpleList(output);
  } catch (error) {
    runtime.showWarningMessage(`Failed to query resources: ${runtime.formatError(error)}`);
    return [];
  }
}

export function resolveDefaultPartitionName(info: ClusterInfo): string | undefined {
  if (info.defaultPartition) {
    return info.defaultPartition;
  }
  const flagged = info.partitions.find((partition) => partition.isDefault);
  return flagged ? flagged.name : undefined;
}

export function resolvePartitionDefaultTime(info: ClusterInfo, partition?: string): string | undefined {
  const effective = partition || resolveDefaultPartitionName(info);
  if (!effective) {
    return undefined;
  }
  const match = info.partitions.find((item) => item.name === effective);
  return match?.defaultTime;
}

export function validateGpuRequestAgainstCachedClusterInfo(
  runtime: Pick<ClusterQueryRuntime, 'getCachedClusterInfo'>,
  loginHost: string,
  partition: string | undefined,
  gpuCount: number,
  gpuType?: string
): CachedGpuValidationResult {
  if (!Number.isFinite(gpuCount) || gpuCount <= 0) {
    return { blocked: false };
  }
  const cachedInfo = runtime.getCachedClusterInfo(loginHost)?.info;
  if (!cachedInfo || !Array.isArray(cachedInfo.partitions) || cachedInfo.partitions.length === 0) {
    return {
      blocked: false,
      note: `GPU validation skipped for ${loginHost}: no cached cluster info.`
    };
  }
  const requestedPartition = (partition || '').trim();
  const effectivePartition = requestedPartition || resolveDefaultPartitionName(cachedInfo);
  if (!effectivePartition) {
    return {
      blocked: false,
      note: `GPU validation skipped for ${loginHost}: partition is not set and no default partition was found in cache.`
    };
  }
  const partitionInfo = cachedInfo.partitions.find((entry) => entry.name === effectivePartition);
  if (!partitionInfo) {
    return {
      blocked: false,
      note: `GPU validation skipped for ${loginHost}: partition "${effectivePartition}" not found in cached cluster info.`
    };
  }
  if (partitionInfo.gpuMax > 0) {
    return { blocked: false };
  }
  const requestedType = (gpuType || '').trim();
  const resourceText = requestedType ? `GPU type "${requestedType}"` : 'GPU resources';
  return {
    blocked: true,
    message:
      `Partition "${effectivePartition}" does not advertise GPUs in cached cluster info, ` +
      `but this request asks for ${gpuCount} ${resourceText}. ` +
      'Set GPU count to 0 or choose a GPU partition.'
  };
}

export async function fetchExistingSessions(
  runtime: ClusterQueryRuntime,
  loginHost: string,
  cfg: SlurmConnectConfig
): Promise<SessionSummary[]> {
  const stateDir = cfg.sessionStateDir.trim() || DEFAULT_SESSION_STATE_DIR;
  const command = buildSessionQueryCommand(runtime, stateDir);
  const output = await runtime.runSshCommand(loginHost, cfg, command);
  return applySessionIdleInfoUtil(parseSessionListOutputUtil(output));
}

export async function fetchClusterInfoWithSessions(
  runtime: ClusterQueryRuntime,
  loginHost: string,
  cfg: SlurmConnectConfig
): Promise<{ info: ClusterInfo; sessions: SessionSummary[] }> {
  const { commands, freeResourceIndexes } = buildClusterInfoCommandSetUtil(cfg);
  const log = runtime.getOutputChannel();
  const sessionsScript = buildSessionQueryScript(cfg.sessionStateDir.trim() || DEFAULT_SESSION_STATE_DIR);
  const combinedScript = buildCombinedClusterInfoScript(commands, sessionsScript);
  log.appendLine(`Cluster info single-call (with sessions) script bytes: ${combinedScript.length}`);
  const output = await runtime.runSshCommandWithInput(loginHost, cfg, 'bash -s', combinedScript);
  const { commandOutputs, modulesOutput, sessionsOutput } = parseCombinedClusterInfoOutput(output, commands.length);
  const info = buildClusterInfoFromOutputs(commandOutputs, modulesOutput, freeResourceIndexes, log);
  const sessions = applySessionIdleInfoUtil(parseSessionListOutputUtil(sessionsOutput));
  return { info, sessions };
}

export async function fetchClusterInfo(
  runtime: ClusterQueryRuntime,
  loginHost: string,
  cfg: SlurmConnectConfig
): Promise<ClusterInfo> {
  const { commands, freeResourceIndexes } = buildClusterInfoCommandSetUtil(cfg);
  const log = runtime.getOutputChannel();
  let info: ClusterInfo | undefined;
  try {
    info = await fetchClusterInfoSingleCall(runtime, loginHost, cfg, commands, freeResourceIndexes);
  } catch (error) {
    log.appendLine(`Single-call cluster info failed, falling back. ${runtime.formatError(error)}`);
  }

  if (!info) {
    let lastInfo: ClusterInfo = { partitions: [] };
    let bestInfo: ClusterInfo | undefined;
    let partitionDefaults: Record<string, PartitionDefaults> = {};
    let defaultPartitionFromScontrol: string | undefined;
    const fallbackCommands = commands.slice(0, freeResourceIndexes.infoCommandCount);
    for (const command of fallbackCommands) {
      log.appendLine(`Cluster info command: ${command}`);
      const output = await runtime.runSshCommand(loginHost, cfg, command);
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
    const modules = await queryAvailableModules(runtime, loginHost, cfg);
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

export async function resolveDefaultPartitionForHost(
  runtime: ClusterQueryRuntime,
  loginHost: string,
  cfg: SlurmConnectConfig
): Promise<string | undefined> {
  const cached = runtime.getCachedClusterInfo(loginHost)?.info;
  if (cached) {
    const cachedPartition = resolveDefaultPartitionName(cached);
    if (cachedPartition) {
      return cachedPartition;
    }
  }
  try {
    const info = await fetchClusterInfo(runtime, loginHost, cfg);
    runtime.cacheClusterInfo(loginHost, info);
    return resolveDefaultPartitionName(info);
  } catch (error) {
    runtime.getOutputChannel().appendLine(`Failed to resolve default partition: ${runtime.formatError(error)}`);
    return undefined;
  }
}

export async function resolvePartitionDefaultTimeForHost(
  runtime: ClusterQueryRuntime,
  loginHost: string,
  partition: string | undefined,
  cfg: SlurmConnectConfig
): Promise<string | undefined> {
  const cached = runtime.getCachedClusterInfo(loginHost)?.info;
  if (cached) {
    const cachedTime = resolvePartitionDefaultTime(cached, partition);
    if (cachedTime) {
      return cachedTime;
    }
  }
  try {
    const info = await fetchClusterInfo(runtime, loginHost, cfg);
    runtime.cacheClusterInfo(loginHost, info);
    return resolvePartitionDefaultTime(info, partition);
  } catch (error) {
    runtime.getOutputChannel().appendLine(
      `Failed to resolve partition default time: ${runtime.formatError(error)}`
    );
    return undefined;
  }
}

export async function buildClusterInfoWebviewMessage(
  runtime: ClusterInfoMessageRuntime,
  values: Partial<UiValues>
): Promise<ClusterInfoWebviewMessage> {
  const overrides = runtime.buildOverridesFromUi(values);
  const cfg = runtime.getConfigWithOverrides(overrides);
  const loginHosts = runtime.parseListInput(String(values.loginHosts ?? ''));
  const log = runtime.getOutputChannel();
  let sessions: SessionSummary[] = [];

  if (loginHosts.length === 0) {
    const message = 'Enter a login host before fetching cluster info.';
    runtime.showErrorMessage(message);
    const agentStatus = await runtime.buildAgentStatusMessage(String(values.identityFile ?? ''));
    return {
      command: 'clusterInfoError',
      message,
      sessions: [],
      agentStatus: agentStatus.text,
      agentStatusError: agentStatus.isError
    };
  }

  const loginHost = loginHosts[0];
  log.appendLine(`Fetching cluster info from ${loginHost}...`);
  const proceed = await runtime.maybePromptForSshAuthOnConnect(cfg, loginHost);
  if (!proceed) {
    const message = 'Set an SSH identity file in the Slurm Connect view, then retry.';
    const agentStatus = await runtime.buildAgentStatusMessage(String(values.identityFile ?? ''));
    return {
      command: 'clusterInfoError',
      message,
      sessions: [],
      agentStatus: agentStatus.text,
      agentStatusError: agentStatus.isError
    };
  }

  try {
    let info: ClusterInfo;
    try {
      const result = await fetchClusterInfoWithSessions(runtime, loginHost, cfg);
      info = result.info;
      sessions = result.sessions;
    } catch (error) {
      log.appendLine(`Single-call cluster info failed, falling back. ${runtime.formatError(error)}`);
      info = await fetchClusterInfo(runtime, loginHost, cfg);
    }
    runtime.cacheClusterInfo(loginHost, info);
    const cached = runtime.getCachedClusterInfo(loginHost);
    const agentStatus = await runtime.buildAgentStatusMessage(String(values.identityFile ?? ''));
    return {
      command: 'clusterInfo',
      info,
      sessions,
      fetchedAt: cached?.fetchedAt,
      agentStatus: agentStatus.text,
      agentStatusError: agentStatus.isError
    };
  } catch (error) {
    const message = runtime.formatError(error);
    runtime.showErrorMessage(`Failed to fetch cluster info: ${message}`);
    const agentStatus = await runtime.buildAgentStatusMessage(String(values.identityFile ?? ''));
    return {
      command: 'clusterInfoError',
      message,
      sessions,
      agentStatus: agentStatus.text,
      agentStatusError: agentStatus.isError
    };
  }
}

export function buildSessionQueryScript(stateDir: string): string {
  const dirLiteral = JSON.stringify(stateDir);
  return [
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
}

export function buildSessionQueryCommand(runtime: ClusterQueryRuntime, stateDir: string): string {
  const script = buildSessionQueryScript(stateDir);
  const encodedScript = Buffer.from(script, 'utf8').toString('base64');
  return runtime.wrapPythonScriptCommand(encodedScript, '"[]"');
}

export function buildCombinedClusterInfoScript(commands: string[], sessionsScript?: string): string {
  const encodedCommands = commands.map((command) => Buffer.from(command, 'utf8').toString('base64'));
  const commandArray = encodedCommands.map((encoded) => `'${encoded}'`).join(' ');
  const sessionLines: string[] = [];
  if (sessionsScript) {
    const encodedSessions = Buffer.from(sessionsScript, 'utf8').toString('base64');
    sessionLines.push(`echo "${CLUSTER_SESSIONS_START}"`);
    sessionLines.push('PYTHON=$(command -v python3 || command -v python || true)');
    sessionLines.push('if [ -z "$PYTHON" ]; then');
    sessionLines.push('  echo "[]"');
    sessionLines.push('else');
    sessionLines.push(`  printf %s ${encodedSessions} | base64 -d | "$PYTHON" -`);
    sessionLines.push('fi');
    sessionLines.push(`echo "${CLUSTER_SESSIONS_END}"`);
  }
  return [
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
    ...sessionLines,
    `echo "${CLUSTER_MODULES_START}"`,
    'modules=$(module -t avail 2>&1)',
    'status=$?',
    'if [ $status -ne 0 ] || echo "$modules" | grep -qi "command not found\\|module: not found"; then',
    '  modules=$(bash -lc "module -t avail 2>&1")',
    'fi',
    'printf "%s\\n" "$modules"',
    `echo "${CLUSTER_MODULES_END}"`
  ].join('\n');
}

export function parseCombinedClusterInfoOutput(
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

export function buildClusterInfoFromOutputs(
  commandOutputs: string[],
  modulesOutput: string,
  freeResourceIndexes: FreeResourceCommandIndexes | undefined,
  log: { appendLine(value: string): void }
): ClusterInfo {
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
  info.modules = parseModulesOutputUtil(modulesOutput);
  if (defaultPartitionFromScontrol && !info.defaultPartition) {
    info.defaultPartition = defaultPartitionFromScontrol;
  }
  applyPartitionDefaultTimes(info, partitionDefaults);
  if (freeResourceIndexes) {
    const nodeOutput = commandOutputs[freeResourceIndexes.nodeIndex] || '';
    const jobOutput = commandOutputs[freeResourceIndexes.jobIndex] || '';
    const freeSummary = computeFreeResourceSummaryUtil(nodeOutput, jobOutput) as
      | Map<string, PartitionFreeSummary>
      | undefined;
    if (freeSummary) {
      applyFreeResourceSummaryUtil(info, freeSummary);
    }
  }
  return info;
}

function uniqueList(values: string[]): string[] {
  return Array.from(new Set(values.filter(Boolean)));
}

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

async function queryAvailableModules(
  runtime: ClusterQueryRuntime,
  loginHost: string,
  cfg: SlurmConnectConfig
): Promise<string[]> {
  const log = runtime.getOutputChannel();
  const commands = ['module -t avail 2>&1', 'bash -lc "module -t avail 2>&1"'];

  for (const command of commands) {
    try {
      log.appendLine(`Module list command: ${command}`);
      const output = await runtime.runSshCommand(loginHost, cfg, command);
      const modules = parseModulesOutputUtil(output);
      if (modules.length > 0) {
        return modules;
      }
    } catch (error) {
      log.appendLine(`Module list command failed: ${runtime.formatError(error)}`);
    }
  }

  return [];
}

async function fetchClusterInfoSingleCall(
  runtime: ClusterQueryRuntime,
  loginHost: string,
  cfg: SlurmConnectConfig,
  commands: string[],
  freeResourceIndexes: FreeResourceCommandIndexes | undefined
): Promise<ClusterInfo> {
  const log = runtime.getOutputChannel();
  const combinedScript = buildCombinedClusterInfoScript(commands);
  log.appendLine(`Cluster info single-call script bytes: ${combinedScript.length}`);
  const output = await runtime.runSshCommandWithInput(loginHost, cfg, 'bash -s', combinedScript);
  const { commandOutputs, modulesOutput } = parseCombinedClusterInfoOutput(output, commands.length);
  return buildClusterInfoFromOutputs(commandOutputs, modulesOutput, freeResourceIndexes, log);
}
