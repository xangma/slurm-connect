import type { ClusterInfo } from './clusterInfo';

export const FREE_RESOURCE_NODE_INFO_COMMAND = 'sinfo -h -N -o "%n|%c|%t|%P|%G"';
export const FREE_RESOURCE_SQUEUE_COMMAND = 'squeue -h -o "%t|%C|%b|%N"';
export const FREE_RESOURCE_BAD_STATES = new Set(['down', 'drain', 'drng', 'maint', 'fail']);

export interface NodeInfo {
  name: string;
  cpus: number;
  state: string;
  partitions: string[];
  gpuTypes: Record<string, number>;
}

export interface PartitionFreeSummary {
  freeNodes: number;
  freeCpuTotal: number;
  freeCpusPerNode: number;
  freeGpuTotal: number;
  freeGpuMax: number;
  freeGpuTypes: Record<string, number>;
}

export interface FreeResourceCommandIndexes {
  infoCommandCount: number;
  nodeIndex: number;
  jobIndex: number;
}

export interface ClusterInfoCommandConfig {
  partitionInfoCommand: string;
}

export function buildClusterInfoCommandSet(
  cfg: ClusterInfoCommandConfig
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

export function normalizePartitionName(value: string): string {
  return value.replace(/\*/g, '').trim();
}

export function parseGresField(raw: string): Record<string, number> {
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

export function parseNodeInfoOutput(output: string): Map<string, NodeInfo> {
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

export function splitSlurmList(input: string): string[] {
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

export function expandNumericRange(value: string): string[] {
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
    for (let current = start; current <= end; current += step) {
      results.push(padNumber(current, width));
    }
  } else {
    for (let current = start; current >= end; current -= step) {
      results.push(padNumber(current, width));
    }
  }
  return results;
}

export function expandBracketValues(value: string): string[] {
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

export function expandHostToken(token: string): string[] {
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

export function expandSlurmHostList(input: string): string[] {
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

export function parseSqueueUsageOutput(output: string): {
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

export function computeFreeResourceSummary(
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

export function applyFreeResourceSummary(
  info: ClusterInfo,
  summary: Map<string, PartitionFreeSummary>
): void {
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
