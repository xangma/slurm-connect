export interface PartitionInfo {
  name: string;
  nodes: number;
  cpus: number;
  memMb: number;
  gpuMax: number;
  gpuTypes: Record<string, number>;
  isDefault: boolean;
  defaultTime?: string;
  defaultNodes?: number;
  defaultTasksPerNode?: number;
  defaultCpusPerTask?: number;
  defaultMemoryMb?: number;
  defaultGpuType?: string;
  defaultGpuCount?: number;
}

export interface ClusterInfo {
  partitions: PartitionInfo[];
  defaultPartition?: string;
  modules?: string[];
}

export function hasMeaningfulClusterInfo(info: ClusterInfo): boolean {
  if (!info.partitions || info.partitions.length === 0) {
    return false;
  }
  return info.partitions.some((partition) =>
    partition.cpus > 0 || partition.memMb > 0 || partition.gpuMax > 0
  );
}

export function getMaxFieldCount(output: string): number {
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

export function parsePartitionInfoOutput(output: string): ClusterInfo {
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

    const isField1Numeric = /^\d+$/.test(field1);
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
