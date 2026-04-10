import { describe, expect, it } from 'vitest';

import type { ClusterInfo } from '../../utils/clusterInfo';
import {
  applyFreeResourceSummary,
  buildClusterInfoCommandSet,
  computeFreeResourceSummary,
  expandNumericRange,
  expandSlurmHostList,
  parseGresField
} from '../../utils/slurmResources';

describe('slurmResources utilities', () => {
  it('builds the cluster-info command set with stable indexes', () => {
    const result = buildClusterInfoCommandSet({
      partitionInfoCommand: 'custom partition info'
    });

    expect(result.commands.slice(0, 4)).toEqual([
      'custom partition info',
      'sinfo -h -N -o "%P|%n|%c|%m|%G"',
      'sinfo -h -o "%P|%D|%c|%m|%G"',
      'scontrol show partition -o'
    ]);
    expect(result.freeResourceIndexes).toEqual({
      infoCommandCount: 4,
      nodeIndex: 4,
      jobIndex: 5
    });
  });

  it('parses GPU resource strings from multiple Slurm formats', () => {
    expect(parseGresField('gpu:a100:4,gres/gpu:h100:2,gpu')).toEqual({
      a100: 4,
      h100: 2,
      '': 1
    });
  });

  it('expands numeric ranges and Slurm host lists', () => {
    expect(expandNumericRange('001-005:2')).toEqual(['001', '003', '005']);
    expect(expandSlurmHostList('node[001-002],gpu[7-6]')).toEqual([
      'node001',
      'node002',
      'gpu7',
      'gpu6'
    ]);
  });

  it('computes and applies free-resource summaries per partition', () => {
    const nodeInfoOutput = [
      'node001|16|idle|gpu,debug|gpu:a100:4',
      'node002|16|mix|gpu|gpu:a100:4',
      'node003|32|drain|cpu|'
    ].join('\n');
    const squeueOutput = 'R|10|gpu:a100:3|node[001-002]';

    const summary = computeFreeResourceSummary(nodeInfoOutput, squeueOutput);
    expect(summary).toBeDefined();
    expect(summary?.get('gpu')).toEqual({
      freeNodes: 2,
      freeCpuTotal: 22,
      freeCpusPerNode: 11,
      freeGpuTotal: 5,
      freeGpuMax: 3,
      freeGpuTypes: {
        a100: 3
      }
    });
    expect(summary?.get('debug')).toEqual({
      freeNodes: 1,
      freeCpuTotal: 11,
      freeCpusPerNode: 11,
      freeGpuTotal: 2,
      freeGpuMax: 2,
      freeGpuTypes: {
        a100: 2
      }
    });
    expect(summary?.get('cpu')).toEqual({
      freeNodes: 0,
      freeCpuTotal: 0,
      freeCpusPerNode: 0,
      freeGpuTotal: 0,
      freeGpuMax: 0,
      freeGpuTypes: {}
    });

    const info: ClusterInfo = {
      partitions: [
        {
          name: 'gpu',
          nodes: 2,
          cpus: 16,
          memMb: 64000,
          gpuMax: 4,
          gpuTypes: { a100: 4 },
          isDefault: false
        },
        {
          name: 'debug',
          nodes: 1,
          cpus: 16,
          memMb: 64000,
          gpuMax: 4,
          gpuTypes: { a100: 4 },
          isDefault: false
        }
      ]
    };
    applyFreeResourceSummary(info, summary!);

    expect(info.partitions[0]).toMatchObject({
      freeNodes: 2,
      freeCpuTotal: 22,
      freeGpuMax: 3
    });
    expect(info.partitions[1]).toMatchObject({
      freeNodes: 1,
      freeCpuTotal: 11,
      freeGpuMax: 2
    });
  });
});
