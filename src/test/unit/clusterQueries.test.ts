import { describe, expect, it } from 'vitest';

import {
  parseCombinedClusterInfoOutput,
  parsePartitionDefaultTimesOutput,
  parsePartitionOutput,
  parseSimpleList
} from '../../connect/clusterQueries';

describe('cluster query helpers', () => {
  it('parses partition names and default markers', () => {
    expect(parsePartitionOutput('cpu gpu* gpu debug*')).toEqual({
      partitions: ['cpu', 'gpu', 'debug'],
      defaultPartition: 'gpu'
    });
  });

  it('parses simple unique token lists', () => {
    expect(parseSimpleList('normal\nnormal debug   gpu')).toEqual(['normal', 'debug', 'gpu']);
  });

  it('extracts partition defaults from scontrol output', () => {
    const output = [
      'PartitionName=gpu Default=YES DefaultTime=02:00:00 DefaultNodes=2 DefaultTasksPerNode=4 DefCpuPerTask=8 DefMemPerNode=64G DefGRES=gpu:a100:2',
      'PartitionName=cpu Default=NO JobDefaults=DefMemPerNode=8192,DefaultTasksPerNode=2'
    ].join('\n');

    expect(parsePartitionDefaultTimesOutput(output)).toEqual({
      defaultPartition: 'gpu',
      defaults: {
        gpu: {
          defaultTime: '02:00:00',
          defaultNodes: 2,
          defaultTasksPerNode: 4,
          defaultCpusPerTask: 8,
          defaultMemoryMb: 65536,
          defaultGpuType: 'a100',
          defaultGpuCount: 2
        },
        cpu: {
          defaultMemoryMb: 8192,
          defaultTasksPerNode: 2
        }
      }
    });
  });

  it('splits combined cluster output into command, module, and session sections', () => {
    const output = [
      '__SC_CMD_START__0__',
      'partition data',
      '__SC_CMD_END__0__',
      '__SC_CMD_START__1__',
      'node data',
      '__SC_CMD_END__1__',
      '__SC_SESSIONS_START__',
      '[{"sessionKey":"abc"}]',
      '__SC_SESSIONS_END__',
      '__SC_MODULES_START__',
      'gcc/13.2.0',
      'cuda/12.2',
      '__SC_MODULES_END__'
    ].join('\n');

    expect(parseCombinedClusterInfoOutput(output, 2)).toEqual({
      commandOutputs: ['partition data', 'node data'],
      modulesOutput: 'gcc/13.2.0\ncuda/12.2',
      sessionsOutput: '[{"sessionKey":"abc"}]'
    });
  });
});
