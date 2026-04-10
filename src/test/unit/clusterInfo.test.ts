import { describe, expect, it } from 'vitest';

import { parsePartitionInfoOutput } from '../../utils/clusterInfo';

describe('clusterInfo utilities', () => {
  it('counts node names and node totals correctly', () => {
    const output = 'gpu*|2gpu-01|64|128000|gpu:a100:4';
    const info = parsePartitionInfoOutput(output);
    expect(info.partitions[0]?.nodes).toBe(1);

    const outputWithCount = 'cpu|10|32|64000|';
    const infoWithCount = parsePartitionInfoOutput(outputWithCount);
    expect(infoWithCount.partitions[0]?.nodes).toBe(10);
  });

  it('parses CPU-only partitions cleanly', () => {
    const output = 'lrd_all_serial*|node001|32|256000|';
    const info = parsePartitionInfoOutput(output);
    expect(info.defaultPartition).toBe('lrd_all_serial');
    expect(info.partitions).toHaveLength(1);
    expect(info.partitions[0]).toMatchObject({
      name: 'lrd_all_serial',
      gpuMax: 0,
      gpuTypes: {}
    });
  });

  it('tracks the highest GPU capacity per partition', () => {
    const output = ['gpuq|node001|64|256000|gpu:a100:4', 'gpuq|node002|64|256000|gpu:a100:8'].join('\n');
    const info = parsePartitionInfoOutput(output);
    expect(info.partitions).toHaveLength(1);
    expect(info.partitions[0]?.gpuMax).toBe(8);
    expect(info.partitions[0]?.gpuTypes.a100).toBe(8);
  });
});
