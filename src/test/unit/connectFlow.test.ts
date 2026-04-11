import { describe, expect, it, vi } from 'vitest';

vi.mock('vscode', () => ({
  ProgressLocation: {
    Notification: 15
  }
}));

import { buildDefaultAlias, buildSallocArgs } from '../../connect/flow';

describe('connect flow helpers', () => {
  it('builds salloc args with optional resources', () => {
    expect(
      buildSallocArgs({
        partition: 'gpu',
        nodes: 2,
        tasksPerNode: 4,
        cpusPerTask: 8,
        time: '04:00:00',
        memoryMb: 65536,
        gpuType: 'a100',
        gpuCount: 2,
        qos: 'interactive',
        account: 'research'
      })
    ).toEqual([
      '--partition=gpu',
      '--nodes=2',
      '--ntasks-per-node=4',
      '--cpus-per-task=8',
      '--time=04:00:00',
      '--qos=interactive',
      '--account=research',
      '--mem=65536',
      '--gres=gpu:a100:2'
    ]);
  });

  it('omits optional salloc args when values are unset', () => {
    expect(
      buildSallocArgs({
        partition: undefined,
        nodes: 1,
        tasksPerNode: 1,
        cpusPerTask: 1,
        time: '',
        memoryMb: 0,
        gpuType: '',
        gpuCount: 0
      })
    ).toEqual(['--nodes=1', '--ntasks-per-node=1', '--cpus-per-task=1']);
  });

  it('builds sanitized default aliases from connect selections', () => {
    expect(buildDefaultAlias('slurm', 'login01.cluster.example', 'gpu/a100', 2, 8)).toBe(
      'slurm-login01-gpu-a100-2n-8c'
    );
  });
});
