import { describe, expect, it } from 'vitest';

import { applySessionIdleInfo, parseSessionListOutput } from '../../utils/sessionInfo';

describe('sessionInfo utilities', () => {
  it('parses session lists and drops invalid entries', () => {
    const output = JSON.stringify([
      {
        sessionKey: 'alpha ',
        jobId: ' 123 ',
        state: ' RUNNING ',
        partition: ' gpu ',
        nodes: 2,
        cpus: '64',
        clients: '0',
        lastSeen: '1700000000',
        idleTimeoutSeconds: '300'
      },
      {
        sessionKey: 'missing-job-id'
      },
      null,
      'bad'
    ]);

    expect(parseSessionListOutput(output)).toEqual([
      {
        sessionKey: 'alpha',
        jobId: '123',
        state: 'RUNNING',
        partition: 'gpu',
        nodes: 2,
        cpus: 64,
        clients: 0,
        lastSeenEpoch: 1700000000,
        idleTimeoutSeconds: 300
      }
    ]);
  });

  it('returns an empty list for non-array or invalid JSON payloads', () => {
    expect(parseSessionListOutput('')).toEqual([]);
    expect(parseSessionListOutput('{}')).toEqual([]);
    expect(parseSessionListOutput('{not json')).toEqual([]);
  });

  it('applies idle timeout calculations deterministically', () => {
    const sessions = [
      {
        sessionKey: 'alpha',
        jobId: '1',
        state: 'RUNNING',
        clients: 0,
        lastSeenEpoch: 1_700_000_000,
        idleTimeoutSeconds: 300
      },
      {
        sessionKey: 'beta',
        jobId: '2',
        state: 'RUNNING',
        clients: 2,
        lastSeenEpoch: 1_700_000_000,
        idleTimeoutSeconds: 300
      }
    ];

    const result = applySessionIdleInfo(sessions, { nowMs: 1_700_000_090_000 });
    expect(result[0]?.idleRemainingSeconds).toBe(210);
    expect(result[1]?.idleRemainingSeconds).toBeUndefined();
  });
});
