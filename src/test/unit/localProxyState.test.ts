import { describe, expect, it, vi } from 'vitest';

vi.mock('vscode', () => ({}));

import {
  applyStoredConnectionStateIfMissing,
  getCachedRemoteHome,
  getStoredConnectionState,
  resolveSessionKey,
  storeConnectionState,
  storeRemoteHome
} from '../../localProxy/state';

function createMemento() {
  const values: Record<string, unknown> = {};
  const updates: Array<{ key: string; value: unknown }> = [];
  return {
    values,
    updates,
    get<T>(key: string): T | undefined {
      return values[key] as T | undefined;
    },
    async update(key: string, value: unknown): Promise<void> {
      updates.push({ key, value });
      if (value === undefined) {
        delete values[key];
      } else {
        values[key] = value;
      }
    }
  };
}

describe('local proxy state helpers', () => {
  it('stores connection state while preserving existing values for blank updates', () => {
    const memento = createMemento();

    storeConnectionState(memento as never, {
      alias: 'slurm-login',
      sessionKey: 'session-1',
      sessionMode: 'persistent',
      loginHost: 'login1'
    });
    storeConnectionState(memento as never, {
      alias: ' ',
      sessionKey: '',
      sessionMode: 'ephemeral',
      loginHost: undefined
    });

    expect(getStoredConnectionState(memento as never)).toMatchObject({
      alias: 'slurm-login',
      sessionKey: 'session-1',
      sessionMode: 'ephemeral',
      loginHost: 'login1'
    });
    expect(memento.updates).toHaveLength(2);
  });

  it('caches remote homes by login host', () => {
    const memento = createMemento();

    storeRemoteHome(memento as never, 'login1', '/home/alice');
    storeRemoteHome(memento as never, 'login2', '/scratch/alice');

    expect(getCachedRemoteHome(memento as never, 'login1')).toBe('/home/alice');
    expect(getCachedRemoteHome(memento as never, 'login2')).toBe('/scratch/alice');
    expect(getCachedRemoteHome(memento as never, '')).toBeUndefined();
  });

  it('applies stored connection values only when the current snapshot is missing them', () => {
    expect(
      applyStoredConnectionStateIfMissing(
        { alias: 'current', sessionMode: 'ephemeral' },
        {
          alias: 'stored',
          sessionKey: 'session-1',
          sessionMode: 'persistent',
          loginHost: 'login1'
        }
      )
    ).toEqual({
      alias: 'current',
      sessionKey: 'session-1',
      sessionMode: 'ephemeral',
      loginHost: 'login1'
    });
  });

  it('prefers an explicit session key over the generated alias', () => {
    expect(resolveSessionKey({ sessionKey: ' configured ' }, 'slurm-login')).toBe('configured');
    expect(resolveSessionKey({ sessionKey: ' ' }, ' slurm-login ')).toBe('slurm-login');
  });
});
