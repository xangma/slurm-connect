import { describe, expect, it } from 'vitest';

import {
  cancelActiveConnectToken,
  createConnectToken,
  finalizeConnectToken,
  isConnectCancelled,
  type ConnectTokenState
} from '../../state/connectionState';

describe('connectionState helpers', () => {
  it('tracks and cancels the active connect token', () => {
    const state: ConnectTokenState = { connectTokenCounter: 0 };

    const first = createConnectToken(state);
    const second = createConnectToken(state);

    expect(first.id).toBe(1);
    expect(second.id).toBe(2);
    expect(state.activeConnectToken).toBe(second);

    cancelActiveConnectToken(state);
    expect(isConnectCancelled(second)).toBe(true);

    finalizeConnectToken(state, second);
    expect(state.activeConnectToken).toBeUndefined();
  });

  it('does not clear a newer token when finalizing an older one', () => {
    const state: ConnectTokenState = { connectTokenCounter: 0 };

    const first = createConnectToken(state);
    const second = createConnectToken(state);

    finalizeConnectToken(state, first);

    expect(state.activeConnectToken).toBe(second);
    expect(isConnectCancelled(second)).toBe(false);
  });
});
