export type ConnectionState = 'idle' | 'connecting' | 'connected' | 'disconnecting';

export interface ConnectToken {
  id: number;
  cancelled: boolean;
}

export interface ConnectTokenState {
  activeConnectToken?: ConnectToken;
  connectTokenCounter: number;
}

export function createConnectToken(state: ConnectTokenState): ConnectToken {
  const token = { id: state.connectTokenCounter + 1, cancelled: false };
  state.connectTokenCounter += 1;
  state.activeConnectToken = token;
  return token;
}

export function cancelActiveConnectToken(state: ConnectTokenState): void {
  if (state.activeConnectToken) {
    state.activeConnectToken.cancelled = true;
  }
}

export function isConnectCancelled(token?: ConnectToken): boolean {
  return Boolean(token?.cancelled);
}

export function finalizeConnectToken(state: ConnectTokenState, token?: ConnectToken): void {
  if (token && state.activeConnectToken === token) {
    state.activeConnectToken = undefined;
  }
}
