import * as vscode from 'vscode';

import type { SessionMode, SlurmConnectConfig } from '../config/types';

const LAST_CONNECTION_STATE_KEY = 'slurmConnect.lastConnectionState';
const REMOTE_HOME_CACHE_KEY = 'slurmConnect.remoteHomeCache';

export interface StoredConnectionState {
  alias?: string;
  sessionKey?: string;
  sessionMode?: SessionMode;
  loginHost?: string;
  updatedAt?: number;
}

export interface ConnectionSnapshot {
  alias?: string;
  sessionKey?: string;
  sessionMode?: SessionMode;
  loginHost?: string;
}

type RemoteHomeCache = Record<string, string>;

function normalizeStoredValue(value?: string): string | undefined {
  const trimmed = (value || '').trim();
  return trimmed ? trimmed : undefined;
}

export function getStoredConnectionState(
  globalState: vscode.Memento | undefined
): StoredConnectionState | undefined {
  if (!globalState) {
    return undefined;
  }
  const stored = globalState.get<StoredConnectionState>(LAST_CONNECTION_STATE_KEY);
  return stored || undefined;
}

export function storeConnectionState(
  globalState: vscode.Memento | undefined,
  state: StoredConnectionState
): void {
  if (!globalState) {
    return;
  }
  const current = getStoredConnectionState(globalState) || {};
  const merged: StoredConnectionState = {
    alias: normalizeStoredValue(state.alias) || normalizeStoredValue(current.alias),
    sessionKey: normalizeStoredValue(state.sessionKey) || normalizeStoredValue(current.sessionKey),
    sessionMode: state.sessionMode || current.sessionMode,
    loginHost: normalizeStoredValue(state.loginHost) || normalizeStoredValue(current.loginHost),
    updatedAt: Date.now()
  };
  if (!merged.alias && !merged.sessionKey && !merged.loginHost) {
    return;
  }
  void globalState.update(LAST_CONNECTION_STATE_KEY, merged);
}

function getRemoteHomeCache(globalState: vscode.Memento | undefined): RemoteHomeCache {
  if (!globalState) {
    return {};
  }
  return globalState.get<RemoteHomeCache>(REMOTE_HOME_CACHE_KEY) || {};
}

export function getCachedRemoteHome(
  globalState: vscode.Memento | undefined,
  loginHost: string
): string | undefined {
  if (!loginHost) {
    return undefined;
  }
  return getRemoteHomeCache(globalState)[loginHost];
}

export function storeRemoteHome(
  globalState: vscode.Memento | undefined,
  loginHost: string,
  homeDir: string
): void {
  if (!globalState || !loginHost || !homeDir) {
    return;
  }
  const cache = getRemoteHomeCache(globalState);
  cache[loginHost] = homeDir;
  void globalState.update(REMOTE_HOME_CACHE_KEY, cache);
}

export function applyStoredConnectionStateIfMissing(
  current: ConnectionSnapshot,
  stored: StoredConnectionState | undefined
): ConnectionSnapshot {
  if (!stored) {
    return current;
  }
  return {
    alias: current.alias || stored.alias,
    sessionKey: current.sessionKey || stored.sessionKey,
    sessionMode: current.sessionMode || stored.sessionMode,
    loginHost: current.loginHost || stored.loginHost
  };
}

export function resolveSessionKey(
  cfg: Pick<SlurmConnectConfig, 'sessionKey'>,
  alias: string
): string {
  const trimmed = (cfg.sessionKey || '').trim();
  if (trimmed) {
    return trimmed;
  }
  return alias.trim();
}
