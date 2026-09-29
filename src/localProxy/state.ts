import * as vscode from 'vscode';

import type { SessionMode, SlurmConnectConfig } from '../config/types';
import { splitShellArgs } from '../utils/shellArgs';

const LAST_CONNECTION_STATE_KEY = 'slurmConnect.lastConnectionState';
const REMOTE_HOME_CACHE_KEY = 'slurmConnect.remoteHomeCache';
const PENDING_SESSION_CANCEL_KEY = 'slurmConnect.pendingSessionCancel';

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

export interface PendingSessionCancel {
  loginHost: string;
  sessionKey: string;
  requestedAt: number;
}

// Cancel requests outlive the extension host: closing the remote window reloads it.
export const PENDING_SESSION_CANCEL_MAX_AGE_MS = 5 * 60 * 1000;

export async function storePendingSessionCancel(
  globalState: vscode.Memento | undefined,
  pending: PendingSessionCancel
): Promise<void> {
  if (!globalState) {
    return;
  }
  await globalState.update(PENDING_SESSION_CANCEL_KEY, pending);
}

export async function takePendingSessionCancel(
  globalState: vscode.Memento | undefined,
  now: number = Date.now()
): Promise<PendingSessionCancel | undefined> {
  if (!globalState) {
    return undefined;
  }
  const pending = globalState.get<PendingSessionCancel>(PENDING_SESSION_CANCEL_KEY);
  if (!pending) {
    return undefined;
  }
  await globalState.update(PENDING_SESSION_CANCEL_KEY, undefined);
  const age = now - (pending.requestedAt || 0);
  if (!pending.loginHost || !pending.sessionKey || age < 0 || age > PENDING_SESSION_CANCEL_MAX_AGE_MS) {
    return undefined;
  }
  return pending;
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

// The generated SSH host entry carries the session key the proxy was started with,
// nested inside `bash -lc '...'`, so unwrap quoted words until the flag turns up.
export function extractSessionKeyFromRemoteCommand(remoteCommand?: string): string | undefined {
  let queue = remoteCommand ? [remoteCommand] : [];
  for (let depth = 0; depth < 4 && queue.length > 0; depth += 1) {
    const next: string[] = [];
    for (const command of queue) {
      for (const token of splitShellArgs(command)) {
        if (token.startsWith('--session-key=')) {
          const key = token.slice('--session-key='.length).trim();
          if (key) {
            return key;
          }
        } else if (/\s/.test(token)) {
          next.push(token);
        }
      }
    }
    queue = next;
  }
  return undefined;
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
