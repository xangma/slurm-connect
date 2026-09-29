import { execFileSync } from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { afterEach, describe, expect, it, vi } from 'vitest';

vi.mock('vscode', () => ({}));

import { buildLoginHostCancelPersistentSessionCommand } from '../../localProxy/runtime';
import {
  PENDING_SESSION_CANCEL_MAX_AGE_MS,
  storePendingSessionCancel,
  takePendingSessionCancel
} from '../../localProxy/state';
import { splitShellArgs } from '../../utils/shellArgs';

function createMemento() {
  const values: Record<string, unknown> = {};
  return {
    values,
    get<T>(key: string): T | undefined {
      return values[key] as T | undefined;
    },
    async update(key: string, value: unknown): Promise<void> {
      if (value === undefined) {
        delete values[key];
      } else {
        values[key] = value;
      }
    }
  };
}

describe('pending session cancel state', () => {
  it('returns a fresh pending cancel once and then clears it', async () => {
    const memento = createMemento();
    await storePendingSessionCancel(memento as never, {
      loginHost: 'login1',
      sessionKey: 'session-1',
      requestedAt: 1_000
    });

    await expect(takePendingSessionCancel(memento as never, 2_000)).resolves.toEqual({
      loginHost: 'login1',
      sessionKey: 'session-1',
      requestedAt: 1_000
    });
    await expect(takePendingSessionCancel(memento as never, 2_000)).resolves.toBeUndefined();
  });

  it('drops stale pending cancels', async () => {
    const memento = createMemento();
    await storePendingSessionCancel(memento as never, {
      loginHost: 'login1',
      sessionKey: 'session-1',
      requestedAt: 1_000
    });

    await expect(
      takePendingSessionCancel(memento as never, 1_000 + PENDING_SESSION_CANCEL_MAX_AGE_MS + 1)
    ).resolves.toBeUndefined();
    expect(memento.values).toEqual({});
  });
});

describe.skipIf(process.platform === 'win32')('buildLoginHostCancelPersistentSessionCommand', () => {
  const tempDirs: string[] = [];

  afterEach(() => {
    for (const dir of tempDirs.splice(0)) {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  function setup(): { stateDir: string; binDir: string; logPath: string } {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'slurm-connect-cancel-'));
    tempDirs.push(root);
    const stateDir = path.join(root, 'state');
    const binDir = path.join(root, 'bin');
    const logPath = path.join(root, 'scancel.log');
    fs.mkdirSync(binDir);
    fs.writeFileSync(path.join(binDir, 'scancel'), `#!/bin/sh\necho "$@" >> "${logPath}"\n`, { mode: 0o755 });
    return { stateDir, binDir, logPath };
  }

  function writeJob(stateDir: string, user: string, key: string, jobId: string): void {
    const dir = path.join(stateDir, 'sessions', user, key);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'job.json'), JSON.stringify({ job_id: jobId, session_key: key }));
  }

  function run(command: string, binDir: string, user: string): { status: number; stdout: string } {
    // runSshCommand hands this string to the remote shell; emulate that locally.
    const [shell, ...args] = splitShellArgs(command);
    try {
      const stdout = execFileSync(shell, args, {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'pipe'],
        env: { ...process.env, USER: user, PATH: `${binDir}:${process.env.PATH}` }
      });
      return { status: 0, stdout };
    } catch (error) {
      const err = error as { status?: number; stdout?: string };
      return { status: err.status ?? 1, stdout: err.stdout ?? '' };
    }
  }

  it('cancels the job recorded for the session key', () => {
    const { stateDir, binDir, logPath } = setup();
    writeJob(stateDir, 'alice', 'slurm-login', '4242');
    writeJob(stateDir, 'alice', 'other', '9999');

    const result = run(buildLoginHostCancelPersistentSessionCommand(stateDir, 'slurm-login'), binDir, 'alice');

    expect(result.status).toBe(0);
    expect(result.stdout).toContain('cancelling job 4242');
    expect(fs.readFileSync(logPath, 'utf8').trim()).toBe('4242');
  });

  it('fails without calling scancel when no job is recorded', () => {
    const { stateDir, binDir, logPath } = setup();

    const result = run(buildLoginHostCancelPersistentSessionCommand(stateDir, 'missing'), binDir, 'alice');

    expect(result.status).not.toBe(0);
    expect(fs.existsSync(logPath)).toBe(false);
  });
});
