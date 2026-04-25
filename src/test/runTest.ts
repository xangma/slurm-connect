import * as fs from 'fs/promises';
import { createRequire } from 'module';
import * as os from 'os';
import * as path from 'path';
import type * as childProcessTypes from 'child_process';

import { runTests } from '@vscode/test-electron';

const TEST_RESULT_ENV = 'SLURM_CONNECT_TEST_RESULT_PATH';
const TEST_RESULT_TIMEOUT_MS = 300000;
const WORKBENCH_SHUTDOWN_GRACE_MS = 15000;
const loadModule = createRequire(__filename);
const childProcess = loadModule('child_process') as {
  spawn: (...args: unknown[]) => childProcessTypes.ChildProcess;
  spawnSync: typeof childProcessTypes.spawnSync;
};

type TestResultMarker = {
  status: 'passed' | 'failed';
  message?: string;
};

function buildExtensionTestEnv(): Record<string, string | undefined> {
  const env: Record<string, string | undefined> = {};
  if (process.argv.includes('--slurm-fixture-e2e')) {
    env.SLURM_CONNECT_FIXTURE = '1';
  }
  if (process.argv.includes('--slurm-client-e2e')) {
    env.SLURM_CONNECT_CLIENT_FIXTURE = '1';
  }
  for (const key of [
    'SLURM_CONNECT_CLIENT_FIXTURE',
    'SLURM_FIXTURE_STATE_DIR',
    'SLURM_FIXTURE_SSH_HOST',
    'SLURM_FIXTURE_SSH_PORT',
    'SLURM_FIXTURE_USER',
    'SLURM_FIXTURE_UID',
    'SLURM_FIXTURE_GID',
    'SLURM_FIXTURE_GROUP',
    'SLURM_FIXTURE_HOME',
    'SLURM_CLIENT_DEFAULT_PARTITION',
    'SLURM_CLIENT_EXPECTED_SINFO_PATTERN'
  ]) {
    if (process.env[key] !== undefined) {
      env[key] = process.env[key];
    }
  }
  return env;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function captureSpawnedWorkbench(): { getChild: () => childProcessTypes.ChildProcess | undefined; restore: () => void } {
  const originalSpawn = childProcess.spawn;
  let workbenchChild: childProcessTypes.ChildProcess | undefined;
  childProcess.spawn = (...args: unknown[]) => {
    const child = originalSpawn(...args);
    const commandArgs = args[1];
    const hasExtensionTestsPath =
      Array.isArray(commandArgs) &&
      commandArgs.some((arg) => typeof arg === 'string' && arg.startsWith('--extensionTestsPath='));
    if (hasExtensionTestsPath) {
      workbenchChild = child;
    }
    return child;
  };
  return {
    getChild: () => workbenchChild,
    restore: () => {
      childProcess.spawn = originalSpawn;
    }
  };
}

async function readResultMarker(resultPath: string): Promise<TestResultMarker | undefined> {
  try {
    const content = await fs.readFile(resultPath, 'utf8');
    return JSON.parse(content) as TestResultMarker;
  } catch (error) {
    const nodeError = error as NodeJS.ErrnoException;
    if (nodeError.code === 'ENOENT') {
      return undefined;
    }
    throw error;
  }
}

async function waitForResultMarker(resultPath: string): Promise<TestResultMarker> {
  const deadline = Date.now() + TEST_RESULT_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const result = await readResultMarker(resultPath);
    if (result) {
      return result;
    }
    await delay(250);
  }
  throw new Error(`Timed out waiting for VS Code integration test result marker at ${resultPath}`);
}

async function waitForPromise<T>(promise: Promise<T>, timeoutMs: number): Promise<{ settled: true; value: T } | { settled: false }> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise.then((value) => ({ settled: true as const, value })),
      new Promise<{ settled: false }>((resolve) => {
        timeout = setTimeout(() => resolve({ settled: false }), timeoutMs);
      })
    ]);
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

function terminateProcessTree(pid: number): void {
  if (process.platform === 'win32') {
    childProcess.spawnSync('taskkill', ['/pid', String(pid), '/T', '/F'], { stdio: 'ignore' });
    return;
  }
  childProcess.spawnSync('pkill', ['-TERM', '-P', String(pid)], { stdio: 'ignore' });
  try {
    process.kill(pid, 'SIGTERM');
  } catch {
    return;
  }
  childProcess.spawnSync('pkill', ['-KILL', '-P', String(pid)], { stdio: 'ignore' });
  try {
    process.kill(pid, 'SIGKILL');
  } catch {
    // The process exited between signals.
  }
}

async function runTestsWithResultMarker(options: Parameters<typeof runTests>[0], resultPath: string): Promise<void> {
  const capture = captureSpawnedWorkbench();
  const runTestsPromise = runTests(options);
  const observedRunTestsPromise = runTestsPromise.catch((error: unknown) => {
    throw error;
  });

  try {
    const first = await Promise.race([
      observedRunTestsPromise.then(() => ({ kind: 'runTestsExited' as const })),
      waitForResultMarker(resultPath).then((result) => ({ kind: 'testResult' as const, result }))
    ]);

    if (first.kind === 'runTestsExited') {
      return;
    }

    if (first.result.status === 'failed') {
      const child = capture.getChild();
      if (child?.pid && child.exitCode === null && child.signalCode === null) {
        terminateProcessTree(child.pid);
      }
      await waitForPromise(observedRunTestsPromise.catch(() => undefined), 5000);
      throw new Error(first.result.message || 'VS Code integration tests failed.');
    }

    const shutdown = await waitForPromise(observedRunTestsPromise, WORKBENCH_SHUTDOWN_GRACE_MS);
    if (shutdown.settled) {
      return;
    }

    const child = capture.getChild();
    if (!child?.pid) {
      throw new Error('VS Code integration tests passed, but the workbench process did not exit and its PID was unavailable.');
    }
    console.warn(`VS Code integration tests passed; terminating lingering workbench process ${child.pid}.`);
    terminateProcessTree(child.pid);
    await waitForPromise(observedRunTestsPromise.catch(() => undefined), 5000);
  } catch (error) {
    const child = capture.getChild();
    if (child?.pid && child.exitCode === null && child.signalCode === null) {
      terminateProcessTree(child.pid);
      await waitForPromise(observedRunTestsPromise.catch(() => undefined), 5000);
    }
    throw error;
  } finally {
    capture.restore();
  }
}

async function main(): Promise<void> {
  try {
    delete process.env.ELECTRON_RUN_AS_NODE;
    const extensionDevelopmentPath = path.resolve(__dirname, '../..');
    const extensionTestsPath = path.resolve(__dirname, './suite');
    const testWorkspace = path.resolve(__dirname, '../../test-fixtures/workspace');
    const resultDir = await fs.mkdtemp(path.join(os.tmpdir(), 'slurm-connect-tests-'));
    const resultPath = path.join(resultDir, 'result.json');

    try {
      await runTestsWithResultMarker(
        {
          extensionDevelopmentPath,
          extensionTestsPath,
          extensionTestsEnv: {
            ...buildExtensionTestEnv(),
            [TEST_RESULT_ENV]: resultPath
          },
          launchArgs: [testWorkspace, '--disable-extensions']
        },
        resultPath
      );
    } finally {
      await fs.rm(resultDir, { recursive: true, force: true });
    }
  } catch (error) {
    console.error('Failed to run VS Code integration tests.');
    console.error(error);
    process.exit(1);
  }
}

void main();
