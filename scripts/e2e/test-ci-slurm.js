#!/usr/bin/env node

const cp = require('child_process');

const npmCommand = process.platform === 'win32' ? 'npm.cmd' : 'npm';

function runStep(args) {
  const result = cp.spawnSync(npmCommand, args, {
    stdio: 'inherit',
    env: process.env,
    shell: false
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    const error = new Error(`${npmCommand} ${args.join(' ')} exited with code ${result.status || 1}`);
    error.exitCode = result.status || 1;
    throw error;
  }
}

let runError;
let cleanupError;

try {
  runStep(['run', 'e2e:slurm:smoke']);
  runStep(['run', 'test:integration:slurm']);
  runStep(['run', 'e2e:slurm:remote-session']);
} catch (error) {
  runError = error;
} finally {
  try {
    runStep(['run', 'e2e:slurm:clean']);
  } catch (error) {
    cleanupError = error;
    process.stderr.write(`[test-ci-slurm] cleanup failed: ${error instanceof Error ? error.message : String(error)}\n`);
  }
}

if (runError) {
  throw runError;
}

if (cleanupError) {
  process.exitCode = cleanupError.exitCode || 1;
}
