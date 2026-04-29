#!/usr/bin/env node

const cp = require('child_process');

const { prepareClientFixtureFromEnv } = require('./prepare-slurm-client-fixture');

const npmCommand = process.platform === 'win32' ? 'npm.cmd' : 'npm';

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = cp.spawn(command, args, {
      stdio: 'inherit',
      env: options.env || process.env,
      shell: process.platform === 'win32' && /\.(?:cmd|bat)$/i.test(command)
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${command} ${args.join(' ')} exited with code ${code || 1}`));
    });
  });
}

async function main() {
  const prepared = await prepareClientFixtureFromEnv();
  if (prepared.skipped) {
    return;
  }
  const env = {
    ...process.env,
    ...prepared.env
  };
  await runCommand(npmCommand, ['run', 'compile'], { env });
  await runCommand(
    'node',
    ['./scripts/e2e/run-with-display.js', 'node', './out/test/runTest.js', '--slurm-client-e2e'],
    { env }
  );
}

main().catch((error) => {
  process.stderr.write(`[slurm-client] ERROR: ${error instanceof Error ? error.stack || error.message : String(error)}\n`);
  process.exit(1);
});
