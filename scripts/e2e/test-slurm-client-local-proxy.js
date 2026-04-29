#!/usr/bin/env node

const cp = require('child_process');
const os = require('os');
const path = require('path');

const { prepareClientFixtureFromEnv } = require('./prepare-slurm-client-fixture');

const npmCommand = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const defaultStateDir =
  process.platform === 'darwin'
    ? '/tmp/sc-client-lp'
    : path.join(os.tmpdir(), 'slurm-connect-client-local-proxy-session');

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
    ...prepared.env,
    SLURM_REMOTE_SESSION_EXTERNAL_FIXTURE: '1',
    SLURM_REMOTE_SESSION_ENABLE_LOCAL_PROXY_E2E: '1',
    SLURM_REMOTE_SESSION_SKIP_REMOTE_FS_MARKER: process.env.SLURM_REMOTE_SESSION_SKIP_REMOTE_FS_MARKER || '1',
    SLURM_REMOTE_SESSION_STATE_DIR:
      process.env.SLURM_REMOTE_SESSION_STATE_DIR || defaultStateDir
  };
  await runCommand(npmCommand, ['run', 'compile'], { env });
  await runCommand(
    'node',
    ['./scripts/e2e/run-with-display.js', 'node', './scripts/e2e/vscode-remote-session.js'],
    { env }
  );
}

main().catch((error) => {
  process.stderr.write(`[slurm-client-local-proxy] ERROR: ${error instanceof Error ? error.stack || error.message : String(error)}\n`);
  process.exit(1);
});
