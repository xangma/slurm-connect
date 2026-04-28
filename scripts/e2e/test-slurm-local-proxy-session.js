#!/usr/bin/env node

const cp = require('child_process');
const os = require('os');
const path = require('path');

const ROOT_DIR = path.resolve(__dirname, '../..');
const npmCommand = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const defaultStateDir =
  process.platform === 'darwin'
    ? '/tmp/sc-lp'
    : path.join(os.tmpdir(), 'slurm-connect-local-proxy-session');

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = cp.spawn(command, args, {
      cwd: ROOT_DIR,
      stdio: 'inherit',
      env: options.env || process.env,
      shell: false
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
  const env = {
    ...process.env,
    SLURM_REMOTE_SESSION_ENABLE_LOCAL_PROXY_E2E: '1',
    SLURM_CLIENT_LOCAL_PROXY_REMOTE_HOST:
      process.env.SLURM_CLIENT_LOCAL_PROXY_REMOTE_HOST || 'slurmctld',
    SLURM_REMOTE_SESSION_SKIP_REMOTE_FS_MARKER: process.env.SLURM_REMOTE_SESSION_SKIP_REMOTE_FS_MARKER || '1',
    SLURM_REMOTE_SESSION_STATE_DIR:
      process.env.SLURM_REMOTE_SESSION_STATE_DIR ||
      defaultStateDir
  };
  await runCommand(npmCommand, ['run', 'compile'], { env });
  await runCommand(
    'node',
    ['./scripts/e2e/run-with-display.js', 'node', './scripts/e2e/vscode-remote-session.js'],
    { env }
  );
}

main().catch((error) => {
  process.stderr.write(`[slurm-local-proxy-session] ERROR: ${error instanceof Error ? error.stack || error.message : String(error)}\n`);
  process.exit(1);
});
