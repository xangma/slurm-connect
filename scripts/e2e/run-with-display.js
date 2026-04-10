#!/usr/bin/env node

const cp = require('child_process');

function die(message) {
  process.stderr.write(`[run-with-display] ERROR: ${message}\n`);
  process.exit(1);
}

function spawnAndExit(command, args) {
  const result = cp.spawnSync(command, args, {
    stdio: 'inherit',
    env: process.env,
    shell: false
  });
  if (result.error) {
    if (result.error.code === 'ENOENT') {
      die(`command not found: ${command}`);
    }
    throw result.error;
  }
  if (typeof result.status === 'number') {
    process.exit(result.status);
  }
  if (result.signal) {
    process.kill(process.pid, result.signal);
  }
  process.exit(1);
}

function hasXvfbRun() {
  const result = cp.spawnSync('xvfb-run', ['--help'], {
    stdio: 'ignore',
    env: process.env,
    shell: false
  });
  return !(result.error && result.error.code === 'ENOENT');
}

const args = process.argv.slice(2);
if (args.length === 0) {
  die('missing command to run');
}

if (process.platform !== 'linux') {
  spawnAndExit(args[0], args.slice(1));
}

if (process.env.DISPLAY || process.env.WAYLAND_DISPLAY) {
  spawnAndExit(args[0], args.slice(1));
}

if (!hasXvfbRun()) {
  die('headless Linux requires xvfb-run (xvfb) for Electron-based tests.');
}

spawnAndExit('xvfb-run', ['-a', ...args]);
