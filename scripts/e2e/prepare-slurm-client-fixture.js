#!/usr/bin/env node

const cp = require('child_process');
const fs = require('fs/promises');
const path = require('path');

const ROOT_DIR = path.resolve(__dirname, '../..');
const DEFAULT_STATE_DIR = path.join(ROOT_DIR, '.e2e', 'slurm-client', process.platform);
const SSH_ALIAS = 'slurm-client-e2e';

function log(message) {
  process.stdout.write(`[slurm-client] ${message}\n`);
}

function normalizeBoolean(value) {
  return /^(1|true|yes)$/i.test(String(value || '').trim());
}

function formatSshConfigValue(value) {
  const normalized = String(value).replace(/\\/g, '\\\\');
  if (!/[\s#"]/u.test(normalized)) {
    return normalized;
  }
  return `"${normalized.replace(/"/g, '\\"')}"`;
}

async function writeSensitiveFile(filePath, content) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, content.replace(/\r\n/g, '\n').trimEnd() + '\n', 'utf8');
  await restrictFilePermissions(filePath);
}

async function restrictFilePermissions(filePath) {
  try {
    if (process.platform === 'win32') {
      const username = process.env.USERNAME || process.env.USER || '';
      if (username) {
        await runCommand('icacls', [filePath, '/inheritance:r'], { timeoutMs: 10000 });
        await runCommand('icacls', [filePath, '/grant:r', `${username}:R`], { timeoutMs: 10000 });
      }
      return;
    }
    await fs.chmod(filePath, 0o600);
  } catch {
    // Permission commands vary across local OpenSSH installs; connection verification will catch unusable keys.
  }
}

function decodeOptionalBase64(value) {
  const trimmed = String(value || '').trim();
  return trimmed ? Buffer.from(trimmed, 'base64').toString('utf8') : '';
}

async function resolvePrivateKey(env) {
  if (env.SLURM_CLIENT_SSH_PRIVATE_KEY) {
    return env.SLURM_CLIENT_SSH_PRIVATE_KEY;
  }
  if (env.SLURM_CLIENT_SSH_PRIVATE_KEY_B64) {
    return decodeOptionalBase64(env.SLURM_CLIENT_SSH_PRIVATE_KEY_B64);
  }
  if (env.SLURM_CLIENT_SSH_PRIVATE_KEY_PATH) {
    return await fs.readFile(env.SLURM_CLIENT_SSH_PRIVATE_KEY_PATH, 'utf8');
  }
  return '';
}

async function resolveKnownHosts(env) {
  if (env.SLURM_CLIENT_KNOWN_HOSTS) {
    return env.SLURM_CLIENT_KNOWN_HOSTS;
  }
  if (env.SLURM_CLIENT_KNOWN_HOSTS_B64) {
    return decodeOptionalBase64(env.SLURM_CLIENT_KNOWN_HOSTS_B64);
  }
  if (env.SLURM_CLIENT_KNOWN_HOSTS_PATH) {
    return await fs.readFile(env.SLURM_CLIENT_KNOWN_HOSTS_PATH, 'utf8');
  }
  return '';
}

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = cp.spawn(command, args, {
      cwd: options.cwd || ROOT_DIR,
      env: options.env || process.env,
      stdio: options.stdio || 'pipe',
      shell: false
    });
    let stdout = '';
    let stderr = '';
    const timeoutMs = options.timeoutMs || 0;
    const timeout = timeoutMs > 0
      ? setTimeout(() => {
          child.kill();
        }, timeoutMs)
      : undefined;
    if (child.stdout) {
      child.stdout.on('data', (chunk) => {
        stdout += chunk.toString('utf8');
      });
    }
    if (child.stderr) {
      child.stderr.on('data', (chunk) => {
        stderr += chunk.toString('utf8');
      });
    }
    child.on('error', (error) => {
      if (timeout) {
        clearTimeout(timeout);
      }
      reject(error);
    });
    child.on('close', (code, signal) => {
      if (timeout) {
        clearTimeout(timeout);
      }
      if (code === 0) {
        resolve({ stdout, stderr });
        return;
      }
      const error = new Error(
        `${command} ${args.join(' ')} exited with ${code === null ? signal : `code ${code}`}`
      );
      error.stdout = stdout;
      error.stderr = stderr;
      reject(error);
    });
  });
}

async function maybeCollectKnownHost(host, port) {
  try {
    const { stdout } = await runCommand('ssh-keyscan', ['-p', String(port), host], {
      timeoutMs: 15000
    });
    return stdout;
  } catch {
    return '';
  }
}

async function writeSshConfig({ sshConfigPath, privateKeyPath, knownHostsPath, host, port, user }) {
  const content = [
    `Host ${SSH_ALIAS}`,
    `  HostName ${host}`,
    `  User ${user}`,
    `  Port ${port}`,
    `  IdentityFile ${formatSshConfigValue(privateKeyPath)}`,
    '  IdentitiesOnly yes',
    '  StrictHostKeyChecking accept-new',
    `  UserKnownHostsFile ${formatSshConfigValue(knownHostsPath)}`,
    '  LogLevel ERROR',
    '',
    `Host ${host}`,
    `  HostName ${host}`,
    `  User ${user}`,
    `  Port ${port}`,
    `  IdentityFile ${formatSshConfigValue(privateKeyPath)}`,
    '  IdentitiesOnly yes',
    '  StrictHostKeyChecking accept-new',
    `  UserKnownHostsFile ${formatSshConfigValue(knownHostsPath)}`,
    '  LogLevel ERROR',
    ''
  ].join('\n');
  await fs.writeFile(sshConfigPath, content, 'utf8');
  try {
    await fs.chmod(sshConfigPath, 0o600);
  } catch {
    // Ignore chmod portability failures.
  }
}

async function queryRemoteHome(sshConfigPath) {
  const { stdout } = await runCommand(
    'ssh',
    ['-F', sshConfigPath, '-T', SSH_ALIAS, 'printf "%s\\n" "$HOME"'],
    { timeoutMs: 30000 }
  );
  return stdout.split(/\r?\n/).map((line) => line.trim()).find(Boolean) || '';
}

async function verifySlurmAccess(sshConfigPath) {
  const { stdout } = await runCommand(
    'ssh',
    ['-F', sshConfigPath, '-T', SSH_ALIAS, 'sinfo -h -o "%P|%D|%t" | head -n 1'],
    { timeoutMs: 30000 }
  );
  const line = stdout.split(/\r?\n/).map((entry) => entry.trim()).find(Boolean);
  if (!line || !line.includes('|')) {
    throw new Error(`sinfo did not return expected partition data: ${stdout}`);
  }
  return line;
}

function shouldSkip(env, privateKey) {
  const hasMinimumConfig =
    Boolean(env.SLURM_CLIENT_SSH_HOST) &&
    Boolean(env.SLURM_CLIENT_SSH_USER) &&
    Boolean(privateKey);
  if (hasMinimumConfig) {
    return false;
  }
  return !normalizeBoolean(env.SLURM_CLIENT_E2E_REQUIRED);
}

async function prepareClientFixtureFromEnv(env = process.env) {
  const privateKey = await resolvePrivateKey(env);
  if (shouldSkip(env, privateKey)) {
    log('Skipping external Slurm client fixture; set SLURM_CLIENT_SSH_HOST, SLURM_CLIENT_SSH_USER, and a private key to enable it.');
    return { skipped: true };
  }
  const missing = [];
  if (!env.SLURM_CLIENT_SSH_HOST) missing.push('SLURM_CLIENT_SSH_HOST');
  if (!env.SLURM_CLIENT_SSH_USER) missing.push('SLURM_CLIENT_SSH_USER');
  if (!privateKey) missing.push('SLURM_CLIENT_SSH_PRIVATE_KEY or SLURM_CLIENT_SSH_PRIVATE_KEY_B64 or SLURM_CLIENT_SSH_PRIVATE_KEY_PATH');
  if (missing.length > 0) {
    throw new Error(`Missing required external Slurm client fixture settings: ${missing.join(', ')}`);
  }

  const stateDir = env.SLURM_CLIENT_FIXTURE_STATE_DIR || DEFAULT_STATE_DIR;
  const sshDir = path.join(stateDir, 'ssh');
  const privateKeyPath = path.join(sshDir, 'id_ed25519');
  const knownHostsPath = path.join(sshDir, 'known_hosts');
  const sshConfigPath = path.join(sshDir, 'ssh_config');
  const host = env.SLURM_CLIENT_SSH_HOST;
  const port = env.SLURM_CLIENT_SSH_PORT || '22';
  const user = env.SLURM_CLIENT_SSH_USER;

  await fs.mkdir(sshDir, { recursive: true });
  await writeSensitiveFile(privateKeyPath, privateKey);
  let knownHosts = await resolveKnownHosts(env);
  if (!knownHosts) {
    knownHosts = await maybeCollectKnownHost(host, port);
  }
  await writeSensitiveFile(knownHostsPath, knownHosts || '# populated by StrictHostKeyChecking accept-new\n');
  await writeSshConfig({ sshConfigPath, privateKeyPath, knownHostsPath, host, port, user });

  const home = env.SLURM_CLIENT_HOME || await queryRemoteHome(sshConfigPath);
  const sinfoLine = await verifySlurmAccess(sshConfigPath);
  log(`External Slurm client fixture ready: ${user}@${host}:${port} (${sinfoLine})`);

  return {
    skipped: false,
    env: {
      SLURM_CONNECT_CLIENT_FIXTURE: '1',
      SLURM_FIXTURE_STATE_DIR: stateDir,
      SLURM_FIXTURE_SSH_HOST: host,
      SLURM_FIXTURE_SSH_PORT: port,
      SLURM_FIXTURE_USER: user,
      SLURM_FIXTURE_HOME: home,
      SLURM_CLIENT_DEFAULT_PARTITION: env.SLURM_CLIENT_DEFAULT_PARTITION || '',
      SLURM_CLIENT_EXPECTED_SINFO_PATTERN: env.SLURM_CLIENT_EXPECTED_SINFO_PATTERN || '',
      SLURM_CLIENT_EXPECTED_COMPUTE_HOST_PATTERN: env.SLURM_CLIENT_EXPECTED_COMPUTE_HOST_PATTERN || '',
      SLURM_CLIENT_EXTRA_SALLOC_ARGS_JSON: env.SLURM_CLIENT_EXTRA_SALLOC_ARGS_JSON || '',
      SLURM_CLIENT_REMOTE_WORKSPACE_PATH: env.SLURM_CLIENT_REMOTE_WORKSPACE_PATH || '',
      SLURM_CLIENT_STARTUP_COMMAND: env.SLURM_CLIENT_STARTUP_COMMAND || '',
      SLURM_CLIENT_MODULE_LOAD: env.SLURM_CLIENT_MODULE_LOAD || '',
      SLURM_CLIENT_PROXY_COMMAND: env.SLURM_CLIENT_PROXY_COMMAND || ''
    },
    stateDir,
    sshConfigPath,
    privateKeyPath,
    knownHostsPath,
    home,
    sinfoLine
  };
}

async function main() {
  const result = await prepareClientFixtureFromEnv();
  if (result.skipped) {
    return;
  }
  log(`SSH config: ${result.sshConfigPath}`);
}

if (require.main === module) {
  main().catch((error) => {
    process.stderr.write(`[slurm-client] ERROR: ${error instanceof Error ? error.stack || error.message : String(error)}\n`);
    process.exit(1);
  });
}

module.exports = {
  prepareClientFixtureFromEnv
};
