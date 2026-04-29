#!/usr/bin/env node

const cp = require('child_process');
const fs = require('fs/promises');
const path = require('path');

const ROOT_DIR = path.resolve(__dirname, '../..');
const DEFAULT_REPO = 'xangma/slurm-connect';

const secretEnvNames = [
  'SLURM_CLIENT_SSH_HOST',
  'SLURM_CLIENT_SSH_PORT',
  'SLURM_CLIENT_SSH_USER'
];

const optionalVariableNames = [
  'SLURM_CLIENT_HOME',
  'SLURM_CLIENT_DEFAULT_PARTITION',
  'SLURM_CLIENT_EXPECTED_SINFO_PATTERN',
  'SLURM_CLIENT_EXPECTED_COMPUTE_HOST_PATTERN',
  'SLURM_CLIENT_EXTRA_SALLOC_ARGS_JSON',
  'SLURM_CLIENT_REMOTE_WORKSPACE_PATH',
  'SLURM_CLIENT_STARTUP_COMMAND',
  'SLURM_CLIENT_MODULE_LOAD',
  'SLURM_CLIENT_PROXY_COMMAND',
  'SLURM_CLIENT_LOCAL_PROXY_REMOTE_HOST',
  'SLURM_CLIENT_LOCAL_PROXY_TARGET_HOST',
  'SLURM_CLIENT_LOCAL_PROXY_COMPUTE_TUNNEL'
];

function log(message) {
  process.stdout.write(`[github-slurm-client] ${message}\n`);
}

function usage() {
  process.stdout.write(`Usage: node scripts/e2e/configure-github-slurm-client.js [options]

Options:
  --repo OWNER/REPO       Repository to configure. Defaults to ${DEFAULT_REPO}.
  --env-file PATH         Load SLURM_CLIENT_* values from a dotenv-style file.
  --verify                Run the local external Slurm fixture preparation before writing GitHub settings.
  --dry-run               Print the settings that would be written, without writing them.
  --help                  Show this help.

Required input values:
  SLURM_CLIENT_SSH_HOST
  SLURM_CLIENT_SSH_USER
  one of SLURM_CLIENT_SSH_PRIVATE_KEY_B64, SLURM_CLIENT_SSH_PRIVATE_KEY, or SLURM_CLIENT_SSH_PRIVATE_KEY_PATH

The script stores the private key as the GitHub secret SLURM_CLIENT_SSH_PRIVATE_KEY_B64.
`);
}

function parseArgs(argv) {
  const options = {
    repo: process.env.GITHUB_REPOSITORY || DEFAULT_REPO,
    envFile: '',
    verify: false,
    dryRun: false
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg === '--verify') {
      options.verify = true;
      continue;
    }
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }
    if (arg === '--repo') {
      options.repo = argv[index + 1] || '';
      index += 1;
      continue;
    }
    if (arg.startsWith('--repo=')) {
      options.repo = arg.slice('--repo='.length);
      continue;
    }
    if (arg === '--env-file') {
      options.envFile = argv[index + 1] || '';
      index += 1;
      continue;
    }
    if (arg.startsWith('--env-file=')) {
      options.envFile = arg.slice('--env-file='.length);
      continue;
    }
    throw new Error(`Unknown argument: ${arg}`);
  }
  if (!/^[^/\s]+\/[^/\s]+$/.test(options.repo)) {
    throw new Error(`Invalid repository ${options.repo || '(empty)'}. Expected OWNER/REPO.`);
  }
  return options;
}

function unquoteDotenvValue(value) {
  const trimmed = value.trim();
  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    return trimmed.slice(1, -1)
      .replace(/\\n/g, '\n')
      .replace(/\\r/g, '\r')
      .replace(/\\"/g, '"')
      .replace(/\\\\/g, '\\');
  }
  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    return trimmed.slice(1, -1);
  }
  return trimmed.replace(/\s+#.*$/u, '');
}

async function readEnvFile(filePath) {
  if (!filePath) {
    return {};
  }
  const resolved = path.resolve(ROOT_DIR, filePath);
  const content = await fs.readFile(resolved, 'utf8');
  const env = {};
  for (const line of content.split(/\r?\n/u)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }
    const match = /^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(trimmed);
    if (!match) {
      throw new Error(`Unsupported dotenv line in ${filePath}: ${line}`);
    }
    env[match[1]] = unquoteDotenvValue(match[2]);
  }
  return env;
}

function normalizeSecretText(value) {
  return String(value).replace(/\r\n/g, '\n').trimEnd() + '\n';
}

async function readOptionalFile(filePath) {
  if (!filePath) {
    return '';
  }
  return await fs.readFile(path.resolve(ROOT_DIR, filePath), 'utf8');
}

async function resolvePrivateKeyB64(env) {
  if (env.SLURM_CLIENT_SSH_PRIVATE_KEY_B64) {
    return String(env.SLURM_CLIENT_SSH_PRIVATE_KEY_B64).trim();
  }
  const privateKey = env.SLURM_CLIENT_SSH_PRIVATE_KEY_PATH
    ? await readOptionalFile(env.SLURM_CLIENT_SSH_PRIVATE_KEY_PATH)
    : env.SLURM_CLIENT_SSH_PRIVATE_KEY || '';
  return privateKey ? Buffer.from(normalizeSecretText(privateKey), 'utf8').toString('base64') : '';
}

async function resolveKnownHostsB64(env) {
  if (env.SLURM_CLIENT_KNOWN_HOSTS_B64) {
    return String(env.SLURM_CLIENT_KNOWN_HOSTS_B64).trim();
  }
  const knownHosts = env.SLURM_CLIENT_KNOWN_HOSTS_PATH
    ? await readOptionalFile(env.SLURM_CLIENT_KNOWN_HOSTS_PATH)
    : env.SLURM_CLIENT_KNOWN_HOSTS || '';
  return knownHosts ? Buffer.from(normalizeSecretText(knownHosts), 'utf8').toString('base64') : '';
}

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = cp.spawn(command, args, {
      cwd: options.cwd || ROOT_DIR,
      env: options.env || process.env,
      stdio: ['pipe', 'pipe', 'pipe'],
      shell: false
    });
    let stdout = '';
    let stderr = '';
    if (options.input !== undefined) {
      child.stdin.end(options.input, 'utf8');
    } else {
      child.stdin.end();
    }
    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString('utf8');
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
        return;
      }
      const error = new Error(`${command} ${args.join(' ')} exited with code ${code || 1}: ${stderr.trim()}`);
      error.stdout = stdout;
      error.stderr = stderr;
      reject(error);
    });
  });
}

async function findGh() {
  try {
    const { stdout } = await runCommand('sh', ['-lc', 'command -v gh || true']);
    const fromPath = stdout.trim();
    if (fromPath) {
      return fromPath;
    }
  } catch {
    // Fall back to common Homebrew locations.
  }
  for (const candidate of ['/opt/homebrew/bin/gh', '/usr/local/bin/gh']) {
    try {
      await fs.access(candidate);
      return candidate;
    } catch {
      // Try the next location.
    }
  }
  throw new Error('GitHub CLI not found. Install gh or set PATH to include it.');
}

async function assertRepoAdmin(ghPath, repo) {
  const { stdout } = await runCommand(ghPath, [
    'repo',
    'view',
    repo,
    '--json',
    'viewerPermission'
  ]);
  const payload = JSON.parse(stdout);
  if (payload.viewerPermission !== 'ADMIN') {
    throw new Error(`Active GitHub account has ${payload.viewerPermission || 'unknown'} permission on ${repo}; ADMIN is required.`);
  }
}

async function runLocalVerification(env) {
  log('Verifying external Slurm fixture locally before writing GitHub settings.');
  await runCommand(process.execPath, ['scripts/e2e/prepare-slurm-client-fixture.js'], {
    env: {
      ...process.env,
      ...env,
      SLURM_CLIENT_E2E_REQUIRED: '1'
    }
  });
}

async function setSecret(ghPath, repo, name, value, dryRun) {
  if (dryRun) {
    log(`Would set secret ${name}`);
    return;
  }
  await runCommand(ghPath, ['secret', 'set', name, '--repo', repo], { input: value });
  log(`Set secret ${name}`);
}

async function setVariable(ghPath, repo, name, value, dryRun) {
  if (dryRun) {
    log(`Would set variable ${name}=${name === 'SLURM_CLIENT_E2E_REQUIRED' ? value : '<configured>'}`);
    return;
  }
  await runCommand(ghPath, ['variable', 'set', name, '--repo', repo], { input: String(value) });
  log(`Set variable ${name}`);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    usage();
    return;
  }

  const fileEnv = await readEnvFile(options.envFile);
  const env = { ...fileEnv, ...process.env };
  const privateKeyB64 = await resolvePrivateKeyB64(env);
  const knownHostsB64 = await resolveKnownHostsB64(env);

  const missing = [];
  if (!env.SLURM_CLIENT_SSH_HOST) missing.push('SLURM_CLIENT_SSH_HOST');
  if (!env.SLURM_CLIENT_SSH_USER) missing.push('SLURM_CLIENT_SSH_USER');
  if (!privateKeyB64) {
    missing.push('SLURM_CLIENT_SSH_PRIVATE_KEY_B64 or SLURM_CLIENT_SSH_PRIVATE_KEY or SLURM_CLIENT_SSH_PRIVATE_KEY_PATH');
  }
  if (missing.length > 0) {
    throw new Error(`Missing required settings: ${missing.join(', ')}`);
  }

  const ghPath = options.dryRun ? '' : await findGh();
  if (!options.dryRun) {
    await assertRepoAdmin(ghPath, options.repo);
  }
  if (options.verify) {
    await runLocalVerification(env);
  }

  const secrets = {
    SLURM_CLIENT_SSH_HOST: String(env.SLURM_CLIENT_SSH_HOST).trim(),
    SLURM_CLIENT_SSH_USER: String(env.SLURM_CLIENT_SSH_USER).trim(),
    SLURM_CLIENT_SSH_PRIVATE_KEY_B64: privateKeyB64
  };
  if (env.SLURM_CLIENT_SSH_PORT) {
    secrets.SLURM_CLIENT_SSH_PORT = String(env.SLURM_CLIENT_SSH_PORT).trim();
  }
  if (knownHostsB64) {
    secrets.SLURM_CLIENT_KNOWN_HOSTS_B64 = knownHostsB64;
  }

  const variables = {
    SLURM_CLIENT_E2E_REQUIRED: env.SLURM_CLIENT_E2E_REQUIRED || '1'
  };
  for (const name of optionalVariableNames) {
    if (env[name] !== undefined && String(env[name]).length > 0) {
      variables[name] = env[name];
    }
  }

  log(`Configuring ${options.repo} for the Slurm client OS matrix.`);
  for (const [name, value] of Object.entries(secrets)) {
    if (secretEnvNames.includes(name) && !value) {
      continue;
    }
    await setSecret(ghPath, options.repo, name, value, options.dryRun);
  }
  for (const [name, value] of Object.entries(variables)) {
    await setVariable(ghPath, options.repo, name, value, options.dryRun);
  }
  log(options.dryRun ? 'Dry run complete.' : 'GitHub Slurm client matrix settings are configured.');
}

main().catch((error) => {
  process.stderr.write(`[github-slurm-client] ERROR: ${error instanceof Error ? error.message : String(error)}\n`);
  process.exit(1);
});
