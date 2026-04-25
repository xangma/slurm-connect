#!/usr/bin/env node

const cp = require('child_process');
const fs = require('fs/promises');
const os = require('os');
const path = require('path');

const { downloadAndUnzipVSCode } = require('@vscode/test-electron');
const {
  resolveCliArgsFromVSCodeExecutablePath,
  systemDefaultPlatform,
  killTree
} = require('@vscode/test-electron/out/util');

const ROOT_DIR = path.resolve(__dirname, '../..');
const FIXTURE_SCRIPT = path.join(ROOT_DIR, 'scripts/e2e/slurm-fixture.sh');
const TEST_WORKSPACE_TEMPLATE = path.join(ROOT_DIR, 'test-fixtures/workspace');
const STATE_DIR = process.env.SLURM_REMOTE_SESSION_STATE_DIR || '/tmp/slurm-connect-rsess';
const FIXTURE_STATE_DIR = process.env.SLURM_FIXTURE_STATE_DIR || path.join(ROOT_DIR, '.e2e/slurm-fixture');
const REMOTE_SSH_EXTENSION_ID = process.env.SLURM_REMOTE_SSH_EXTENSION_ID || 'ms-vscode-remote.remote-ssh';
const SSH_HOST_PREFIX = process.env.SLURM_REMOTE_SESSION_HOST_PREFIX || 'slurm-session-e2e';
const REMOTE_SESSION_TIMEOUT_MS = Number(process.env.SLURM_REMOTE_SESSION_TIMEOUT_MS || 180000);

function log(message) {
  process.stdout.write(`[remote-session] ${message}\n`);
}

function die(message) {
  process.stderr.write(`[remote-session] ERROR: ${message}\n`);
  process.exit(1);
}

async function ensureDir(dirPath) {
  await fs.mkdir(dirPath, { recursive: true });
}

async function readJson(filePath) {
  const text = await fs.readFile(filePath, 'utf8');
  return JSON.parse(text);
}

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function resolveFixtureConfig() {
  const sshDir = path.join(FIXTURE_STATE_DIR, 'ssh');
  const user = process.env.SLURM_FIXTURE_USER || 'slurmconnect';
  const config = {
    host: process.env.SLURM_FIXTURE_SSH_HOST || '127.0.0.1',
    port: process.env.SLURM_FIXTURE_SSH_PORT || '3022',
    user,
    home: process.env.SLURM_FIXTURE_HOME || `/data/home/${user}`,
    sshConfigPath: path.join(sshDir, 'ssh_config'),
    privateKeyPath: path.join(sshDir, 'id_ed25519'),
    knownHostsPath: path.join(sshDir, 'known_hosts')
  };

  for (const filePath of [config.sshConfigPath, config.privateKeyPath, config.knownHostsPath]) {
    if (!(await fileExists(filePath))) {
      die(`Fixture file not found: ${filePath}. Run npm run e2e:slurm:up first.`);
    }
  }

  return config;
}

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = cp.spawn(command, args, {
      cwd: options.cwd || ROOT_DIR,
      env: options.env || process.env,
      stdio: options.stdio || 'inherit',
      shell: false
    });
    let stdout = '';
    let stderr = '';
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
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
      } else {
        const error = new Error(`${command} ${args.join(' ')} exited with code ${code}`);
        error.stdout = stdout;
        error.stderr = stderr;
        reject(error);
      }
    });
  });
}

async function ensureFixtureUp() {
  log('Ensuring the Slurm fixture is running');
  await runCommand('bash', [FIXTURE_SCRIPT, 'up']);
}

async function installProxyScript() {
  log('Installing the local proxy package into the fixture');
  await runCommand('bash', [FIXTURE_SCRIPT, 'install-proxy']);
}

async function installRemoteSshExtension(vscodeExecutablePath, userDataDir, extensionsDir) {
  const cliArgs = resolveCliArgsFromVSCodeExecutablePath(vscodeExecutablePath, {
    platform: systemDefaultPlatform,
    reuseMachineInstall: true
  });
  const [cli, ...baseArgs] = cliArgs;
  log(`Installing ${REMOTE_SSH_EXTENSION_ID} into isolated extensions dir`);
  await runCommand(
    cli,
    [
      ...baseArgs,
      `--user-data-dir=${userDataDir}`,
      `--extensions-dir=${extensionsDir}`,
      '--install-extension',
      REMOTE_SSH_EXTENSION_ID,
      '--force'
    ],
    {
      env: {
        ...process.env,
        NODE_TLS_REJECT_UNAUTHORIZED: process.env.NODE_TLS_REJECT_UNAUTHORIZED || '0'
      }
    }
  );
}

async function prepareWorkspace(sessionDir) {
  const workspaceDir = path.join(sessionDir, 'workspace');
  await fs.cp(TEST_WORKSPACE_TEMPLATE, workspaceDir, { recursive: true });
  return workspaceDir;
}

async function writeUserSettings(userDataDir, baseConfigPath) {
  const settingsPath = path.join(userDataDir, 'User', 'settings.json');
  await ensureDir(path.dirname(settingsPath));
  const settings = {
    'extensions.supportNodeGlobalNavigator': true,
    'remote.SSH.configFile': baseConfigPath,
    'remote.SSH.useLocalServer': true,
    'remote.SSH.useExecServer': false,
    'remote.SSH.enableRemoteCommand': true,
    'remote.SSH.lockfilesInTmp': true,
    'remote.SSH.connectTimeout': 300
  };
  await fs.writeFile(settingsPath, JSON.stringify(settings, null, 2), 'utf8');
}

function escapeRegex(input) {
  return input.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function waitForRemoteMarker(remoteMarkerPath, statusMarkerPath, child) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < REMOTE_SESSION_TIMEOUT_MS) {
    if (child.exitCode !== null) {
      throw new Error(`VS Code exited before the remote session marker appeared (exit code ${child.exitCode}).`);
    }

    if (await fileExists(remoteMarkerPath)) {
      const marker = await readJson(remoteMarkerPath);
      if (marker.error) {
        throw new Error(`Remote marker reported an error: ${marker.error}`);
      }
      if (marker.expectedAlias && marker.alias !== marker.expectedAlias) {
        throw new Error(`Remote alias mismatch: expected ${marker.expectedAlias}, got ${marker.alias}`);
      }
      return marker;
    }

    if (await fileExists(statusMarkerPath)) {
      const status = await readJson(statusMarkerPath);
      if (
        status.phase === 'local-command-error' ||
        status.phase === 'local-command-failed' ||
        status.phase === 'remote-marker-error' ||
        status.phase === 'local-command-missing'
      ) {
        throw new Error(`Session helper failed during phase ${status.phase}: ${JSON.stringify(status)}`);
      }
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  throw new Error(`Timed out after ${REMOTE_SESSION_TIMEOUT_MS}ms waiting for the remote session marker.`);
}

async function verifyRemoteFsMarker(fixture, remoteFsMarkerPath) {
  const remoteCommand = `bash -lc 'test -f "${remoteFsMarkerPath}" && cat "${remoteFsMarkerPath}"'`;
  const { stdout } = await runCommand(
    'ssh',
    ['-F', fixture.sshConfigPath, '-T', fixture.host, remoteCommand],
    {
      env: {
        ...process.env
      },
      stdio: 'pipe'
    }
  );
  const marker = JSON.parse(stdout.trim());
  if (marker.remoteName !== 'ssh-remote') {
    throw new Error(`Unexpected remote filesystem marker payload: ${stdout}`);
  }
  return marker;
}

function assertSlurmAllocationMarker(marker, label) {
  if (!/^c[0-9]+$/.test(String(marker.hostname || ''))) {
    throw new Error(`${label} did not report a compute-node hostname: ${JSON.stringify(marker)}`);
  }
  if (!String(marker.slurmJobId || '').trim()) {
    throw new Error(`${label} did not report a Slurm job id: ${JSON.stringify(marker)}`);
  }
}

async function main() {
  await ensureDir(STATE_DIR);
  await ensureFixtureUp();
  await installProxyScript();
  const fixture = await resolveFixtureConfig();

  const sessionDir = await fs.mkdtemp(path.join(STATE_DIR, 'run-'));
  const userDataDir = path.join(sessionDir, 'user-data');
  const extensionsDir = path.join(sessionDir, 'extensions');
  const workspaceDir = await prepareWorkspace(sessionDir);
  const baseConfigPath = path.join(sessionDir, 'ssh-config');
  const includeConfigPath = path.join(sessionDir, 'slurm-connect.conf');
  const statusMarkerPath = path.join(sessionDir, 'status.json');
  const remoteMarkerPath = path.join(sessionDir, 'remote.json');
  const remoteFsMarkerPath = `${fixture.home}/.slurm-connect/vscode-remote-session-marker.json`;
  const remoteAlias = `${SSH_HOST_PREFIX}-${fixture.host.split('.')[0]}-cpu-1n-1c`.replace(/[^a-zA-Z0-9_-]/g, '-');
  const stdoutLogPath = path.join(sessionDir, 'vscode.stdout.log');
  const stderrLogPath = path.join(sessionDir, 'vscode.stderr.log');

  await ensureDir(userDataDir);
  await ensureDir(extensionsDir);
  await fs.writeFile(baseConfigPath, '# Remote-SSH session E2E base config\n', 'utf8');
  await writeUserSettings(userDataDir, baseConfigPath);

  const vscodeExecutablePath = await downloadAndUnzipVSCode({
    extensionDevelopmentPath: ROOT_DIR
  });
  await installRemoteSshExtension(vscodeExecutablePath, userDataDir, extensionsDir);

  log('Launching a real VS Code instance for the Remote-SSH session test');
  const launchEnv = {
    ...process.env,
    SLURM_CONNECT_SESSION_E2E_MODE: 'noninteractive',
    SLURM_CONNECT_SESSION_E2E_STATUS_MARKER: statusMarkerPath,
    SLURM_CONNECT_SESSION_E2E_REMOTE_MARKER: remoteMarkerPath,
    SLURM_CONNECT_SESSION_E2E_REMOTE_FS_MARKER: remoteFsMarkerPath,
    SLURM_CONNECT_SESSION_E2E_EXPECTED_ALIAS: remoteAlias,
    SLURM_CONNECT_SESSION_E2E_CONNECT_OVERRIDES: JSON.stringify({
      loginHosts: [fixture.host],
      user: fixture.user,
      identityFile: '',
      additionalSshOptions: {
        IdentityFile: fixture.privateKeyPath,
        IdentitiesOnly: 'yes',
        Port: fixture.port,
        UserKnownHostsFile: fixture.knownHostsPath
      },
      sshQueryConfigPath: fixture.sshConfigPath,
      temporarySshConfigPath: includeConfigPath,
      defaultPartition: 'cpu',
      defaultTime: '',
      defaultNodes: 1,
      defaultTasksPerNode: 1,
      defaultCpusPerTask: 1,
      sshHostPrefix: SSH_HOST_PREFIX,
      localProxyEnabled: false,
      sessionMode: 'ephemeral',
      openInNewWindow: true,
      remoteWorkspacePath: fixture.home,
      proxyCommand: 'python3 ~/.slurm-connect/vscode-proxy.py',
      proxyDebugLogging: true
    }),
    NODE_TLS_REJECT_UNAUTHORIZED: process.env.NODE_TLS_REJECT_UNAUTHORIZED || '0'
  };
  delete launchEnv.ELECTRON_RUN_AS_NODE;
  const child = cp.spawn(
    vscodeExecutablePath,
    [
      workspaceDir,
      '--new-window',
      '--no-sandbox',
      '--disable-gpu-sandbox',
      '--disable-updates',
      '--skip-welcome',
      '--skip-release-notes',
      '--disable-workspace-trust',
      `--user-data-dir=${userDataDir}`,
      `--extensions-dir=${extensionsDir}`,
      `--extensionDevelopmentPath=${ROOT_DIR}`,
      '--log',
      'info'
    ],
    {
      cwd: ROOT_DIR,
      env: launchEnv,
      stdio: ['ignore', 'pipe', 'pipe']
    }
  );

  const stdoutStream = [];
  const stderrStream = [];
  child.stdout.on('data', (chunk) => {
    const text = chunk.toString('utf8');
    stdoutStream.push(text);
    process.stdout.write(text);
  });
  child.stderr.on('data', (chunk) => {
    const text = chunk.toString('utf8');
    stderrStream.push(text);
    process.stderr.write(text);
  });

  try {
    const marker = await waitForRemoteMarker(remoteMarkerPath, statusMarkerPath, child);
    assertSlurmAllocationMarker(marker, 'Remote marker');
    log(`Remote window reported authority ${marker.authority}`);

    const remoteFsMarker = await verifyRemoteFsMarker(fixture, remoteFsMarkerPath);
    assertSlurmAllocationMarker(remoteFsMarker, 'Remote filesystem marker');
    log(`Remote filesystem marker confirmed for authority ${remoteFsMarker.authority}`);

    const includeContent = await fs.readFile(includeConfigPath, 'utf8');
    if (!new RegExp(`^Host ${escapeRegex(remoteAlias)}$`, 'm').test(includeContent)) {
      throw new Error(`Generated include file did not contain alias ${remoteAlias}`);
    }
    if (!/^\s*RemoteCommand .*vscode-proxy\.py/m.test(includeContent)) {
      throw new Error('Generated include file did not contain the expected proxy RemoteCommand.');
    }

    log(`Session artifacts: ${sessionDir}`);
  } finally {
    await fs.writeFile(stdoutLogPath, stdoutStream.join(''), 'utf8').catch(() => undefined);
    await fs.writeFile(stderrLogPath, stderrStream.join(''), 'utf8').catch(() => undefined);
    if (child.pid) {
      await killTree(child.pid, true).catch(() => undefined);
    }
  }
}

main().catch((error) => {
  die(error instanceof Error ? error.stack || error.message : String(error));
});
