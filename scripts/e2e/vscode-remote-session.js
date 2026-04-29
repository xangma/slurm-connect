#!/usr/bin/env node

const cp = require('child_process');
const crypto = require('crypto');
const fs = require('fs/promises');
const http = require('http');
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
const DEFAULT_STATE_DIR =
  process.platform === 'darwin'
    ? '/tmp/sc-rsess'
    : path.join(os.tmpdir(), 'slurm-connect-rsess');
const STATE_DIR = process.env.SLURM_REMOTE_SESSION_STATE_DIR || DEFAULT_STATE_DIR;
const FIXTURE_STATE_DIR = process.env.SLURM_FIXTURE_STATE_DIR || path.join(ROOT_DIR, '.e2e/slurm-fixture');
const REMOTE_SSH_EXTENSION_ID = process.env.SLURM_REMOTE_SSH_EXTENSION_ID || 'ms-vscode-remote.remote-ssh';
const SSH_HOST_PREFIX = process.env.SLURM_REMOTE_SESSION_HOST_PREFIX || 'slurm-session-e2e';
const REMOTE_SESSION_TIMEOUT_MS = Number(process.env.SLURM_REMOTE_SESSION_TIMEOUT_MS || 180000);
const WINDOWS_GIT_SSH = 'C:\\Program Files\\Git\\usr\\bin\\ssh.exe';
const WINDOWS_GIT_SCP = 'C:\\Program Files\\Git\\usr\\bin\\scp.exe';
const isExternalFixture =
  process.env.SLURM_REMOTE_SESSION_EXTERNAL_FIXTURE === '1' ||
  process.env.SLURM_CONNECT_CLIENT_FIXTURE === '1';
const enableLocalProxyE2E = process.env.SLURM_REMOTE_SESSION_ENABLE_LOCAL_PROXY_E2E === '1';
const skipRemoteFsMarker = process.env.SLURM_REMOTE_SESSION_SKIP_REMOTE_FS_MARKER === '1';

function log(message) {
  process.stdout.write(`[remote-session] ${message}\n`);
}

function die(message) {
  process.stderr.write(`[remote-session] ERROR: ${message}\n`);
  process.exit(1);
}

function getSshCommand() {
  return process.platform === 'win32' ? WINDOWS_GIT_SSH : 'ssh';
}

function getScpCommand() {
  return process.platform === 'win32' ? WINDOWS_GIT_SCP : 'scp';
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
      die(
        isExternalFixture
          ? `External fixture file not found: ${filePath}. Run npm run e2e:slurm:client:prepare first.`
          : `Fixture file not found: ${filePath}. Run npm run e2e:slurm:up first.`
      );
    }
  }

  return config;
}

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let timeout;
    const stdio = options.stdio === 'pipe'
      ? (options.input === undefined ? ['ignore', 'pipe', 'pipe'] : ['pipe', 'pipe', 'pipe'])
      : options.stdio || 'inherit';
    const settle = (callback, value) => {
      if (settled) {
        return;
      }
      settled = true;
      if (timeout) {
        clearTimeout(timeout);
      }
      callback(value);
    };
    const child = cp.spawn(command, args, {
      cwd: options.cwd || ROOT_DIR,
      env: options.env || process.env,
      stdio,
      shell: process.platform === 'win32' && /\.(?:cmd|bat)$/i.test(command)
    });
    let stdout = '';
    let stderr = '';
    const maybeResolveFromStdout = () => {
      if (process.platform !== 'win32') {
        return;
      }
      if (options.resolveOnStdoutIncludes && stdout.includes(options.resolveOnStdoutIncludes)) {
        settle(resolve, { stdout, stderr });
        child.kill();
      }
    };
    if (child.stdout) {
      child.stdout.on('data', (chunk) => {
        stdout += chunk.toString('utf8');
        maybeResolveFromStdout();
      });
    }
    if (child.stderr) {
      child.stderr.on('data', (chunk) => {
        stderr += chunk.toString('utf8');
      });
    }
    if (options.input !== undefined && child.stdin) {
      child.stdin.end(options.input, 'utf8');
    }
    if (options.timeoutMs) {
      timeout = setTimeout(() => {
        child.kill();
        const error = new Error(`${command} ${args.join(' ')} timed out after ${options.timeoutMs}ms`);
        error.stdout = stdout;
        error.stderr = stderr;
        settle(reject, error);
      }, options.timeoutMs);
    }
    child.on('error', (error) => settle(reject, error));
    child.on('close', (code) => {
      if (code === 0) {
        settle(resolve, { stdout, stderr });
      } else {
        const error = new Error(`${command} ${args.join(' ')} exited with code ${code}`);
        error.stdout = stdout;
        error.stderr = stderr;
        settle(reject, error);
      }
    });
  });
}

async function ensureFixtureUp() {
  log('Ensuring the Slurm fixture is running');
  await runCommand('bash', [FIXTURE_SCRIPT, 'up']);
}

async function installDockerProxyScript() {
  log('Installing the local proxy package into the fixture');
  await runCommand('bash', [FIXTURE_SCRIPT, 'install-proxy']);
}

function shellQuote(value) {
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

async function installExternalProxyScript(fixture) {
  log('Installing the local proxy package into the external Slurm login node');
  await runCommand(getSshCommand(), ['-F', fixture.sshConfigPath, '-T', fixture.host, 'mkdir -p ~/.slurm-connect']);
  await runCommand(getScpCommand(), [
    '-F',
    fixture.sshConfigPath,
    'media/vscode-proxy.py',
    `${fixture.host}:~/.slurm-connect/vscode-proxy.py`
  ]);
  await runCommand(getScpCommand(), [
    '-r',
    '-F',
    fixture.sshConfigPath,
    'media/vscode_proxy',
    `${fixture.host}:~/.slurm-connect/`
  ]);
  await runCommand(getSshCommand(), ['-F', fixture.sshConfigPath, '-T', fixture.host, 'chmod 700 ~/.slurm-connect/vscode-proxy.py']);
}

async function installProxyPackage(fixture) {
  if (isExternalFixture) {
    await installExternalProxyScript(fixture);
    return;
  }
  await installDockerProxyScript();
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
  if (process.platform === 'win32') {
    settings['remote.SSH.path'] = WINDOWS_GIT_SSH;
  }
  await fs.writeFile(settingsPath, JSON.stringify(settings, null, 2), 'utf8');
}

function escapeRegex(input) {
  return input.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function buildDefaultAlias(prefix, loginHost, partition, nodes, cpusPerTask) {
  const hostShort = loginHost.split('.')[0];
  const pieces = [prefix || 'slurm', hostShort];
  if (partition) {
    pieces.push(partition);
  }
  if (nodes && nodes > 0) {
    pieces.push(`${nodes}n`);
  }
  if (cpusPerTask && cpusPerTask > 0) {
    pieces.push(`${cpusPerTask}c`);
  }
  return pieces.join('-').replace(/[^a-zA-Z0-9_-]/g, '-');
}

function sanitizeRemotePathComponent(value) {
  return String(value).replace(/[^a-zA-Z0-9_.-]/g, '-');
}

function toMsysPath(filePath) {
  const normalized = path.resolve(filePath);
  const match = /^([A-Za-z]):[\\/](.*)$/.exec(normalized);
  if (!match) {
    return normalized.split(path.sep).join('/');
  }
  return `/${match[1].toLowerCase()}/${match[2].replace(/\\/g, '/')}`;
}

function parseJsonStringArray(value, label) {
  if (!value) {
    return [];
  }
  const parsed = JSON.parse(value);
  if (!Array.isArray(parsed) || parsed.some((entry) => typeof entry !== 'string')) {
    throw new Error(`${label} must be a JSON array of strings.`);
  }
  return parsed;
}

function getIpv4Candidates() {
  const configured = String(process.env.SLURM_CLIENT_LOCAL_PROXY_TARGET_HOST || '').trim();
  if (configured) {
    return [configured];
  }
  const candidates = [];
  for (const entries of Object.values(os.networkInterfaces())) {
    for (const entry of entries || []) {
      if (entry.family === 'IPv4' && !entry.internal && entry.address) {
        candidates.push(entry.address);
      }
    }
  }
  return [...new Set([...candidates, '127-0-0-1.sslip.io'])];
}

function requestLocalUrl(url, timeoutMs = 1500) {
  return new Promise((resolve, reject) => {
    const request = http.get(url, { timeout: timeoutMs }, (response) => {
      response.resume();
      response.on('end', () => {
        resolve(response.statusCode || 0);
      });
    });
    request.on('timeout', () => {
      request.destroy(new Error(`Timed out probing ${url}`));
    });
    request.on('error', reject);
  });
}

async function startLocalProxyProbeServer(sessionDir) {
  if (!enableLocalProxyE2E) {
    return undefined;
  }
  const token = crypto.randomBytes(16).toString('hex');
  const probePath = `/slurm-connect-local-proxy-e2e/${token}`;
  const requests = [];
  const server = http.createServer((request, response) => {
    requests.push({
      method: request.method || '',
      url: request.url || '',
      headers: request.headers,
      remoteAddress: request.socket.remoteAddress || '',
      at: new Date().toISOString()
    });
    if (request.url === `/health/${token}`) {
      response.writeHead(204);
      response.end();
      return;
    }
    if (request.url === probePath) {
      response.writeHead(200, { 'Content-Type': 'text/plain' });
      response.end(`slurm-connect-local-proxy-e2e:${token}`);
      return;
    }
    response.writeHead(404, { 'Content-Type': 'text/plain' });
    response.end('not found');
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '0.0.0.0', () => {
      server.removeListener('error', reject);
      resolve();
    });
  });
  const port = server.address().port;
  const candidates = getIpv4Candidates();
  let selectedHost = '';
  for (const candidate of candidates) {
    try {
      const statusCode = await requestLocalUrl(`http://${candidate}:${port}/health/${token}`);
      if (statusCode === 204) {
        selectedHost = candidate;
        break;
      }
    } catch {
      // Try the next interface address.
    }
  }
  if (!selectedHost) {
    await new Promise((resolve) => server.close(resolve));
    throw new Error(
      `Local proxy E2E could not find a reachable non-loopback client IPv4 address. Candidates: ${candidates.join(', ') || '(none)'}`
    );
  }
  requests.length = 0;
  const targetUrl = `http://${selectedHost}:${port}${probePath}`;
  const requestsPath = path.join(sessionDir, 'local-proxy-requests.json');
  log(`Local proxy E2E target listening at ${targetUrl}`);
  return {
    targetUrl,
    token,
    requestsPath,
    getRequests: () => requests.slice(),
    close: async () => {
      await fs.writeFile(requestsPath, JSON.stringify(requests, null, 2), 'utf8').catch(() => undefined);
      await new Promise((resolve) => server.close(resolve));
    }
  };
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

async function waitForLocalProxyProbeRequest(localProxyProbe, child) {
  const targetPath = new URL(localProxyProbe.targetUrl).pathname;
  const startedAt = Date.now();
  while (Date.now() - startedAt < 60000) {
    const requests = localProxyProbe.getRequests();
    if (
      requests.some(
        (request) =>
          request.url === targetPath &&
          request.headers['x-slurm-connect-proxy-probe'] === localProxyProbe.token
      )
    ) {
      return requests;
    }
    if (child.exitCode !== null) {
      throw new Error(
        `VS Code exited before the local proxy probe reached the client server (exit code ${child.exitCode}). Requests: ${JSON.stringify(requests)}`
      );
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(
    `Timed out waiting for the remote local proxy probe to reach the client server. Requests: ${JSON.stringify(
      localProxyProbe.getRequests()
    )}`
  );
}

async function verifyRemoteFsMarker(fixture, remoteFsMarkerPath) {
  const sentinel = '__SLURM_CONNECT_MARKER_END__';
  const remoteCommand = `bash -lc 'test -f "${remoteFsMarkerPath}" && cat "${remoteFsMarkerPath}" && printf "\\n${sentinel}\\n"'`;
  const { stdout } = await runCommand(
    getSshCommand(),
    ['-F', fixture.sshConfigPath, '-T', fixture.host, remoteCommand],
    {
      env: {
        ...process.env
      },
      stdio: 'pipe',
      resolveOnStdoutIncludes: sentinel,
      timeoutMs: 30000
    }
  );
  const marker = JSON.parse(stdout.replace(sentinel, '').trim());
  if (marker.remoteName !== 'ssh-remote') {
    throw new Error(`Unexpected remote filesystem marker payload: ${stdout}`);
  }
  return marker;
}

async function readProxyLogForSession(fixture, sessionKey) {
  const sentinel = '__SLURM_CONNECT_PROXY_LOG_END__';
  const script = [
    `needle=${shellQuote(`session_key=${sessionKey}`)}`,
    'latest=$(grep -l -- "$needle" ~/.slurm-connect/vscode-proxy-*.log 2>/dev/null | xargs ls -t 2>/dev/null | head -n 1)',
    'if [ -z "$latest" ]; then echo "No vscode-proxy log found for $needle." >&2; exit 1; fi',
    'printf "%s\\n" "$latest"',
    'cat "$latest"',
    `printf "\\n${sentinel}\\n"`
  ].join('\n');
  const remoteCommand = `bash -lc ${shellQuote(script)}`;
  const { stdout } = await runCommand(
    getSshCommand(),
    ['-F', fixture.sshConfigPath, '-T', fixture.host, remoteCommand],
    {
      env: {
        ...process.env
      },
      stdio: 'pipe',
      resolveOnStdoutIncludes: sentinel,
      timeoutMs: 30000
    }
  );
  const [logPath, ...lines] = stdout
    .split(/\r?\n/)
    .filter((line) => line.trim() !== sentinel);
  return {
    path: logPath.trim(),
    text: lines.join('\n')
  };
}

function tailForError(text) {
  return text.split(/\r?\n/).slice(-40).join('\n');
}

function assertProxyLogShowsSlurmAllocation(proxyLog, expectedHostPattern) {
  if (!/Connection path selected: (ephemeral allocation via salloc\+srun|persistent session via srun)/.test(proxyLog)) {
    throw new Error(`Proxy log did not show a Slurm allocation path:\n${tailForError(proxyLog)}`);
  }
  if (!/Proxy server listening on 127\.0\.0\.1:\d+/.test(proxyLog)) {
    throw new Error(`Proxy log did not report a listening proxy server:\n${tailForError(proxyLog)}`);
  }
  const computeHostMatch = proxyLog.match(/Resolved compute host\s+([^\s]+)\s+->/);
  const computeHost = computeHostMatch ? computeHostMatch[1] : '';
  if (expectedHostPattern && !computeHost) {
    throw new Error(`Proxy log did not report a resolved compute host:\n${tailForError(proxyLog)}`);
  }
  if (expectedHostPattern && !new RegExp(expectedHostPattern).test(computeHost)) {
    throw new Error(`Proxy log compute host ${computeHost} did not match ${expectedHostPattern}:\n${tailForError(proxyLog)}`);
  }
  return computeHost;
}

async function main() {
  await ensureDir(STATE_DIR);
  if (!isExternalFixture) {
    await ensureFixtureUp();
  }
  const fixture = await resolveFixtureConfig();
  await installProxyPackage(fixture);

  const sessionDir = await fs.mkdtemp(path.join(STATE_DIR, 'run-'));
  const localProxyProbe = await startLocalProxyProbeServer(sessionDir);
  const userDataDir = path.join(sessionDir, 'user-data');
  const extensionsDir = path.join(sessionDir, 'extensions');
  const workspaceDir = await prepareWorkspace(sessionDir);
  const baseConfigPath = path.join(sessionDir, 'ssh-config');
  const includeConfigFilePath = path.join(sessionDir, 'slurm-connect.conf');
  const includeConfigPath = process.platform === 'win32' ? toMsysPath(includeConfigFilePath) : includeConfigFilePath;
  const statusMarkerPath = path.join(sessionDir, 'status.json');
  const remoteMarkerPath = path.join(sessionDir, 'remote.json');
  const remoteMarkerName = [
    'vscode-remote-session-marker',
    sanitizeRemotePathComponent(process.platform),
    sanitizeRemotePathComponent(path.basename(sessionDir))
  ].join('-');
  const remoteFsMarkerPath = `${fixture.home}/.slurm-connect/${remoteMarkerName}.json`;
  const defaultPartition = isExternalFixture ? process.env.SLURM_CLIENT_DEFAULT_PARTITION || '' : 'cpu';
  const runScopedHostPrefix = `${SSH_HOST_PREFIX}-${sanitizeRemotePathComponent(path.basename(sessionDir))}`;
  const remoteAlias = buildDefaultAlias(runScopedHostPrefix, fixture.host, defaultPartition, 1, 1);
  const expectedComputeHostPattern = isExternalFixture
    ? process.env.SLURM_CLIENT_EXPECTED_COMPUTE_HOST_PATTERN || ''
    : '^c[0-9]+$';
  const stdoutLogPath = path.join(sessionDir, 'vscode.stdout.log');
  const stderrLogPath = path.join(sessionDir, 'vscode.stderr.log');
  const extraSallocArgs = parseJsonStringArray(process.env.SLURM_CLIENT_EXTRA_SALLOC_ARGS_JSON, 'SLURM_CLIENT_EXTRA_SALLOC_ARGS_JSON');

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
    SLURM_CONNECT_SESSION_E2E_REMOTE_FS_MARKER: skipRemoteFsMarker ? '' : remoteFsMarkerPath,
    SLURM_CONNECT_SESSION_E2E_SKIP_REMOTE_FS_MARKER: skipRemoteFsMarker ? '1' : '',
    SLURM_CONNECT_SESSION_E2E_EXPECTED_ALIAS: remoteAlias,
    SLURM_CONNECT_SESSION_E2E_PROXY_PROBE_TARGET_URL: localProxyProbe?.targetUrl || '',
    SLURM_CONNECT_SESSION_E2E_PROXY_PROBE_EXPECTED_TOKEN: localProxyProbe?.token || '',
    SLURM_CONNECT_SESSION_E2E_CONNECT_OVERRIDES: JSON.stringify({
      loginHosts: [fixture.host],
      user: fixture.user,
      identityFile: '',
      additionalSshOptions: {
        IdentityFile: fixture.privateKeyPath,
        IdentitiesOnly: 'yes',
        Port: fixture.port,
        StrictHostKeyChecking: 'accept-new',
        UserKnownHostsFile: fixture.knownHostsPath
      },
      sshQueryConfigPath: fixture.sshConfigPath,
      temporarySshConfigPath: includeConfigPath,
      defaultPartition,
      defaultTime: '',
      defaultNodes: 1,
      defaultTasksPerNode: 1,
      defaultCpusPerTask: 1,
      extraSallocArgs,
      sshHostPrefix: runScopedHostPrefix,
      localProxyEnabled: enableLocalProxyE2E,
      localProxyTunnelMode: 'remoteSsh',
      localProxyComputeTunnel:
        process.env.SLURM_CLIENT_LOCAL_PROXY_COMPUTE_TUNNEL === undefined
          ? true
          : /^(1|true|yes)$/i.test(process.env.SLURM_CLIENT_LOCAL_PROXY_COMPUTE_TUNNEL),
      localProxyRemoteHost: process.env.SLURM_CLIENT_LOCAL_PROXY_REMOTE_HOST || '',
      sessionMode: 'ephemeral',
      openInNewWindow: true,
      remoteWorkspacePath: process.env.SLURM_CLIENT_REMOTE_WORKSPACE_PATH || fixture.home,
      startupCommand: process.env.SLURM_CLIENT_STARTUP_COMMAND || '',
      moduleLoad: process.env.SLURM_CLIENT_MODULE_LOAD || '',
      proxyCommand: process.env.SLURM_CLIENT_PROXY_COMMAND || 'python3 ~/.slurm-connect/vscode-proxy.py',
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
    if (marker.remoteName !== 'ssh-remote') {
      throw new Error(`Remote marker did not report an SSH remote window: ${JSON.stringify(marker)}`);
    }
    log(`Remote window reported authority ${marker.authority}`);

    if (!skipRemoteFsMarker) {
      const remoteFsMarker = await verifyRemoteFsMarker(fixture, remoteFsMarkerPath);
      log(`Remote filesystem marker confirmed for authority ${remoteFsMarker.authority}`);
    }

    const includeContent = await fs.readFile(includeConfigFilePath, 'utf8');
    if (!new RegExp(`^Host ${escapeRegex(remoteAlias)}$`, 'm').test(includeContent)) {
      throw new Error(`Generated include file did not contain alias ${remoteAlias}`);
    }
    if (!/^\s*RemoteCommand .*vscode-proxy\.py/m.test(includeContent)) {
      throw new Error('Generated include file did not contain the expected proxy RemoteCommand.');
    }
    const proxyLog = await readProxyLogForSession(fixture, remoteAlias);
    const computeHost = assertProxyLogShowsSlurmAllocation(proxyLog.text, expectedComputeHostPattern);
    log(`Proxy log confirmed Slurm allocation${computeHost ? ` on ${computeHost}` : ''} (${proxyLog.path})`);
    if (localProxyProbe) {
      if (!/^\s*RemoteForward /m.test(includeContent)) {
        throw new Error('Generated include file did not contain a RemoteForward for the local proxy.');
      }
      await waitForLocalProxyProbeRequest(localProxyProbe, child);
      log(`Local proxy probe confirmed through client server (${localProxyProbe.requestsPath})`);
    }

    log(`Session artifacts: ${sessionDir}`);
  } finally {
    await fs.writeFile(stdoutLogPath, stdoutStream.join(''), 'utf8').catch(() => undefined);
    await fs.writeFile(stderrLogPath, stderrStream.join(''), 'utf8').catch(() => undefined);
    if (child.pid) {
      await killTree(child.pid, true).catch(() => undefined);
    }
    if (localProxyProbe) {
      await localProxyProbe.close();
    }
    await fs.rm(includeConfigFilePath, { force: true }).catch(() => undefined);
  }
}

main().catch((error) => {
  die(error instanceof Error ? error.stack || error.message : String(error));
});
