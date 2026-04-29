import * as assert from 'assert';
import { spawn } from 'child_process';
import * as fs from 'fs/promises';
import * as os from 'os';
import * as path from 'path';

import { suite, test } from 'mocha';
import * as vscode from 'vscode';

import { __resetForTests, __runNonInteractiveConnectForTests } from '../../extension';
import { formatSshConfigValue } from '../../utils/sshConfig';

const FIXTURE_ENV_FLAG = 'SLURM_CONNECT_FIXTURE';
const CLIENT_FIXTURE_ENV_FLAG = 'SLURM_CONNECT_CLIENT_FIXTURE';

type ConfigTarget = vscode.ConfigurationTarget.Workspace;

type FixtureConnection = {
  stateDir: string;
  host: string;
  port: string;
  user: string;
  home: string;
  sshConfigPath: string;
  privateKeyPath: string;
  knownHostsPath: string;
};

function repoRootPath(): string {
  return path.resolve(__dirname, '../../../');
}

function resolveFixtureConnection(): FixtureConnection {
  const stateDir = process.env.SLURM_FIXTURE_STATE_DIR || path.join(repoRootPath(), '.e2e', 'slurm-fixture');
  const sshDir = path.join(stateDir, 'ssh');
  const user = process.env.SLURM_FIXTURE_USER || 'slurmconnect';
  return {
    stateDir,
    host: process.env.SLURM_FIXTURE_SSH_HOST || '127.0.0.1',
    port: process.env.SLURM_FIXTURE_SSH_PORT || '3022',
    user,
    home: process.env.SLURM_FIXTURE_HOME || `/data/home/${user}`,
    sshConfigPath: path.join(sshDir, 'ssh_config'),
    privateKeyPath: path.join(sshDir, 'id_ed25519'),
    knownHostsPath: path.join(sshDir, 'known_hosts')
  };
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function assertExpectedSinfoLine(stdout: string, isClientFixture: boolean): void {
  const lines = stdout
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (isClientFixture) {
    const expectedPattern = process.env.SLURM_CLIENT_EXPECTED_SINFO_PATTERN || '';
    if (expectedPattern) {
      assert.ok(
        lines.some((line) => new RegExp(expectedPattern).test(line)),
        `Expected sinfo output to match ${expectedPattern}. Output: ${stdout}`
      );
      return;
    }
    assert.ok(
      lines.some((line) => line.split('|').length >= 3),
      `Expected sinfo output through generated alias. Output: ${stdout}`
    );
    return;
  }

  assert.ok(
    lines.some((line) => line.startsWith('cpu') && line.includes('|2|idle')),
    `Expected sinfo output through generated alias. Output: ${stdout}`
  );
}

function hasProbeOutput(stdout: string, expectedLineCount: number): boolean {
  return stdout
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .length >= expectedLineCount;
}

function runSshProbe(args: string[], timeoutMs: number, expectedLineCount = 1): Promise<{ stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    let stdout = '';
    let stderr = '';
    let settled = false;
    let timeout: NodeJS.Timeout | undefined;
    const child = spawn('ssh', args, {
      stdio: ['ignore', 'pipe', 'pipe'],
      shell: false
    });
    const markSettled = (): boolean => {
      if (settled) {
        return false;
      }
      settled = true;
      if (timeout) {
        clearTimeout(timeout);
      }
      return true;
    };
    const resolveOnce = (value: { stdout: string; stderr: string }) => {
      if (markSettled()) {
        resolve(value);
      }
    };
    const rejectOnce = (error: Error) => {
      if (markSettled()) {
        reject(error);
      }
    };
    const buildError = (message: string): Error => {
      const error = new Error(`${message}\nstdout:\n${stdout}\nstderr:\n${stderr}`);
      return error;
    };
    const maybeResolveWindowsProbe = () => {
      if (process.platform !== 'win32' || !hasProbeOutput(stdout, expectedLineCount)) {
        return;
      }
      resolveOnce({ stdout, stderr });
      child.kill();
    };
    timeout = setTimeout(() => {
      child.kill();
      if (process.platform === 'win32' && hasProbeOutput(stdout, expectedLineCount)) {
        resolveOnce({ stdout, stderr });
        return;
      }
      rejectOnce(buildError(`ssh ${args.join(' ')} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    child.stdout?.on('data', (chunk) => {
      stdout += chunk.toString('utf8');
      maybeResolveWindowsProbe();
    });
    child.stderr?.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
    });
    child.on('error', (error) => {
      rejectOnce(error);
    });
    child.on('close', (code, signal) => {
      if (code === 0) {
        resolveOnce({ stdout, stderr });
        return;
      }
      rejectOnce(buildError(`ssh ${args.join(' ')} exited with ${code === null ? signal : `code ${code}`}`));
    });
  });
}

async function rememberConfigValue(
  section: string,
  key: string,
  value: unknown,
  target: ConfigTarget,
  restoreSteps: Array<() => Promise<void>>
): Promise<void> {
  const config = vscode.workspace.getConfiguration(section);
  const inspected = config.inspect(key);
  const previous = inspected?.workspaceValue;
  restoreSteps.unshift(async () => {
    await config.update(key, previous, target);
  });
  await config.update(key, value, target);
}

suite('Slurm Fixture E2E', function () {
  this.timeout(120000);

  test('runs the connect flow against a Slurm fixture', async function () {
    const isDockerFixture = process.env[FIXTURE_ENV_FLAG] === '1';
    const isClientFixture = process.env[CLIENT_FIXTURE_ENV_FLAG] === '1';
    if (!isDockerFixture && !isClientFixture) {
      this.skip();
      return;
    }

    const extension = vscode.extensions.getExtension('xangma.slurm-connect');
    assert.ok(extension, 'Expected Slurm Connect extension metadata to be available.');
    await extension.activate();

    const fixture = resolveFixtureConnection();
    await fs.access(fixture.sshConfigPath);
    await fs.access(fixture.privateKeyPath);
    await fs.access(fixture.knownHostsPath);

    const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'slurm-connect-fixture-'));
    const baseSshConfigPath = path.join(tempDir, 'ssh-config');
    const includeAlias = isClientFixture ? `slurm-client-${process.platform}` : 'slurm-fixture-integration';
    const restoreSteps: Array<() => Promise<void>> = [];

    await __resetForTests();
    await fs.writeFile(baseSshConfigPath, '# Slurm Connect integration test SSH config\n', 'utf8');

    try {
      await rememberConfigValue('slurmConnect', 'loginHosts', [fixture.host], vscode.ConfigurationTarget.Workspace, restoreSteps);
      await rememberConfigValue('slurmConnect', 'user', fixture.user, vscode.ConfigurationTarget.Workspace, restoreSteps);
      await rememberConfigValue('slurmConnect', 'identityFile', '', vscode.ConfigurationTarget.Workspace, restoreSteps);
      await rememberConfigValue(
        'slurmConnect',
        'additionalSshOptions',
        {
          IdentityFile: fixture.privateKeyPath,
          IdentitiesOnly: 'yes',
          Port: fixture.port,
          StrictHostKeyChecking: 'accept-new',
          UserKnownHostsFile: fixture.knownHostsPath
        },
        vscode.ConfigurationTarget.Workspace,
        restoreSteps
      );
      await rememberConfigValue(
        'slurmConnect',
        'sshQueryConfigPath',
        fixture.sshConfigPath,
        vscode.ConfigurationTarget.Workspace,
        restoreSteps
      );
      await rememberConfigValue(
        'slurmConnect',
        'temporarySshConfigPath',
        path.join(tempDir, 'slurm-connect.conf'),
        vscode.ConfigurationTarget.Workspace,
        restoreSteps
      );
      await rememberConfigValue(
        'slurmConnect',
        'defaultPartition',
        isClientFixture ? process.env.SLURM_CLIENT_DEFAULT_PARTITION || '' : '',
        vscode.ConfigurationTarget.Workspace,
        restoreSteps
      );
      await rememberConfigValue('slurmConnect', 'defaultTime', '', vscode.ConfigurationTarget.Workspace, restoreSteps);
      await rememberConfigValue('slurmConnect', 'defaultNodes', 1, vscode.ConfigurationTarget.Workspace, restoreSteps);
      await rememberConfigValue('slurmConnect', 'defaultTasksPerNode', 1, vscode.ConfigurationTarget.Workspace, restoreSteps);
      await rememberConfigValue('slurmConnect', 'defaultCpusPerTask', 1, vscode.ConfigurationTarget.Workspace, restoreSteps);
      await rememberConfigValue('slurmConnect', 'localProxyEnabled', false, vscode.ConfigurationTarget.Workspace, restoreSteps);
      await rememberConfigValue('slurmConnect', 'sessionMode', 'ephemeral', vscode.ConfigurationTarget.Workspace, restoreSteps);
      await rememberConfigValue(
        'slurmConnect',
        'proxyCommand',
        'python3 ~/.slurm-connect/vscode-proxy.py',
        vscode.ConfigurationTarget.Workspace,
        restoreSteps
      );

      const result = await __runNonInteractiveConnectForTests(
        {},
        {
          aliasOverride: includeAlias,
          bypassRemoteSshSettings: true,
          remoteSshConfigPath: baseSshConfigPath
        }
      );
      assert.strictEqual(result.didConnect, true);
      assert.strictEqual(result.alias, includeAlias);
      assert.strictEqual(result.loginHost, fixture.host);
      assert.strictEqual(result.connectInvocations.length, 1);
      assert.strictEqual(result.connectInvocations[0]?.alias, includeAlias);
      assert.strictEqual(result.connectInvocations[0]?.openInNewWindow, true);

      const includeContent = await fs.readFile(result.includeFilePath, 'utf8');
      assert.match(includeContent, new RegExp(`^Host ${includeAlias}$`, 'm'));
      assert.match(includeContent, new RegExp(`^  HostName ${escapeRegExp(fixture.host)}$`, 'm'));
      assert.match(includeContent, new RegExp(`^  User ${fixture.user}$`, 'm'));
      assert.match(includeContent, new RegExp(`^  Port ${fixture.port}$`, 'm'));

      const baseConfigContent = await fs.readFile(baseSshConfigPath, 'utf8');
      const expectedIncludeTarget = formatSshConfigValue(result.includeFilePath);
      assert.ok(
        baseConfigContent.includes(expectedIncludeTarget),
        `Expected base SSH config to include ${expectedIncludeTarget}`
      );

      const logContent = await fs.readFile(result.logFilePath, 'utf8');
      assert.ok(logContent.includes('Slurm Connect started.'));
      assert.ok(logContent.includes(`Login hosts resolved: ${fixture.host}`));
      assert.ok(logContent.includes('Generated SSH host entry:'));

      const probeBaseArgs = [
        '-F',
        baseSshConfigPath,
        '-o',
        'RemoteCommand=none',
        '-T',
        includeAlias
      ];
      const userProbe = await runSshProbe(
        [
          ...probeBaseArgs,
          'whoami'
        ],
        20000
      );
      const homeProbe = await runSshProbe(
        [
          ...probeBaseArgs,
          'printenv HOME'
        ],
        20000
      );
      const sinfoProbe = await runSshProbe(
        [
          ...probeBaseArgs,
          "sinfo -h -o '%P|%D|%t' | head -n 1"
        ],
        20000
      );
      const stdout = `${userProbe.stdout}${homeProbe.stdout}${sinfoProbe.stdout}`;

      const lines = stdout
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean);

      assert.ok(lines.includes(fixture.user), `Expected SSH alias to log in as ${fixture.user}. Output: ${stdout}`);
      assert.ok(lines.includes(fixture.home), `Expected SSH alias to use home ${fixture.home}. Output: ${stdout}`);
      assertExpectedSinfoLine(stdout, isClientFixture);
    } finally {
      for (const restore of restoreSteps) {
        await restore();
      }
      await __resetForTests();
      await fs.rm(tempDir, { recursive: true, force: true });
    }
  });
});
