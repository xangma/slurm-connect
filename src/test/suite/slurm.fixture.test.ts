import * as assert from 'assert';
import { execFile } from 'child_process';
import * as fs from 'fs/promises';
import * as os from 'os';
import * as path from 'path';
import { promisify } from 'util';

import { suite, test } from 'mocha';
import * as vscode from 'vscode';

import { __resetForTests, __runNonInteractiveConnectForTests } from '../../extension';

const execFileAsync = promisify(execFile);
const FIXTURE_ENV_FLAG = 'SLURM_CONNECT_FIXTURE';

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

  test('runs the connect flow against the disposable Slurm fixture', async function () {
    if (process.env[FIXTURE_ENV_FLAG] !== '1') {
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
    const includeAlias = 'slurm-fixture-integration';
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
      await rememberConfigValue('slurmConnect', 'defaultPartition', '', vscode.ConfigurationTarget.Workspace, restoreSteps);
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
      assert.match(includeContent, new RegExp(`^  HostName ${fixture.host.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'm'));
      assert.match(includeContent, new RegExp(`^  User ${fixture.user}$`, 'm'));
      assert.match(includeContent, new RegExp(`^  Port ${fixture.port}$`, 'm'));

      const baseConfigContent = await fs.readFile(baseSshConfigPath, 'utf8');
      assert.ok(
        baseConfigContent.includes(result.includeFilePath),
        `Expected base SSH config to include ${result.includeFilePath}`
      );

      const logContent = await fs.readFile(result.logFilePath, 'utf8');
      assert.ok(logContent.includes('Slurm Connect started.'));
      assert.ok(logContent.includes(`Login hosts resolved: ${fixture.host}`));
      assert.ok(logContent.includes('Generated SSH host entry:'));

      const { stdout } = await execFileAsync(
        'ssh',
        [
          '-F',
          baseSshConfigPath,
          '-o',
          'RemoteCommand=none',
          '-T',
          includeAlias,
          `bash -lc 'whoami; printf "%s\\n" "$HOME"; sinfo -h -o "%P|%D|%t" | head -n 1'`
        ],
        { timeout: 20000 }
      );

      const lines = stdout
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean);

      assert.ok(lines.includes(fixture.user), `Expected SSH alias to log in as ${fixture.user}. Output: ${stdout}`);
      assert.ok(lines.includes(fixture.home), `Expected SSH alias to use home ${fixture.home}. Output: ${stdout}`);
      assert.ok(
        lines.some((line) => line.startsWith('cpu') && line.includes('|2|idle')),
        `Expected sinfo output through generated alias. Output: ${stdout}`
      );
    } finally {
      for (const restore of restoreSteps) {
        await restore();
      }
      await __resetForTests();
      await fs.rm(tempDir, { recursive: true, force: true });
    }
  });
});
