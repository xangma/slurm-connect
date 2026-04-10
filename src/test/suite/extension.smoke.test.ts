import * as assert from 'assert';

import { suite, test } from 'mocha';
import * as vscode from 'vscode';

import {
  __resetForTests,
  __runProxyOverrideCleanupForTests,
  __runStartupMigrationsForTests
} from '../../extension';

suite('Extension Smoke', () => {
  test('activates and registers the main command', async () => {
    const extension = vscode.extensions.getExtension('xangma.slurm-connect');
    assert.ok(extension, 'Expected Slurm Connect extension metadata to be available.');

    await extension.activate();

    assert.strictEqual(extension.isActive, true);
    const commands = await vscode.commands.getCommands(true);
    assert.ok(commands.includes('slurmConnect.connect'));
  });

  test('preserves configured proxy overrides during cleanup migration', async () => {
    const extension = vscode.extensions.getExtension('xangma.slurm-connect');
    assert.ok(extension, 'Expected Slurm Connect extension metadata to be available.');

    await extension.activate();
    await __resetForTests();

    const config = vscode.workspace.getConfiguration('slurmConnect');
    const proxyCommandInspect = config.inspect<string>('proxyCommand');
    const proxyArgsInspect = config.inspect<string[]>('proxyArgs');
    const previousProxyCommand = proxyCommandInspect?.globalValue;
    const previousProxyArgs = proxyArgsInspect?.globalValue;

    try {
      await config.update(
        'proxyCommand',
        'python3 ~/.slurm-connect/custom-proxy.py',
        vscode.ConfigurationTarget.Global
      );
      await config.update(
        'proxyArgs',
        ['--ephemeral-exec-server-bypass', '--log-file=/tmp/proxy.log'],
        vscode.ConfigurationTarget.Global
      );

      assert.strictEqual(
        config.inspect<string>('proxyCommand')?.globalValue,
        'python3 ~/.slurm-connect/custom-proxy.py'
      );
      assert.deepStrictEqual(config.inspect<string[]>('proxyArgs')?.globalValue, [
        '--ephemeral-exec-server-bypass',
        '--log-file=/tmp/proxy.log'
      ]);

      await __runProxyOverrideCleanupForTests();

      assert.strictEqual(
        config.inspect<string>('proxyCommand')?.globalValue,
        'python3 ~/.slurm-connect/custom-proxy.py'
      );
      assert.deepStrictEqual(config.inspect<string[]>('proxyArgs')?.globalValue, [
        '--ephemeral-exec-server-bypass',
        '--log-file=/tmp/proxy.log'
      ]);
    } finally {
      await config.update(
        'proxyCommand',
        previousProxyCommand,
        vscode.ConfigurationTarget.Global
      );
      await config.update(
        'proxyArgs',
        previousProxyArgs,
        vscode.ConfigurationTarget.Global
      );
      await __resetForTests();
    }
  });

  test('preserves configured cluster settings during startup migrations', async () => {
    const extension = vscode.extensions.getExtension('xangma.slurm-connect');
    assert.ok(extension, 'Expected Slurm Connect extension metadata to be available.');

    await extension.activate();
    await __resetForTests();

    const config = vscode.workspace.getConfiguration('slurmConnect');
    const loginHostsInspect = config.inspect<string[]>('loginHosts');
    const userInspect = config.inspect<string>('user');
    const previousLoginHosts = loginHostsInspect?.globalValue;
    const previousUser = userInspect?.globalValue;

    try {
      await config.update('loginHosts', ['login.cluster.example'], vscode.ConfigurationTarget.Global);
      await config.update('user', 'cluster-user', vscode.ConfigurationTarget.Global);

      await __runStartupMigrationsForTests();

      assert.deepStrictEqual(config.inspect<string[]>('loginHosts')?.globalValue, ['login.cluster.example']);
      assert.strictEqual(config.inspect<string>('user')?.globalValue, 'cluster-user');
    } finally {
      await config.update('loginHosts', previousLoginHosts, vscode.ConfigurationTarget.Global);
      await config.update('user', previousUser, vscode.ConfigurationTarget.Global);
      await __resetForTests();
    }
  });
});
