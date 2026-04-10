import * as path from 'path';

import { runTests } from '@vscode/test-electron';

function buildExtensionTestEnv(): Record<string, string | undefined> {
  const env: Record<string, string | undefined> = {};
  if (process.argv.includes('--slurm-fixture-e2e')) {
    env.SLURM_CONNECT_FIXTURE = '1';
  }
  for (const key of [
    'SLURM_FIXTURE_STATE_DIR',
    'SLURM_FIXTURE_SSH_HOST',
    'SLURM_FIXTURE_SSH_PORT',
    'SLURM_FIXTURE_USER',
    'SLURM_FIXTURE_UID',
    'SLURM_FIXTURE_GID',
    'SLURM_FIXTURE_GROUP',
    'SLURM_FIXTURE_HOME'
  ]) {
    env[key] = process.env[key];
  }
  return env;
}

async function main(): Promise<void> {
  try {
    delete process.env.ELECTRON_RUN_AS_NODE;
    const extensionDevelopmentPath = path.resolve(__dirname, '../..');
    const extensionTestsPath = path.resolve(__dirname, './suite');
    const testWorkspace = path.resolve(__dirname, '../../test-fixtures/workspace');

    await runTests({
      extensionDevelopmentPath,
      extensionTestsPath,
      extensionTestsEnv: buildExtensionTestEnv(),
      launchArgs: [testWorkspace, '--disable-extensions']
    });
  } catch (error) {
    console.error('Failed to run VS Code integration tests.');
    console.error(error);
    process.exit(1);
  }
}

void main();
