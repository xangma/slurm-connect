import * as fs from 'fs/promises';
import * as path from 'path';

import { glob } from 'glob';
import Mocha from 'mocha';
import * as vscode from 'vscode';

type TestResultMarker = {
  status: 'passed' | 'failed';
  message?: string;
};

async function writeResultMarker(result: TestResultMarker): Promise<void> {
  const markerPath = process.env.SLURM_CONNECT_TEST_RESULT_PATH;
  if (!markerPath) {
    return;
  }
  await fs.writeFile(markerPath, JSON.stringify(result), 'utf8');
}

function formatError(error: unknown): string {
  return error instanceof Error ? error.stack || error.message : String(error);
}

export async function run(): Promise<void> {
  const mocha = new Mocha({
    ui: 'tdd',
    color: true,
    timeout: 20000
  });

  const testsRoot = path.resolve(__dirname);
  const files = await glob('**/*.test.js', {
    cwd: testsRoot
  });

  for (const file of files) {
    mocha.addFile(path.resolve(testsRoot, file));
  }

  let testError: unknown;
  try {
    await new Promise<void>((resolve, reject) => {
      mocha.run((failures) => {
        if (failures > 0) {
          reject(new Error(`${failures} integration test(s) failed.`));
          return;
        }
        resolve();
      });
    });
    await writeResultMarker({ status: 'passed' });
  } catch (error) {
    testError = error;
    await writeResultMarker({ status: 'failed', message: formatError(error) });
    throw error;
  } finally {
    try {
      await vscode.commands.executeCommand('workbench.action.quit');
    } catch (error) {
      console.error(
        testError
          ? 'Failed to request VS Code shutdown after failed integration tests.'
          : 'Failed to request VS Code shutdown after integration tests.'
      );
      console.error(error);
    }
  }
}

if (require.main === module) {
  void run();
}
