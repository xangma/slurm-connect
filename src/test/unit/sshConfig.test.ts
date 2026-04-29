import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs/promises';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { SlurmConnectConfig } from '../../config/types';
import {
  collectSshConfigHosts,
  msysDrivePathToWindowsPath,
  resolveSshHostFromConfig,
  writeSlurmConnectIncludeFile
} from '../../ssh/config';
import {
  buildHostEntry,
  buildSlurmConnectIncludeBlock,
  buildSlurmConnectIncludeContent,
  buildTemporarySshConfigContent,
  expandHome,
  formatSshConfigValue
} from '../../utils/sshConfig';

const { execFileMock } = vi.hoisted(() => ({
  execFileMock: vi.fn()
}));

vi.mock('child_process', async (importOriginal) => {
  const actual = await importOriginal<typeof import('child_process')>();
  return {
    ...actual,
    execFile: execFileMock
  };
});

describe('sshConfig utilities', () => {
  beforeEach(() => {
    execFileMock.mockReset();
  });

  it('quotes SSH config values when needed', () => {
    expect(formatSshConfigValue('simple')).toBe('simple');
    expect(formatSshConfigValue('path with space')).toBe('"path with space"');
    expect(formatSshConfigValue('"already quoted"')).toBe('"already quoted"');

    const windowsPath = 'C:\\Users\\Test User\\id_rsa';
    expect(formatSshConfigValue(windowsPath)).toBe('"C:/Users/Test User/id_rsa"');
    expect(formatSshConfigValue('C:\\Users\\xangm\\id_ed25519')).toBe('"C:/Users/xangm/id_ed25519"');
  });

  it('builds host entries with quoted options', () => {
    const cfg = {
      user: '',
      requestTTY: false,
      forwardAgent: false,
      identityFile: '/Users/Test User/.ssh/id_rsa',
      additionalSshOptions: {
        LocalCommand: 'echo hello world'
      },
      extraSshOptions: ['RemoteForward 0.0.0.0:1234 127.0.0.1:5678']
    };

    const entry = buildHostEntry('alias', 'login.example.com', cfg, 'echo hi');
    expect(entry).toContain('IdentityFile "/Users/Test User/.ssh/id_rsa"');
    expect(entry).toContain('LocalCommand "echo hello world"');
    expect(entry).toContain('RemoteForward 0.0.0.0:1234 127.0.0.1:5678');
  });

  it('writes include directives into temporary SSH configs', () => {
    const content = buildTemporarySshConfigContent('Host alias\n  HostName login', [
      '/Users/Test User/.ssh/config'
    ]);
    expect(content).toContain('Include "/Users/Test User/.ssh/config"');
  });

  it('builds managed include helper content', () => {
    const block = buildSlurmConnectIncludeBlock('/Users/Test User/.ssh/slurm-connect.conf');
    expect(block).toContain('Slurm Connect');
    expect(block).toContain('Include "/Users/Test User/.ssh/slurm-connect.conf"');
    expect(buildSlurmConnectIncludeBlock('C:\\Users\\xangm\\slurm-connect.conf')).toContain(
      'Include "C:/Users/xangm/slurm-connect.conf"'
    );

    const content = buildSlurmConnectIncludeContent('Host alias\n  HostName login');
    expect(content.startsWith('# Slurm Connect SSH hosts')).toBe(true);
  });

  it('writes managed include files with restricted permissions', async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'slurm-connect-ssh-include-'));
    const includePath = path.join(dir, 'slurm-connect.conf');
    if (process.platform === 'win32') {
      execFileMock.mockImplementation((_file: string, _args: string[], callback: (...args: unknown[]) => void) => {
        callback(null, { stdout: '', stderr: '' });
        return undefined;
      });
    }

    await writeSlurmConnectIncludeFile('Host alias\n  HostName login', includePath);

    const content = await fs.readFile(includePath, 'utf8');
    expect(content).toContain('Host alias');
    if (process.platform === 'win32') {
      expect(execFileMock).toHaveBeenCalledWith('icacls', [includePath, '/inheritance:r'], expect.any(Function));
      expect(execFileMock).toHaveBeenCalledWith(
        'icacls',
        [includePath, '/grant:r', `${process.env.USERNAME || os.userInfo().username}:RW`],
        expect.any(Function)
      );
    } else {
      const stat = await fs.stat(includePath);
      expect(stat.mode & 0o777).toBe(0o600);
    }
  });

  it('expands home-directory paths', () => {
    const home = os.homedir();
    expect(expandHome('~')).toBe(home);
    expect(expandHome('~/config')).toBe(path.join(home, 'config'));
  });

  it('converts MSYS drive paths to Windows paths for filesystem writes', () => {
    expect(msysDrivePathToWindowsPath('/c/Users/Test/slurm-connect.conf')).toBe(
      'C:\\Users\\Test\\slurm-connect.conf'
    );
    expect(msysDrivePathToWindowsPath('/z')).toBe('Z:\\');
    expect(msysDrivePathToWindowsPath('/tmp/slurm-connect.conf')).toBe('/tmp/slurm-connect.conf');
  });

  it('collects hosts from OpenSSH Include globs', async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'slurm-connect-ssh-config-'));
    const includeDir = path.join(dir, 'config.d');
    await fs.mkdir(includeDir, { recursive: true });
    await fs.writeFile(path.join(includeDir, 'cluster.conf'), 'Host included-cluster\n  HostName login.example.com\n', 'utf8');
    await fs.writeFile(path.join(includeDir, 'ignored.txt'), 'Host ignored\n  HostName ignored.example.com\n', 'utf8');
    const configPath = path.join(dir, 'config');
    await fs.writeFile(configPath, `Host base\n  HostName base.example.com\nInclude ${includeDir}/*.conf\n`, 'utf8');

    const hosts = await collectSshConfigHosts(configPath, {
      fileExists: async (filePath) => {
        try {
          const stat = await fs.stat(filePath);
          return stat.isFile();
        } catch {
          return false;
        }
      }
    });

    expect(hosts).toEqual(['base', 'included-cluster']);
  });

  it('resolves explicit host details from top-level included configs', async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'slurm-connect-ssh-resolve-'));
    const includeDir = path.join(dir, 'config.d');
    const identityFile = path.join(dir, 'id_ed25519');
    await fs.mkdir(includeDir, { recursive: true });
    await fs.writeFile(identityFile, 'test-key', 'utf8');
    await fs.writeFile(
      path.join(includeDir, 'cluster.conf'),
      [
        'Host included-cluster',
        '  HostName login.example.com',
        '  User cluster-user',
        `  IdentityFile ${identityFile}`,
        ''
      ].join('\n'),
      'utf8'
    );
    const configPath = path.join(dir, 'config');
    await fs.writeFile(configPath, `Include ${includeDir}/*.conf\n`, 'utf8');
    execFileMock.mockImplementationOnce((_file: string, _args: string[], callback: (...args: unknown[]) => void) => {
      callback(null, { stdout: 'hostname login.example.com\nport 22\n', stderr: '' });
      return undefined;
    });

    const resolved = await resolveSshHostFromConfig(
      'included-cluster',
      {
        sshQueryConfigPath: configPath
      } as SlurmConnectConfig,
      {
        fileExists: async (filePath) => {
          try {
            const stat = await fs.stat(filePath);
            return stat.isFile();
          } catch {
            return false;
          }
        },
        getOutputChannel: () => ({ appendLine: () => undefined }),
        resolveSshToolPath: async () => 'ssh',
        normalizeSshErrorText: (error) => String(error),
        pickSshErrorSummary: (text) => text
      }
    );

    expect(resolved.hostname).toBe('login.example.com');
    expect(resolved.user).toBe('cluster-user');
    expect(resolved.identityFile).toBe(identityFile);
    expect(resolved.hasExplicitHost).toBe(true);
  });
});
