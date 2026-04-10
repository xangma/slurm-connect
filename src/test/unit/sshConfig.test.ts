import * as os from 'os';
import * as path from 'path';
import { describe, expect, it } from 'vitest';

import {
  buildHostEntry,
  buildSlurmConnectIncludeBlock,
  buildSlurmConnectIncludeContent,
  buildTemporarySshConfigContent,
  expandHome,
  formatSshConfigValue
} from '../../utils/sshConfig';

describe('sshConfig utilities', () => {
  it('quotes SSH config values when needed', () => {
    expect(formatSshConfigValue('simple')).toBe('simple');
    expect(formatSshConfigValue('path with space')).toBe('"path with space"');
    expect(formatSshConfigValue('"already quoted"')).toBe('"already quoted"');

    const windowsPath = 'C:\\Users\\Test User\\id_rsa';
    expect(formatSshConfigValue(windowsPath)).toBe('"C:\\\\Users\\\\Test User\\\\id_rsa"');
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

    const content = buildSlurmConnectIncludeContent('Host alias\n  HostName login');
    expect(content.startsWith('# Slurm Connect SSH hosts')).toBe(true);
  });

  it('expands home-directory paths', () => {
    const home = os.homedir();
    expect(expandHome('~')).toBe(home);
    expect(expandHome('~/config')).toBe(path.join(home, 'config'));
  });
});
