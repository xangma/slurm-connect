import { createRequire } from 'module';
import * as fs from 'fs';
import * as path from 'path';
import { describe, expect, it } from 'vitest';

const requireScript = createRequire(__filename);
const { SINFO_PARTITION_SUMMARY_COMMAND } = requireScript('../../../scripts/e2e/prepare-slurm-client-fixture.js') as {
  SINFO_PARTITION_SUMMARY_COMMAND: string;
};
const repoRoot = path.resolve(__dirname, '../../..');

describe('Slurm client e2e fixture helpers', () => {
  it('keeps pipe-separated sinfo output quoted for Windows OpenSSH', () => {
    expect(SINFO_PARTITION_SUMMARY_COMMAND).toBe("sinfo -h -o '%P|%D|%t' | head -n 1");
    expect(SINFO_PARTITION_SUMMARY_COMMAND).not.toContain('-o "%P|%D|%t"');
  });

  it('configures the disposable Docker login sshd for key-only non-PAM access', () => {
    const fixtureScript = fs.readFileSync(path.join(repoRoot, 'scripts/e2e/slurm-fixture.sh'), 'utf8');
    expect(fixtureScript).toContain('/etc/ssh/sshd_config.d/00-slurm-connect-e2e.conf');
    expect(fixtureScript).toContain('chpasswd');
    expect(fixtureScript).toContain('UsePAM no');
    expect(fixtureScript).toContain('PasswordAuthentication no');
  });
});
