import { createRequire } from 'module';
import { describe, expect, it } from 'vitest';

const requireScript = createRequire(__filename);
const { SINFO_PARTITION_SUMMARY_COMMAND } = requireScript('../../../scripts/e2e/prepare-slurm-client-fixture.js') as {
  SINFO_PARTITION_SUMMARY_COMMAND: string;
};

describe('Slurm client e2e fixture helpers', () => {
  it('keeps pipe-separated sinfo output quoted for Windows OpenSSH', () => {
    expect(SINFO_PARTITION_SUMMARY_COMMAND).toBe("sinfo -h -o '%P|%D|%t' | head -n 1");
    expect(SINFO_PARTITION_SUMMARY_COMMAND).not.toContain('-o "%P|%D|%t"');
  });
});
