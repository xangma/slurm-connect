import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';

const root = process.cwd();

function readVsCodeIgnore(): string[] {
  const ignorePath = path.join(root, '.vscodeignore');
  expect(fs.existsSync(ignorePath)).toBe(true);

  return fs.readFileSync(ignorePath, 'utf8')
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith('#'));
}

describe('VSIX package manifest', () => {
  it('keeps package metadata versions in sync', () => {
    const manifest = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8')) as {
      version: string;
    };
    const lockfile = JSON.parse(fs.readFileSync(path.join(root, 'package-lock.json'), 'utf8')) as {
      version: string;
      packages: Record<string, { version?: string }>;
    };

    expect(lockfile.version).toBe(manifest.version);
    expect(lockfile.packages['']?.version).toBe(manifest.version);
  });

  it('does not reference SVG images from the Marketplace README', () => {
    const readme = fs.readFileSync(path.join(root, 'README.md'), 'utf8');

    expect(readme).not.toMatch(/<img\b[^>]*\bsrc=["'][^"']+\.svg(?:[#?][^"']*)?["']/iu);
    expect(readme).not.toMatch(/!\[[^\]]*\]\([^)]*\.svg(?:[#?][^)]*)?\)/iu);
  });

  it('excludes local-only and generated artifacts from release packages', () => {
    const ignored = readVsCodeIgnore();
    const requiredIgnores = [
      '.e2e/**',
      '.vscode/**',
      '.pytest_cache/**',
      '.ruff_cache/**',
      '.coverage',
      'coverage/**',
      'src/**',
      'tests/**',
      'test-fixtures/**',
      'scripts/**',
      'docs/release.md',
      '**/__pycache__/**',
      '**/*.pyc',
      'media/*.emf',
      'media/remote_ssh.svg',
      'media/slurm-connect-ui-thumb.png',
      'out/test/**',
      'out/**/*.map',
      'out/**/* *.js',
    ];

    for (const pattern of requiredIgnores) {
      expect(ignored, `missing ${pattern}`).toContain(pattern);
    }
  });

  it('keeps runtime dependencies and shipped proxy files eligible for packaging', () => {
    const ignored = readVsCodeIgnore();

    expect(ignored).not.toContain('out/**');
    expect(ignored).not.toContain('media/**');
    expect(ignored).toContain('node_modules/**');
    expect(ignored).toContain('!node_modules/jsonc-parser/**');
  });
});
