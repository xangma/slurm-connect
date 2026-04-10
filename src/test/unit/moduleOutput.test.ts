import { describe, expect, it } from 'vitest';

import { parseModulesOutput, sanitizeModuleOutput } from '../../utils/moduleOutput';

describe('moduleOutput utilities', () => {
  it('strips ANSI and OSC escape sequences', () => {
    const dirty = '\u001b]0;title\u0007\u001b[32mfoo/1.0(default)\u001b[0m';
    expect(sanitizeModuleOutput(dirty)).toBe('foo/1.0(default)');
  });

  it('parses module output while skipping legends and deduplicating entries', () => {
    const output = [
      '----- /apps/core -----',
      'foo/1.0(default)   bar/2.0(a)',
      '/apps/extra:',
      'bar/2.0(a)',
      'where:',
      '  D: module is loaded',
      'to get more help use "module help"',
      ''
    ].join('\n');

    expect(parseModulesOutput(output)).toEqual([
      '/apps/core:',
      'foo/1.0 (default)',
      'bar/2.0',
      '/apps/extra:'
    ]);
  });

  it('returns no modules when environment-modules is unavailable', () => {
    expect(parseModulesOutput('bash: module: command not found')).toEqual([]);
  });
});
