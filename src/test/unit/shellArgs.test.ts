import { describe, expect, it } from 'vitest';

import { joinShellCommand, splitShellArgs } from '../../utils/shellArgs';

describe('shellArgs utilities', () => {
  it('splits quoted shell arguments', () => {
    expect(splitShellArgs('--comment "foo bar" --mem=32G')).toEqual([
      '--comment',
      'foo bar',
      '--mem=32G'
    ]);
  });

  it('splits loose comma-separated tokens', () => {
    expect(splitShellArgs('--gres=gpu:1, --mem=32G')).toEqual(['--gres=gpu:1', '--mem=32G']);
  });

  it('quotes arguments when joining shell commands', () => {
    expect(joinShellCommand(['python', '/path with space', '--session-state-dir=/tmp/foo bar'])).toBe(
      "python '/path with space' '--session-state-dir=/tmp/foo bar'"
    );
  });

  it('preserves shell operators when joining commands', () => {
    expect(joinShellCommand(['echo', 'hi', '&&', 'echo', 'bye'])).toBe('echo hi && echo bye');
  });
});
