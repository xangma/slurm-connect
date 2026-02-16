import * as assert from 'assert';
import * as os from 'os';
import * as path from 'path';
import {
  buildHostEntry,
  buildSlurmConnectIncludeBlock,
  buildSlurmConnectIncludeContent,
  buildTemporarySshConfigContent,
  expandHome,
  formatSshConfigValue
} from '../utils/sshConfig';
import { parsePartitionInfoOutput } from '../utils/clusterInfo';
import { joinShellCommand, splitShellArgs } from '../utils/shellArgs';

function testFormatSshConfigValue(): void {
  assert.strictEqual(formatSshConfigValue('simple'), 'simple');
  assert.strictEqual(formatSshConfigValue('path with space'), '"path with space"');
  assert.strictEqual(formatSshConfigValue('"already quoted"'), '"already quoted"');

  const windowsPath = 'C:\\Users\\Test User\\id_rsa';
  assert.strictEqual(
    formatSshConfigValue(windowsPath),
    '"C:\\\\Users\\\\Test User\\\\id_rsa"'
  );
}

function testBuildHostEntryQuotes(): void {
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
  assert.ok(entry.includes('IdentityFile "/Users/Test User/.ssh/id_rsa"'));
  assert.ok(entry.includes('LocalCommand "echo hello world"'));
  assert.ok(entry.includes('RemoteForward 0.0.0.0:1234 127.0.0.1:5678'));
}

function testBuildTemporarySshConfigContentIncludes(): void {
  const content = buildTemporarySshConfigContent('Host alias\n  HostName login', [
    '/Users/Test User/.ssh/config'
  ]);
  assert.ok(content.includes('Include "/Users/Test User/.ssh/config"'));
}

function testBuildSlurmConnectIncludeHelpers(): void {
  const block = buildSlurmConnectIncludeBlock('/Users/Test User/.ssh/slurm-connect.conf');
  assert.ok(block.includes('Slurm Connect'));
  assert.ok(block.includes('Include "/Users/Test User/.ssh/slurm-connect.conf"'));

  const content = buildSlurmConnectIncludeContent('Host alias\n  HostName login');
  assert.ok(content.startsWith('# Slurm Connect SSH hosts'));
}

function testExpandHome(): void {
  const home = os.homedir();
  assert.strictEqual(expandHome('~'), home);
  assert.strictEqual(expandHome('~/config'), path.join(home, 'config'));
}

function testParsePartitionInfoOutputNodeNames(): void {
  const output = 'gpu*|2gpu-01|64|128000|gpu:a100:4';
  const info = parsePartitionInfoOutput(output);
  assert.strictEqual(info.partitions[0].nodes, 1);

  const outputWithCount = 'cpu|10|32|64000|';
  const infoWithCount = parsePartitionInfoOutput(outputWithCount);
  assert.strictEqual(infoWithCount.partitions[0].nodes, 10);
}

function testParsePartitionInfoOutputNoGpuPartition(): void {
  const output = 'lrd_all_serial*|node001|32|256000|';
  const info = parsePartitionInfoOutput(output);
  assert.strictEqual(info.defaultPartition, 'lrd_all_serial');
  assert.strictEqual(info.partitions.length, 1);
  assert.strictEqual(info.partitions[0].name, 'lrd_all_serial');
  assert.strictEqual(info.partitions[0].gpuMax, 0);
  assert.deepStrictEqual(info.partitions[0].gpuTypes, {});
}

function testParsePartitionInfoOutputGpuPartitionMax(): void {
  const output = [
    'gpuq|node001|64|256000|gpu:a100:4',
    'gpuq|node002|64|256000|gpu:a100:8'
  ].join('\n');
  const info = parsePartitionInfoOutput(output);
  assert.strictEqual(info.partitions.length, 1);
  assert.strictEqual(info.partitions[0].gpuMax, 8);
  assert.strictEqual(info.partitions[0].gpuTypes.a100, 8);
}

function testSplitShellArgsQuoted(): void {
  const tokens = splitShellArgs('--comment "foo bar" --mem=32G');
  assert.deepStrictEqual(tokens, ['--comment', 'foo bar', '--mem=32G']);
}

function testSplitShellArgsComma(): void {
  const tokens = splitShellArgs('--gres=gpu:1, --mem=32G');
  assert.deepStrictEqual(tokens, ['--gres=gpu:1', '--mem=32G']);
}

function testJoinShellCommandQuotes(): void {
  const cmd = joinShellCommand(['python', '/path with space', '--session-state-dir=/tmp/foo bar']);
  assert.strictEqual(cmd, "python '/path with space' '--session-state-dir=/tmp/foo bar'");
}

function testJoinShellCommandOperators(): void {
  const cmd = joinShellCommand(['echo', 'hi', '&&', 'echo', 'bye']);
  assert.strictEqual(cmd, 'echo hi && echo bye');
}

function run(): void {
  testFormatSshConfigValue();
  testBuildHostEntryQuotes();
  testBuildTemporarySshConfigContentIncludes();
  testBuildSlurmConnectIncludeHelpers();
  testExpandHome();
  testParsePartitionInfoOutputNodeNames();
  testParsePartitionInfoOutputNoGpuPartition();
  testParsePartitionInfoOutputGpuPartitionMax();
  testSplitShellArgsQuoted();
  testSplitShellArgsComma();
  testJoinShellCommandQuotes();
  testJoinShellCommandOperators();
  console.log('extension tests passed');
}

run();
