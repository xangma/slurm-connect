import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs/promises';

import type { ResolvedSshHostInfo, SlurmConnectConfig } from '../config/types';
import {
  buildSlurmConnectIncludeBlock,
  buildSlurmConnectIncludeContent,
  expandHome,
  formatSshConfigValue,
  SLURM_CONNECT_INCLUDE_END,
  SLURM_CONNECT_INCLUDE_START
} from '../utils/sshConfig';

export interface SshConfigRuntime {
  fileExists(filePath: string): Promise<boolean>;
  getOutputChannel(): { appendLine(message: string): void };
  resolveSshToolPath(tool: 'ssh'): Promise<string>;
  normalizeSshErrorText(error: unknown): string;
  pickSshErrorSummary(text: string): string;
}

export function defaultSshConfigPath(): string {
  return path.join(os.homedir(), '.ssh', 'config');
}

export function resolveSlurmConnectIncludePath(cfg: SlurmConnectConfig): string {
  const trimmed = (cfg.temporarySshConfigPath || '').trim();
  return trimmed || '~/.ssh/slurm-connect.conf';
}

export function normalizeSshPath(value: string): string {
  const expanded = expandHome(value);
  if (!path.isAbsolute(expanded)) {
    return path.resolve(os.homedir(), expanded);
  }
  return path.resolve(expanded);
}

export function resolveSlurmConnectIncludeFilePath(cfg: SlurmConnectConfig): string {
  return normalizeSshPath(resolveSlurmConnectIncludePath(cfg));
}

function detectLineEnding(content: string): string {
  return content.includes('\r\n') ? '\r\n' : '\n';
}

export function splitSshConfigArgs(input: string): string[] {
  const result: string[] = [];
  let current = '';
  let inSingle = false;
  let inDouble = false;
  let escaping = false;
  for (const ch of input) {
    if (escaping) {
      current += ch;
      escaping = false;
      continue;
    }
    if (inSingle) {
      if (ch === '\\') {
        escaping = true;
        continue;
      }
      if (ch === "'") {
        inSingle = false;
      } else {
        current += ch;
      }
      continue;
    }
    if (inDouble) {
      if (ch === '\\') {
        escaping = true;
        continue;
      }
      if (ch === '"') {
        inDouble = false;
      } else {
        current += ch;
      }
      continue;
    }
    if (ch === "'") {
      inSingle = true;
      continue;
    }
    if (ch === '"') {
      inDouble = true;
      continue;
    }
    if (/\s/.test(ch)) {
      if (current.length > 0) {
        result.push(current);
        current = '';
      }
      continue;
    }
    current += ch;
  }
  if (current.length > 0) {
    result.push(current);
  }
  return result;
}

function uniqueList(values: string[]): string[] {
  return Array.from(new Set(values.map((value) => value.trim()).filter(Boolean)));
}

function extractIncludeTargets(line: string): string[] {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith('#')) {
    return [];
  }
  const match = /^Include\s+(.+)$/i.exec(trimmed);
  if (!match) {
    return [];
  }
  return splitSshConfigArgs(match[1]);
}

function sshConfigHasIncludePath(content: string, includePath: string): boolean {
  const target = normalizeSshPath(includePath);
  const lines = content.split(/\r?\n/);
  for (const line of lines) {
    const targets = extractIncludeTargets(line);
    for (const candidate of targets) {
      if (normalizeSshPath(candidate) === target) {
        return true;
      }
    }
  }
  return false;
}

function parseSshConfigIncludePaths(content: string): string[] {
  const results: string[] = [];
  const lines = content.split(/\r?\n/);
  for (const line of lines) {
    const targets = extractIncludeTargets(line);
    if (targets.length > 0) {
      results.push(...targets);
    }
  }
  return results;
}

function isSshHostPattern(value: string): boolean {
  return value.startsWith('!') || /[*?\[]/.test(value);
}

function parseSshConfigHosts(content: string): string[] {
  const results: string[] = [];
  const lines = content.split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }
    const match = /^Host\s+(.+)$/i.exec(trimmed);
    if (!match) {
      continue;
    }
    const tokens = splitSshConfigArgs(match[1]);
    for (const token of tokens) {
      if (!token || isSshHostPattern(token)) {
        continue;
      }
      results.push(token);
    }
  }
  return uniqueList(results);
}

function hasGlobPattern(value: string): boolean {
  return /[*?\[]/.test(value);
}

function globSegmentToRegex(segment: string): RegExp {
  const escaped = segment.replace(/[.+^${}()|\\]/g, '\\$&');
  const withWildcards = escaped.replace(/\\\*/g, '.*').replace(/\\\?/g, '.');
  return new RegExp(`^${withWildcards}$`);
}

async function expandGlobPattern(pattern: string): Promise<string[]> {
  const resolved = path.resolve(pattern);
  const root = path.parse(resolved).root;
  const relative = resolved.slice(root.length);
  const segments = relative.split(path.sep).filter(Boolean);
  let candidates = [root];
  for (const segment of segments) {
    if (!hasGlobPattern(segment)) {
      candidates = candidates.map((base) => path.join(base, segment));
      continue;
    }
    const regex = globSegmentToRegex(segment);
    const next: string[] = [];
    for (const base of candidates) {
      let entries: Array<{ name: string }> = [];
      try {
        entries = await fs.readdir(base, { withFileTypes: true });
      } catch {
        continue;
      }
      for (const entry of entries) {
        if (regex.test(entry.name)) {
          next.push(path.join(base, entry.name));
        }
      }
    }
    candidates = next;
  }
  const results: string[] = [];
  for (const candidate of candidates) {
    try {
      const stat = await fs.stat(candidate);
      if (stat.isFile()) {
        results.push(candidate);
      }
    } catch {
      // Ignore missing files.
    }
  }
  return results;
}

async function expandSshIncludeTarget(value: string, baseDir: string, runtime: Pick<SshConfigRuntime, 'fileExists'>): Promise<string[]> {
  const expanded = expandHome(value);
  const resolved = path.isAbsolute(expanded) ? expanded : path.resolve(baseDir, expanded);
  if (hasGlobPattern(resolved)) {
    return await expandGlobPattern(resolved);
  }
  return (await runtime.fileExists(resolved)) ? [resolved] : [];
}

export async function collectSshConfigHosts(
  configPath: string | undefined,
  runtime: Pick<SshConfigRuntime, 'fileExists'>,
  seen = new Set<string>()
): Promise<string[]> {
  if (!configPath) {
    return [];
  }
  const resolved = normalizeSshPath(configPath);
  if (seen.has(resolved)) {
    return [];
  }
  seen.add(resolved);
  let content = '';
  try {
    content = await fs.readFile(resolved, 'utf8');
  } catch {
    return [];
  }
  const hosts = parseSshConfigHosts(content);
  const includeTargets = parseSshConfigIncludePaths(content);
  if (includeTargets.length === 0) {
    return hosts;
  }
  const baseDir = path.dirname(resolved);
  for (const target of includeTargets) {
    const expandedPaths = await expandSshIncludeTarget(target, baseDir, runtime);
    for (const includePath of expandedPaths) {
      const nested = await collectSshConfigHosts(includePath, runtime, seen);
      hosts.push(...nested);
    }
  }
  return uniqueList(hosts);
}

interface ExplicitSshHostConfig {
  user?: string;
  identityFiles: string[];
  hasExplicitHost: boolean;
}

function normalizeHostValue(value: string): string {
  return value.trim().toLowerCase();
}

function isExplicitHostToken(token: string): boolean {
  return !isSshHostPattern(token);
}

function matchesExplicitHostToken(host: string, token: string): boolean {
  return normalizeHostValue(host) === normalizeHostValue(token);
}

function isHostExplicitMatch(host: string, tokens: string[]): boolean {
  let explicitMatch = false;
  for (const raw of tokens) {
    if (!raw) {
      continue;
    }
    if (raw.startsWith('!')) {
      const negated = raw.slice(1);
      if (isExplicitHostToken(negated) && matchesExplicitHostToken(host, negated)) {
        return false;
      }
      continue;
    }
    if (isExplicitHostToken(raw) && matchesExplicitHostToken(host, raw)) {
      explicitMatch = true;
    }
  }
  return explicitMatch;
}

function parseSshConfigKeyValue(line: string): { key: string; values: string[] } | undefined {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith('#')) {
    return undefined;
  }
  const match = /^(\S+)\s+(.+)$/.exec(trimmed);
  if (!match) {
    return undefined;
  }
  const key = match[1].toLowerCase();
  const values = splitSshConfigArgs(match[2]);
  return { key, values };
}

async function collectExplicitHostConfig(
  configPath: string | undefined,
  host: string,
  runtime: Pick<SshConfigRuntime, 'fileExists'>,
  seen = new Set<string>()
): Promise<ExplicitSshHostConfig> {
  const explicit: ExplicitSshHostConfig = { identityFiles: [], hasExplicitHost: false };
  if (!configPath) {
    return explicit;
  }
  const resolved = normalizeSshPath(configPath);
  if (seen.has(resolved)) {
    return explicit;
  }
  seen.add(resolved);
  let content = '';
  try {
    content = await fs.readFile(resolved, 'utf8');
  } catch {
    return explicit;
  }
  const lines = content.split(/\r?\n/);
  let inExplicitHost = false;
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }
    const includeTargets = extractIncludeTargets(line);
    if (includeTargets.length > 0) {
      if (!inExplicitHost) {
        continue;
      }
      const baseDir = path.dirname(resolved);
      for (const target of includeTargets) {
        const expandedPaths = await expandSshIncludeTarget(target, baseDir, runtime);
        for (const includePath of expandedPaths) {
          const nested = await collectExplicitHostConfig(includePath, host, runtime, seen);
          if (!explicit.user && nested.user) {
            explicit.user = nested.user;
          }
          if (nested.identityFiles.length > 0) {
            explicit.identityFiles.push(...nested.identityFiles);
          }
          if (nested.hasExplicitHost) {
            explicit.hasExplicitHost = true;
          }
        }
      }
      continue;
    }
    const hostMatch = /^Host\s+(.+)$/i.exec(trimmed);
    if (hostMatch) {
      const tokens = splitSshConfigArgs(hostMatch[1]);
      inExplicitHost = isHostExplicitMatch(host, tokens);
      if (inExplicitHost) {
        explicit.hasExplicitHost = true;
      }
      continue;
    }
    if (/^Match\b/i.test(trimmed)) {
      inExplicitHost = false;
      continue;
    }
    if (!inExplicitHost) {
      continue;
    }
    const parsed = parseSshConfigKeyValue(trimmed);
    if (!parsed) {
      continue;
    }
    if (parsed.key === 'user') {
      const value = parsed.values[0];
      if (value && !explicit.user) {
        explicit.user = value;
      }
      continue;
    }
    if (parsed.key === 'identityfile') {
      for (const value of parsed.values) {
        if (!value || value.toLowerCase() === 'none') {
          continue;
        }
        explicit.identityFiles.push(value);
      }
      continue;
    }
  }
  explicit.identityFiles = uniqueList(explicit.identityFiles);
  return explicit;
}

export async function resolveSshHostsConfigPath(
  cfg: SlurmConnectConfig,
  runtime: Pick<SshConfigRuntime, 'fileExists'>
): Promise<string | undefined> {
  const override = (cfg.sshQueryConfigPath || '').trim();
  const basePath = override ? normalizeSshPath(override) : normalizeSshPath(defaultSshConfigPath());
  return (await runtime.fileExists(basePath)) ? basePath : undefined;
}

function parseSshConfigOutput(output: string): Record<string, string[]> {
  const result: Record<string, string[]> = {};
  const lines = output.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  for (const line of lines) {
    const parts = line.split(/\s+/);
    if (parts.length < 2) {
      continue;
    }
    const key = parts.shift();
    if (!key) {
      continue;
    }
    const value = parts.join(' ').trim();
    if (!value) {
      continue;
    }
    const lowerKey = key.toLowerCase();
    if (!result[lowerKey]) {
      result[lowerKey] = [];
    }
    result[lowerKey].push(value);
  }
  return result;
}

function pickFirstValue(values?: string[]): string | undefined {
  if (!values || values.length === 0) {
    return undefined;
  }
  const trimmed = values.map((value) => value.trim()).filter(Boolean);
  return trimmed.length > 0 ? trimmed[0] : undefined;
}

async function pickFirstExistingPath(
  values: string[] | undefined,
  runtime: Pick<SshConfigRuntime, 'fileExists'>
): Promise<string | undefined> {
  if (!values || values.length === 0) {
    return undefined;
  }
  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed) {
      continue;
    }
    const expanded = expandHome(trimmed);
    if (await runtime.fileExists(expanded)) {
      return trimmed;
    }
  }
  return pickFirstValue(values);
}

export async function resolveSshHostFromConfig(
  host: string,
  cfg: SlurmConnectConfig,
  runtime: SshConfigRuntime
): Promise<ResolvedSshHostInfo> {
  const configPath = await resolveSshHostsConfigPath(cfg, runtime);
  const sshPath = await runtime.resolveSshToolPath('ssh');
  const args: string[] = [];
  if (configPath) {
    args.push('-F', configPath);
  }
  args.push('-G', host);
  try {
    const { stdout } = await (await import('util')).promisify((await import('child_process')).execFile)(sshPath, args);
    const parsed = parseSshConfigOutput(stdout);
    const hostname = pickFirstValue(parsed.hostname);
    const port = pickFirstValue(parsed.port);
    const explicit = await collectExplicitHostConfig(configPath, host, runtime);
    const user = explicit.user;
    const identityFile = explicit.identityFiles.length > 0
      ? await pickFirstExistingPath(explicit.identityFiles, runtime)
      : undefined;
    const certificateFile = await pickFirstExistingPath(parsed.certificatefile, runtime);
    const proxyCommand = pickFirstValue(parsed.proxycommand);
    const proxyJump = pickFirstValue(parsed.proxyjump);
    const hasProxyCommand = Boolean(proxyCommand && proxyCommand.toLowerCase() !== 'none');
    const hasProxyJump = Boolean(proxyJump && proxyJump.toLowerCase() !== 'none');
    return {
      host,
      hostname,
      user,
      port,
      identityFile,
      certificateFile,
      hasProxyCommand,
      hasProxyJump,
      hasExplicitHost: explicit.hasExplicitHost
    };
  } catch (error) {
    const summary = runtime.pickSshErrorSummary(runtime.normalizeSshErrorText(error));
    throw new Error(summary || 'Failed to resolve SSH host.');
  }
}

function removeIncludePathFromLines(
  content: string,
  includePath: string
): { content: string; removed: boolean } {
  const lineEnding = detectLineEnding(content);
  const lines = content.split(/\r?\n/);
  const target = normalizeSshPath(includePath);
  let removed = false;
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = lines[i];
    const targets = extractIncludeTargets(line);
    if (targets.length === 0) {
      continue;
    }
    const remaining = targets.filter((candidate) => normalizeSshPath(candidate) !== target);
    if (remaining.length === targets.length) {
      continue;
    }
    removed = true;
    if (remaining.length === 0) {
      lines.splice(i, 1);
      continue;
    }
    const indentMatch = line.match(/^(\s*)Include\b/i);
    const indent = indentMatch ? indentMatch[1] : '';
    const rebuilt = `${indent}Include ${remaining.map((value) => formatSshConfigValue(value)).join(' ')}`;
    lines[i] = rebuilt;
  }
  return { content: lines.join(lineEnding), removed };
}

function stripManagedIncludeBlock(content: string): { content: string; removed: boolean } {
  const blockRegex = new RegExp(
    `${SLURM_CONNECT_INCLUDE_START.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}[\\s\\S]*?${SLURM_CONNECT_INCLUDE_END.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`,
    'm'
  );
  if (!blockRegex.test(content)) {
    return { content, removed: false };
  }
  return { content: content.replace(blockRegex, ''), removed: true };
}

function ensureTrailingNewline(value: string, lineEnding: string): string {
  if (!value) {
    return value;
  }
  return value.endsWith(lineEnding) ? value : `${value}${lineEnding}`;
}

async function createSshConfigBackupPath(baseConfigPath: string): Promise<string> {
  const dir = path.dirname(baseConfigPath);
  const baseName = path.basename(baseConfigPath);
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  let candidate = path.join(dir, `${baseName}.slurm-connect.backup-${timestamp}`);
  let counter = 1;
  while (true) {
    try {
      await fs.access(candidate);
      candidate = path.join(dir, `${baseName}.slurm-connect.backup-${timestamp}-${counter}`);
      counter += 1;
    } catch {
      break;
    }
  }
  return candidate;
}

async function backupSshConfigFile(
  baseConfigPath: string,
  content: string,
  runtime: Pick<SshConfigRuntime, 'getOutputChannel'>
): Promise<void> {
  const backupPath = await createSshConfigBackupPath(baseConfigPath);
  await fs.writeFile(backupPath, content, 'utf8');
  runtime.getOutputChannel().appendLine(`Backed up SSH config to ${backupPath}.`);
}

export async function ensureSshIncludeInstalled(
  baseConfigPath: string,
  includePath: string,
  runtime: Pick<SshConfigRuntime, 'getOutputChannel'>
): Promise<'added' | 'updated' | 'already'> {
  const resolvedBase = normalizeSshPath(baseConfigPath);
  const resolvedInclude = normalizeSshPath(includePath);
  if (resolvedBase === resolvedInclude) {
    throw new Error('Slurm Connect include path must not be the same as the SSH config path.');
  }
  let content = '';
  let fileExists = true;
  try {
    content = await fs.readFile(resolvedBase, 'utf8');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
      throw error;
    }
    fileExists = false;
  }
  const lineEnding = detectLineEnding(content);
  const block = buildSlurmConnectIncludeBlock(includePath).split('\n').join(lineEnding);
  const hadInclude = sshConfigHasIncludePath(content, includePath);
  const stripped = stripManagedIncludeBlock(content);
  const withoutBlock = stripped.content;
  const withoutTarget = removeIncludePathFromLines(withoutBlock, includePath);
  const bom = withoutTarget.content.startsWith('\uFEFF') ? '\uFEFF' : '';
  const bodyRaw = bom ? withoutTarget.content.slice(1) : withoutTarget.content;
  const body = bodyRaw.replace(/^[\r\n]+/, '');
  const combined = body ? `${bom}${block}${lineEnding}${lineEnding}${body}` : `${bom}${block}${lineEnding}`;
  const next = ensureTrailingNewline(combined, lineEnding);
  let status: 'added' | 'updated' | 'already' = 'already';
  if (!stripped.removed && !hadInclude) {
    status = 'added';
  } else if (next === content) {
    status = 'already';
  } else {
    status = 'updated';
  }
  if (next !== content) {
    await fs.mkdir(path.dirname(resolvedBase), { recursive: true });
    if (fileExists) {
      await backupSshConfigFile(resolvedBase, content, runtime);
    }
    await fs.writeFile(resolvedBase, ensureTrailingNewline(next, lineEnding), 'utf8');
  }
  return status;
}

export async function writeSlurmConnectIncludeFile(entry: string, includePath: string): Promise<string> {
  const dir = path.dirname(includePath);
  await fs.mkdir(dir, { recursive: true });
  const content = buildSlurmConnectIncludeContent(entry);
  await fs.writeFile(includePath, content, 'utf8');
  return includePath;
}

export function normalizeRemoteConfigPath(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

export async function resolveRemoteConfigContext(
  cfg: SlurmConnectConfig,
  runtime: Pick<SshConfigRuntime, 'fileExists' | 'getOutputChannel'>,
  currentRemoteConfig?: string
): Promise<{
  baseConfigPath: string;
  includePath: string;
  includeFilePath: string;
}> {
  const baseConfigPath = normalizeSshPath(currentRemoteConfig || defaultSshConfigPath());
  if (!currentRemoteConfig) {
    try {
      await fs.access(baseConfigPath);
    } catch {
      runtime.getOutputChannel().appendLine(`SSH config not found at ${baseConfigPath}; creating.`);
    }
  }
  const includePath = resolveSlurmConnectIncludePath(cfg);
  const includeFilePath = resolveSlurmConnectIncludeFilePath(cfg);
  return { baseConfigPath, includePath, includeFilePath };
}
