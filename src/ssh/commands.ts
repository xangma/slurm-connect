import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs/promises';
import { execFile, spawn } from 'child_process';
import { promisify } from 'util';

import { expandHome } from '../utils/sshConfig';
import type { SlurmConnectConfig } from '../config/types';

const execFileAsync = promisify(execFile);

export type SshToolName = 'ssh' | 'ssh-add' | 'ssh-keygen';

export interface StoredSshAgentEnv {
  sock: string;
  pid?: string;
}

type SshAuthMode = 'agent' | 'terminal';

type SshAuthRetry =
  | { kind: 'agent' }
  | { kind: 'terminal' };

type SshAgentStatus = 'available' | 'empty' | 'unavailable';

interface SshAgentInfo {
  status: SshAgentStatus;
  output: string;
}

enum SshCommandKind {
  NotFound = 0,
  WindowsSsh = 1,
  Other = 2
}

export interface SshCommandDependencies {
  getOutputChannel(): vscode.OutputChannel;
  fileExists(filePath: string): Promise<boolean>;
  getStoredSshAgentEnv(): StoredSshAgentEnv | undefined;
  storeSshAgentEnv(env: { sock: string; pid?: string }): void;
  formatError(error: unknown): string;
  refreshAgentStatus(identityFile: string): Promise<void>;
  openSlurmConnectView(): Promise<void>;
  ensurePreSshCommand(cfg: SlurmConnectConfig, reason: string): Promise<void>;
  runSshCommandInTerminal(
    host: string,
    cfg: SlurmConnectConfig,
    command: string,
    input?: string
  ): Promise<string>;
  runSshAddInTerminal(identityPath: string): Promise<void>;
  runSshKeygenPublicKeyInTerminal(identityPath: string): Promise<void>;
}

export interface SshCommandRuntime {
  normalizeSshErrorText(error: unknown): string;
  getSshAgentEnv(): { sock: string; pid: string };
  isGitSshPath(value: string): boolean;
  hasSshOption(options: Record<string, string> | undefined, key: string): boolean;
  resolveSshToolPath(tool: SshToolName): Promise<string>;
  ensureSshAgentEnvForCurrentSsh(identityPathRaw?: string): Promise<void>;
  pickSshErrorSummary(text: string): string;
  buildAgentStatusMessage(identityPathRaw: string): Promise<{ text: string; isError: boolean }>;
  maybePromptForSshAuthOnConnect(cfg: SlurmConnectConfig, loginHost: string): Promise<boolean>;
  runSshCommand(host: string, cfg: SlurmConnectConfig, command: string): Promise<string>;
  runSshCommandWithInput(
    host: string,
    cfg: SlurmConnectConfig,
    command: string,
    input: string
  ): Promise<string>;
  resolveSshIdentityAgentOption(sshPath: string): string | undefined;
  resetState(): void;
}

const WINDOWS_OPENSSH_AGENT_PIPE = '\\\\.\\pipe\\openssh-ssh-agent';
const SSH_VERSION_TIMEOUT_MS = 10_000;
const HAS_WOW64 = Object.prototype.hasOwnProperty.call(process.env, 'PROCESSOR_ARCHITEW6432');

export function createSshCommandRuntime(deps: SshCommandDependencies): SshCommandRuntime {
  const sshToolPathCache: Partial<Record<SshToolName, string>> = {};
  let cachedRemoteSshPathSetting = '';
  let cachedRemoteUseLocalServer: boolean | undefined;
  let lastSshAuthPrompt: { identityPath: string; timestamp: number; mode: SshAuthMode } | undefined;

  function normalizeSshErrorText(error: unknown): string {
    if (error && typeof error === 'object') {
      const err = error as { stderr?: string; stdout?: string; message?: string };
      return [err.message, err.stderr, err.stdout].filter(Boolean).join('\n');
    }
    return String(error);
  }

  function getSshAgentEnv(): { sock: string; pid: string } {
    return {
      sock: process.env.SSH_AUTH_SOCK || '',
      pid: process.env.SSH_AGENT_PID || ''
    };
  }

  function isWindowsNamedPipe(value: string): boolean {
    const lower = value.toLowerCase();
    return lower.startsWith('\\\\.\\pipe\\') || lower.startsWith('//./pipe/');
  }

  function stripOuterQuotes(value: string): string {
    const trimmed = value.trim();
    if (trimmed.length < 2) {
      return trimmed;
    }
    const first = trimmed[0];
    const last = trimmed[trimmed.length - 1];
    if ((first === '"' && last === '"') || (first === "'" && last === "'")) {
      return trimmed.slice(1, -1);
    }
    return trimmed;
  }

  function resolveRemoteSshPathSetting(): string {
    const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
    const configured = String(remoteCfg.get<string>('path') || '').trim();
    return configured ? stripOuterQuotes(configured) : '';
  }

  function isGitSshPath(value: string): boolean {
    const normalized = value.toLowerCase();
    return normalized.includes('\\git\\') || normalized.includes('/git/');
  }

  async function resolveGitSshAgentPath(sshPath: string): Promise<string | undefined> {
    if (!sshPath) {
      return undefined;
    }
    const candidate = path.join(path.dirname(sshPath), 'ssh-agent.exe');
    if (await deps.fileExists(candidate)) {
      return candidate;
    }
    return undefined;
  }

  function hasSshOption(
    options: Record<string, string> | undefined,
    key: string
  ): boolean {
    if (!options) {
      return false;
    }
    const target = key.toLowerCase();
    return Object.keys(options).some((optionKey) => optionKey.toLowerCase() === target);
  }

  function resetSshToolCacheIfNeeded(): void {
    const remoteCfg = vscode.workspace.getConfiguration('remote.SSH');
    const pathSetting = String(remoteCfg.get<string>('path') || '').trim();
    const useLocalServer = remoteCfg.get<boolean>('useLocalServer', false);
    if (pathSetting === cachedRemoteSshPathSetting && useLocalServer === cachedRemoteUseLocalServer) {
      return;
    }
    cachedRemoteSshPathSetting = pathSetting;
    cachedRemoteUseLocalServer = useLocalServer;
    for (const key of Object.keys(sshToolPathCache)) {
      delete sshToolPathCache[key as SshToolName];
    }
  }

  function collectWindowsSshCandidates(): string[] {
    const candidates: string[] = [];
    const pathEnv = process.env.PATH;
    if (pathEnv) {
      const parts = pathEnv
        .split(';')
        .map((entry) => entry.trim())
        .filter(Boolean)
        .filter((entry) => path.isAbsolute(entry))
        .map((entry) => path.join(entry, 'ssh.exe'));
      candidates.push(...parts);
    }
    if (process.env.windir) {
      candidates.push(path.join(process.env.windir, 'System32', 'OpenSSH', 'ssh.exe'));
    }
    if (process.env.ProgramFiles) {
      candidates.push(path.join(process.env.ProgramFiles, 'Git', 'usr', 'bin', 'ssh.exe'));
    }
    const programFilesX86 = process.env['ProgramFiles(x86)'];
    if (programFilesX86) {
      candidates.push(path.join(programFilesX86, 'Git', 'usr', 'bin', 'ssh.exe'));
    }
    if (process.env.LOCALAPPDATA) {
      candidates.push(path.join(process.env.LOCALAPPDATA, 'Programs', 'Git', 'usr', 'bin', 'ssh.exe'));
    }
    if (HAS_WOW64) {
      const root = process.env.SystemRoot || 'C:\\WINDOWS';
      candidates.unshift(path.join(root, 'Sysnative', 'OpenSSH', 'ssh.exe'));
    }
    const seen = new Set<string>();
    return candidates.filter((candidate) => {
      const key = candidate.toLowerCase();
      if (seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });
  }

  async function detectSshCommandKind(sshCommand: string, log: vscode.OutputChannel): Promise<SshCommandKind> {
    return new Promise((resolve) => {
      log.appendLine(`Checking ssh with "${sshCommand} -V"`);
      const stdoutChunks: Buffer[] = [];
      const stderrChunks: Buffer[] = [];
      let command = sshCommand;
      let options: { shell?: boolean } | undefined;
      if (process.platform === 'win32' && (sshCommand.endsWith('.bat') || sshCommand.endsWith('.cmd'))) {
        command = `"${sshCommand}"`;
        options = { shell: true };
      }
      const child = spawn(command, ['-V'], options);
      let timedOut = false;
      const timeout = setTimeout(() => {
        timedOut = true;
        log.appendLine('ssh is not exiting, continuing');
        resolve(SshCommandKind.NotFound);
        child.kill();
      }, SSH_VERSION_TIMEOUT_MS);
      child.stdout.on('data', (chunk) => {
        stdoutChunks.push(chunk);
      });
      child.stderr.on('data', (chunk) => {
        stderrChunks.push(chunk);
      });
      child.on('error', (error) => {
        if (timedOut) {
          return;
        }
        clearTimeout(timeout);
        log.appendLine(`Got error from ssh: ${error.message}`);
        resolve(SshCommandKind.NotFound);
      });
      child.on('exit', (code) => {
        if (timedOut) {
          return;
        }
        clearTimeout(timeout);
        if (code) {
          log.appendLine(`ssh exited with code: ${code}`);
          resolve(SshCommandKind.NotFound);
          return;
        }
        const output = Buffer.concat([...stdoutChunks, ...stderrChunks]).toString('utf8').trim();
        if (output.match(/OpenSSH_for_Windows/i)) {
          resolve(SshCommandKind.WindowsSsh);
          return;
        }
        if (output.match(/OpenSSH/i)) {
          resolve(SshCommandKind.Other);
          return;
        }
        log.appendLine('ssh output did not match /OpenSSH/');
        resolve(SshCommandKind.NotFound);
      });
    });
  }

  async function isValidSshCommand(sshCommand: string, log: vscode.OutputChannel): Promise<boolean> {
    return (await detectSshCommandKind(sshCommand, log)) !== SshCommandKind.NotFound;
  }

  async function resolveSshCommandPath(): Promise<string> {
    resetSshToolCacheIfNeeded();
    const cached = sshToolPathCache.ssh;
    if (cached) {
      return cached;
    }
    const log = deps.getOutputChannel();
    const configured = resolveRemoteSshPathSetting();
    if (configured) {
      if (await deps.fileExists(configured)) {
        const kind = await detectSshCommandKind(configured, log);
        if (kind === SshCommandKind.NotFound) {
          log.appendLine(`Using configured SSH path despite version check mismatch: ${configured}`);
        }
        sshToolPathCache.ssh = configured;
        return configured;
      }
      const kind = await detectSshCommandKind(configured, log);
      if (kind !== SshCommandKind.NotFound) {
        sshToolPathCache.ssh = configured;
        return configured;
      }
      log.appendLine(`The specified path ${configured} is not a valid SSH binary`);
    }

    if (process.platform !== 'win32') {
      if (await isValidSshCommand('ssh', log)) {
        sshToolPathCache.ssh = 'ssh';
        return 'ssh';
      }
      throw new Error('ssh is not on the PATH');
    }

    const candidates = collectWindowsSshCandidates();
    const useLocalServer = vscode.workspace.getConfiguration('remote.SSH').get<boolean>('useLocalServer', false);
    if (useLocalServer) {
      for (const candidate of candidates) {
        if (await detectSshCommandKind(candidate, log) === SshCommandKind.Other) {
          sshToolPathCache.ssh = candidate;
          return candidate;
        }
        log.appendLine('Preferring non-Windows OpenSSH, skipping');
      }
    }
    for (const candidate of candidates) {
      if (await isValidSshCommand(candidate, log)) {
        sshToolPathCache.ssh = candidate;
        return candidate;
      }
    }

    throw new Error('ssh installation not found');
  }

  async function resolveSshToolPath(tool: SshToolName): Promise<string> {
    resetSshToolCacheIfNeeded();
    const cached = sshToolPathCache[tool];
    if (cached) {
      return cached;
    }
    const sshPath = await resolveSshCommandPath();
    sshToolPathCache.ssh = sshPath;
    if (tool === 'ssh') {
      return sshPath;
    }
    if (sshPath === 'ssh') {
      sshToolPathCache[tool] = tool;
      return tool;
    }
    const ext = path.extname(sshPath);
    const candidate = path.join(path.dirname(sshPath), ext ? `${tool}${ext}` : tool);
    if (await deps.fileExists(candidate)) {
      sshToolPathCache[tool] = candidate;
      return candidate;
    }
    sshToolPathCache[tool] = candidate;
    return candidate;
  }

  function pickSshErrorSummary(text: string): string {
    const lines = text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
    if (lines.length === 0) {
      return text.trim();
    }
    const patterns = [
      /permission denied/i,
      /no identities/i,
      /could not open a connection to your authentication agent/i,
      /agent has no identities/i,
      /sign_and_send_pubkey/i,
      /identity file .* not accessible/i
    ];
    for (const line of lines) {
      if (patterns.some((pattern) => pattern.test(line))) {
        return line;
      }
    }
    return lines[lines.length - 1];
  }

  function looksLikeAuthFailure(text: string): boolean {
    return [
      /permission denied/i,
      /no identities/i,
      /authentication agent/i,
      /read_passphrase/i,
      /enter passphrase/i,
      /sign_and_send_pubkey/i,
      /identity file .* not accessible/i
    ].some((pattern) => pattern.test(text));
  }

  async function execSshWithInput(
    sshPath: string,
    args: string[],
    input: string,
    timeoutMs: number
  ): Promise<{ stdout: string; stderr: string }> {
    return await new Promise((resolve, reject) => {
      const stdoutChunks: Buffer[] = [];
      const stderrChunks: Buffer[] = [];
      const child = spawn(sshPath, args, { windowsHide: true });
      let timedOut = false;

      const timeout = setTimeout(() => {
        timedOut = true;
        child.kill();
      }, timeoutMs);

      child.stdout.on('data', (chunk) => stdoutChunks.push(chunk));
      child.stderr.on('data', (chunk) => stderrChunks.push(chunk));
      child.on('error', (error) => {
        clearTimeout(timeout);
        reject(error);
      });
      child.on('close', (code) => {
        clearTimeout(timeout);
        const stdout = Buffer.concat(stdoutChunks).toString('utf8');
        const stderr = Buffer.concat(stderrChunks).toString('utf8');
        if (timedOut) {
          const error = new Error(`SSH command timed out after ${timeoutMs}ms`);
          (error as { stdout?: string; stderr?: string }).stdout = stdout;
          (error as { stdout?: string; stderr?: string }).stderr = stderr;
          reject(error);
          return;
        }
        if (code !== 0) {
          const error = new Error(`SSH command exited with code ${code ?? 'unknown'}`);
          (error as { stdout?: string; stderr?: string }).stdout = stdout;
          (error as { stdout?: string; stderr?: string }).stderr = stderr;
          reject(error);
          return;
        }
        resolve({ stdout, stderr });
      });

      const normalizedInput = input.endsWith('\n') ? input : `${input}\n`;
      child.stdin.write(normalizedInput);
      child.stdin.end();
    });
  }

  function appendSshHostKeyCheckingArgs(args: string[], cfg: SlurmConnectConfig): void {
    args.push('-o', `StrictHostKeyChecking=${cfg.sshHostKeyChecking}`);
  }

  function buildSshArgs(
    host: string,
    cfg: SlurmConnectConfig,
    command: string,
    options: { batchMode: boolean }
  ): string[] {
    const args: string[] = [];
    if (cfg.sshQueryConfigPath) {
      args.push('-F', expandHome(cfg.sshQueryConfigPath));
    }
    args.push('-T', '-o', `BatchMode=${options.batchMode ? 'yes' : 'no'}`, '-o', `ConnectTimeout=${cfg.sshConnectTimeoutSeconds}`);
    appendSshHostKeyCheckingArgs(args, cfg);
    if (cfg.identityFile) {
      args.push('-i', expandHome(cfg.identityFile));
    }
    const target = cfg.user ? `${cfg.user}@${host}` : host;
    args.push(target, command);
    return args;
  }

  function getPublicKeyPath(identityPath: string): string {
    return identityPath.endsWith('.pub') ? identityPath : `${identityPath}.pub`;
  }

  async function hasPublicKeyFile(identityPath: string): Promise<boolean> {
    if (!identityPath) {
      return false;
    }
    return await deps.fileExists(getPublicKeyPath(identityPath));
  }

  async function getPublicKeyFingerprint(identityPath: string): Promise<string | undefined> {
    if (!identityPath) {
      return undefined;
    }
    const pubPath = getPublicKeyPath(identityPath);
    try {
      await fs.access(pubPath);
    } catch {
      return undefined;
    }
    try {
      const sshKeygen = await resolveSshToolPath('ssh-keygen');
      const { stdout } = await execFileAsync(sshKeygen, ['-lf', pubPath, '-E', 'sha256']);
      const match = stdout.match(/SHA256:[A-Za-z0-9+/=]+/);
      return match ? match[0] : undefined;
    } catch {
      return undefined;
    }
  }

  function parsePublicKeyTokens(line: string): { type: string; key: string } | undefined {
    const trimmed = line.trim();
    if (!trimmed) {
      return undefined;
    }
    const parts = trimmed.split(/\s+/);
    if (parts.length < 2) {
      return undefined;
    }
    const [type, key] = parts;
    if (!type || !key) {
      return undefined;
    }
    return { type, key };
  }

  async function readPublicKeyTokens(identityPath: string): Promise<{ type: string; key: string } | undefined> {
    const pubPath = getPublicKeyPath(identityPath);
    try {
      const text = await fs.readFile(pubPath, 'utf8');
      const line = text
        .split(/\r?\n/)
        .map((entry) => entry.trim())
        .find((entry) => entry.length > 0);
      return line ? parsePublicKeyTokens(line) : undefined;
    } catch {
      return undefined;
    }
  }

  async function getSshAgentPublicKeys(): Promise<Array<{ type: string; key: string }>> {
    try {
      const sshAdd = await resolveSshToolPath('ssh-add');
      const result = await execFileAsync(sshAdd, ['-L']);
      const output = [result.stdout, result.stderr].filter(Boolean).join('\n');
      if (!output.trim() || /no identities/i.test(output)) {
        return [];
      }
      return output
        .split(/\r?\n/)
        .map((line) => parsePublicKeyTokens(line))
        .filter((entry): entry is { type: string; key: string } => Boolean(entry));
    } catch {
      return [];
    }
  }

  function escapeRegExp(value: string): string {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  function looksLikeAgentUnavailable(text: string): boolean {
    return [
      /could not open a connection to your authentication agent/i,
      /error connecting to agent/i,
      /communication with agent failed/i,
      /agent refused operation/i,
      /no such file or directory/i
    ].some((pattern) => pattern.test(text));
  }

  async function probeSshAgentWithEnv(
    sshAdd: string,
    env: { sock: string; pid?: string },
    timeoutMs = 3000
  ): Promise<SshAgentInfo> {
    if (!env.sock) {
      return { status: 'unavailable', output: '' };
    }
    const execEnv: NodeJS.ProcessEnv = { ...process.env, SSH_AUTH_SOCK: env.sock };
    if (env.pid) {
      execEnv.SSH_AGENT_PID = env.pid;
    }
    try {
      const result = await execFileAsync(sshAdd, ['-l'], { env: execEnv, timeout: timeoutMs });
      const output = [result.stdout, result.stderr].filter(Boolean).join('\n');
      if (!output.trim() || /no identities/i.test(output)) {
        return { status: 'empty', output };
      }
      return { status: 'available', output };
    } catch (error) {
      const errorText = normalizeSshErrorText(error);
      if (/no identities/i.test(errorText)) {
        return { status: 'empty', output: errorText };
      }
      if (looksLikeAgentUnavailable(errorText)) {
        return { status: 'unavailable', output: errorText };
      }
      return { status: 'unavailable', output: errorText };
    }
  }

  async function ensureSshAgentEnvForCurrentSsh(identityPathRaw?: string): Promise<void> {
    if (process.platform !== 'win32') {
      return;
    }
    const log = deps.getOutputChannel();
    const sshPath = await resolveSshToolPath('ssh');
    if (!isGitSshPath(sshPath)) {
      const existingSock = process.env.SSH_AUTH_SOCK || '';
      const existingPid = process.env.SSH_AGENT_PID || '';
      if (existingSock) {
        let keepSocket = false;
        if (isWindowsNamedPipe(existingSock)) {
          try {
            const sshAdd = await resolveSshToolPath('ssh-add');
            const probe = await probeSshAgentWithEnv(sshAdd, { sock: existingSock, pid: existingPid || undefined });
            keepSocket = probe.status !== 'unavailable';
          } catch {
            keepSocket = false;
          }
        }
        if (!keepSocket) {
          log.appendLine(`Clearing SSH_AUTH_SOCK for Windows OpenSSH (${existingSock}).`);
          delete process.env.SSH_AUTH_SOCK;
          delete process.env.SSH_AGENT_PID;
        }
      } else if (existingPid) {
        log.appendLine('Clearing SSH_AGENT_PID without SSH_AUTH_SOCK for Windows OpenSSH.');
        delete process.env.SSH_AGENT_PID;
      }
      log.appendLine(`Skipping Git ssh-agent setup (SSH path: ${sshPath}).`);
      return;
    }
    const sshAdd = await resolveSshToolPath('ssh-add');
    const identityPath = identityPathRaw ? expandHome(identityPathRaw) : '';
    const identityFingerprint = identityPath ? await getPublicKeyFingerprint(identityPath) : undefined;

    const candidates: Array<{ sock: string; pid?: string; label: string }> = [];
    const seen = new Set<string>();
    const currentEnv = getSshAgentEnv();
    if (currentEnv.sock && !seen.has(currentEnv.sock)) {
      seen.add(currentEnv.sock);
      candidates.push({ sock: currentEnv.sock, pid: currentEnv.pid, label: 'current' });
    }
    const storedEnv = deps.getStoredSshAgentEnv();
    if (storedEnv?.sock && !seen.has(storedEnv.sock)) {
      seen.add(storedEnv.sock);
      candidates.push({ sock: storedEnv.sock, pid: storedEnv.pid, label: 'stored' });
    }
    if (!seen.has(WINDOWS_OPENSSH_AGENT_PIPE)) {
      seen.add(WINDOWS_OPENSSH_AGENT_PIPE);
      candidates.push({ sock: WINDOWS_OPENSSH_AGENT_PIPE, label: 'windows' });
    }

    const probes: Array<{
      candidate: { sock: string; pid?: string; label: string };
      info: SshAgentInfo;
      matchesIdentity: boolean;
    }> = [];

    for (const candidate of candidates) {
      const info = await probeSshAgentWithEnv(sshAdd, candidate);
      if (info.status === 'unavailable') {
        continue;
      }
      const matchesIdentity = identityFingerprint ? info.output.includes(identityFingerprint) : false;
      probes.push({ candidate, info, matchesIdentity });
    }

    if (probes.length > 0) {
      const matched = probes.find((probe) => probe.matchesIdentity);
      const available = probes.find((probe) => probe.info.status === 'available');
      const fallback = probes.find((probe) => probe.info.status === 'empty');
      const selected = matched ?? available ?? fallback;
      if (selected) {
        process.env.SSH_AUTH_SOCK = selected.candidate.sock;
        if (selected.candidate.pid) {
          process.env.SSH_AGENT_PID = selected.candidate.pid;
        } else {
          delete process.env.SSH_AGENT_PID;
        }
        if (selected.candidate.label === 'windows') {
          log.appendLine(`Using Windows OpenSSH agent pipe for Git SSH: ${WINDOWS_OPENSSH_AGENT_PIPE}`);
        } else if (selected.candidate.label === 'stored') {
          log.appendLine(`Restored SSH agent socket from state: ${selected.candidate.sock}`);
        }
        if (selected.info.status === 'available') {
          deps.storeSshAgentEnv({ sock: selected.candidate.sock, pid: selected.candidate.pid });
        }
        return;
      }
    }

    const agentPath = await resolveGitSshAgentPath(sshPath);
    if (!agentPath) {
      log.appendLine(`Git ssh-agent not found next to ${sshPath}.`);
      return;
    }
    try {
      log.appendLine(`Starting Git ssh-agent: ${agentPath}`);
      const result = await execFileAsync(agentPath, ['-s']);
      const output = [result.stdout, result.stderr].filter(Boolean).join('\n');
      const sockMatch = /SSH_AUTH_SOCK=([^;\r\n]+)/.exec(output);
      const pidMatch = /SSH_AGENT_PID=([^;\r\n]+)/.exec(output);
      if (sockMatch && sockMatch[1]) {
        process.env.SSH_AUTH_SOCK = sockMatch[1].trim();
      }
      if (pidMatch && pidMatch[1]) {
        process.env.SSH_AGENT_PID = pidMatch[1].trim();
      }
      if (process.env.SSH_AUTH_SOCK) {
        log.appendLine(`Git SSH_AUTH_SOCK set to ${process.env.SSH_AUTH_SOCK}`);
        deps.storeSshAgentEnv({ sock: process.env.SSH_AUTH_SOCK, pid: process.env.SSH_AGENT_PID });
      } else {
        log.appendLine(`Git ssh-agent did not return SSH_AUTH_SOCK. Output: ${output.trim() || '(empty)'}`);
      }
    } catch {
      log.appendLine('Failed to start Git ssh-agent.');
    }
  }

  async function getSshAgentInfo(identityPathRaw?: string): Promise<SshAgentInfo> {
    try {
      await ensureSshAgentEnvForCurrentSsh(identityPathRaw);
      const sshAdd = await resolveSshToolPath('ssh-add');
      const agentEnv = getSshAgentEnv();
      let info: SshAgentInfo;
      if (!agentEnv.sock && process.platform === 'win32') {
        const sshPath = await resolveSshToolPath('ssh');
        if (!isGitSshPath(sshPath)) {
          try {
            const result = await execFileAsync(sshAdd, ['-l']);
            const output = [result.stdout, result.stderr].filter(Boolean).join('\n');
            if (!output.trim() || /no identities/i.test(output)) {
              info = { status: 'empty', output };
            } else {
              info = { status: 'available', output };
            }
          } catch (error) {
            const errorText = normalizeSshErrorText(error);
            if (/no identities/i.test(errorText)) {
              info = { status: 'empty', output: errorText };
            } else if (looksLikeAgentUnavailable(errorText)) {
              info = { status: 'unavailable', output: errorText };
            } else {
              info = { status: 'unavailable', output: errorText };
            }
          }
        } else {
          info = await probeSshAgentWithEnv(sshAdd, agentEnv);
        }
      } else {
        info = await probeSshAgentWithEnv(sshAdd, agentEnv);
      }
      if (info.status === 'available') {
        deps.storeSshAgentEnv(agentEnv);
      }
      return info;
    } catch (error) {
      const errorText = normalizeSshErrorText(error);
      if (/no identities/i.test(errorText)) {
        return { status: 'empty', output: errorText };
      }
      if (looksLikeAgentUnavailable(errorText)) {
        return { status: 'unavailable', output: errorText };
      }
      return { status: 'unavailable', output: errorText };
    }
  }

  async function isSshKeyListedInAgentOutput(identityPath: string, output: string): Promise<boolean> {
    if (!identityPath || !output.trim() || /no identities/i.test(output)) {
      return false;
    }

    const fingerprint = await getPublicKeyFingerprint(identityPath);
    if (fingerprint) {
      const pattern = new RegExp(escapeRegExp(fingerprint));
      if (pattern.test(output)) {
        return true;
      }
    }
    const basename = path.basename(identityPath);
    if (basename && output.includes(basename)) {
      return true;
    }
    if (output.includes(identityPath)) {
      return true;
    }
    const pubTokens = await readPublicKeyTokens(identityPath);
    if (pubTokens) {
      const agentKeys = await getSshAgentPublicKeys();
      if (agentKeys.some((entry) => entry.type === pubTokens.type && entry.key === pubTokens.key)) {
        return true;
      }
    }
    return false;
  }

  async function buildAgentStatusMessage(identityPathRaw: string): Promise<{ text: string; isError: boolean }> {
    const agentInfo = await getSshAgentInfo(identityPathRaw);
    const agentBase = agentInfo.status === 'available'
      ? 'SSH agent running'
      : agentInfo.status === 'empty'
        ? 'SSH agent running (no keys loaded)'
        : 'SSH agent unavailable';
    if (!identityPathRaw) {
      return { text: `${agentBase}. No identity file set (using SSH agent/config).`, isError: false };
    }
    const identityPath = expandHome(identityPathRaw);
    const identityExists = await deps.fileExists(identityPath);
    if (!identityExists) {
      return { text: `${agentBase}. Identity file not found.`, isError: true };
    }
    const pubExists = await hasPublicKeyFile(identityPath);
    if (agentInfo.status === 'unavailable') {
      return {
        text: `SSH agent unavailable. Passphrase required for ${identityPath}.`,
        isError: true
      };
    }
    if (agentInfo.status === 'empty') {
      return { text: 'SSH agent running, no keys loaded.', isError: true };
    }
    if (!pubExists) {
      return {
        text: `SSH agent running, public key file missing (${getPublicKeyPath(identityPath)}). Generate it to verify the loaded key.`,
        isError: true
      };
    }
    const keyLoaded = await isSshKeyListedInAgentOutput(identityPath, agentInfo.output);
    if (keyLoaded) {
      return { text: 'SSH agent running, key loaded.', isError: false };
    }
    return { text: 'SSH agent running, key not loaded.', isError: true };
  }

  async function maybePromptForSshAuth(
    cfg: SlurmConnectConfig,
    errorText: string
  ): Promise<SshAuthRetry | undefined> {
    if (!looksLikeAuthFailure(errorText)) {
      return undefined;
    }
    const identityPath = cfg.identityFile ? expandHome(cfg.identityFile) : '';
    const identityExists = identityPath ? await deps.fileExists(identityPath) : false;
    const pubExists = identityPath && identityExists ? await hasPublicKeyFile(identityPath) : false;
    const canTerminalPassphrase = Boolean(identityPath && identityExists);
    const agentInfo = identityPath && identityExists ? await getSshAgentInfo(identityPath) : undefined;
    if (identityPath && identityExists && agentInfo?.status === 'available') {
      if (await isSshKeyListedInAgentOutput(identityPath, agentInfo.output)) {
        return undefined;
      }
    }
    const agentStatus = agentInfo?.status ?? 'unavailable';
    const canAddToAgent = Boolean(identityPath && identityExists && agentStatus !== 'unavailable');
    const now = Date.now();
    if (lastSshAuthPrompt) {
      const sameIdentity = lastSshAuthPrompt.identityPath === identityPath;
      const recentlyPrompted = now - lastSshAuthPrompt.timestamp < 60_000;
      if (sameIdentity && recentlyPrompted) {
        if (lastSshAuthPrompt.mode === 'terminal' && canTerminalPassphrase) {
          lastSshAuthPrompt = { identityPath, timestamp: now, mode: 'terminal' };
          return { kind: 'terminal' };
        }
        return undefined;
      }
    }

    const actions: string[] = [];
    if (canTerminalPassphrase) {
      actions.push('Enter Passphrase in Terminal');
    }
    if (canAddToAgent) {
      actions.push('Add to Agent');
    }
    if (identityPath && identityExists && !pubExists) {
      actions.push('Generate Public Key');
    }
    if (!identityPath || !identityExists) {
      actions.push('Open Slurm Connect');
    }
    actions.push('Dismiss');
    const message = identityPath && !identityExists
      ? `SSH authentication failed while querying the cluster. The identity file ${identityPath} was not found. Update it in the Slurm Connect view.`
      : identityPath
        ? agentStatus === 'unavailable'
          ? `SSH authentication failed while querying the cluster. SSH agent is unavailable, enter the passphrase for ${identityPath} in the terminal.`
          : !pubExists
            ? `SSH authentication failed while querying the cluster. Public key file is missing for ${identityPath}; generate it to verify the agent key.`
            : `SSH authentication failed while querying the cluster. Add ${identityPath} to your ssh-agent or enter its passphrase in a terminal.`
        : 'SSH authentication failed while querying the cluster. Ensure your SSH config/agent can authenticate, or set an identity file in the Slurm Connect view.';
    const choice = await vscode.window.showWarningMessage(message, { modal: true }, ...actions);
    const defaultMode: SshAuthMode = canAddToAgent ? 'agent' : 'terminal';
    const mode: SshAuthMode = choice === 'Enter Passphrase in Terminal'
      ? 'terminal'
      : choice === 'Add to Agent'
        ? 'agent'
        : defaultMode;
    lastSshAuthPrompt = { identityPath, timestamp: now, mode };
    if (choice === 'Generate Public Key' && identityPath && identityExists && !pubExists) {
      try {
        await vscode.window.withProgress(
          {
            location: vscode.ProgressLocation.Notification,
            title: 'Generating public key',
            cancellable: false
          },
          async () => await deps.runSshKeygenPublicKeyInTerminal(identityPath)
        );
        await deps.refreshAgentStatus(cfg.identityFile);
        if (agentInfo?.status === 'available') {
          const refreshed = await getSshAgentInfo(identityPath);
          if (await isSshKeyListedInAgentOutput(identityPath, refreshed.output)) {
            return { kind: 'agent' };
          }
        }
      } catch (error) {
        void vscode.window.showWarningMessage(`Failed to generate public key: ${deps.formatError(error)}`);
      }
      return undefined;
    }
    if (choice === 'Enter Passphrase in Terminal' && identityPath && identityExists && canTerminalPassphrase) {
      return { kind: 'terminal' };
    }
    if (choice === 'Add to Agent' && identityPath && canAddToAgent) {
      try {
        await vscode.window.withProgress(
          {
            location: vscode.ProgressLocation.Notification,
            title: 'Waiting for ssh-add to complete',
            cancellable: false
          },
          async () => await deps.runSshAddInTerminal(identityPath)
        );
        await deps.refreshAgentStatus(cfg.identityFile);
        return { kind: 'agent' };
      } catch (error) {
        void vscode.window.showWarningMessage(`ssh-add failed: ${deps.formatError(error)}`);
        return undefined;
      }
    }
    if (choice === 'Open Slurm Connect') {
      await deps.openSlurmConnectView();
    }
    return undefined;
  }

  async function maybePromptForSshAuthOnConnect(cfg: SlurmConnectConfig, loginHost: string): Promise<boolean> {
    const identityPath = cfg.identityFile ? expandHome(cfg.identityFile) : '';
    if (!identityPath) {
      return true;
    }
    const identityExists = await deps.fileExists(identityPath);
    const pubExists = identityExists ? await hasPublicKeyFile(identityPath) : false;
    const agentInfo = identityExists ? await getSshAgentInfo(identityPath) : undefined;
    if (identityExists && agentInfo?.status === 'available') {
      const loaded = await isSshKeyListedInAgentOutput(identityPath, agentInfo.output);
      if (loaded) {
        return true;
      }
    }
    if (identityExists && agentInfo?.status === 'unavailable') {
      return true;
    }

    const now = Date.now();
    if (lastSshAuthPrompt) {
      const sameIdentity = lastSshAuthPrompt.identityPath === identityPath;
      const recentlyPrompted = now - lastSshAuthPrompt.timestamp < 60_000;
      if (sameIdentity && recentlyPrompted) {
        return true;
      }
    }

    const canAddToAgent = Boolean(identityExists && agentInfo?.status !== 'unavailable');

    const actions: string[] = [];
    if (canAddToAgent) {
      actions.push('Add to Agent');
    }
    if (identityExists && !pubExists) {
      actions.push('Generate Public Key');
    }
    if (!identityExists) {
      actions.push('Open Slurm Connect');
    }
    actions.push('Dismiss');

    const message = !identityExists
      ? `SSH identity file ${identityPath} was not found. Update it in the Slurm Connect view.`
      : !pubExists
        ? `Public key file is missing for ${identityPath}; generate it to verify the agent key.`
        : `SSH key is not loaded in the agent. Add ${identityPath} to avoid extra prompts.`;

    const choice = await vscode.window.showWarningMessage(message, { modal: true }, ...actions);
    const mode: SshAuthMode = choice === 'Add to Agent' ? 'agent' : 'terminal';
    lastSshAuthPrompt = { identityPath, timestamp: now, mode };

    if (choice === 'Add to Agent' && canAddToAgent) {
      try {
        await vscode.window.withProgress(
          {
            location: vscode.ProgressLocation.Notification,
            title: 'Waiting for ssh-add to complete',
            cancellable: false
          },
          async () => await deps.runSshAddInTerminal(identityPath)
        );
        await deps.refreshAgentStatus(cfg.identityFile);
      } catch (error) {
        void vscode.window.showWarningMessage(`ssh-add failed: ${deps.formatError(error)}`);
      }
      return true;
    }

    if (choice === 'Generate Public Key' && identityExists && !pubExists) {
      try {
        await vscode.window.withProgress(
          {
            location: vscode.ProgressLocation.Notification,
            title: 'Generating public key',
            cancellable: false
          },
          async () => await deps.runSshKeygenPublicKeyInTerminal(identityPath)
        );
        await deps.refreshAgentStatus(cfg.identityFile);
      } catch (error) {
        void vscode.window.showWarningMessage(`Failed to generate public key: ${deps.formatError(error)}`);
      }
      return true;
    }

    if (choice === 'Open Slurm Connect') {
      await deps.openSlurmConnectView();
      return false;
    }
    return true;
  }

  async function runSshCommand(host: string, cfg: SlurmConnectConfig, command: string): Promise<string> {
    await deps.ensurePreSshCommand(cfg, `SSH query to ${host}`);
    const args = buildSshArgs(host, cfg, command, { batchMode: true });
    const sshPath = await resolveSshToolPath('ssh');

    try {
      const { stdout } = await execFileAsync(sshPath, args, { timeout: cfg.sshConnectTimeoutSeconds * 1000 });
      return stdout.trim();
    } catch (error) {
      const errorText = normalizeSshErrorText(error);
      const retry = await maybePromptForSshAuth(cfg, errorText);
      if (retry?.kind === 'agent') {
        try {
          const { stdout } = await execFileAsync(sshPath, args, { timeout: cfg.sshConnectTimeoutSeconds * 1000 });
          return stdout.trim();
        } catch (retryError) {
          const retryText = normalizeSshErrorText(retryError);
          const retrySummary = pickSshErrorSummary(retryText);
          throw new Error(retrySummary || 'SSH command failed');
        }
      }
      if (retry?.kind === 'terminal') {
        try {
          return await vscode.window.withProgress(
            {
              location: vscode.ProgressLocation.Notification,
              title: 'Waiting for SSH passphrase in terminal',
              cancellable: false
            },
            async () => await deps.runSshCommandInTerminal(host, cfg, command)
          );
        } catch (retryError) {
          const retryText = normalizeSshErrorText(retryError);
          const retrySummary = pickSshErrorSummary(retryText);
          throw new Error(retrySummary || 'SSH command failed');
        }
      }
      const summary = pickSshErrorSummary(errorText);
      throw new Error(summary || 'SSH command failed');
    }
  }

  async function runSshCommandWithInput(
    host: string,
    cfg: SlurmConnectConfig,
    command: string,
    input: string
  ): Promise<string> {
    await deps.ensurePreSshCommand(cfg, `SSH query to ${host}`);
    const args = buildSshArgs(host, cfg, command, { batchMode: true });
    const sshPath = await resolveSshToolPath('ssh');
    const timeoutMs = cfg.sshConnectTimeoutSeconds * 1000;

    const runOnce = async (): Promise<string> => {
      const { stdout } = await execSshWithInput(sshPath, args, input, timeoutMs);
      return stdout.trim();
    };

    try {
      return await runOnce();
    } catch (error) {
      const errorText = normalizeSshErrorText(error);
      const retry = await maybePromptForSshAuth(cfg, errorText);
      if (retry?.kind === 'agent') {
        try {
          return await runOnce();
        } catch (retryError) {
          const retryText = normalizeSshErrorText(retryError);
          const retrySummary = pickSshErrorSummary(retryText);
          throw new Error(retrySummary || 'SSH command failed');
        }
      }
      if (retry?.kind === 'terminal') {
        try {
          return await vscode.window.withProgress(
            {
              location: vscode.ProgressLocation.Notification,
              title: 'Waiting for SSH passphrase in terminal',
              cancellable: false
            },
            async () => await deps.runSshCommandInTerminal(host, cfg, command, input)
          );
        } catch (retryError) {
          const retryText = normalizeSshErrorText(retryError);
          const retrySummary = pickSshErrorSummary(retryText);
          throw new Error(retrySummary || 'SSH command failed');
        }
      }
      const summary = pickSshErrorSummary(errorText);
      throw new Error(summary || 'SSH command failed');
    }
  }

  function resolveSshIdentityAgentOption(sshPath: string): string | undefined {
    const sock = process.env.SSH_AUTH_SOCK || '';
    if (!sock || !isGitSshPath(sshPath)) {
      return undefined;
    }
    return sock;
  }

  function resetState(): void {
    lastSshAuthPrompt = undefined;
  }

  return {
    normalizeSshErrorText,
    getSshAgentEnv,
    isGitSshPath,
    hasSshOption,
    resolveSshToolPath,
    ensureSshAgentEnvForCurrentSsh,
    pickSshErrorSummary,
    buildAgentStatusMessage,
    maybePromptForSshAuthOnConnect,
    runSshCommand,
    runSshCommandWithInput,
    resolveSshIdentityAgentOption,
    resetState
  };
}
