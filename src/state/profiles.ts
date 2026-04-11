import * as vscode from 'vscode';

import {
  ProfileOverrideDetail,
  ProfileResourceSummary,
  ProfileStore,
  ProfileSummary,
  ProfileValues,
  UiValues
} from './types';

export interface ProfileSummaryHelpers {
  firstLoginHostFromInput(input: string): string | undefined;
  parsePositiveNumberInput(input: string): number;
  parseNonNegativeNumberInput(input: string): number;
}

export function getProfileStore(state: vscode.Memento | undefined, storeKey: string): ProfileStore {
  if (!state) {
    return {};
  }
  return state.get<ProfileStore>(storeKey) || {};
}

export async function updateProfileStore(
  state: vscode.Memento | undefined,
  storeKey: string,
  store: ProfileStore
): Promise<void> {
  if (!state) {
    return;
  }
  await state.update(storeKey, store);
}

export function getActiveProfileName(state: vscode.Memento | undefined, activeProfileKey: string): string | undefined {
  if (!state) {
    return undefined;
  }
  const name = state.get<string>(activeProfileKey);
  return name || undefined;
}

export async function setActiveProfileName(
  state: vscode.Memento | undefined,
  activeProfileKey: string,
  name?: string
): Promise<void> {
  if (!state) {
    return;
  }
  await state.update(activeProfileKey, name);
}

export function getProfileSummaries(store: ProfileStore): ProfileSummary[] {
  return Object.values(store)
    .map((profile) => ({ name: profile.name, updatedAt: profile.updatedAt }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

export function getProfileValues(
  name: string,
  store: ProfileStore,
  mergeUiValuesWithDefaults: (values: Partial<UiValues> | undefined, defaults: UiValues) => UiValues,
  pickProfileValues: (values: Partial<UiValues>) => ProfileValues,
  defaults: UiValues
): UiValues | undefined {
  const entry = store[name];
  if (!entry) {
    return undefined;
  }
  return mergeUiValuesWithDefaults(pickProfileValues(entry.values), defaults);
}

export function normalizeWhitespace(value: string | undefined | null): string {
  if (!value) {
    return '';
  }
  return value.replace(/\s+/g, ' ').trim();
}

export function normalizeList(values: string[] | undefined): string[] {
  if (!Array.isArray(values)) {
    return [];
  }
  return values.map((entry) => entry.trim()).filter(Boolean);
}

export function listsEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}

export function buildProfileOverrides(values: UiValues, defaults: UiValues): ProfileOverrideDetail[] {
  const overrides: ProfileOverrideDetail[] = [];
  const add = (label: string, value: string): void => {
    const normalized = normalizeWhitespace(value);
    if (normalized) {
      overrides.push({ label, value: normalized });
    }
  };
  const diffString = (value: string, fallback: string): boolean => {
    const normalized = normalizeWhitespace(value);
    if (!normalized) {
      return false;
    }
    return normalized !== normalizeWhitespace(fallback);
  };
  const diffBool = (value: boolean, fallback: boolean): boolean => value !== fallback;

  const loginHostsValue = normalizeWhitespace(values.loginHosts);
  const loginHostTokens = loginHostsValue.split(/[,\s]+/).filter(Boolean);
  if (loginHostTokens.length > 1 && diffString(values.loginHosts, defaults.loginHosts)) {
    add('Login hosts', values.loginHosts);
  }
  if (diffString(values.user, defaults.user)) add('User', values.user);
  if (diffString(values.identityFile, defaults.identityFile)) add('Identity file', values.identityFile);
  if (diffString(values.preSshCommand, defaults.preSshCommand)) add('Pre-SSH command', values.preSshCommand);
  if (diffString(values.preSshCheckCommand, defaults.preSshCheckCommand)) {
    add('Pre-SSH check command', values.preSshCheckCommand);
  }
  if (diffString(values.startupCommand, defaults.startupCommand)) add('Startup command', values.startupCommand);
  if (diffString(values.additionalSshOptions, defaults.additionalSshOptions)) {
    add('Additional SSH options', values.additionalSshOptions);
  }
  if (diffString(values.remoteWorkspacePath, defaults.remoteWorkspacePath)) {
    add('Remote folder', values.remoteWorkspacePath);
  }
  if (diffBool(values.openInNewWindow, defaults.openInNewWindow)) {
    add('Open in new window', values.openInNewWindow ? 'Yes' : 'No');
  }
  if (diffBool(values.filterFreeResources, defaults.filterFreeResources)) {
    add('Free-resource filter', values.filterFreeResources ? 'On' : 'Off');
  }

  const selections = normalizeList(values.moduleSelections);
  const defaultSelections = normalizeList(defaults.moduleSelections);
  const custom = normalizeWhitespace(values.moduleCustomCommand || '');
  const defaultCustom = normalizeWhitespace(defaults.moduleCustomCommand || '');
  const moduleLoad = normalizeWhitespace(values.moduleLoad || '');
  const defaultModuleLoad = normalizeWhitespace(defaults.moduleLoad || '');
  if (selections.length > 0 && !listsEqual(selections, defaultSelections)) {
    add('Modules', selections.join(', '));
  } else if (custom && custom !== defaultCustom) {
    add('Module command', custom);
  } else if (moduleLoad && moduleLoad !== defaultModuleLoad) {
    add('Module load', moduleLoad);
  }

  if (diffString(values.extraSallocArgs, defaults.extraSallocArgs)) add('Extra salloc args', values.extraSallocArgs);
  if (diffBool(values.promptForExtraSallocArgs, defaults.promptForExtraSallocArgs)) {
    add('Prompt for extra args', values.promptForExtraSallocArgs ? 'Yes' : 'No');
  }

  if (diffString(values.sessionMode, defaults.sessionMode)) add('Session mode', values.sessionMode);
  if (diffString(values.sessionKey, defaults.sessionKey)) add('Session key', values.sessionKey);
  if (diffString(values.sessionIdleTimeoutSeconds, defaults.sessionIdleTimeoutSeconds)) {
    add('Session idle timeout', values.sessionIdleTimeoutSeconds + 's');
  }

  if (diffString(values.loginHostsCommand, defaults.loginHostsCommand)) {
    add('Login hosts command', values.loginHostsCommand);
  }
  if (diffString(values.loginHostsQueryHost, defaults.loginHostsQueryHost)) {
    add('Login hosts query host', values.loginHostsQueryHost);
  }
  if (diffString(values.partitionInfoCommand, defaults.partitionInfoCommand)) {
    add('Partition info command', values.partitionInfoCommand);
  }
  if (diffString(values.partitionCommand, defaults.partitionCommand)) {
    add('Partition list command', values.partitionCommand);
  }
  if (diffString(values.qosCommand, defaults.qosCommand)) add('QoS command', values.qosCommand);
  if (diffString(values.accountCommand, defaults.accountCommand)) add('Account command', values.accountCommand);

  if (diffBool(values.forwardAgent, defaults.forwardAgent)) {
    add('Forward agent', values.forwardAgent ? 'Enabled' : 'Disabled');
  }
  if (diffBool(values.requestTTY, defaults.requestTTY)) {
    add('Request TTY', values.requestTTY ? 'Enabled' : 'Disabled');
  }

  return overrides;
}

export function buildProfileResourceSummary(
  values: UiValues,
  defaults: UiValues,
  helpers: ProfileSummaryHelpers
): ProfileResourceSummary {
  const host = helpers.firstLoginHostFromInput(values.loginHosts);
  const partition = values.defaultPartition.trim() || undefined;
  const nodesRaw = values.defaultNodes.trim();
  const tasksRaw = values.defaultTasksPerNode.trim();
  const cpusRaw = values.defaultCpusPerTask.trim();
  const memoryRaw = values.defaultMemoryMb.trim();
  const gpuCountRaw = values.defaultGpuCount.trim();
  const gpuType = values.defaultGpuType.trim() || undefined;
  const time = values.defaultTime.trim() || undefined;

  const nodes = nodesRaw ? helpers.parsePositiveNumberInput(nodesRaw) || undefined : undefined;
  const tasksPerNode = tasksRaw ? helpers.parsePositiveNumberInput(tasksRaw) || undefined : undefined;
  const cpusPerTask = cpusRaw ? helpers.parsePositiveNumberInput(cpusRaw) || undefined : undefined;
  const memoryMb = memoryRaw ? helpers.parseNonNegativeNumberInput(memoryRaw) || undefined : undefined;
  const gpuCount = gpuCountRaw ? helpers.parseNonNegativeNumberInput(gpuCountRaw) : undefined;

  const totalCpu =
    nodes && (tasksPerNode || cpusPerTask)
      ? nodes * (tasksPerNode || 1) * (cpusPerTask || 1)
      : undefined;
  const totalMemoryMb = nodes && memoryMb ? nodes * memoryMb : undefined;
  const totalGpu = nodes && gpuCount !== undefined ? nodes * gpuCount : undefined;

  return {
    host,
    partition,
    nodes,
    tasksPerNode,
    cpusPerTask,
    memoryMb,
    gpuType,
    gpuCount,
    time,
    totalCpu,
    totalMemoryMb,
    totalGpu,
    overrides: buildProfileOverrides(values, defaults)
  };
}

export function buildProfileSummaryMap(
  store: ProfileStore,
  defaults: UiValues,
  mergeUiValuesWithDefaults: (values: Partial<UiValues> | undefined, defaults: UiValues) => UiValues,
  pickProfileValues: (values: Partial<UiValues>) => ProfileValues,
  helpers: ProfileSummaryHelpers
): Record<string, ProfileResourceSummary> {
  const summaries: Record<string, ProfileResourceSummary> = {};
  Object.entries(store).forEach(([name, entry]) => {
    const merged = mergeUiValuesWithDefaults(pickProfileValues(entry.values), defaults);
    summaries[name] = buildProfileResourceSummary(merged, defaults, helpers);
  });
  return summaries;
}
