export interface SessionSummary {
  sessionKey: string;
  jobId: string;
  state: string;
  jobName?: string;
  createdAt?: string;
  partition?: string;
  nodes?: number;
  cpus?: number;
  timeLimit?: string;
  clients?: number;
  lastSeenEpoch?: number;
  idleRemainingSeconds?: number;
  idleTimeoutSeconds?: number;
}

export function parseSessionListOutput(output: string): SessionSummary[] {
  const trimmed = output.trim();
  if (!trimmed) {
    return [];
  }

  try {
    const parsed = JSON.parse(trimmed);
    if (!Array.isArray(parsed)) {
      return [];
    }

    const results: SessionSummary[] = [];
    for (const entry of parsed) {
      if (!entry || typeof entry !== 'object') {
        continue;
      }

      const sessionKey = typeof entry.sessionKey === 'string' ? entry.sessionKey.trim() : '';
      const jobId = typeof entry.jobId === 'string' ? entry.jobId.trim() : '';
      if (!sessionKey || !jobId) {
        continue;
      }

      const state = typeof entry.state === 'string' ? entry.state.trim() : '';
      const jobName = typeof entry.jobName === 'string' ? entry.jobName.trim() : '';
      const createdAt = typeof entry.createdAt === 'string' ? entry.createdAt.trim() : '';
      const partition = typeof entry.partition === 'string' ? entry.partition.trim() : '';
      const timeLimit = typeof entry.timeLimit === 'string' ? entry.timeLimit.trim() : '';
      const nodes = Number(entry.nodes);
      const cpus = Number(entry.cpus);
      const clientsValue = Number(entry.clients);
      const lastSeenValue = Number(entry.lastSeen);
      const idleTimeoutValue = Number(entry.idleTimeoutSeconds);

      results.push({
        sessionKey,
        jobId,
        state,
        jobName: jobName || undefined,
        createdAt: createdAt || undefined,
        partition: partition || undefined,
        nodes: Number.isFinite(nodes) && nodes > 0 ? nodes : undefined,
        cpus: Number.isFinite(cpus) && cpus > 0 ? cpus : undefined,
        timeLimit: timeLimit || undefined,
        clients: Number.isFinite(clientsValue) && clientsValue >= 0 ? clientsValue : undefined,
        lastSeenEpoch: Number.isFinite(lastSeenValue) && lastSeenValue > 0 ? lastSeenValue : undefined,
        idleTimeoutSeconds:
          Number.isFinite(idleTimeoutValue) && idleTimeoutValue > 0 ? idleTimeoutValue : undefined
      });
    }

    return results;
  } catch {
    return [];
  }
}

export function applySessionIdleInfo(
  sessions: SessionSummary[],
  options?: { nowMs?: number }
): SessionSummary[] {
  const nowSeconds = Math.floor((options?.nowMs ?? Date.now()) / 1000);
  return sessions.map((session) => {
    const next: SessionSummary = { ...session };
    const timeout = typeof session.idleTimeoutSeconds === 'number' ? session.idleTimeoutSeconds : 0;
    if (timeout <= 0) {
      return next;
    }

    const clients = typeof session.clients === 'number' ? session.clients : 0;
    const lastSeen = session.lastSeenEpoch;
    if (clients !== 0 || typeof lastSeen !== 'number' || !Number.isFinite(lastSeen)) {
      return next;
    }

    const idleSeconds = Math.max(0, nowSeconds - lastSeen);
    next.idleRemainingSeconds = Math.max(0, timeout - idleSeconds);
    return next;
  });
}
