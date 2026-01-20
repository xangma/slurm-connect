# Persistent sessions

By default, Slurm Connect uses persistent sessions so allocations survive reconnects and are cancelled after an idle timeout
(default 10 minutes). Set the timeout to 0 to disable auto-cancel.

## Requirements
- A shared filesystem between login and compute nodes (used for session state/markers).

## Settings
- `slurmConnect.sessionMode`: `persistent` (default) or `ephemeral`.
- `slurmConnect.sessionKey`: Optional identifier for reuse; defaults to the SSH alias.
- `slurmConnect.sessionIdleTimeoutSeconds`: Seconds of idle time before cancelling (0 = never).
- `slurmConnect.sessionStateDir`: Optional base directory for session state (default handled by the proxy).

Note: The idle timeout is saved when the allocation is created; changing the setting later does not affect existing sessions.

## How it works
In persistent mode, the proxy submits an allocation via `sbatch`, reuses it on reconnect, and launches each
VS Code connection as a job step (`srun --overlap`). When no session markers remain, the idle timer starts and
the allocation is cancelled once it expires.

Session state is stored under `sessionStateDir/sessions/<username>/<sessionKey>` to avoid cross-user clashes on
shared filesystems. Legacy sessions that live directly under `sessionStateDir/sessions/<sessionKey>` are still
recognized.

Note: Reloading the VS Code window stops active debug sessions (the debugger is owned by the VS Code process).
If you want a debug session to survive a reconnect, start your app with `debugpy` (or similar) and use an
**attach** configuration when you reconnect to the persistent allocation.
