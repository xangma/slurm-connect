# Changelog
All notable changes to this project are documented in this file based on git tags and commit history.

## Unreleased
### Fixed
- Windows: keep the managed SSH Include path in its configured form (default `~/.ssh/slurm-connect.conf`) so POSIX-style ssh clients do not misinterpret absolute `C:\...` paths.
- Windows: always offer to persist `remote.SSH.useLocalServer=true` in user settings even when the UI default appears enabled.
- Windows: `ssh-add` now completes reliably by running in a terminal and waiting on its exit status (avoids agent detection mismatches).
- Connect: allow cancelling a pending connection before Remote-SSH starts.
- Agent status hint now refreshes immediately after ssh-add completes.
- Agent key detection now matches against public key material when the ssh-add list output omits file paths.
- ssh-add now runs with the same SSH_AUTH_SOCK/SSH_AGENT_PID as the extension so agent checks match on Windows.
- Added a Generate Public Key option when the identity `.pub` file is missing (required for strict agent verification).
- SSH tools now respect `remote.SSH.path` so ssh-add/key detection aligns with the SSH binary Remote-SSH uses.
- Windows: prompt to switch Remote-SSH from Git SSH to Windows OpenSSH when agent passphrase prompts are likely.
- Windows: if Remote-SSH uses Git SSH, Slurm Connect starts the Git ssh-agent and exports SSH_AUTH_SOCK/SSH_AGENT_PID so agent checks align.
- SSH queries and tunnels now follow Remote-SSH's SSH executable preference order (including Git OpenSSH on Windows when local server mode is enabled).
- Windows: ensure Git ssh-agent environment is refreshed before Remote-SSH connects to reduce unexpected passphrase prompts.
- Windows: when Git SSH is in use and an agent socket is available, Slurm Connect now writes `IdentityAgent` in generated host entries to ensure Remote-SSH uses the correct agent.
- Terminal-based prompts now use a safe local working directory to avoid failures when VS Code's default cwd is missing.
- Windows: remember and reuse the last working SSH agent socket so reconnecting does not require re-adding keys.
- Cluster info: stream the combined query script over stdin to avoid shell quoting/length errors on some hosts.
- Windows: strip CRLF when piping stdin scripts through PowerShell to prevent bash `$'\r'` errors.
- SSH detection: honor an explicit remote.SSH.path even if `ssh -V` output is unexpected.
- Windows: clear stale MSYS SSH_AUTH_SOCK when using Windows OpenSSH so the native agent is detected.

### Added
- Added `slurmConnect.sshHostKeyChecking` (default `accept-new`) to control host key checking for non-interactive SSH queries and proxy tunnels.

## 0.5.2 - 2026-01-22
### Added
- Built-in local HTTP(S) proxy for outbound access from compute nodes.
- Local proxy enable toggle in the Advanced settings view.
- Optional compute-node SSH tunnel for clusters with `GatewayPorts` disabled.

### Changed
- Compute-node tunnel toggle is settings-only (removed from the panel UI).
- Advanced local proxy settings moved to VS Code Settings; the panel only shows the enable toggle.

### Fixed
- Proxy auto-install payload is compressed and prefers python3 (with gzip fallback).
- Compute-node proxy tunnel setup is more robust (timeouts, shell wrapping, host key handling, better diagnostics).
- Local proxy now blocks loopback (including IPv6-mapped), fixes IPv6 Host headers, and avoids port conflicts with retries/dedicated tunnels.
- Remote-SSH exec-server downloads now inherit proxy env; proxy state persists and tunnels shut down on deactivation.
- Added `slurmConnect.localProxyTunnelMode` for Remote-SSH vs dedicated tunneling.

### Docs
- Documented local proxy settings and usage guidance.

## 0.5.0 - 2026-01-20
### Added
- Pre-SSH auth command hook with an optional check command to support interactive 2FA flows before SSH queries and Remote-SSH connects.
- Advanced setting field for additional SSH options (one per line) in the Slurm Connect view.
- Auto-install bundled proxy script on connect (default on).
- Bundled `vscode-proxy.py` implementation for the Remote-SSH Slurm stdio/TCP proxy.
- Persistent session support in the bundled proxy (sbatch allocation reuse + idle cancel).
### Changed
- Default proxy command now points at the bundled proxy script under `~/.slurm-connect/vscode-proxy.py`.
- Proxy command and proxy args are no longer shown in the UI (advanced overrides remain in settings.json).
- Existing proxy command/args overrides are reset to defaults on upgrade.
- Auto-install of the bundled proxy script now runs on connect instead of on Get cluster info.
- Advanced settings in the view now only show per-profile fields; global-only settings are managed in VS Code settings.
- Profile saves now capture the full UI state, and UI edits no longer write back to settings (last-used values are cached per scope).
### Fixed
- Profile summary rendering no longer crashes when older profiles omit newer fields.
- Cluster info errors now clear after a successful refresh.
- Bundled proxy now resolves `workgroup` via the login shell when it is not on PATH, and falls back to direct Slurm commands when `workgroup` is unavailable.
- Bundled proxy now recreates persistent sessions when the saved job id is invalid.
- Bundled proxy now skips `srun --pty` when no TTY is available to avoid Slurm warnings.
- Bundled proxy now bootstraps XDG runtime dirs, improves localhost binding rewrites, and enables reuse_port for the local proxy.
- Persistent sessions now wait for RUNNING, pin to a chosen node, and clean up stale client markers.
- Bundled proxy now rewrites exec-server `listeningOn==...==` lines correctly and filters marker echoes to avoid handshake hangs.
- Bundled proxy now detects `listeningOn` lines on stderr when stdout is redirected, preventing missing-port hangs.
- Bundled proxy now uses the legacy listeningOn regex from the upstream script to match decorated port lines.
- Bundled proxy stdin rewrite logic now matches the legacy proxy’s conservative patterns to avoid mangling Remote-SSH bootstrap scripts.
- Bundled proxy now uses blocking stdin reads to align with Remote-SSH bootstrap timing and avoid duplicate subshell detection output.
- Bundled proxy now writes persistent session job.json files in the legacy format (compact JSON + args/heartbeat fields).
- Bundled proxy now places the session lock file under the namespaced session directory (sessions/$USER/<key>/lock) to match the legacy layout.
- Bundled proxy no longer writes a default log file; logging is disabled unless --log-file is provided.
- Slurm Connect logs now shorten the embedded proxy base64 payload in RemoteCommand (keeps first/last chars) to avoid huge logs.
- Bundled proxy now bypasses Slurm allocation for exec-server bootstraps in ephemeral mode using a short-lived lock keyed by the session/alias to avoid interfering with Remote-SSH stdin.
- Bundled proxy now respects loopback hosts in exec-server `listeningOn` output to avoid connect hangs.
- Local Slurm Connect logs are now capped in size and truncated when they exceed 5 MB.
### Docs
- Split the README into shorter docs under `docs/` (usage, cluster info, SSH/auth, persistent sessions, settings reference).

## 0.5.1 - 2026-01-20
### Changed
- Profile cards now collapse the "Other saved settings" list by default.

## 0.4.4 - 2026-01-18
### Added
- Sticky action bar with Connect/status and cluster info timestamp.
- Profile summary row with derived totals and inline validation hints.
- Profile override list to show non-default saved fields.
- Identity file browse button.
- Module chips with clear-all and paste support.
- Module paste now accepts `module load ...` lines.
- Advanced settings reset button to restore defaults.
- Cancel job (disconnects) button for persistent sessions.
- Existing session picker now shows connected client counts (deduped per window) and idle cancel timing based on the session's saved timeout.
### Changed
- Clear cluster info is now a secondary action and is disabled while fetching.
- Remote folder field no longer shows a local browse button (remote-only path).
- SSH host and agent info hints now appear only when relevant (errors or focused field).
- Cluster info success hints are no longer shown by default.
- Profile summary and overrides are now grouped into a single card.
- Profile details now avoid duplicating the single login host in the overrides list.
- Legacy Remote.SSH configFile override is removed; the managed Include block is now always used.
- Removed the SSH Include status hint from the action bar.
- Dropdown menus now render above the sticky action bar.
### Fixed
- Webview module parsing now escapes regex sequences correctly to avoid load errors.
- Session/job lookups now fall back to `python` if `python3` is unavailable on the login host.
- Cancel job now runs before disconnect; it cancels via `scancel $SLURM_JOB_ID` from an active remote session to avoid nested SSH calls.
- Session client counts now use active (non-stale) markers to avoid inflating counts after reconnects.

## 0.4.3 - 2026-01-17
### Changed
- Extra salloc args in the UI now honor shell-style quoting for multi-word values.
- Loading a profile now applies once; subsequent reloads keep your last edits instead of reapplying the active profile.
### Fixed
- Remote folder paths with spaces or special characters now open reliably after connect.
- Terminal-based SSH passphrase entry no longer times out while waiting for completion.
- RemoteCommand assembly now quotes proxy/session args to avoid failures with spaces.
- Free-resource filtering now attributes shared nodes to all partitions they belong to.
- Connecting to an existing session now uses the session key as the SSH alias to keep Remote-SSH caching consistent.

## 0.4.2 - 2026-01-17
### Changed
- Version bump only (no user-visible changes).

## 0.4.1 - 2026-01-16
### Added
- SSH config host picker that can resolve a selected host and auto-fill login host/user/identity fields.
### Changed
- SSH identity file is now optional; authentication warnings appear after failures instead of on connect.

## 0.4.0 - 2026-01-16
### Added
- Persistent session mode for Slurm allocations, with configurable session keys, idle timeouts, and state directory settings.
- Existing session selector in the cluster info UI, allowing connections to reuse a running allocation.
- Collapsible webview sections for connection/resource/profiles/advanced settings, with expansion state persistence.
- New connection options to open in a new window and to set a remote workspace path.
- Resource warning hints for invalid or likely-to-fail Slurm requests.
### Changed
- Default session mode is now persistent with a 10-minute idle timeout (set to 0 to disable).
- Persistent session state is now namespaced by username under `sessionStateDir/sessions/<user>` to avoid collisions on shared filesystems (legacy layout still supported).
- The extension prompts to disable `remote.SSH.useExecServer` when persistent sessions are enabled to improve reconnect reliability.
- Get cluster info always collects free-resource data in the single SSH call; the toggle only filters UI suggestions.
- Updated the cluster info hint text to reflect free-resource data collection and filtering behavior.
### Fixed
- SSH config Include block is now pinned to the top of the SSH config so the Slurm Connect hosts are read before other host entries.
- Module load commands now shell-escape module names with special characters to avoid RemoteCommand parsing errors.
- Module lists now show default markers from `module avail` while stripping them from module load selections.
- Wall-time validation ignores hidden characters and punctuation, preventing false warnings.
### Docs
- Documented that window reloads stop debug sessions; use attach workflows to reconnect in persistent allocations.

## 0.3.4 - 2026-01-14
### Added
- Stale-data warning for free-resource filtering to prompt refreshes.
- Module list section headers as non-selectable dividers, with section-aware searching.
### Fixed
- Module list parsing now strips terminal control codes and legend markers from `module -t avail`.

## 0.3.3 - 2026-01-13
### Added
- Free-resource filtering for cluster info suggestions (default on) with a UI toggle and setting.
- Free-resource parsing from `sinfo` + `squeue` embedded in the existing single SSH cluster-info call.

### Docs
- Documented how free-resource filtering is computed and how it affects UI suggestions.

## 0.3.2 - 2026-01-12
### Added
- SSH config backup support and a new setting for a managed Include block.
- Default partition properties to `PartitionInfo`.

### Changed
- Migrated legacy module commands and sanitized module cache handling in the extension.
- Clarified SSH include file usage and connection handling in configuration and the README.

## 0.3.1 - 2026-01-09
### Docs
- Expanded README details for cluster info and profiles.

## 0.3.0 - 2026-01-09
### Changed
- Improved SSH authentication flow and prompts.
- Improved cluster info fetching logic.
- Refactored code structure for readability and maintainability.

## 0.2.1 - 2026-01-09
### Added
- Unit tests and utility functions for SSH config and cluster info parsing.
- Auto-save for settings with enhanced input validation.

### Changed
- Renamed the extension from Sciama to Slurm Connect and migrated legacy settings.
- Updated README requirements (Python 3.9+) and proxy script accessibility notes.
- Bumped the package version to 0.2.0.

## 0.1.4 - 2026-01-08
### Docs
- Clarified SSH agent requirements and remote workspace path configuration in the README.

## 0.1.3 - 2026-01-08
### Added
- Module management features and improved connection state synchronization.

## 0.1.2 - 2026-01-08
### Changed
- Improved SSH authentication handling with prompts and normalized errors.

## 0.1.1 - 2026-01-07
### Added
- Initial public release.
- Quick start and development setup documentation.
- Repository metadata in `package.json`.

### Changed
- Refactored code structure for readability and maintainability.
- Updated proxy command path and SSH config restoration logic.
