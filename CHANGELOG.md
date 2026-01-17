# Changelog
All notable changes to this project are documented in this file based on git tags and commit history.

## Unreleased
### Added
- Sticky action bar with Connect/status and cluster info timestamp.
- Profile summary row with derived totals and inline validation hints.
- Profile override list to show non-default saved fields.
- Identity file browse button.
- Module chips with clear-all and paste support.
- Module paste now accepts `module load ...` lines.
- Advanced settings reset button to restore defaults.
### Changed
- Clear cluster info is now a secondary action and is disabled while fetching.
- Remote folder field no longer shows a local browse button (remote-only path).
- SSH include status text is now shorter in the action bar.
- SSH host and agent info hints now appear only when relevant (errors or focused field).
- Cluster info success hints are no longer shown by default.
- Profile summary and overrides are now grouped into a single card.
- Profile details now avoid duplicating the single login host in the overrides list.
### Fixed
- Webview module parsing now escapes regex sequences correctly to avoid load errors.

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
