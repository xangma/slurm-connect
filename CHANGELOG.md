# Changelog
All notable changes to this project are documented in this file based on git tags and commit history.

## Unreleased
### Added
- Persistent session mode for Slurm allocations, with configurable session keys, idle timeouts, and state directory settings.
- Existing session selector in the cluster info UI, allowing connections to reuse a running allocation.
### Changed
- Default session mode is now persistent with a 10-minute idle timeout (set to 0 to disable).
- Persistent session state is now namespaced by username under `sessionStateDir/sessions/<user>` to avoid collisions on shared filesystems (legacy layout still supported).
- The extension prompts to disable `remote.SSH.useExecServer` when persistent sessions are enabled to improve reconnect reliability.
- Documented that window reloads stop debug sessions; use attach workflows to reconnect in persistent allocations.
### Fixed
- SSH config Include block is now pinned to the top of the SSH config so the Slurm Connect hosts are read before other host entries.
- Module load commands now shell-escape module names with special characters to avoid RemoteCommand parsing errors.
- Module lists now show default markers from `module avail` while stripping them from module load selections.

## 0.3.4 - 2026-01-14
### Added
- Stale-data warning for free-resource filtering to prompt refreshes.
- Module list section headers as non-selectable dividers, with section-aware searching.
- Webview remembers collapsed/expanded states for connection/resource/profiles/advanced sections.
- Resource warning hints for invalid or likely-to-fail Slurm requests.

### Fixed
- Wall-time validation ignores hidden characters and punctuation, preventing false warnings.

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
