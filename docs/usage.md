# Usage

This doc covers the Slurm Connect UI and workflow. For cluster discovery details, see `docs/cluster-info.md`.

## UI reference
### Connection settings
- **Login host**: Login node to connect/query. You can enter multiple hosts separated by spaces/commas/newlines; the first is used by default.
- **SSH user**: Overrides the SSH username for queries and generated host entries. Leave blank to use your local OS username or SSH config.
- **Identity file (optional)**: SSH private key path to use for queries and generated host entries. Leave blank to rely on your SSH config/agent.
- **Remote folder to open (optional)**: Remote path that VS Code opens after connecting.
- **Get cluster info**: Fetches partitions/resources via SSH and fills pickers/suggestions.
- **Clear**: Clears the cached cluster info shown in the UI.
- **Existing sessions**: When persistent sessions are enabled and found, lets you pick one to reuse.

### Resource settings
- **Show only free resources**: Filters suggestions to currently free resources (data is still collected even when off).
- **Partition**: Slurm partition to allocate; blank uses the cluster default if available.
- **Nodes**: Number of nodes to request.
- **Tasks per node**: Tasks per node (Slurm `--ntasks-per-node`).
- **CPUs per task**: CPUs per task (Slurm `--cpus-per-task`).
- **Memory per node**: Memory per node in MB (Slurm `--mem`).
- **GPU type**: GPU type to request (when available).
- **GPU count**: GPUs per node (Slurm `--gres=gpu:N`).
- **Wall time**: Allocation time (HH:MM:SS or D-HH:MM:SS depending on cluster).
- **Use partition defaults**: Fills resource fields using the cluster defaults (requires cluster info).
- **Clear resources**: Resets resource fields to empty/defaults.
- **Modules**: Select modules to load on connect (generates a `module load ...` command).
- **Clear all**: Removes all selected modules.
- **+**: Adds the typed module entry.

### Profiles
- **Saved profiles**: List of stored profiles.
- **Load**: Applies the selected profile values to the form.
- **Save profile**: Saves current form values under the selected profile name (or creates a new one).
- **Delete**: Removes the selected profile.

### Advanced settings
- **Login hosts command (optional)**: Remote command that outputs login hosts; run over SSH to discover hosts.
- **Login hosts query host (optional)**: Host used to run the login hosts command (defaults to the first login host).
- **Partition info command**: Remote command that outputs partition detail used for limits/suggestions.
- **Partition list command**: Remote command that outputs partition names.
- **QoS command (optional)**: Remote command that outputs QoS names.
- **Account command (optional)**: Remote command that outputs account names.
- **Extra salloc args**: Extra Slurm `salloc` arguments appended to every request.
- **Prompt for extra salloc args**: Prompts for extra `salloc` args at connect time.
- **Session mode**: `ephemeral` (new job per connect) or `persistent` (reuse allocation).
- **Session key (optional)**: Identifier used to reuse a persistent session (defaults to SSH alias).
- **Session idle timeout (seconds, 0 = never)**: Idle duration before auto-cancel in persistent mode.
- **Session state directory (optional)**: Base directory for persistent session state.
- **SSH host prefix**: Prefix for generated SSH host aliases (e.g., `slurm`).
- **Slurm Connect include file path**: Path for the managed SSH include file written by the extension.
- **SSH query config path**: SSH config file used for host discovery and query commands.
- **Pre-SSH auth command (optional)**: Local command run before SSH queries/connect (e.g., `step ssh login`).
- **Pre-SSH check command (optional)**: Local non-interactive check; exit 0 skips the pre-SSH auth command.
- **Auto-install bundled proxy script on connect**: Installs/updates the bundled `vscode-proxy.py` on the login host during connect.
- **Additional SSH options (one per line)**: Extra SSH config lines added to generated host entries.
- **Forward agent**: Adds `ForwardAgent yes` to generated host entries.
- **Request TTY**: Adds `RequestTTY yes` to generated host entries.
- **Save settings to**: Writes changes to user or workspace settings.
- **Reset advanced to defaults**: Resets advanced fields to extension defaults.
- **Reload saved values**: Reloads settings/cache into the UI.
- **Open Settings**: Opens VS Code settings.
- **Open logs**: Opens the Slurm Connect output log (capped at 5 MB; older entries are truncated).
- **Remote-SSH log**: Opens the Remote-SSH log.

### Action bar
- **Connect**: Creates the SSH host entry, opens Remote-SSH, and starts the allocation.
- **Cancel job (disconnects)**: Cancels the persistent session allocation and disconnects.
- **Open in new window**: Opens the connection in a new VS Code window.
- **Status hints**: Show connection state and last cluster info timestamp.

## UI tips
- The sticky action bar keeps **Connect**, connection status, and the cluster info timestamp visible while you scroll.
- The Profiles section shows a summary row with saved resource defaults and derived totals before you load a profile.
- Other saved settings (non-default fields) are listed under the resource summary so you can audit what will be applied.
- Inline validation hints appear under resource fields (nodes, memory, wall time).
- The **Identity file** field includes a Browse button to insert local paths quickly.
- The **Remote folder** field expects a path on the cluster (enter manually).
- Module selection supports chips, clear-all, and pasting multiple module names (space/newline separated), including `module load ...` lines.
- Advanced settings include a **Reset advanced to defaults** button to restore command/SSH settings.
- When connected to a persistent session, a **Cancel job (disconnects)** button appears to terminate the allocation (cancel is only available from an active remote session).
- Existing sessions show connected client counts (per window) and, when idle, an estimated cancel timeout (from the session's saved timeout).
