# Slurm Connect (VS Code extension)

This extension helps users allocate Slurm resources on a cluster and connect VS Code Remote-SSH through a compute node. It discovers partitions (and optionally QoS/accounts), builds the `RemoteCommand` for a proxy script, writes a Slurm Connect SSH include file, installs a small Include block (with a note) in your SSH config, and optionally connects right away.

## Requirements
- VS Code with **Remote - SSH** installed.
- SSH authentication configured for the cluster (SSH config or agent; agent forwarding recommended).
- Python 3.9+ available on the login nodes.
- [`vscode-shell-proxy.py`](https://github.com/xangma/vscode-shell-proxy/blob/main/vscode-shell-proxy.py) available somewhere on the login nodes.
- ssh-agent running with your key added (recommended for encrypted keys and non-interactive SSH).

## Quick start (users)
1. Install the extension.
2. Open the **Slurm Connect** view from the activity bar.
3. Select a login host (or pick from SSH config), username if needed, and an optional identity file.
4. Click **Get cluster info**, choose resources, then **Connect**.

The extension will query the login host, write a Slurm Connect SSH include file, ensure your SSH config contains a small Slurm Connect Include block (with a note), and connect. It does not replace your SSH config; it only adds the managed block.

## Usage details

### UI tips
- The sticky action bar keeps **Connect**, connection status, and the cluster info timestamp visible while you scroll.
- The Profiles section shows a summary row with saved resource defaults and derived totals before you load a profile.
- Other saved settings (non-default fields) are listed under the resource summary so you can audit what will be applied.
- Inline validation hints appear under resource fields (nodes, memory, wall time).
- The **Identity file** field includes a Browse button to insert local paths quickly.
- The **Remote folder** field expects a path on the cluster (enter manually).
- Module selection supports chips, clear-all, and pasting multiple module names (space/newline separated), including `module load ...` lines.
- Advanced settings include a **Reset advanced to defaults** button to restore command/SSH settings.

### SSH config hosts (optional)
The **Login host** field can read your SSH config and list explicit `Host` entries (non-wildcard). Selecting one resolves
the host via `ssh -G` and auto-fills the login host (resolved HostName), **User**, and **Identity file** when those fields are blank.
ProxyJump/ProxyCommand settings are intentionally not imported; if your host relies on them, add them manually.

The picker reads from `slurmConnect.sshQueryConfigPath` when set; otherwise it uses `~/.ssh/config`.

### Get cluster info
The **Get cluster info** button queries your login host to discover available Slurm partitions and their limits
(nodes, CPUs, memory, GPUs). It also tries to read the available modules list. The results are used to populate
the dropdowns and suggestions in the UI so you can pick valid values quickly. You can still type values manually,
and the fetched data is cached per login host to speed up the next load.

When cluster info is available, the UI shows warning hints if selected resources exceed partition limits or (when free-resource filtering is on) the currently free capacity. Warnings are advisory and do not block connecting.

When you click **Get cluster info**, Slurm Connect also checks for existing persistent sessions. If any are found,
an **Existing sessions** selector appears. Choosing one disables the resource fields and attaches the connection
to that allocation when you click **Connect**.

#### Free-resource filtering (default on)
When enabled, the UI filters suggestions to **currently free** resources. This is computed from the same SSH
cluster-info call (no extra prompts) by combining:
- `sinfo -h -N -o "%n|%c|%t|%P|%G"` for per-node totals + state
- `squeue -h -o "%t|%C|%b|%N"` for running job usage

Bad nodes (down/drain/maint) are treated as unavailable, and pending jobs are ignored. The filter limits:
- **Partition list** to partitions with any free CPU/GPU capacity.
- **Nodes** to the count of nodes with free CPU.
- **CPUs per task** to the largest free CPU block on a single node.
- **GPU type/count** to free GPU/MIG slices currently available.

Get cluster info always collects the free-resource inputs in the single SSH call; the toggle just switches whether the UI filters suggestions.
Toggle in the UI or via `slurmConnect.filterFreeResources`.

### Profiles
Profiles let you save and switch between sets of Slurm Connect inputs (login host, identity file, partitions,
resource defaults, module selections, etc.). Use **Save profile** to store the current form values, **Load** to
apply a saved profile, and **Delete** to remove one you no longer need. Profiles are stored in the extension
state and do not change your VS Code settings. Loading a profile fills the form once; subsequent reloads keep
your last edits instead of reapplying the active profile.

### SSH authentication for cluster info
Cluster info queries use non-interactive SSH (`BatchMode=yes`). If your key is encrypted, you can either use ssh-agent or enter the passphrase in a terminal when prompted.

What is ssh-agent?
- `ssh-agent` is a background service that securely stores your SSH keys in memory after you unlock them once.
- It avoids repeated passphrase prompts and enables non-interactive SSH commands (like cluster info queries).
- It is especially useful when the extension runs multiple SSH calls (connect + resource discovery).

- If the key is **not** in your agent, the extension will prompt you to either enter a passphrase in the terminal or add it.
- When you choose to add it, the extension opens a terminal, runs `ssh-add`, and waits for the key to appear in your agent.
- If the SSH agent is unavailable, you will be prompted to enter your passphrase in a terminal for cluster info and connect.
- If no identity file is set, the extension relies on your SSH config and agent; it only prompts after an authentication failure.

If you prefer to do this manually:
```bash
ssh-add /path/to/your/key
ssh-add -l
```

Windows note:
- If you do not have an ssh-agent available, you can enable the OpenSSH Authentication Agent service (requires admin).
- Instructions:
```
https://learn.microsoft.com/en-us/windows-server/administration/openssh/openssh_keymanagement#user-key-generation
```
- Otherwise you will be prompted for your passphrase when getting cluster info or connecting.

### Proxy command (cluster-specific)
This extension expects a proxy script on your cluster's login nodes. The default is:

```
python /usr/bin/vscode-shell-proxy.py
```

Remote-SSH needs the proxy to attach to the compute allocation. If your cluster uses a different path or script name, update `slurmConnect.proxyCommand` accordingly.

If you're installing/using this on a different cluster, make sure the proxy script is accessible and the login nodes have Python 3.9+:
```
https://github.com/xangma/vscode-shell-proxy/blob/main/vscode-shell-proxy.py
```
The "slurmConnect.proxyCommand" setting must execute it, e.g.:

```json
"slurmConnect.proxyCommand": "python /usr/bin/vscode-shell-proxy.py"
```

### Persistent sessions (optional)
By default, Slurm Connect uses persistent sessions so allocations survive reconnects and are cancelled after an idle timeout
(default 10 minutes). Set the timeout to 0 to disable auto-cancel.

Requirements:
- An updated `vscode-shell-proxy.py` with persistent session support.
- A shared filesystem between login and compute nodes (used for session state/markers).

Settings:
- `slurmConnect.sessionMode`: `persistent` (default) or `ephemeral`.
- `slurmConnect.sessionKey`: Optional identifier for reuse; defaults to the SSH alias.
- `slurmConnect.sessionIdleTimeoutSeconds`: Seconds of idle time before cancelling (0 = never).
- `slurmConnect.sessionStateDir`: Optional base directory for session state (default handled by the proxy).

In persistent mode, the proxy submits an allocation via `sbatch`, reuses it on reconnect, and launches each
VS Code connection as a job step (`srun --overlap`). When no session markers remain, the idle timer starts and
the allocation is cancelled once it expires.

Session state is stored under `sessionStateDir/sessions/<username>/<sessionKey>` to avoid cross-user clashes on
shared filesystems. Legacy sessions that live directly under `sessionStateDir/sessions/<sessionKey>` are still
recognized.

Note: Reloading the VS Code window stops active debug sessions (the debugger is owned by the VS Code process).
If you want a debug session to survive a reconnect, start your app with `debugpy` (or similar) and use an
**attach** configuration when you reconnect to the persistent allocation.

### Remote folder (recommended)
If you forget to set a remote folder, VS Code may reconnect and create a new Slurm job when you later open a folder. To avoid that, you should set a remote folder up front in the side panel, set **Remote folder** (recommended).


## Notes
- Ensure **Remote.SSH: Enable Remote Command** is enabled (the extension will prompt to enable it).
- **Remote.SSH: Lockfiles In Tmp** is recommended on shared filesystems (the extension will prompt to enable it).
- The extension will also prompt to put `"remote.SSH.useLocalServer": true` in your vscode settings file if you're on Windows due to a bug with the Remote-SSH extension not respecting the default value from the GUI.
- `remote.SSH.useExecServer` may need to be disabled to reliably reconnect to persistent Slurm sessions (the extension will prompt).
- This extension installs a managed Include block (with a note) at the top of your SSH config that points at the Slurm Connect include file and updates that file on each connection.
- The include file path defaults to `~/.ssh/slurm-connect.conf` and can be overridden with `slurmConnect.temporarySshConfigPath`.
- When updating your SSH config, the extension writes a timestamped backup alongside it (prefixed with `.slurm-connect.backup-`).
- Module load commands will shell-escape module names that contain special characters so the RemoteCommand can be parsed correctly.
- Set `slurmConnect.useSshIncludeBlock` to false to use the legacy temporary Remote.SSH configFile override instead.
- Use `slurmConnect.openInNewWindow` to control whether the connection opens in a new window (default: false).
- `slurmConnect.partitionInfoCommand` controls how cluster info is fetched (default: `sinfo -h -N -o "%P|%n|%c|%m|%G"`).
- To add GPUs or other flags, use `slurmConnect.extraSallocArgs` (e.g. `["--gres=gpu:1"]`). The UI field supports shell-style quoting for multi-word values (e.g. `--comment "foo bar"`).

## Issues
Please report bugs and feature requests on the GitHub repository:
```
https://github.com/xangma/slurm-connect
```


## Development
1. Install dependencies:
   ```bash
   npm install
   ```
2. Build the extension:
   ```bash
   npm run compile
   ```
3. Press **F5** to launch the Extension Development Host.
