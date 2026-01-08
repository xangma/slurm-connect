# Slurm Connect (VS Code extension)

This extension helps users allocate Slurm resources on a cluster and connect VS Code Remote-SSH through a compute node. It discovers partitions (and optionally QoS/accounts), builds the `RemoteCommand` for a proxy script, creates a temporary SSH host entry, and optionally connects right away.

## Requirements
- VS Code with **Remote - SSH** installed.
- SSH keys configured for the cluster (agent forwarding recommended).
- `vscode-shell-proxy.py` available on the login nodes (via module load or PATH).
- ssh-agent running with your key added.

## Quick start (users)
1. Install the extension.
2. Open the **Slurm Connect** view from the activity bar.
3. Enter login host, username, and identity file.
4. Click **Get cluster info**, choose resources, then **Connect**.

The extension will query the login host, create a temporary SSH config entry, connect, and then restore your previous Remote-SSH config setting. Your main SSH config is not modified.

## Usage details

### SSH agent requirement
Cluster info queries use non-interactive SSH (`BatchMode=yes`), so your SSH key must be available to the agent.

- If the key is **not** in your agent, the extension will prompt you to add it.
- When prompted, it will open a terminal, run `ssh-add`, and wait for the key to appear in your agent.

If you prefer to do this manually:
```bash
ssh-add /path/to/your/key
ssh-add -l
```

### Modules
The extension runs your **module load** command before starting the proxy on the login node. This is where you should add any required cluster modules (e.g. anaconda, cuda, or custom environment setup).

Example:
```json
{
  "slurmConnect.moduleLoad": "module load anaconda3/2024.02"
}
```

### Proxy command (cluster-specific)
This extension expects a proxy script on your cluster's login nodes. The default is:

```
python /usr/bin/vscode-shell-proxy.py
```

Remote-SSH needs the proxy to attach to the compute allocation. If your cluster uses a different path or script name, update `slurmConnect.proxyCommand` accordingly.

### Remote folder (recommended)
If you forget to set a remote folder, VS Code may reconnect and create a new Slurm job when you later open a folder. To avoid that, you should set a remote folder up front:

- In the side panel, set **Remote folder** (recommended)
- Or set it in settings:
```json
{
  "slurmConnect.remoteWorkspacePath": "/home/youruser/project"
}
```

## Example settings
```json
{
  "slurmConnect.loginHosts": [
    "hostname1.com",
  ],
  "slurmConnect.loginHostsCommand": "",
  "slurmConnect.proxyCommand": "python /usr/bin/vscode-shell-proxy.py",
  "slurmConnect.identityFile": "~/.ssh/id_rsa",
  "slurmConnect.defaultNodes": 1,
  "slurmConnect.defaultTasksPerNode": 1,
  "slurmConnect.defaultCpusPerTask": 8,
  "slurmConnect.defaultTime": "24:00:00",
  "slurmConnect.remoteWorkspacePath": "/home/youruser/project"
}
```

## Discover login hosts
If your cluster can return login hosts via a command, set:
```json
{
  "slurmConnect.loginHostsCommand": "your-command-here",
  "slurmConnect.loginHostsQueryHost": "hostname1.com"
}
```
The command should output hostnames separated by whitespace or newlines.

## Notes
- Ensure **Remote.SSH: Enable Remote Command** is enabled (the extension will prompt to enable it).
- **Remote.SSH: Lockfiles In Tmp** is recommended on shared filesystems (the extension will prompt to enable it).
- This extension uses a temporary SSH config for each connection and does not modify your main SSH config.
- Use `slurmConnect.openInNewWindow` to control whether the connection opens in a new window (default: false).
- `slurmConnect.partitionInfoCommand` controls how cluster info is fetched (default: `sinfo -h -N -o "%P|%n|%c|%m|%G"`).
- To add GPUs or other flags, use `slurmConnect.extraSallocArgs` (e.g. `["--gres=gpu:1"]`).

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
