# Sciama Slurm Connect (VS Code extension)

This extension helps users allocate Slurm resources on SCIAMA and connect VS Code Remote-SSH through a compute node. It discovers partitions (and optionally QoS/accounts), builds the `RemoteCommand` for `vscode-shell-proxy.py`, creates a temporary SSH host entry, and optionally connects right away.

## Requirements
- VS Code with **Remote - SSH** installed.
- SSH keys configured for the cluster (agent forwarding recommended).
- `vscode-shell-proxy.py` available on the login nodes (via module load or PATH).

## Quick start (users)
1. Install the extension.
2. Open the **Sciama Slurm** view from the activity bar.
3. Enter login host, username, and identity file.
4. Click **Get cluster info**, choose resources, then **Connect**.

The extension will query the login host, create a temporary SSH config entry, connect, and then restore your previous Remote-SSH config setting. Your main SSH config is not modified.

## Example settings (SCIAMA)
```json
{
  "sciamaSlurm.loginHosts": [
    "hostname1.com",
  ],
  "sciamaSlurm.loginHostsCommand": "",
  "sciamaSlurm.moduleLoad": "module load anaconda3/2024.02",
  "sciamaSlurm.proxyCommand": "python vscode-shell-proxy.py",
  "sciamaSlurm.identityFile": "~/.ssh/id_rsa",
  "sciamaSlurm.defaultNodes": 1,
  "sciamaSlurm.defaultTasksPerNode": 1,
  "sciamaSlurm.defaultCpusPerTask": 8,
  "sciamaSlurm.defaultTime": "24:00:00"
}
```

## Discover login hosts
If your cluster can return login hosts via a command, set:
```json
{
  "sciamaSlurm.loginHostsCommand": "your-command-here",
  "sciamaSlurm.loginHostsQueryHost": "hostname1.com"
}
```
The command should output hostnames separated by whitespace or newlines.

## Notes
- Ensure **Remote.SSH: Enable Remote Command** is enabled (the extension will prompt to enable it).
- This extension uses a temporary SSH config for each connection and does not modify your main SSH config.
- Use `sciamaSlurm.openInNewWindow` to control whether the connection opens in a new window (default: false).
- Set `sciamaSlurm.remoteWorkspacePath` to open a specific remote folder. Leave it empty to connect without opening a folder.
- `sciamaSlurm.partitionInfoCommand` controls how cluster info is fetched (default: `sinfo -h -N -o "%P|%n|%c|%m|%G"`).
- To add GPUs or other flags, use `sciamaSlurm.extraSallocArgs` (e.g. `["--gres=gpu:1"]`).

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
