# Slurm Connect (VS Code extension)

This extension helps users allocate Slurm resources on a cluster and connect VS Code Remote-SSH through a compute node.

Key features:
- One-click discovery of partitions, QoS/accounts (optional), modules, and free-resource hints.
- Persistent or ephemeral sessions, with reconnect to existing allocations.
- Automatic SSH include management for Remote-SSH (no manual host entries).
- Resource validation hints and profile support for repeatable runs.
- Module picker with paste, chips, and clear-all.
- Optional remote folder open + window targeting.

## Requirements
- VS Code with **Remote - SSH** installed.
- SSH authentication configured for the cluster (SSH config or agent; agent forwarding recommended).
- Python 3.9+ available on the login nodes.
- Bundled `vscode-proxy.py` auto-installed to `~/.slurm-connect/vscode-proxy.py` on connect (can be disabled).

## Quick start (users)
1. Install the extension.
2. Open the **Slurm Connect** view from the activity bar.
3. Select a login host (or pick from SSH config), username if needed, and an optional identity file.
4. Click **Get cluster info**, choose resources, then **Connect**.

The extension will query the login host, write a Slurm Connect SSH include file, ensure your SSH config contains a small Slurm Connect Include block (with a note), and connect. It does not replace your SSH config; it only adds the managed block. Local Slurm Connect logs are capped (currently 5 MB) to avoid unbounded growth.

## Docs
- Usage and UI reference: `docs/usage.md`
- Cluster info and free-resource filtering: `docs/cluster-info.md`
- SSH and authentication (ssh-agent, step, additional options, proxy command): `docs/ssh.md`
- Persistent sessions: `docs/persistent-sessions.md`
- Settings reference: `docs/settings.md`
