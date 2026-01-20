# SSH and authentication

## SSH config hosts (optional)
The **Login host** field can read your SSH config and list explicit `Host` entries (non-wildcard). Selecting one resolves
the host via `ssh -G` and auto-fills the login host (resolved HostName), **User**, and **Identity file** when those fields are blank.
ProxyJump/ProxyCommand settings are intentionally not imported; if your host relies on them, add them manually.

The picker reads from `slurmConnect.sshQueryConfigPath` when set; otherwise it uses `~/.ssh/config`.

## SSH authentication for cluster info
Cluster info queries use non-interactive SSH (`BatchMode=yes`). If your key is encrypted, you can either use ssh-agent or enter the passphrase in a terminal when prompted. ssh-agent is recommended but optional.

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

## Pre-SSH auth command (2FA / step)
If your SSH config uses an interactive 2FA step (for example `step ssh login` inside `ProxyCommand`), Slurm Connect can run a local command before SSH queries and before Remote-SSH connects.

Configure a local command and an optional check (Step example):
```json
"slurmConnect.preSshCommand": "step ssh login <EMAIL> --provisioner cineca-hpc",
"slurmConnect.preSshCheckCommand": "! step ssh list --raw <EMAIL> | step ssh needs-renewal"
```

The auth command runs in a terminal so you can complete any prompts. The check command should be non-interactive; if it exits 0, Slurm Connect skips the auth command. If no check command is set, the auth command runs each time.

Windows note (untested):
```json
"slurmConnect.preSshCommand": "step ssh login <EMAIL> --provisioner cineca-hpc",
"slurmConnect.preSshCheckCommand": "powershell -NoProfile -Command \"step ssh list --raw <EMAIL> | step ssh needs-renewal; if ($LASTEXITCODE -eq 0) { exit 1 } else { exit 0 }\""
```

## Additional SSH options
Use **Advanced settings -> Additional SSH options (one per line)** to add extra SSH config entries to the generated Slurm Connect hosts.

Example:
```
StrictHostKeyChecking no
UserKnownHostsFile /dev/null
```

Settings JSON example:
```json
"slurmConnect.additionalSshOptions": {
  "StrictHostKeyChecking": "no",
  "UserKnownHostsFile": "/dev/null"
}
```

## Proxy command (cluster-specific)
This extension uses a proxy script on your cluster's login nodes. By default, Slurm Connect auto-installs the **bundled** `vscode-proxy.py` to `~/.slurm-connect/vscode-proxy.py` on connect, and the default proxy command points at that file:

```
python ~/.slurm-connect/vscode-proxy.py
```

Remote-SSH needs the proxy to attach to the compute allocation. The proxy command (and optional `proxyArgs`) are advanced, settings.json-only overrides (not shown in the UI). If you disable auto-install or use a different path or script name, update `slurmConnect.proxyCommand` accordingly.
Auto-install is controlled by `slurmConnect.autoInstallProxyScriptOnClusterInfo` (default on) and uses the bundled script (no GitHub fetch at runtime).
If you disable auto-install, make sure the proxy script already exists at the configured path.

If you're installing/using this on a different cluster, make sure the proxy script is accessible and the login nodes have Python 3.9+.
The bundled script lives in the extension, and its upstream source is:
```
https://github.com/xangma/vscode-shell-proxy/blob/main/vscode-proxy.py
```
The "slurmConnect.proxyCommand" setting must execute it, e.g.:

```json
"slurmConnect.proxyCommand": "python ~/.slurm-connect/vscode-proxy.py"
```
