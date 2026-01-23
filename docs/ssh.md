# SSH and authentication

## SSH config hosts (optional)
The **Login host** field can read your SSH config and list explicit `Host` entries (non-wildcard). Selecting one resolves
the host via `ssh -G` and auto-fills the login host (resolved HostName), **User**, and **Identity file** when those fields are blank.
ProxyJump/ProxyCommand settings are intentionally not imported; if your host relies on them, add them manually.

The picker reads from `slurmConnect.sshQueryConfigPath` when set; otherwise it uses `~/.ssh/config`.

## SSH authentication for cluster info
Cluster info queries use non-interactive SSH (`BatchMode=yes`). If your key is encrypted, you can either use ssh-agent or enter the passphrase in a terminal when prompted. ssh-agent is recommended but optional.
Host key prompts are also non-interactive; `slurmConnect.sshHostKeyChecking` controls StrictHostKeyChecking for these queries (default `accept-new` to auto-trust first-seen keys). If you see "Host key verification failed", set it to `accept-new` or `no`. Use `ask` to require interactive confirmation, or `yes` to enforce strict checking.
Slurm Connect runs SSH commands using the same SSH executable selection as Remote-SSH (`remote.SSH.path` if set; otherwise Remote-SSH's preference order). On Windows with `remote.SSH.useLocalServer=true`, this typically prefers Git's OpenSSH when available.

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

## Local proxy for blocked outbound sites
If compute/login nodes cannot reach a site but your local machine can, Slurm Connect can run a **built-in local HTTP(S) proxy** and expose it to the cluster via SSH. When enabled, the extension starts the proxy locally (no download required), exposes it on the login host, and sets `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` in the remote session.

Example:
```json
"slurmConnect.localProxyEnabled": true,
"slurmConnect.localProxyNoProxy": ["localhost", "127.0.0.1", ".cluster.local"],
"slurmConnect.localProxyRemoteBind": "0.0.0.0",
"slurmConnect.localProxyComputeTunnel": true
```

Notes:
- The local proxy routes all non-loopback hosts; use `localProxyNoProxy` to bypass internal destinations.
- Loopback targets like `localhost`, `127.0.0.1`, and `::1` are never proxied.
- The proxy URL includes short-lived credentials to limit access. Slurm Connect redacts these in logs.
- By default the proxy is exposed via the Remote-SSH connection (single SSH session). This requires `remote.SSH.useExecServer=false` so Remote-SSH does not open a second SSH connection.
- If you need to avoid changing Remote-SSH settings, set `slurmConnect.localProxyTunnelMode` to `dedicated` to use a separate SSH reverse tunnel.
- Compute nodes must be able to reach the login host bind address. If your SSH server disallows non-loopback remote forwards (`GatewayPorts`), enable the compute-node SSH tunnel (`slurmConnect.localProxyComputeTunnel`) so requests route through `127.0.0.1` on the compute node instead. The tunnel uses non-interactive SSH (host key checks disabled to avoid prompts).
- If compute nodes cannot resolve the login host you connect to, set `slurmConnect.localProxyRemoteHost` to an internal hostname or IP they can resolve.

### Compute-node proxy tunnel (slurmConnect.localProxyComputeTunnel)
When enabled (default), Slurm Connect starts an SSH tunnel from the compute node back to the login host proxy:
- The compute node connects to the login host and forwards `127.0.0.1:<random>` on the compute node to `127.0.0.1:<proxyPort>` on the login host.
- Proxy environment variables on the compute node are set to `http://127.0.0.1:<localPort>` so tools (e.g. `curl`, `pip`) can reach the local proxy even when `GatewayPorts` is disabled.
- SSH is non-interactive (`BatchMode=yes`) and disables host key checks to avoid blocking prompts; failures are logged in the Slurm Connect output.
- The tunnel respects `slurmConnect.sshConnectTimeoutSeconds` (or `remote.SSH.connectTimeout` if you set it to 0).

Disable it only if your compute nodes can directly reach the login-host bind address specified by `slurmConnect.localProxyRemoteBind`, and your SSH server allows those remote forwards.
