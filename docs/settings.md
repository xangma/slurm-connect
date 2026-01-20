# Settings reference

All settings live under the `slurmConnect` namespace.

| Setting | Type | Default | Description |
| --- | --- | --- | --- |
| `slurmConnect.loginHosts` | array | `[""]` | Login hosts available for connection. Used when loginHostsCommand is empty. |
| `slurmConnect.loginHostsCommand` | string | `""` | Remote command that outputs login hosts (newline or whitespace separated). If set, it is executed over SSH to discover login nodes. |
| `slurmConnect.loginHostsQueryHost` | string | `""` | Host used to run loginHostsCommand. Defaults to the first login host or the selected login host. |
| `slurmConnect.partitionCommand` | string | `"sinfo -h -o \"%P\""` | Remote command that outputs partition names. |
| `slurmConnect.partitionInfoCommand` | string | `"sinfo -h -N -o \"%P\|%n\|%c\|%m\|%G\""` | Remote command that outputs partition info in the format name\|node\|cpus\|memMB\|gres. |
| `slurmConnect.filterFreeResources` | boolean | `true` | Filter cluster info suggestions to currently free resources. |
| `slurmConnect.qosCommand` | string | `""` | Optional remote command that outputs QoS names. |
| `slurmConnect.accountCommand` | string | `""` | Optional remote command that outputs account names. |
| `slurmConnect.user` | string | `""` | SSH username. Leave blank to use the local OS username. |
| `slurmConnect.identityFile` | string | `""` | Optional SSH identity file to include in generated host entries and used for resource queries. |
| `slurmConnect.preSshCommand` | string | `""` | Optional local command to run before SSH queries and Remote-SSH connections (e.g. step ssh login ...). |
| `slurmConnect.preSshCheckCommand` | string | `""` | Optional local command used to determine whether pre-SSH authentication is still valid (exit 0 = skip pre-SSH command). |
| `slurmConnect.autoInstallProxyScriptOnClusterInfo` | boolean | `true` | Auto-install/update the bundled proxy script on the login host when connecting. |
| `slurmConnect.forwardAgent` | boolean | `true` | Whether to set ForwardAgent yes in generated host entries. |
| `slurmConnect.requestTTY` | boolean | `true` | Whether to set RequestTTY yes in generated host entries. |
| `slurmConnect.moduleLoad` | string | `""` | Optional module load command prepended to the RemoteCommand. |
| `slurmConnect.proxyCommand` | string | `"python ~/.slurm-connect/vscode-proxy.py"` | Advanced: Command that launches the Slurm proxy script on the remote host (not shown in the UI). |
| `slurmConnect.proxyArgs` | array | `[]` | Extra arguments appended to the proxy command. |
| `slurmConnect.extraSallocArgs` | array | `[]` | Extra salloc arguments appended to every request (e.g. --gres=gpu:1). |
| `slurmConnect.promptForExtraSallocArgs` | boolean | `false` | Prompt for additional salloc arguments each time you connect. |
| `slurmConnect.sessionMode` | string | `"persistent"` | Allocation mode. Persistent reuses a Slurm allocation across reconnects. |
| `slurmConnect.sessionKey` | string | `""` | Optional key to identify a persistent session. Leave blank to reuse the SSH alias. |
| `slurmConnect.sessionIdleTimeoutSeconds` | number | `600` | Idle timeout in seconds before cancelling a persistent allocation. Set to 0 to disable. |
| `slurmConnect.sessionStateDir` | string | `""` | Optional directory for persistent session state (markers/job info). Leave blank to use the proxy default. |
| `slurmConnect.defaultPartition` | string | `""` | Default partition selection. |
| `slurmConnect.defaultNodes` | number | `1` | Default number of nodes. |
| `slurmConnect.defaultTasksPerNode` | number | `1` | Default tasks per node. |
| `slurmConnect.defaultCpusPerTask` | number | `8` | Default CPUs per task. |
| `slurmConnect.defaultTime` | string | `"24:00:00"` | Default wall time in HH:MM:SS or D-HH:MM:SS format. |
| `slurmConnect.defaultMemoryMb` | number | `0` | Default memory per node in MB. Set to 0 to leave unset. |
| `slurmConnect.defaultGpuType` | string | `""` | Default GPU type (e.g. A100). Leave blank for any type. |
| `slurmConnect.defaultGpuCount` | number | `0` | Default GPU count. Set to 0 to leave unset. |
| `slurmConnect.sshHostPrefix` | string | `"slurm"` | Prefix for generated SSH host aliases. |
| `slurmConnect.openInNewWindow` | boolean | `false` | Open the Remote-SSH connection in a new window. |
| `slurmConnect.remoteWorkspacePath` | string | `""` | Remote folder to open after connecting (e.g. /home/user/project). Leave blank to just connect. |
| `slurmConnect.temporarySshConfigPath` | string | `"~/.ssh/slurm-connect.conf"` | Path for the Slurm Connect SSH include file. The extension installs a small Include block (with a note) in your SSH config that points here. |
| `slurmConnect.additionalSshOptions` | object | `{}` | Additional SSH config options to include in generated host entries. |
| `slurmConnect.sshQueryConfigPath` | string | `""` | Optional SSH config path to use when querying the cluster. |
| `slurmConnect.sshConnectTimeoutSeconds` | number | `15` | Timeout for SSH resource queries in seconds. |

## Notes
- `proxyCommand` and `proxyArgs` are advanced overrides and are not exposed in the UI.
- Array settings accept JSON arrays in settings.json; the UI uses newline-separated values where applicable.
- `additionalSshOptions` is a map of SSH config keys to values (written into generated host entries).
