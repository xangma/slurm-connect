# Slurm E2E Fixture

This repo includes a disposable local Slurm fixture built on top of the upstream
[`giovtorres/slurm-docker-cluster`](https://github.com/giovtorres/slurm-docker-cluster)
project.

The wrapper script in this repo does three things:

1. Clones and pins the upstream fixture repo to a known commit.
2. Generates a disposable SSH keypair under `.e2e/` at runtime.
3. Creates a disposable non-root SSH user inside the fixture and runs real Slurm smoke checks through it.

No SSH private keys are checked in. Generated keys, known-hosts files, and the
cloned upstream fixture are all written under `.e2e/slurm-fixture/`.

## Requirements

- Docker and Docker Compose
- `ssh`, `scp`, `ssh-keygen`, `ssh-keyscan`

## Quick Start

```bash
npm run e2e:slurm:up
npm run e2e:slurm:smoke
npm run test:integration:slurm
npm run e2e:slurm:remote-session
npm run e2e:slurm:local-proxy-session
```

To stop the fixture:

```bash
npm run e2e:slurm:down
```

To remove the fixture containers and volumes:

```bash
npm run e2e:slurm:clean
```

## External OS Client Test

The Docker fixture is Linux-only. To cover client behavior across operating
systems, run the external client tests from real client machines that can reach
the fixture host. Current release validation uses this split:

- macOS client coverage runs locally on a Mac.
- Linux client coverage runs against the external SSH host `len`.
- Windows client coverage runs against the external SSH host `mac-remote`.

Linux client runs need `xvfb-run` for Electron-based VS Code tests; on Ubuntu,
install the `xvfb` package.

Run it locally with:

```bash
npm run test:integration:slurm-client
npm run test:integration:slurm-client-remote-session
npm run test:integration:slurm-client-local-proxy
```

The command skips with exit code 0 unless the minimum external fixture settings
are present:

- `SLURM_CLIENT_SSH_HOST`
- `SLURM_CLIENT_SSH_USER`
- one of `SLURM_CLIENT_SSH_PRIVATE_KEY`, `SLURM_CLIENT_SSH_PRIVATE_KEY_B64`, or
  `SLURM_CLIENT_SSH_PRIVATE_KEY_PATH`

Optional settings:

- `SLURM_CLIENT_SSH_PORT`, default `22`
- `SLURM_CLIENT_KNOWN_HOSTS`, `SLURM_CLIENT_KNOWN_HOSTS_B64`, or
  `SLURM_CLIENT_KNOWN_HOSTS_PATH`
- `SLURM_CLIENT_HOME`, otherwise queried over SSH
- `SLURM_CLIENT_DEFAULT_PARTITION`, useful for clusters without a default
  partition
- `SLURM_CLIENT_EXPECTED_SINFO_PATTERN`, a regular expression checked against
  `sinfo -h -o "%P|%D|%t"`
- `SLURM_CLIENT_EXPECTED_COMPUTE_HOST_PATTERN`, a regular expression checked
  against the hostname reported by the remote filesystem marker after allocation
- `SLURM_CLIENT_EXTRA_SALLOC_ARGS_JSON`, a JSON array of extra `salloc`
  arguments for clusters that require account, QoS, reservation, or similar
  flags
- `SLURM_CLIENT_REMOTE_WORKSPACE_PATH`, otherwise the remote home directory is
  opened
- `SLURM_CLIENT_STARTUP_COMMAND`, `SLURM_CLIENT_MODULE_LOAD`, or
  `SLURM_CLIENT_PROXY_COMMAND` for clusters that need environment setup before
  running the proxy
- `SLURM_CLIENT_LOCAL_PROXY_REMOTE_HOST` if compute nodes must SSH back to a
  different login-host name when creating the local-proxy compute tunnel
- `SLURM_CLIENT_LOCAL_PROXY_TARGET_HOST` if the client runner must advertise a
  specific non-loopback IPv4 address for the client-side probe server
- `SLURM_CLIENT_LOCAL_PROXY_COMPUTE_TUNNEL=0` to test without the compute-node
  SSH tunnel on clusters where the login-host remote forward is reachable
  directly from compute nodes
- `SLURM_CLIENT_E2E_REQUIRED=1`, which turns missing configuration into a hard
  failure instead of a skip. The GitHub Actions client matrix defaults this to
  `1` for same-repository runs so missing fixture configuration is reported
  immediately.

GitHub Actions has a disabled-by-default `slurm-client-e2e` job for future
hosted-runner coverage. Set the repository variable
`SLURM_CLIENT_E2E_ON_GITHUB=1` only when GitHub-hosted macOS and Windows
runners can reach the configured external Slurm login node. When enabled,
GitHub Actions reads the same values from repository secrets and variables. The
job skips for fork pull requests where repository secrets are intentionally not
available. For same-repository pushes and pull requests, missing required
fixture settings fail the `slurm-client-e2e` matrix instead of silently skipping.

To configure the GitHub repository from a local shell that has the GitHub CLI
authenticated with repository admin access, export the fixture values or put them
in a dotenv-style file, then run:

```bash
npm run e2e:slurm:github:configure -- --env-file .env.slurm-client --verify
```

The setup command writes these repository secrets:

- `SLURM_CLIENT_SSH_HOST`
- `SLURM_CLIENT_SSH_USER`
- `SLURM_CLIENT_SSH_PORT`, when set
- `SLURM_CLIENT_SSH_PRIVATE_KEY_B64`
- `SLURM_CLIENT_KNOWN_HOSTS_B64`, when set

It writes optional cluster settings as repository variables, including
`SLURM_CLIENT_E2E_REQUIRED=1`. The command accepts
`SLURM_CLIENT_SSH_PRIVATE_KEY_PATH` and `SLURM_CLIENT_KNOWN_HOSTS_PATH` locally,
but stores the file contents as base64 GitHub secrets so multiline SSH material
survives every client OS in the matrix. Use `--dry-run` to check which settings
would be written without sending values to GitHub.

`npm run test:integration:slurm-client-remote-session` is the full Remote-SSH
allocation check for the OS matrix. It installs the bundled proxy package on the
external login node over SSH, launches an isolated VS Code window with
Remote-SSH, starts a Slurm allocation through Slurm Connect, and verifies that
the remote filesystem marker reports both a hostname and a Slurm job id. If
`SLURM_CLIENT_EXPECTED_COMPUTE_HOST_PATTERN` is set, the reported hostname must
also match that pattern.

`npm run test:integration:slurm-client-local-proxy` runs the same Remote-SSH
allocation path with `slurmConnect.localProxyEnabled=true`. The client starts a
tokenized HTTP probe server, the remote extension host performs an HTTP request
through the remote `HTTP_PROXY` value, and the client verifies that the probe
server received the request. Run this manually on each release client OS unless
`SLURM_CLIENT_E2E_ON_GITHUB=1` is configured for reachable hosted runners.

## What The Smoke Test Covers

`npm run e2e:slurm:smoke` runs:

- targeted cluster readiness checks through `slurmctld`
- SSH into the exposed login node as the disposable non-root fixture user
- `sinfo` over SSH
- `srun` over SSH
- `salloc ... srun hostname` over SSH
- `sbatch` over SSH with output polling
- execution of the bundled `media/vscode-proxy.py` on the remote host

This is intentionally a real SSH + Slurm path, not a mocked test.

`npm run test:integration:slurm` runs the VS Code extension host against the same fixture and verifies that:

- the extension can query the fixture over SSH
- the connect flow writes a real SSH include file
- the generated SSH alias can connect to the fixture user

`npm run e2e:slurm:remote-session` launches a real VS Code window with
`ms-vscode-remote.remote-ssh` installed into an isolated profile and verifies
that:

- Slurm Connect can hand off to a real Remote-SSH session
- the remote window opens with `ssh-remote` authority for the generated alias
- the remote filesystem provider becomes available
- the remote window can write a marker file back onto the fixture host

`npm run e2e:slurm:local-proxy-session` runs the same local fixture path with
`slurmConnect.localProxyEnabled=true`, starts a tokenized HTTP server on the
client, and verifies that a request from the remote allocation returns to that
client-side server through the local proxy. The probe first tries non-loopback
client IPv4 addresses, then falls back to `127-0-0-1.sslip.io` so local runs work
on machines that cannot connect back to their own LAN address. This proxy-focused
test skips the separate remote filesystem marker assertion; that assertion is
covered by `npm run e2e:slurm:remote-session`.

## Suggested Extension Settings

The fixture script prints a JSON snippet after startup. The simplest manual
setup in the extension is:

```json
{
  "slurmConnect.loginHosts": ["127.0.0.1"],
  "slurmConnect.user": "slurmconnect",
  "slurmConnect.identityFile": "/absolute/path/to/.e2e/slurm-fixture/ssh/id_ed25519",
  "slurmConnect.additionalSshOptions": {
    "Port": "3022",
    "UserKnownHostsFile": "/absolute/path/to/.e2e/slurm-fixture/ssh/known_hosts"
  },
  "slurmConnect.sshQueryConfigPath": "/absolute/path/to/.e2e/slurm-fixture/ssh/ssh_config",
  "slurmConnect.sshHostKeyChecking": "accept-new"
}
```

You can reprint the current fixture paths with:

```bash
npm run e2e:slurm:settings
```

## Notes

- The fixture uses the `slurmctld` container as the login node. That is good
  enough for extension E2E testing even though it is not perfect production
  topology.
- The wrapper pins the upstream fixture repository to a specific commit for
  reproducibility.
- The default fast local pipeline remains `npm run test:ci`.
- The full local Slurm fixture path is intended to be run as three explicit
  commands: `npm run e2e:slurm:smoke`, `npm run test:integration:slurm`, and
  `npm run e2e:slurm:remote-session`.
- GitHub Actions also runs the fixture smoke test, the extension fixture test,
  and the full Remote-SSH session test as a separate Linux job so the real SSH +
  Slurm path is checked automatically without slowing every local developer loop.
- GitHub Actions also defines an optional `slurm-client-e2e` OS matrix job for
  Linux, macOS, and Windows SSH clients. It requires an externally supplied
  Slurm login node. Same-repository runs fail fast when the endpoint secrets are
  missing; fork pull requests skip because GitHub does not expose those secrets
  to forks. When configured, the matrix runs both the login-node connect-flow
  check and the full Remote-SSH allocation session checks, including local proxy
  routing back to the client.
