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
```

To stop the fixture:

```bash
npm run e2e:slurm:down
```

To remove the fixture containers and volumes:

```bash
npm run e2e:slurm:clean
```

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
