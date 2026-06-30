# Devcontainer

**check (default):** root `.devcontainer/devcontainer.json` - `make lint`, `make test`,
`make test-rapid`, `make test-stress`, `make check`. Works locally and on Codespaces.

**docker (opt-in):** `.devcontainer/docker/devcontainer.json` - adds `docker-outside-of-docker`
for `make test-integration`, `make aeronmd`, Java interop, and cluster compose smokes.
Requires a host Docker daemon (standard rootful install).

Both use `mcr.microsoft.com/devcontainers/go:1.26-bookworm` plus dev container features
(Java 26, Maven, shellcheck). Post-create checks container Go matches `go.mod` major.minor.

**Security:** docker config DooD is effectively host-root via the Docker API. Use only on
trusted workstations; use check in cloud workspaces.

## After rebuild

`post-create.sh` runs automatically via `postCreateCommand` in both devcontainer configs (create
and rebuild). Re-run manually if needed: `bash .devcontainer/scripts/post-create.sh`.

1. check: `make lint`, then `make check`.
2. docker (Linux host with Docker): `docker ps`, then `make aeronmd` and integration targets as needed.
