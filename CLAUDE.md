# postkeys

Redis 7 API-compatible server (RESP2/RESP3, pub/sub with RESP3 push, Lua
EVAL/EVALSHA, MULTI/EXEC) that stores everything in PostgreSQL. Go. Ships as a
multi-arch Docker image and a Helm chart on GHCR.

## Commands

- `make build`, `make test` (starts the test Postgres from
  `docker-compose.test.yml` via `make test-up`, runs `./tests/...` with
  `-tags=postgres` and `./internal/...`), `make test-down`
- `make bench`, `make bench-redis`, `make bench-compare` for performance work
- `make docker-build`, `make docker-up` / `make docker-down` for a local stack
- `make deploy` / `make undeploy` push the Helm chart to the current kube
  context. Ask before running them.

## Verify before done

`make test` green. For a new or changed Redis command: add it to the handler,
the README supported/unsupported lists, and an integration test in `tests/`
that exercises it through a real Redis client.

## Layout and conventions

- `internal/handler`: command dispatch (`handler.go`, `handler_ops.go`,
  `handler_pubsub.go`, `lua.go`). `internal/storage`: the Postgres layer
  (`interface.go`, `querier.go`, `transaction.go`). Handlers never write SQL;
  they go through the storage interface.
- `internal/resp` parses and writes the protocol; `internal/pubsub`,
  `internal/listnotify`, `internal/leader`, `internal/cache`, `internal/metrics`
  are the supporting subsystems.
- Match Redis error strings exactly (`ERR ...`, `WRONGTYPE ...`); clients
  depend on them.
- Unsupported commands stay listed in README. Never return OK for something
  that is not implemented.

## Release notes (what differs from the wiki "GitHub Release Process" skill)

- Changelog heading: `## [X.Y.Z] - YYYY-MM-DD`.
- Release commit: `Add <feature> (vX.Y.Z)` or `Fix <thing> (vX.Y.Z)`, changelog
  and readme in that commit.
- `ci.yml` runs on push/PR. `docker-publish.yml` on `v*` tags runs the tests,
  builds the amd64/arm64 image with semver tags, sets `Chart.yaml`
  `version`/`appVersion` from the tag (never bump them by hand), pushes the
  chart to the GHCR OCI registry and creates the GitHub release.
- No version string in Go source; git tags only.
