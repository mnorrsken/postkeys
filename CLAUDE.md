# pg-kv-backend Release Process

## Version scheme
`vMAJOR.MINOR.PATCH` — bump **minor** for new features, **patch** for bug fixes/small changes.

## Step-by-step

### 1. Commit the feature/fix changes first
```bash
git add <files>
git commit -m "Short description of change"
```

### 2. Check current version
```bash
git tag --sort=-v:refname | head -5
```
Latest tag = current version. Determine next version based on change type.

### 3. Update CHANGELOG.md
Add a new section **at the top** (below the intro line), before the previous latest version:
```markdown
## [X.Y.Z] - YYYY-MM-DD

### Added
- **Feature name** — description of what was added.

### Changed
- **Thing** — description of what changed.

### Fixed
- **Bug** — description of the fix.
```
Only include the headings that apply.

### 4. Update README.md if necessary
Only update if the change affects documented behaviour, commands, or configuration.

### 5. Commit changelog (and readme if changed)
```bash
git add CHANGELOG.md README.md
git commit -m "Add <feature> (vX.Y.Z)"
```

### 6. Tag and push
```bash
git tag vX.Y.Z
git push origin main
git push origin vX.Y.Z
```

## What happens automatically on tag push

GitHub Actions (`.github/workflows/docker-publish.yml`) triggers on `v*` tags and runs:

1. **Tests** — unit tests + PostgreSQL integration tests (must pass before anything is published)
2. **build-and-push** — builds multi-platform Docker image (amd64/arm64) and pushes to GHCR with semver tags (`X.Y.Z`, `X.Y`, `X`, `latest`)
3. **helm** — extracts version from tag, sets it in `Chart.yaml` automatically, lints, packages, and pushes the Helm chart to GHCR OCI registry
4. **release** — creates a GitHub release with install instructions

## Notes
- Do **not** manually bump `Chart.yaml` — the CI sets `version` and `appVersion` from the git tag
- No version string in Go source — version is tracked only via git tags
- Commit message convention: `Add <feature> (vX.Y.Z)` or `Fix <thing> (vX.Y.Z)`
- Tests also run on every push to `main` and on pull requests (but no publishing occurs without a tag)
