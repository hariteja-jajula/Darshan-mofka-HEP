# Provenance

The `darshan/`, `diaspora-stream-api/`, and `flowcept/` directories are **vendored
plain source** (not git submodules), snapshotted from the following upstream forks
and commits. They were exported with `git archive HEAD`, so they contain source
only — no `.git` metadata and no build artifacts.

| Vendored tree          | Upstream fork                                          | Branch         | Commit    |
|------------------------|-------------------------------------------------------|----------------|-----------|
| `darshan/`             | https://github.com/hariteja-jajula/darshan            | `ALCF_polaris` | `21c26f42ee843b260fbaf5921f5755f170c072ec` |
| `diaspora-stream-api/` | https://github.com/hariteja-jajula/diaspora-stream-api| `ALCF_polaris` | `c146e0722027a11018889a62f791ffa99fe4304f` |
| `flowcept/`            | https://github.com/hariteja-jajula/flowcept           | `feature/sharded-consumer-targets` | `feba3caa11fb0521880e912122643d3b00435724` |

## Why vendored, not submodules

The `darshan` commit `21c26f42` ("remove custom ring buffer + drain thread; push
directly to Diaspora") — the Phase-1 state these HEP measurements depend on — was
local-only at snapshot time and not reachable from the public fork's published
branch tip. Vendoring the exact source guarantees this repository builds the
intended code without depending on that commit being fetchable.

To re-sync a tree with its upstream fork later:

```bash
git clone <fork-url> /tmp/<name>
git -C /tmp/<name> checkout <commit>
rsync -a --delete --exclude='.git' /tmp/<name>/ ./<name>/
```
