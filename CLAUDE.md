# nmea2s3 — rules for working in this repo

## Versioning

`--version` is how anyone tells what a box is actually running, so it must
change whenever what an installed command does changes.

- The version lives in ONE place: `__version__` in `src/nmea2s3/__init__.py`.
  `pyproject.toml` reads it from there; never add a second copy.
- Any change under `src/` that reaches an installed command — a fix, a new
  field, a changed default — ships with a version bump in the same push:
  - patch (`0.3.1` -> `0.3.2`) by default; minor or major only when asked
  - the bump as its own commit, titled `<version>, <why>`
  - an annotated tag `v<version>` on that commit, message `v<version> — <summary>`
  - `main` and the tag pushed together: `git push origin main v<version>`
- Several fixes going out together share one bump, made after the last of them.
- No bump for changes nothing installs: `tests/`, `sql/`, docs, `TODO.md`.

Why: `pipx upgrade` compares version numbers, so an unbumped change looks like
nothing to install, and a box on old code reports the same version as one on
new code. It happened: 0.3.0 covered ten commits, including the fix for the
RAW_WIND_S range guard, so no `--version` could say whether a box had it.
