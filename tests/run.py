#!/usr/bin/env python3
"""Zero-dependency test runner.

    python tests/run.py              # everything
    python tests/run.py logger gzip  # only modules whose name contains these

The suite is plain functions and plain asserts, so `pytest tests/` works too
and gives nicer failure output. This runner exists so the tests can be run
anywhere the logger can run — on the boat, over a slow link, with nothing
installed but the logger's own dependencies.
"""

import importlib.util
import sys
import unittest
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))


def load(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main(argv: list[str]) -> int:
    files = sorted(HERE.glob("test_*.py"))
    if argv:
        files = [f for f in files if any(a in f.stem for a in argv)]
        if not files:
            print(f"no test module matches {argv}")
            return 2

    passed, skipped, failed = 0, [], []
    for path in files:
        print(f"\n{path.stem}")
        module = load(path)
        for name in sorted(n for n in dir(module) if n.startswith("test_")):
            fn = getattr(module, name)
            if not callable(fn):
                continue
            label = name.removeprefix("test_").replace("_", " ")
            t0 = time.perf_counter()
            try:
                fn()
            except unittest.SkipTest as why:
                skipped.append((path.stem, name, str(why)))
                print(f"  skip  {label}   ({why})")
            except Exception:
                failed.append((path.stem, name, traceback.format_exc()))
                print(f"  FAIL  {label}")
            else:
                dt = time.perf_counter() - t0
                passed += 1
                print(f"  ok    {label}" + (f"   ({dt:.1f}s)" if dt > 0.2 else ""))

    print()
    for mod, name, why in skipped:
        print(f"SKIPPED {mod}.{name}: {why}")
    if failed:
        for mod, name, tb in failed:
            print(f"{'=' * 70}\n{mod}.{name}\n{'=' * 70}\n{tb}")
        print(f"{passed} passed, {len(skipped)} skipped, {len(failed)} FAILED")
        return 1
    print(f"{passed} passed" + (f", {len(skipped)} skipped" if skipped else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
