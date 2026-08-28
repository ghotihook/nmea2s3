"""Who writes to _log/, and who deliberately does not.

The rule: only work that CHANGES the archive writes an entry. A read-only
tool writes none, a dry run writes none. A writer that runs long enough to be
interrupted writes a START and an END paired by run_id, and writes the end
entry whether or not it changed anything — so "completed with nothing to do"
stays distinguishable from "died partway", which no exception-catching
wrapper can detect because a SIGKILL raises no exception. One lived in
audit_log.py until 2026-08-28, called by nothing.

Two tools write here. The logger is the shape that needs the pairing: it
runs until something stops it, so a start with no end is how a killed run
shows up at all. `nmea2s3-update-pg` writes one entry per run that changed
something, and none at all for `--dry-run`.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import helpers as H                                              # noqa: E402


# ── the exporter: read-only, therefore silent ────────────────────────────

def test_the_exporter_has_no_audit_logging_at_all():
    """Reading changes nothing, and entries are permanent: production
    credentials cannot delete, so an entry per export grew the bucket every
    time someone piped a query into jq."""
    from nmea2s3 import export
    assert not hasattr(export, "log_action_safely")


def test_the_exporter_signals_a_partial_export_through_its_exit_code():
    """Exit 2 used to exist only as a field inside an audit entry, so the
    process exited 0 and no caller could detect an incomplete export."""
    import inspect
    from nmea2s3 import export
    src = inspect.getsource(export.main)
    assert "sys.exit(2)" in src, "a real exit code, not a claim in a log entry"


# ── update-pg: writes only when it changed something ─────────────────────

def test_a_dry_run_writes_no_audit_entry():
    """`--dry-run` decodes and reports and touches nothing, so an entry for it
    would be a permanent record of work that never happened — in a bucket
    whose credentials cannot delete. Parsed rather than driven: reaching the
    call site needs a live Postgres, and what has to hold is that the write
    is unreachable on the dry-run branch, which is a property of the code's
    shape.
    """
    import ast
    src = (H.REPO / "src" / "nmea2s3" / "pg" / "update.py").read_text()

    def writes_audit(nodes):
        return any(isinstance(n, ast.Call) and getattr(n.func, "id", None) == "log_action_safely"
                   for node in nodes for n in ast.walk(node))

    guards = [n for n in ast.walk(ast.parse(src))
              if isinstance(n, ast.If) and "dry_run" in ast.dump(n.test)]
    assert guards, "nothing distinguishes a dry run from a real one"
    assert any(writes_audit(g.orelse) and not writes_audit(g.body) for g in guards), \
        "the audit write must sit on the branch a dry run does not take"


# ── the logger: brackets its own writes ──────────────────────────────────

def test_the_logger_names_itself_in_every_entry():
    """Renamed from `flightrecorder_logger` when this was split into its own
    repo. `_log/` is permanent, so both names live in the archive forever and
    a reader has to know which one to expect from which era.

    What each record has to SAY is in test_logger.py, next to the code that
    produces it; this is the name and the counters only.
    """
    import asyncio
    from nmea2s3 import logger as L
    lg = L.N2KLogger("can0")
    lg.s3 = H.FakeS3()
    with H.AuditLog() as log:
        asyncio.run(lg.stop())
    assert len(log.entries) == 1
    assert log.entries[0]["application"] == "nmea2s3-logger"
    assert "stopped" in log.entries[0]["comment"]
    # the counters that say what was written
    for key in ("rx", "spooled", "objects", "dropped"):
        assert key in log.entries[0]
