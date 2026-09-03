"""Operational audit log — one small JSON object per action that CHANGED
something, so "when did we last run the migration, how many rows, did it
succeed" is answerable by looking at the bucket itself rather than digging
through terminal scrollback or local state files.

Only writers write here. A read-only tool (nmea2s3-exporter) writes no entry at
all — it changes nothing, and an entry saying so is noise in the one place you
look to find out what happened. It is also permanent noise: production
credentials cannot delete, so an entry per export meant the one read-only
tool here silently grew the bucket every time someone piped a query into jq.

A writer that runs long enough to be interrupted logs a START and an END,
paired by a `run_id` in `details`, and the end entry is written whether or
not it actually changed anything. That is what makes "did it finish?"
answerable from the bucket: a start with no matching end means the run died
partway. No exception-catching wrapper can cover that case — a SIGKILL, a
dropped connection or a closed terminal raises nothing for it to catch — and
"completed with nothing to do" must stay distinguishable from "never
completed". A wrapper of that shape lived here until 2026-08-28, unused by
every tool for exactly this reason. The logger does this — a start entry
when it comes up and a stop entry saying why it went down; a short writer
whose outcome is known immediately can still write a single entry instead.

Deliberately NOT the same model as ndjson.py: log entries are not
content-addressed, because two entries with identical text are still two
distinct real events (e.g. the same migration command run twice) — the
whole point of ndjson's content hashing is to collapse retries of the
SAME event, which is exactly wrong here. Deliberately not gzipped either:
entries are tiny, and the entire value of an operational log is being able
to `aws s3 cp`/`cat`/`grep` it instantly without a decompress step.

Key layout: _log/<application>/<yyyy>/<mm>/<dd>/<HHMMSS>-<random>.json —
day-partitioned like everything else in the bucket, `_log/` prefixed so it
never collides with a real data source name and sorts distinctly in a bucket
listing.

The APPLICATION is a path segment, and that placement is the whole design.
It was previously only a field inside the body, which made "how many entries
did update-pg write" and "remove them" require a GET of every object to read
a value the key could have carried. `raw/` already solved this by putting
`proto` in the object NAME, but two things make a directory the better
answer here:

  - Application names contain `-` (`nmea2s3-update-pg`), and `-` is what
    delimits an object name in this bucket. `proto` forbids it for exactly
    that reason; an application cannot, since the name is the tool's.
  - Bucket lifecycle rules match a PREFIX and nothing else. Only this shape
    makes "expire the importer's entries after 90 days, keep the logger's
    forever" a rule on the bucket rather than a script somebody has to
    remember to run — and the logger's entries are the ones worth keeping,
    since a start with no stop is the only record of a run that was killed.

The cost is that one day across all tools is no longer a single prefix.
iter_log_keys() pays it by listing `_log/` whole and reading the date one
segment deeper — the same LIST, the same cost — and a caller that wants one
tool gets a prefix that skips everything else.

Functions:
  log_action(s3_client, bucket, application, exit_code, comment, details)
                                      -- write one entry; raises on failure
  log_action_safely(...)             -- same, but never raises (warns to stderr instead) — use this one
"""

import json
import re
import secrets
import socket
import sys
from datetime import datetime, timezone

# The application is a path segment in every key this module writes.
_APPLICATION = re.compile(r"[a-z0-9][a-z0-9._-]*")


def log_action(s3_client, bucket: str, application: str, exit_code: int,
               comment: str, details: dict | None = None) -> str:
    """Write one audit-log entry. Returns the S3 key written.

    A short tool calls this once, when the outcome is known. A long-running
    writer calls it twice — start and end, paired by a `run_id` in details —
    so an interrupted run is visible as a start with no end. See the module
    docstring.

    Raises on failure (network, credentials, ...) rather than swallowing
    it — callers should treat this as best-effort and catch around it
    themselves, since a broken audit log is never a reason to fail the
    real work it's describing.
    """
    if not _APPLICATION.fullmatch(application):
        # The application names a DIRECTORY now, so a name carrying `/`
        # would file entries under a path nothing here can predict or list,
        # and `.`/`..` would resolve somewhere else entirely. Refused at the
        # one place every entry is written, the same way record_line()
        # refuses a `proto` that would break a key. These credentials cannot
        # delete, so a key written to the wrong place is written forever.
        raise ValueError(
            f"application {application!r} is not a usable path segment: "
            "lowercase alphanumerics, then any of . _ -")
    now = datetime.now(timezone.utc)
    entry = {
        "timestamp": now.isoformat(),
        "application": application,
        "host": socket.gethostname(),
        "exit_code": exit_code,
        "comment": comment,
        "details": details or {},
    }
    day = now.strftime("%Y%m%d")
    time_of_day = now.strftime("%H%M%S")
    suffix = secrets.token_hex(4)
    key = f"_log/{application}/{day[0:4]}/{day[4:6]}/{day[6:8]}/{time_of_day}-{suffix}.json"
    body = json.dumps(entry, indent=2, sort_keys=True).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    return key


def log_action_safely(s3_client, bucket: str, application: str, exit_code: int,
                       comment: str, details: dict | None = None) -> None:
    """Same as log_action, but never raises — logs a warning to stderr on
    failure instead. What every call site should actually call, since a
    failed audit-log write must never take down or mask the real result of
    whatever it's recording."""
    try:
        log_action(s3_client, bucket, application, exit_code, comment, details)
    except Exception as e:
        print(f"[audit-log] failed to write entry: {e}", file=sys.stderr)


