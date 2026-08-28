"""nmea2s3 — NMEA 2000 raw capture, SocketCAN to S3.

Three commands, installed by pipx:

  nmea2s3-logger     the logger — reads CAN frames off a SocketCAN interface
                     and uploads them, batched and gzip'd, to an S3-compatible
                     bucket
  nmea2s3-exporter   read the archive back out as ndjson or CSV
  nmea2s3-update-pg  turn the archive into a wide Postgres table

The logger imports boto3 and the stdlib, and nothing else — not the
decoders, not the database driver. That is what keeps the one process which
cannot re-run a missed frame as small as it is, whatever else is installed
alongside it.

Everything that touches the bucket lives in ndjson.py (the key format, the
record shape, the gzip and content-addressing conventions), audit_log.py
(the `_log/` operational log) and retry.py. SCHEMA.md documents the formats
those write, and the tests check the two against each other.
"""

__version__ = "0.2.0"
