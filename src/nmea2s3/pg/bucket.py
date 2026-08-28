"""Archive records -> one wide row per time bucket.

    records (ts, proto, raw, ...)  ->  {bucket_ts: {field: value}}

Two decisions live here and nowhere else.

WHAT `last` MEANS
-----------------
Each bucket keeps, per field, one real reading: the latest sample in that
bucket from the best-ranked device reporting the field (see below). Not a
mean.

The tradeoff is worth stating plainly, because a previous investigation in
the project this came from concluded the opposite for a fixed 1-second grid:
a mean is a better estimator there, and it recorded taking `last` as a
defect. What changed is that this table is per-field and its bucket is
configurable. Both undermine the mean:

  - A mean needs to know which fields are angles. The mean of 359 and 1 is
    180, not 0, so bearings need a circular mean (atan2 of mean sine and
    mean cosine) and relative angles need a different one again. That is a
    per-field policy table which has to be right for every field that ever
    appears — and this table adds columns automatically as new fields turn
    up on the bus, so an unlisted angle would be averaged wrongly and
    silently.
  - `last` is always a real reading that an instrument actually produced at
    a known moment. A mean is a synthetic value that may correspond to no
    observation, which matters more as the bucket grows: the mean of a
    minute of tacking angles describes nothing.

So `last` is the honest primitive for a raw, auto-widening table. If you
want a mean, take it in SQL over these rows, where you can say which
columns are angles.

CHOOSING BETWEEN DEVICES, WHICH IS NOT OPTIONAL
-----------------------------------------------
Two devices can report the same field into the same bucket, and they land in
the same column: a field id carries the PGN's own discriminators, never a
source address. So the bucket has to pick one, and does, in this order:

  1. lowest N2K priority number
  2. then lowest source address
  3. then the latest sample

Device first, sample second. The bucket resolves to ONE device and takes
that device's last reading in the bucket — the same preference the
arbitration chains used, for the same reason. Ordering by time first
instead, with priority only settling an exact tie on the timestamp, meant a
column alternated between two instruments sample by sample, decided by
whichever happened to report last: two devices differing by a known offset
produced a column that was neither of them and that no reader could
reproduce.

The cost is staleness bounded by the bucket. A priority-1 device reporting
at 0.2 Hz wins a 1 s bucket it appears in, even against a priority-3 sample
40 ms later. It cannot pin the column beyond that: no state crosses a bucket
boundary, so in the buckets where the better device says nothing at all, the
next best wins outright and the column carries on.

That rule is also why no device is named anywhere here. Priority is set by
the sending device's firmware and travels with it, which is what separates a
10 Hz sensor at priority 1 from a 1 Hz one at priority 3. Source addresses
are leased by ISO address claiming and change when a bus is repowered, so
they rank devices deterministically but identify none — which is exactly
what a tie-break needs to be, and exactly what a choice must not be.

NMEA 0183 carries neither priority nor source address, so all of a field's
talkers fall to the same sentinel and the genuinely last one wins. Three XDR
talkers share this archive, so that is real rather than hypothetical. The
sentinels only ever compete with each other: a field id is prefixed by its
sentence type or by `n2k_`, so the two protocols never share a column.
"""

import re
from datetime import datetime, timedelta, timezone

from ..decode import n0183 as n0183_sentence
from . import ranges, wire_n0183, wire_n2k

# 0183 has no priority or source address, and the sort key has slots for
# both. These sentinels rank last, so an unknown priority never outranks a
# real one — which in practice only matters between 0183 talkers, since the
# two protocols never share a column.
ANY_PRIO = 1 << 30
ANY_SRC = 1 << 30

UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}
INTERVAL = re.compile(r"\A(\d+(?:\.\d+)?)(ms|s|m|h)\Z")


def parse_interval(text: str) -> timedelta:
    """`1s`, `250ms`, `5m`, `1h` -> a timedelta.

    Deliberately small: a bucket that is not a whole number of units, or
    that does not divide the hour, produces boundaries that drift against
    wall-clock time and rows nobody can join to anything.
    """
    # Matched, not parsed with float(), which accepts surrounding whitespace
    # and would read "1 s" as one second — quietly, on a flag that decides
    # the shape of every row written.
    match = INTERVAL.match(text)
    if not match:
        raise ValueError(f"unrecognised bucket {text!r} — use e.g. 1s, 250ms, 5m, 1h")
    seconds = float(match.group(1)) * UNITS[match.group(2)]
    if seconds <= 0:
        raise ValueError(f"bucket must be positive: {text!r}")
    return timedelta(seconds=seconds)


def truncate(ts: datetime, bucket: timedelta) -> datetime:
    """Floor a timestamp onto the bucket grid, anchored at the UNIX epoch.

    Anchored, not relative to whatever the batch happened to start at: two
    runs over overlapping ranges must produce the same boundaries, or the
    same second lands under two different keys and the upsert stops
    converging.
    """
    step = bucket.total_seconds()
    epoch = ts.timestamp()
    return datetime.fromtimestamp(epoch - (epoch % step), tz=timezone.utc)


def samples(record: dict):
    """One archive record -> (field, value, prio, src) for each value in it.

    Unknown protocols yield nothing rather than raising: the archive is
    allowed to grow a protocol this build has never heard of, and a reader
    that falls over on one is worse than a reader that skips it.
    """
    proto, raw = record.get("proto"), record.get("raw")
    if not proto or raw is None:
        return
    if proto == "n2k":
        for field, value, src, prio in wire_n2k.decode_raw(raw):
            yield field, value, prio, src
    elif proto == "n0183":
        # A sentence whose checksum does not match yields nothing: pynmea2
        # validates any checksum that is present, regardless of its `check`
        # flag (which only governs whether a MISSING one is an error). So
        # corruption on the wire is dropped here, silently and correctly —
        # the archive keeps the raw sentence either way.
        sentence_type = n0183_sentence(raw).sentence_type
        ts = record.get("_ts")
        for field, value in wire_n0183.decode_sentence(sentence_type, raw, ts):
            yield field, value, ANY_PRIO, ANY_SRC


class Buckets:
    """Accumulates records into wide rows, keeping only the winning sample
    per (bucket, field).

    Memory is one entry per field per bucket, not one per sample — a
    5-minute object at a busy bus rate is ~62,000 frames and perhaps 50
    fields across 300 seconds, so this collapses by three orders of
    magnitude as it goes rather than after.

    Records MUST be fed in capture order. wire_n2k reassembles fast-packet
    PGNs across frames and holds partial state between calls, so feeding
    batches out of order silently drops every multi-frame PGN that spans a
    boundary.
    """

    def __init__(self, bucket: timedelta):
        self.bucket = bucket
        # bucket_ts -> field -> (-prio, -src, ts, value); the first three are
        # the sort key that decides which sample wins.
        self._rows: dict[datetime, dict[str, tuple]] = {}
        self.dropped_out_of_range = 0
        self.samples_seen = 0

    def add(self, record: dict) -> None:
        ts = record.get("_ts")
        if ts is None:
            return
        row = self._rows.setdefault(truncate(ts, self.bucket), {})
        for field, value, prio, src in samples(record):
            self.samples_seen += 1
            if not ranges.in_range(field, value):
                self.dropped_out_of_range += 1
                continue
            # Device first, sample second: lower priority number, then lower
            # source address, then later ts. Written once and stored as-is —
            # a second copy of the expression could disagree with this one
            # about which sample is ahead.
            key = (-prio, -src, ts)
            current = row.get(field)
            if current is None or key > current[:3]:
                row[field] = (*key, value)

    def fields(self) -> set[str]:
        return {f for row in self._rows.values() for f in row}

    def rows(self) -> list[dict]:
        """Wide rows, oldest first. `ts` is the bucket's own start."""
        return [{"ts": bucket_ts, **{f: v[3] for f, v in row.items()}}
                for bucket_ts, row in sorted(self._rows.items())
                if row]
