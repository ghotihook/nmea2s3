#!/usr/bin/env python3
"""
wire_n2k — NMEA 2000 CAN frames -> named field values.

N2K is a binary protocol with fast-packet reassembly, per-PGN field layouts
and scaling factors; the `nmea2000` library holds that knowledge and works
one frame at a time, so this is an honest per-frame Python loop.

IT IS NOT STATELESS, and that matters. A single PGN can span several CAN
frames, so the module-level decoder accumulates fragments across calls and
returns nothing until a message completes. Do NOT decode batches out of
order or in parallel — either silently drops every multi-frame PGN at a
batch boundary.

Field ids are `n2k_{field}_{discriminators}`, lowercase — the `proto_field`
shape that becomes a column name directly. Field naming, unit conversion
and the +/-180 wind fold are the originals from the pipeline this was
ported out of; a change here changes a column name, so they are copied
rather than rewritten.

ONLY NUMBERS SURVIVE. A column is DOUBLE PRECISION (see table.py), so a
field reaches one only as an int or a float. The library hands back real
objects for some field types — `datetime.date`/`datetime.time` for DATE and
TIME, `str` for LOOKUP — and until 2026-08-30 every one of those was dropped
here, silently. That filter had real casualties: `method`, the GNSS fix
quality, never reached a column, and PGN 126992 System Time, whose entire
purpose is a clock, contributed nothing whatsoever.

Both are now converted rather than discarded, each into the one number that
carries what the field actually said — see GPS_TIME_PGNS and SKIP_LOOKUPS
below. Anything left over is still dropped: BITLOOKUP is several meanings at
once and wants a mask, not a code, and INDIRECT_LOOKUP means nothing without
the field it depends on. Both would need a naming rule of their own, so they
wait until something needs them.

The exception is that clock, because `ts` is only as good as the capture
box's own clock and this is the archive's independent check on it. The GPS
date and time are combined into ONE POSIX-seconds float, `gps_time`, which
is a number and so rides the ordinary path from here on — bucketed,
arbitrated between devices by priority and source address, and stored like
any other reading. Compare it against `ts` in SQL; nothing here judges it.
A double holds 238 ns at present epochs, against the 0.1 ms the wire
carries, so nothing is lost by the encoding.
"""

from __future__ import annotations

import logging
import math
import re
from datetime import datetime, timezone
from typing import Any, Iterator

from nmea2000.consts import FieldTypes
from nmea2000.decoder import NMEA2000Decoder

from ..decode import n2k as n2k_frame

logger = logging.getLogger("wire_n2k")

_decoder = NMEA2000Decoder()

# Fields that split one PGN into several columns rather than carrying a value
# (e.g. 130306 `reference` -> apparent vs true wind).
DISCRIMINATORS = {"reference", "source", "instance", "type", "temperatureSource"}
SKIP_FIELDS = {"sid"}

# PGNs whose `date` + `time` are a GPS clock worth keeping. Named explicitly
# rather than keyed on the field ids alone, because other PGNs carry a `date`
# and a `time` that are not one — 129033 Local Time Offset, for instance.
#
#   129029 GNSS Position Data — unambiguous, and the one to prefer.
#   126992 System Time        — the same fields, but its `source` can be a
#                               local crystal clock, which is the capture
#                               box's own clock read back and therefore
#                               circular. `source` is a discriminator, so
#                               those land in a column of their own; only the
#                               GPS one is emitted under this name.
GPS_TIME_PGNS = {126992, 129029}
CLOCK_FIELDS = ("date", "time")

# A LOOKUP is a number whose meanings live in a table — `method` 2 is a DGNSS
# fix. The CODE is what gets stored, never the resolved text: it is the raw
# reading, it fits the one column type, and it leaves the table's meaning
# where it can still be corrected instead of freezing today's enum into rows
# that are never rewritten. Same argument as storing `raw` and deriving `pgn`.
# Suffixed `_code` because it is an ordinal, not a measurement — avg() of a
# fix quality is nonsense in a way avg() of a satellite count is not.
#
# An unrecognised code resolves to None and is dropped by the loop's first
# line, along with genuinely absent fields. That costs a code this build has
# not learned yet, and buys not filling a column with the all-ones sentinel
# an unavailable lookup carries; the two are indistinguishable without the
# field's bit width, which the library does not expose.
LOOKUP_SUFFIX = "_code"

# Lookups that describe the FRAME rather than anything measured — the lookup
# equivalent of `sid`. Skipped by name, because they are the commonest of all
# (manufacturerCode and industryCode head every proprietary PGN, 314 and 313
# of them) and a column each would bury the handful worth having.
SKIP_LOOKUPS = {"manufacturerCode", "industryCode", "proprietaryId",
                "messageId", "repeatIndicator"}

_MS_TO_KN = 1.94384


def _deg(x: float) -> float: return round(math.degrees(x), 2)
def _kn(x: float) -> float:  return round(x * _MS_TO_KN, 3)


# SI units from the library -> what we store, keyed by field id. A typo here
# means no conversion fires and the value is stored in radians, which looks
# entirely plausible — hence keeping the keys identical to the old db_ops
# originals.
CONVERSIONS: dict[str, Any] = {
    "heading": _deg, "variation": _deg, "position": _deg, "windAngle": _deg,
    "cog": _deg, "set": _deg, "roll": _deg, "pitch": _deg, "yaw": _deg,
    "leewayAngle": _deg, "rate": _deg,
    "speedWaterReferenced": _kn, "windSpeed": _kn, "sog": _kn, "drift": _kn,
    "actualTemperature": lambda x: round(x - 273.15, 2),
}


def _sanitize(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")


def _frame_to_line(can_id: int, data: str) -> str:
    """Render (can_id, hex payload) as CAN_FRAME_ASCII_RAW, which the library
    detects directly — so nothing is re-parsed from a rendered string."""
    return f"{can_id:08X} " + " ".join(
        data[i:i + 2].upper() for i in range(0, len(data), 2)
    )


def _posix(day, time_of_day) -> float:
    """A GPS date and time -> POSIX seconds, UTC.

    One float rather than two columns, because the pair is one reading and
    half of it answers nothing. Rounded to microseconds like `mono`, which is
    four orders finer than the 0.1 s wire resolution and well inside what a
    double resolves at these magnitudes.
    """
    return round(datetime.combine(day, time_of_day, timezone.utc).timestamp(), 6)


def decode_frame(can_id: int, data: str) -> tuple[dict, dict[str, float]] | None:
    """Return (discriminators, {field_id: converted_value}) or None."""
    try:
        msg = _decoder.decode(_frame_to_line(can_id, data))
    except Exception as e:                       # malformed frame, unknown PGN
        logger.debug("decode error: %s", e)
        return None
    if not msg:
        return None                              # fast-packet fragment, or unknown

    disc: dict[str, Any] = {}
    values: dict[str, float] = {}
    clock: dict[str, Any] = {}
    for f in msg.fields:
        if f.value is None or f.id in SKIP_FIELDS or "reserved" in f.id.lower():
            continue
        if f.id in DISCRIMINATORS:
            disc[f.id] = f.value
        elif f.id in CLOCK_FIELDS and msg.PGN in GPS_TIME_PGNS:
            # date/time come back as datetime objects, which the numeric
            # filter below drops. Held here and combined after the loop.
            clock[f.id] = f.value
        elif f.type is FieldTypes.LOOKUP:
            # .value is the resolved text and .raw_value the code behind it.
            if f.id not in SKIP_LOOKUPS and isinstance(f.raw_value, int):
                values[f.id + LOOKUP_SUFFIX] = float(f.raw_value)
        elif isinstance(f.value, (int, float)) and not isinstance(f.value, bool):
            v = float(f.value)
            values[f.id] = CONVERSIONS[f.id](v) if f.id in CONVERSIONS else v

    if len(clock) == len(CLOCK_FIELDS) and (
            msg.PGN != 126992 or disc.get("source") == "GPS"):
        values["gps_time"] = _posix(clock["date"], clock["time"])

    # Boat-referenced wind angles fold to +/-180 so port is negative; the
    # ground-referenced one is a compass bearing and stays 0-360.
    if "windAngle" in values and disc.get("reference") in (
        "Apparent", "True (boat referenced)",
    ):
        if values["windAngle"] > 180:
            values["windAngle"] -= 360

    return disc, values


def internal_name(field_id: str, disc: dict[str, Any]) -> str:
    """n2k_{field}_{discriminators in sorted key order}."""
    parts = [field_id] + [_sanitize(disc[k]) for k in sorted(disc)]
    return ("n2k_" + "_".join(parts)).lower()


def decode_raw(raw: str) -> Iterator[tuple[str, float, int, int]]:
    """One archive `raw` value -> (field, value, src_addr, priority).

    `raw` is the candump ASCII form this archive stores — see SCHEMA.md.
    Splitting it here means the caller never handles a CAN identifier, and
    nmea2s3.decode stays the single definition of what the format means.

    src and priority come out with each value rather than being consumed:
    two devices can report the same field, and choosing between them is the
    caller's business.
    """
    frame = n2k_frame(raw)
    decoded = decode_frame(frame.can_id, frame.payload.hex())
    if not decoded:
        return
    disc, values = decoded
    for field_id, value in values.items():
        yield internal_name(field_id, disc), value, frame.src_addr, frame.priority
