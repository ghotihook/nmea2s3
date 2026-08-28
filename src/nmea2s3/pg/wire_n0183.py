#!/usr/bin/env python3
"""
wire_n0183 — one NMEA 0183 sentence -> [(field, value)], via pynmea2.

The whole wire-format knowledge for the 0183 stream lives in the two tables
below: which sentences carry numbers we want, and which XDR transducers to
keep. Everything else — bucketing, widening, writing — happens later, so
this file has exactly one job.

Field ids are "{sentence}_{attribute}", lowercase: mwv_wind_angle_r,
vtg_spd_over_grnd_kts, xdr_roll — the `proto_field` shape that becomes a
column name directly. There is no arbitration step above this: every field
gets its own column, so what an instrument actually said is preserved
rather than resolved away.
"""

from __future__ import annotations

import datetime
import logging

import pynmea2

logger = logging.getLogger("wire_n0183")

# These carry a validity flag. Anything but 'A' is the instrument telling you
# the value is not trustworthy — dropped rather than recorded.
STATUS_SENTENCES = frozenset({"MWV", "RMC", "ROT", "GLL"})

# sentence -> [(field suffix, pynmea2 attribute, required attribute values)].
# The third element is None except for MWV, which is the only sentence carrying
# two different measurements distinguished by a `reference` field.
#
# `latitude`/`longitude` are pynmea2's own computed properties — it combines the
# raw ddmm.mmmm value with the N/S/E/W letter into signed decimal degrees. Using
# them is wire-format decoding, not column policy.
SENTENCE_FIELDS: dict[str, list[tuple[str, str, dict | None]]] = {
    "MWV": [("wind_angle_r", "wind_angle", {"reference": "R"}),
            ("wind_angle_t", "wind_angle", {"reference": "T"}),
            ("wind_speed_r", "wind_speed", {"reference": "R"}),
            ("wind_speed_t", "wind_speed", {"reference": "T"})],
    "MWD": [("direction_magnetic", "direction_magnetic", None)],
    "VHW": [("water_speed_knots", "water_speed_knots", None)],
    "VTG": [("spd_over_grnd_kts", "spd_over_grnd_kts", None),
            ("true_track", "true_track", None)],
    "RMC": [("spd_over_grnd", "spd_over_grnd", None),
            ("true_course", "true_course", None),
            ("latitude", "latitude", None),
            ("longitude", "longitude", None)],
    "HDG": [("heading", "heading", None)],
    "HDM": [("heading", "heading", None)],
    "ROT": [("rate_of_turn", "rate_of_turn", None)],
    "RSA": [("rsa_starboard", "rsa_starboard", None)],
    "GLL": [("latitude", "latitude", None), ("longitude", "longitude", None)],
    "DBT": [("depth_meters", "depth_meters", None)],
    "MDA": [("water_temp", "water_temp", None)],
}

# XDR packs several transducers into one sentence. Only these are kept, and the
# match is CASE SENSITIVE on purpose: the archive holds far more 'Roll' than
# 'ROLL', and only the former is the real transducer.
XDR_TRANSDUCERS = frozenset({
    "BATTV", "Roll", "M5_HEEL", "Pitch", "M5_PITCH",
    "RAW_WIND_S", "RAW_WIND_A", "RAW_BSP",
})

SENTENCES = sorted(SENTENCE_FIELDS) + ["XDR"]

# Boat-referenced wind angles fold to +/-180 so port reads negative, which is
# what wire_n2k does to its own. The two streams get separate columns here,
# but a reader comparing n2k_windangle_apparent against mwv_wind_angle_r
# should not have to discover that they use opposite conventions.
#
# MWD's direction_magnetic is deliberately absent: it is a compass bearing and
# stays 0-360, as does every heading and course field here.
FOLD_TO_SIGNED = frozenset({"mwv_wind_angle_r", "mwv_wind_angle_t"})


# ROT is deg/MINUTE on the 0183 wire; PGN 127251 is rad/s, which wire_n2k
# converts to deg/SECOND. They land in separate columns here, but leaving
# them in different units would make rot_rate_of_turn and n2k_rate look
# comparable when they are not. Scaled so both mean deg/second.
SCALE = {"rot_rate_of_turn": 1.0 / 60.0}


def _fold_180(deg: float) -> float:
    """0-360 -> -180..+180, leaving 180 as +180."""
    return deg - 360.0 if deg > 180.0 else deg


def _field_name(sentence_type: str, suffix: str) -> str:
    return f"{sentence_type}_{suffix}".lower()


def _clock_offset(msg, ts) -> float | None:
    """seconds by which the CAPTURE clock leads the GPS clock, or None.

    RMC carries UTC date and time, so it is an independent clock riding
    inside the data — the only reference that works on an SBC with no RTC,
    and it is dense: RMC is ~16% of sentences and arrives at 10 Hz here.

    The normal value is small and POSITIVE: the sentence is timestamped when
    it is received, which is one transmission time after the fix it reports.
    An 80-character sentence takes 167 ms at 4800 baud and 21 ms at 38400,
    which is most of the measured +0.35 s median.

    Returns None rather than 0 when there is nothing to compare against, so
    "no reference" stays distinguishable from "agrees exactly".
    """
    if ts is None:
        return None
    date, time = getattr(msg, "datestamp", None), getattr(msg, "timestamp", None)
    if date is None or time is None:
        return None
    try:
        gps = datetime.datetime.combine(date, time, datetime.timezone.utc)
    except (TypeError, ValueError):
        return None
    return (ts - gps).total_seconds()


def decode_sentence(sentence_type: str, raw: str, ts=None) -> list[tuple[str, float]]:
    """One sentence -> [(field, value)]. Empty is normal and not an error.

    `ts` is the capture timestamp, passed in only so RMC can emit
    `rmc_clock_offset` — the wall clock measured against the GPS clock. It is
    an ordinary field from there on, so the existing bucketing, arbitration
    and column mapping carry it with no special case anywhere downstream.
    """
    # XDR must not go through pynmea2 — it rejects some of these outright, which
    # silently drops every RAW_* transducer.
    if sentence_type == "XDR":
        return _decode_xdr(raw)

    fields = SENTENCE_FIELDS.get(sentence_type)
    if not fields:
        return []

    try:
        msg = pynmea2.parse(raw, check=False)
    except Exception as e:
        logger.debug("parse error: %s", e)
        return []

    if sentence_type in STATUS_SENTENCES and getattr(msg, "status", "A") != "A":
        return []

    out = []
    for suffix, attr, where in fields:
        try:
            if where and any(getattr(msg, k, None) != v for k, v in where.items()):
                continue
            value = getattr(msg, attr, None)
        except Exception as e:
            # Several pynmea2 attributes are computed properties that raise on
            # malformed input; getattr's default does not catch that, so one bad
            # sentence would otherwise kill the run.
            logger.debug("%s.%s unreadable: %s", sentence_type, attr, e)
            continue
        if value is None or value == "":
            continue
        try:
            name = _field_name(sentence_type, suffix)
            v = float(value)
        except (TypeError, ValueError):
            continue
        if name in FOLD_TO_SIGNED:
            v = _fold_180(v)
        out.append((name, v * SCALE[name] if name in SCALE else v))

    if sentence_type == "RMC":
        offset = _clock_offset(msg, ts)
        if offset is not None:
            out.append(("rmc_clock_offset", offset))
    return out


def _decode_xdr(raw: str) -> list[tuple[str, float]]:
    """XDR carries repeating (type, value, unit, name) quadruplets."""
    fields = raw.split("*")[0].split(",")[1:]        # drop the talker/type word
    out = []
    for i in range(0, len(fields) - 3, 4):
        _kind, value, _unit, name = fields[i:i + 4]
        if name not in XDR_TRANSDUCERS or value == "":
            continue
        try:
            out.append((_field_name("XDR", name), float(value)))
        except ValueError:
            continue
    return out
