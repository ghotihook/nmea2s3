"""The fields the archive deliberately does NOT store.

`pgn`, `src_addr`, `priority` and an 0183 sentence type were all columns on
every row once. Each is a pure function of the bytes already in `raw`, so
storing them bought nothing at read time — objects are gzipped ndjson, there
is no predicate pushdown, and you decompress and parse every line regardless
— while costing ~17% of every object and, worse, freezing today's decoder
into a store that is never rewritten. A PDU1/PDU2 bug written into ten years
of objects is permanent; a bug in this file is a fix and a re-derive.

So they are computed here, on read, by whoever wants them. This module is
the definition of what `raw` means for each protocol, and the exact inverse
of what the writers put there.

Nothing in the capture path imports this. It is for readers.

  n2k(raw)    -> N2KFrame(can_id, pgn, src_addr, priority, payload)
  n0183(raw)  -> NMEA0183Sentence(talker, sentence_type, fields, checksum_ok)
"""

from typing import NamedTuple, Optional


class N2KFrame(NamedTuple):
    can_id:   int
    pgn:      int
    src_addr: int
    priority: int
    payload:  bytes


def decode_can_id(can_id: int) -> tuple[int, int, int]:
    """Return (pgn, src_addr, priority) from a 29-bit NMEA 2000 CAN
    identifier."""
    src_addr = can_id & 0xFF
    pf       = (can_id >> 16) & 0xFF
    ps       = (can_id >> 8)  & 0xFF
    dp       = (can_id >> 24) & 0x03
    priority = (can_id >> 26) & 0x07
    # PDU2 (pf >= 240): PS is a group extension → part of the PGN.
    # PDU1 (pf <  240): PS is a destination address → excluded from the PGN.
    pgn = ((dp << 16) | (pf << 8) | ps) if pf >= 240 else ((dp << 16) | (pf << 8))
    return pgn, src_addr, priority


def n2k(raw: str) -> N2KFrame:
    """Split an `n2k` raw value — `<8 hex CAN id>#<payload hex>`, the
    candump ASCII form — and decode the identifier.

    The payload length is implicit in the hex length, which is why no DLC
    is stored: a frame carrying no data is `09f80102#`, and round-trips as
    an empty payload.
    """
    id_hex, _, payload_hex = raw.partition("#")
    can_id = int(id_hex, 16)
    pgn, src_addr, priority = decode_can_id(can_id)
    return N2KFrame(can_id, pgn, src_addr, priority, bytes.fromhex(payload_hex))


class NMEA0183Sentence(NamedTuple):
    talker:        Optional[str]   # "GP", "II", ... None for a proprietary sentence
    sentence_type: str             # "RMC", "MWV", "VDM", or the whole tag if proprietary
    fields:        list[str]
    checksum_ok:   Optional[bool]  # None when the sentence carries no checksum


def _checksum(body: str) -> int:
    value = 0
    for ch in body:
        value ^= ord(ch)
    return value


def n0183(raw: str) -> NMEA0183Sentence:
    """Split an `n0183` raw value — the sentence verbatim, `$` or `!`
    lead-in and `*hh` checksum included.

    The talker/type split is the standard five-character tag (`$GPRMC` ->
    GP + RMC), except for proprietary sentences, which start `P` and are
    not five characters of talker-plus-type at all (`$PGRMZ` is Garmin's
    own, entirely) — those keep the whole tag as the type and report no
    talker. AIS (`!AIVDM`) follows the normal rule.

    `checksum_ok` is None rather than False when a sentence carries no
    `*hh` at all: absent and wrong are different findings, and the archive
    stores plenty of the former from feeds that never sent one.
    """
    body, star, checksum_hex = raw.lstrip("$!").partition("*")
    fields = body.split(",")
    tag = fields[0]

    if tag.startswith("P"):
        talker, sentence_type = None, tag
    elif len(tag) == 5:
        talker, sentence_type = tag[:2], tag[2:]
    else:
        talker, sentence_type = None, tag

    checksum_ok = None
    if star:
        try:
            checksum_ok = _checksum(body) == int(checksum_hex[:2], 16)
        except ValueError:
            checksum_ok = False

    return NMEA0183Sentence(talker, sentence_type, fields[1:], checksum_ok)


DECODERS = {"n2k": n2k, "n0183": n0183}
