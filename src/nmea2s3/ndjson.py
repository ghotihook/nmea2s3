"""Everything that reads or writes the bucket, and nothing else.

The key format, the gzip conventions, the content addressing and the
read-back path all live here, so the logger and the exporter cannot drift
apart on any of them the way two independent copies once did — the
ContentType/ContentEncoding convention below was exactly that bug.

Deliberately stdlib-only except for boto3 and its sibling retry.py. This
runs on a boat's SBC, so nothing heavier gets in.

Writes here need no existence check: the key is content-addressed, so the
same content always resolves to the same key and even a blind re-upload is a
harmless overwrite, never a duplicate. An object_exists() helper lived here
until 2026-08-28 and was called by nothing — what it buys is skipping the
network transfer when nothing changed, and the logger deliberately does not
want that trade: it is a latency-sensitive live service firing one PUT per
flush, and a HEAD before every PUT would only slow it down to skip an
occasional redundant re-upload.

See SCHEMA.md for the full rationale behind the key format and every
convention below; this module is the one place that implements it, and
tests/test_formats.py checks the two against each other.

Functions, in the order they would typically be used:
  required_env(name)                          -- read an env var or exit with a clear message; never hardcode credentials
  gzip_and_id_stream(lines)                    -- one pass over ndjson lines -> (content_id, gzipped bytes)
  s3_key(source, day, time_of_day, cid)        -- build the <source>/<yyyy>/<mm>/<dd>/<time>-<cid>.ndjson.gz key
  put_object_gz(s3_client, bucket, key, body)  -- upload with the one shared ContentType convention
  make_s3_client(endpoint_url, region, access_key_id, secret_access_key) -- construct the boto3 client with the shared zero-retry config
  group_by_day(items)                          -- split a list of `.ts`-bearing items into per-UTC-day runs
  key_date(key)                                -- parse the <yyyy>/<mm>/<dd> out of a key, no download needed
  iter_keys(s3, bucket, source, since, until)  -- paginated listing under <source>/, filtered by key_date
  iter_rows_ndjson_gz(s3, bucket, key)         -- download + gunzip + parse one ndjson.gz object, row by row
"""

import gzip
import hashlib
import io
import json
import os
import re
import sys
from datetime import date, timedelta

import boto3
from botocore.exceptions import ClientError

from .retry import with_retries


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.exit(f"Missing required environment variable: {name}")
    return value


def gzip_and_id_stream(lines) -> tuple[str, bytes]:
    """One pass over an iterable of newline-terminated ndjson lines ->
    (content_id, gzipped bytes), without ever materializing the joined text.

    Peak memory is the COMPRESSED output rather than the input. The obvious
    shape — join the lines, hash the string, compress the string — holds a
    list of every line AND the string joining them alive at once, which at
    the buffer cap roughly doubled the logger's footprint at exactly the
    moment RSS peaks.

    Two properties the archive depends on, both easy to lose:

    The id hashes the PRE-gzip text, never the gzip'd bytes. gzip embeds a
    compression-time header timestamp by default, so compressing identical
    text twice yields different bytes — and therefore a different hash —
    for content that never changed. Hashing the text means identical rows
    always produce the identical id, which is what makes a retry resolve to
    the same S3 key: a safe overwrite, never a duplicate.

    mtime=0 makes the BYTES reproducible too. Writing 0 (RFC 1952's "no
    timestamp available") in place of the current time means anyone holding
    the rows can re-derive the exact stored object and compare it by ETag,
    without decompressing anything. The timestamp itself carried nothing:
    capture time is already in the key and on every row, and compression
    time is not a property of the data.

    Level 9 is Python's default here and is worth it, measured on a real
    10MB capture batch: 9% smaller than level 6 for 0.13s more CPU, in a
    worker thread that runs once every FLUSH_INTERVAL.
    """
    h = hashlib.sha256()
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        for line in lines:
            chunk = line.encode("utf-8")
            h.update(chunk)
            gz.write(chunk)
    return h.hexdigest()[:16], buf.getvalue()


# The capture archive's root. Everything captured off a wire lives under
# here, whatever protocol it is; `_log/` and anything derived stay outside.
RAW_PREFIX = "raw"

# A `proto` value ends up inside a filename that is parsed on `-` and, for
# the spool name, on `_` — so it may contain neither. Enforced at the one
# place records are built, rather than discovered later as an unparseable
# key that cannot be deleted.
PROTO_CHARS = re.compile(r"\A[a-z0-9][a-z0-9.]*\Z")


def valid_proto(proto: str) -> bool:
    return bool(PROTO_CHARS.match(proto))


def record_line(ts, mono, device_id: str, src: str | None,
                 proto: str, raw: str) -> str:
    """One archive record, as a single ndjson line. THE place a row is
    built — the live logger and any importer both come through here, so
    the on-disk shape cannot differ depending on which wrote it.

    The same six fields for every protocol, in this order:

      ts        capture time, tz-aware UTC (CLOCK_REALTIME)
      mono      CLOCK_MONOTONIC at capture, or None where the capture path
                had no monotonic clock to read — a historical import out of
                a database, for instance. Null, never invented: a fabricated
                monotonic reading would silently corrupt `ts - mono`, which
                is the whole reason the field exists.
      device_id which machine captured it
      src       which input on that machine (`can0`, `/dev/ttyUSB0`, a peer
                address), or None where that is not meaningful
      proto     what the bytes are — see the registry in SCHEMA.md
      raw       the frame or sentence VERBATIM, encoded per that registry

    Nothing here is decoded. `pgn`, `src_addr`, `priority` and an 0183
    sentence type were all stored as columns once; every one of them is a
    pure function of `raw`, and storing a derived value in an archive kept
    forever freezes today's decoder — bug and all — into permanent storage.
    They live in decode.py now and are computed on read. Same reasoning
    that keeps `mono` raw rather than as the derived epoch.

    The UTC check is strict, not advisory: every construction path already
    produces a tz-aware UTC timestamp, so a value that fails here is a real
    code bug, and crashing beats writing an ambiguous timestamp into a
    store that is never rewritten.
    """
    if ts.tzinfo is None or ts.utcoffset() != timedelta(0):
        raise ValueError(f"refusing to serialize a non-UTC timestamp: {ts!r}")
    if not valid_proto(proto):
        raise ValueError(f"invalid proto {proto!r}: lowercase alphanumeric, "
                          f"no '-' or '_' (they delimit the key)")
    return json.dumps({
        "ts": ts.isoformat(),
        # µs resolution, matching ts — a full float repr would spend a dozen
        # characters a row on digits below the delivery jitter.
        "mono": None if mono is None else round(mono, 6),
        "device_id": device_id,
        "src": src,
        "proto": proto,
        "raw": raw,
    }, separators=(",", ":")) + "\n"


def s3_key(proto: str, day: str, time_of_day: str, cid: str) -> str:
    """raw/<yyyy>/<mm>/<dd>/<HHMMSS>-<proto>-<content_id>.ndjson.gz

    ONE tree, date first, with the protocol in the object NAME rather than
    as a directory. That combination is deliberate and does two jobs at
    once:

      - "everything captured on this day" is a single prefix, which is what
        a reader consuming the archive actually asks for. Splitting the
        protocols into separate directories made that reader merge two
        listings for no benefit, since it dispatches on each row's `proto`
        field anyway and never looks at the key.

      - "only this protocol, over a long range" stays cheap, because LIST
        returns names without bodies. A reader filters `-<proto>-` on the
        key string and issues GETs for nothing else. Merging the protocols
        into one object stream would have made that impossible: n2k
        outruns n0183 by ~660:1, so a year of 0183 would have meant
        downloading ~45 GB to extract 0.07 GB.

    Each object holds exactly one protocol — that is what makes the name
    meaningful and the filtering possible.

    day is "YYYYMMDD". No device_id anywhere in the key: which physical box
    a row came from isn't a property of the data, and devices aren't a
    stable partition anyway (two real devices reported concurrently for ~2
    months in the historical archive) — device_id is a row field. Nor is
    `src`, for the same reason: one box can have two inputs.

    day/time_of_day are the batch's own capture time, never upload time;
    cid is what makes the key content-addressed.
    """
    return (f"{RAW_PREFIX}/{day[0:4]}/{day[4:6]}/{day[6:8]}/"
            f"{time_of_day}-{proto}-{cid}.ndjson.gz")


GZIP_CONTENT_TYPE = "application/gzip"


def put_object_gz(s3_client, bucket: str, key: str, body: bytes) -> None:
    """Upload a gzip'd ndjson object with the one shared content-type
    convention: ContentType=application/gzip, deliberately no
    ContentEncoding. That header tells HTTP-aware clients (browsers, some
    CLI tools) to transparently decompress on download, which would
    silently leave plain ndjson on disk under a .gz filename — the object
    *is* the gzip archive, not a gzip-transported copy of something else.
    """
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=GZIP_CONTENT_TYPE)


def make_s3_client(endpoint_url: str, region: str,
                    access_key_id: str | None = None, secret_access_key: str | None = None,
                    connect_timeout: int = 10, read_timeout: int = 60):
    """Zero built-in retries on purpose: every caller here has its own
    retry loop already — the logger re-uploads a spool file it failed to
    send on the next pass, the batch tools resume day by day — and boto3
    retrying underneath that would just double the backoff and muddy which
    layer actually recovered.

    access_key_id/secret_access_key are explicit, not left to boto3's
    implicit AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY discovery — this tool's
    credentials are named NMEA2S3_S3_ACCESS_KEY_ID/NMEA2S3_S3_SECRET_ACCESS_KEY
    (see env.example), prefixed so a box running several tools against
    several buckets cannot pick up the wrong one, which means boto3 can no
    longer find them on its own. Passing None falls through to boto3's own
    default credential chain, for a caller that does want standard-name env
    vars or an instance role."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=boto3.session.Config(
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retries={"max_attempts": 0},
        ),
    )


def key_date(key: str) -> date:
    """raw/<yyyy>/<mm>/<dd>/<HHMMSS>-<proto>-<cid>.ndjson.gz -> the day,
    parsed from the key alone, no download needed."""
    _raw, yyyy, mm, dd, _rest = key.split("/", 4)
    return date(int(yyyy), int(mm), int(dd))


def key_proto(key: str) -> str:
    """...<HHMMSS>-<proto>-<cid>.ndjson.gz -> the protocol, from the key
    alone. This is what makes selective retrieval cheap: a LIST returns
    names without bodies, so a single-protocol reader filters here and
    downloads nothing it will throw away.

    Split from the right, because <HHMMSS> is fixed-width and <cid> is a
    hex digest, but a proto is free-form within the registry's character
    rules — and those rules forbid `-` precisely so this parse is
    unambiguous.
    """
    name = key.rsplit("/", 1)[-1].removesuffix(".ndjson.gz")
    _time_of_day, proto, _cid = name.split("-", 2)
    return proto


def object_exists(s3_client, bucket: str, key: str) -> bool:
    """Check whether an exact key is already present.

    Not worth it for the logger — it fires one PUT per flush and a
    HEAD before every PUT would only add a round trip to skip an
    occasional redundant re-upload. It IS worth it for a day-at-a-time
    importer, where the cost avoided is re-reading a day out of a
    database, not the S3 PUT.

    A 404 here is a normal, expected outcome for most keys checked, not a
    failure — quieted accordingly so it doesn't read as an alarming one
    every time.
    """
    try:
        with_retries(s3_client.head_object, Bucket=bucket, Key=key,
                     what=f"check {key}", quiet_codes={"404"})
        return True
    except ClientError as e:
        if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
            return False
        raise


# Above this many days, one paginated listing of the whole source is cheaper
# than a request per day — and an unbounded range (date.min..date.max, which
# is what nmea2s3-exporter uses when no dates are given) is 3.65 MILLION days, so
# day-by-day there is not slow, it never finishes.
MAX_DAYS_TO_LIST_INDIVIDUALLY = 31


def iter_keys(s3_client, bucket: str, since: date, until: date,
               proto: str | None = None):
    """Every capture key whose own day falls in [since, until], optionally
    only those of one protocol.

    Two listing strategies, chosen by how wide the range is.

    A NARROW range lists one day-prefix at a time. The keys are
    raw/<yyyy>/<mm>/<dd>/..., so `raw/2026/08/24/` selects exactly one day
    server-side and nothing else is transferred. Listing the whole archive
    for a short window means walking every object ever written, getting
    slower every day it grows.

    A WIDE range does the opposite — one paginated listing of `raw/`,
    filtered client-side. Beyond a month or so that is fewer requests than
    one per day, and for an unbounded export (date.min..date.max) it is the
    only workable option at all.

    `proto` filters on the key NAME, which costs nothing: a LIST returns
    names, not bodies, so a filtered read never downloads an object it is
    going to discard.
    """
    span_days = (until - since).days + 1
    if 0 < span_days <= MAX_DAYS_TO_LIST_INDIVIDUALLY:
        prefixes = []
        day = since
        while day <= until:
            prefixes.append(f"{RAW_PREFIX}/{day.year:04d}/{day.month:02d}/{day.day:02d}/")
            day += timedelta(days=1)
        filter_by_day = False
    else:
        prefixes = [f"{RAW_PREFIX}/"]
        filter_by_day = True

    paginator = s3_client.get_paginator("list_objects_v2")
    for prefix in prefixes:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if filter_by_day and not (since <= key_date(key) <= until):
                    continue
                if proto is not None and key_proto(key) != proto:
                    continue
                yield key


def iter_log_keys(s3_client, bucket: str, since: date, until: date,
                  application: str | None = None):
    """`_log/` keys in range. The audit log is not capture data and does
    not live under raw/ — it has no protocol, is not gzipped and is not
    content-addressed — so it gets its own short listing rather than a mode
    flag threaded through the one above.

    `application` narrows the LIST itself rather than filtering after it,
    which is the reason that name is a path segment in the key: one tool's
    entries cost a listing of one tool's entries, and none of the others are
    ever returned. Without it every application is listed and the date is
    read one segment deeper — the same single LIST as before the name moved
    into the key."""
    prefix = f"_log/{application}/" if application else "_log/"
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            _log, _application, yyyy, mm, dd, _rest = key.split("/", 5)
            if since <= date(int(yyyy), int(mm), int(dd)) <= until:
                yield key


def iter_rows_ndjson_gz(s3_client, bucket: str, key: str):
    """Download, gunzip and parse one ndjson.gz object, yielding one dict
    per line.

    The COMPRESSED body is fetched whole and held in memory; the
    DECOMPRESSED side is streamed a line at a time and never materialized.
    That split is deliberate, and it is where the memory actually goes:
    compression on this data runs 9-30x, so even a full day of a busy feed
    is under ~100MB compressed, while the same object is several GB
    decompressed. The previous shape did gzip.decompress() into one string
    and then .splitlines() into a list of every line, so it held the whole
    decompressed object twice over — and could not read back a day-sized
    object at all.

    Holding the compressed body whole is also what keeps the download
    retryable as a single unit: with_retries can re-issue a failed GET,
    which it could not do if the caller were already part-way through
    consuming a stream.

    A corrupt/malformed object (bad gzip, invalid JSON) raises past this
    call rather than being swallowed here — callers that want best-effort
    skip-and-continue (like export.py) catch that at the call site.
    """
    def _get():
        return s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    body = with_retries(_get, what=f"read {key}")
    with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
        for line in io.TextIOWrapper(gz, encoding="utf-8"):
            if line.strip():
                yield json.loads(line)


def group_by_day(items) -> list[tuple[str, list]]:
    """Split into contiguous runs sharing one UTC calendar day, keyed off
    each item's own `.ts` (items already arrive in roughly chronological
    order, so this is a single linear pass, not a sort). Every spool file
    and every S3 object should end up holding exactly one day's rows as a
    result — the date partition must always reflect when data was
    CAPTURED, never when it was uploaded."""
    groups: list[tuple[str, list]] = []
    for item in items:
        day = item.ts.strftime("%Y%m%d")
        if groups and groups[-1][0] == day:
            groups[-1][1].append(item)
        else:
            groups.append((day, [item]))
    return groups
