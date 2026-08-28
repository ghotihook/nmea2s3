"""Bounded retry with backoff, for the read side — which has no higher-level
fallback the way the logger does (its spool-then-upload loop IS its retry
strategy: a failed upload is just a file still sitting there next pass).
Deliberately NOT baked into ndjson.make_s3_client/put_object_gz, which the
logger also uses — adding delay there would work against its
fail-fast-to-disk design.

For nmea2s3-exporter, and for anything else reading the archive back: a full
process restart is the LAST resort, not the first response to a single
dropped connection. Retrying in-process first means a transient blip
doesn't cost re-doing however much of a long unattended run already
completed, just to pick up again after a restart.

Backoff uses full jitter (AWS's own recommendation for exactly this):
each delay is random, not a fixed 2/4/8s — a real concern here, not a
hypothetical one, since more than one of these tools has genuinely run
concurrently against the same bucket. Deterministic backoff means two
processes throttled at the same moment (a 503 SlowDown) tend to retry in
lockstep, which is the thing that prolongs throttling rather than
resolving it; randomizing each delay independently breaks that up.

Functions/constants:
  with_retries(fn, *args, attempts, base_delay, what, quiet_codes, **kwargs)
                                      -- call fn with exponential-backoff-with-jitter retry on transient errors
  NON_RETRYABLE_CODES                -- tune this if a new permanent-error code needs to fail fast too
"""

import logging
import random
import time

from botocore.exceptions import BotoCoreError, ClientError

log = logging.getLogger("retry")

DEFAULT_ATTEMPTS = 4
DEFAULT_BASE_DELAY = 2.0  # seconds; doubles each attempt: 2, 4, 8

# ClientError codes retrying can never fix — a permanent condition, not a
# transient one. Retrying these anyway just wastes ~14s (2+4+8) confirming
# what the first attempt already established. Anything NOT in this set
# (RequestTimeout, ServiceUnavailable/SlowDown, InternalError, ...) is
# assumed transient and retried; a bare BotoCoreError (connection reset,
# DNS blip — no HTTP response at all, so no error code) is always retried.
NON_RETRYABLE_CODES = {
    "NoSuchKey", "NoSuchBucket", "NoSuchUpload",
    "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch",
    "404",  # HEAD's 404 body has no specific error code, just the bare status
}


def _is_retryable(e: Exception) -> bool:
    if isinstance(e, ClientError):
        code = e.response.get("Error", {}).get("Code", "")
        return code not in NON_RETRYABLE_CODES
    return isinstance(e, BotoCoreError)


def with_retries(fn, *args, attempts: int = DEFAULT_ATTEMPTS, base_delay: float = DEFAULT_BASE_DELAY,
                  what: str = "S3 call", quiet_codes: frozenset = frozenset(), **kwargs):
    """Call fn(*args, **kwargs), retrying transient boto3/network errors
    with exponential backoff plus full jitter (random delay up to the
    exponential ceiling, not the ceiling itself — see module docstring).
    A permanent error (NON_RETRYABLE_CODES)
    raises immediately, no retry — retrying it can't change the outcome.
    Raises the LAST exception if every attempt fails — never silently
    gives up. What "graceful" means once retries are exhausted (skip and
    warn, or fail the whole run) is the caller's decision, not this
    function's; it only buys time against the transient case.

    quiet_codes: permanent-error codes the CALLER already expects and will
    handle as a normal outcome, not a failure — e.g. a 404 from an
    existence check (does this object exist yet?), where "no" is a
    routine answer, not something worth an alarming log line. Still
    raises (the caller still needs to catch and act on it), just without
    logging — that's the caller's business, not a failure this function
    should announce.
    """
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except (BotoCoreError, ClientError) as e:
            if not _is_retryable(e):
                code = e.response.get("Error", {}).get("Code", "") if isinstance(e, ClientError) else ""
                if code not in quiet_codes:
                    log.warning(f"[retry] {what} failed with a permanent error, not retrying: {e}")
                raise
            last_exc = e
            if attempt == attempts:
                break
            # Full jitter: random in [0, ceiling], not the ceiling itself —
            # see module docstring for why a fixed delay is the wrong call.
            ceiling = base_delay * (2 ** (attempt - 1))
            delay = random.uniform(0, ceiling)
            log.warning(f"[retry] {what} failed (attempt {attempt}/{attempts}): {e} "
                        f"— retrying in {delay:.1f}s (up to {ceiling:.0f}s)")
            time.sleep(delay)
    raise last_exc
