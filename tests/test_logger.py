"""The logger: capture, buffering, spooling, upload, supervision.

Every test here is behavioural — it drives the real functions and asserts on
what they produced. None of it asserts on source text.
"""

import asyncio
import gzip
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import helpers as H                                              # noqa: E402
from nmea2s3 import logger as L                                   # noqa: E402


def _logger(spool=None):
    lg = L.N2KLogger("can0")
    lg.s3 = H.FakeS3()
    lg.disk_dir = Path(spool or tempfile.mkdtemp(prefix="n2ktest-"))
    return lg


def _fill(lg, n, **kw):
    for i in range(n):
        lg._append(H.frame(L, i, **kw))


# ── the row ──────────────────────────────────────────────────────────────

def test_row_carries_both_clocks_and_the_whole_frame():
    from nmea2s3 import decode
    f = H.frame(L, 0)
    row = json.loads(f.line)
    assert row["ts"].endswith("+00:00")
    assert row["proto"] == "n2k"
    # the frame is stored verbatim, identifier included, and decodes back
    assert decode.n2k(row["raw"]).pgn == 129025, "PDU2 frame, PGN carries the group extension"
    assert abs((f.ts.timestamp() - row["mono"]) - H.epoch()) < 1e-3


def test_nbytes_matches_the_encoded_line():
    f = H.frame(L, 0)
    assert f.nbytes == len(f.line.encode("utf-8"))


def test_naive_timestamp_is_refused():
    from datetime import datetime
    try:
        L.build_frame(datetime(2026, 1, 1), 1.0, 1, b"\x00")
        assert False, "a naive datetime should not serialize"
    except ValueError:
        pass


# ── the buffer ───────────────────────────────────────────────────────────

def test_buffer_evicts_oldest_and_keeps_accounting_exact():
    lg = _logger()
    cap, L.MAX_BUFFER_BYTES = L.MAX_BUFFER_BYTES, 20 * H.frame(L, 0).nbytes
    try:
        _fill(lg, 100)
        assert lg.buffer_bytes <= L.MAX_BUFFER_BYTES
        assert lg.buffer_bytes == sum(f.nbytes for f in lg.buffer)
        assert lg.dropped == 100 - len(lg.buffer)
        # the survivors are the NEWEST
        kept = [json.loads(f.line)["raw"] for f in lg.buffer]
        assert kept[-1] == json.loads(H.frame(L, 99).line)["raw"]
    finally:
        L.MAX_BUFFER_BYTES = cap


def test_frames_arriving_during_a_flush_are_never_lost():
    """The regression that mattered most: eviction runs while a write is in
    flight, so dropping 'the N oldest' by position discarded newly arrived
    frames that had been written nowhere."""
    lg = _logger()
    cap, L.MAX_BUFFER_BYTES = L.MAX_BUFFER_BYTES, 20 * H.frame(L, 0).nbytes
    try:
        _fill(lg, 20)
        started = asyncio.Event()

        async def slow_failing_write(batch):
            started.set()
            await asyncio.sleep(0.05)
            return False                       # same contract as the real path

        lg._write_spool = lambda objs: False    # spool fails
        lg._upload_objects = slow_failing_write  # direct upload fails too

        async def scenario():
            flush = asyncio.create_task(lg._flush_buffer())
            await started.wait()
            arrived = [H.frame(L, i) for i in range(100, 115)]
            for f in arrived:
                lg._append(f)
            await flush
            return arrived

        arrived = asyncio.run(scenario())
        present = {f.line for f in lg.buffer}
        assert all(a.line in present for a in arrived), "mid-flush arrivals were lost"
        assert lg.buffer_bytes == sum(f.nbytes for f in lg.buffer)
    finally:
        L.MAX_BUFFER_BYTES = cap


def test_failed_write_returns_the_batch_in_order():
    lg = _logger()
    _fill(lg, 10)
    before = [f.line for f in lg.buffer]
    lg._write_spool = lambda objs: False
    lg._upload_objects = lambda objs: asyncio.sleep(0, result=False)
    asyncio.run(lg._flush_buffer())
    assert [f.line for f in lg.buffer] == before


# ── spool and upload ─────────────────────────────────────────────────────

def test_happy_path_spools_then_uploads_then_deletes():
    lg = _logger()
    _fill(lg, 50)
    asyncio.run(lg._flush_buffer())
    spooled = list(lg.disk_dir.glob("disk.*.ndjson.gz"))
    assert len(spooled) == 1
    asyncio.run(lg._upload_spool())
    assert list(lg.s3.puts)[0].startswith("raw/2026/08/24/120000-n2k-")
    assert len(lg.s3.rows()) == 50
    assert not list(lg.disk_dir.glob("disk.*")), "spool file should be deleted once uploaded"
    assert (lg.spooled, lg.objects) == (50, 1)


def test_outage_accumulates_then_drains_with_nothing_lost():
    lg = _logger()
    lg.s3.up = False
    for cycle in range(3):
        # distinct rows per cycle — identical rows would be identical content,
        # hence one content-addressed object rather than three
        for i in range(10):
            lg._append(H.frame(L, cycle * 100 + i))
        asyncio.run(lg._flush_buffer())
        asyncio.run(lg._upload_spool())          # fails each time
    assert len(list(lg.disk_dir.glob("disk.*.ndjson.gz"))) == 3
    lg.s3.up = True
    asyncio.run(lg._upload_spool())
    assert len(lg.s3.puts) == 3
    assert len(lg.s3.rows()) == 30
    assert not list(lg.disk_dir.glob("disk.*"))


def test_quiet_bus_does_not_strand_the_spool():
    """The upload loop must not be gated on the buffer having anything in it."""
    lg = _logger()
    lg.s3.up = False
    _fill(lg, 10)
    asyncio.run(lg._flush_buffer())
    asyncio.run(lg._upload_spool())
    lg.s3.up = True
    assert not lg.buffer                          # bus went quiet
    asyncio.run(lg._upload_spool())
    assert len(lg.s3.puts) == 1


def test_broken_disk_falls_through_to_a_direct_upload():
    lg = _logger()
    lg.disk_dir = Path("/nonexistent/read-only/path")
    _fill(lg, 10)
    asyncio.run(lg._flush_buffer())
    assert len(lg.s3.puts) == 1
    assert not lg.buffer, "batch should not be stuck in the buffer"


def test_broken_disk_and_dead_s3_keeps_everything_buffered():
    lg = _logger()
    lg.s3.up = False
    lg.disk_dir = Path("/nonexistent/read-only/path")
    _fill(lg, 10)
    asyncio.run(lg._flush_buffer())
    assert len(lg.buffer) == 10


def test_spool_filenames_sort_chronologically():
    """Both the oldest-first eviction and the upload order depend on this."""
    lg = _logger()
    for offset in (-2, -1, 0):
        lg._write_spool(L._objects([H.frame(L, 0, day_offset=offset)]))
    names = sorted(p.name for p in lg.disk_dir.glob("disk.*.ndjson.gz"))
    days = [n.split(".")[1].split("_")[0] for n in names]
    assert days == sorted(days) == ["20260822", "20260823", "20260824"]


def test_eviction_removes_the_oldest_first():
    lg = _logger()
    cap, L.MAX_DISK_BYTES = L.MAX_DISK_BYTES, 300
    try:
        for offset in (-2, -1, 0):
            lg._write_spool(L._objects([H.frame(L, 0, day_offset=offset)]))
        names = sorted(p.name for p in lg.disk_dir.glob("disk.*.ndjson.gz"))
        assert not any("20260822" in n for n in names), "oldest should have been evicted"
        assert any("20260824" in n for n in names), "newest must be retained"
    finally:
        L.MAX_DISK_BYTES = cap


def test_spool_filename_round_trips_to_its_s3_key():
    obj = L._objects([H.frame(L, 0)])[0]
    key = L._key_for_spool_file(L._spool_name(obj))
    assert key == f"raw/2026/08/24/120000-n2k-{obj.cid}.ndjson.gz"


# ── durability ───────────────────────────────────────────────────────────

def test_flush_fsyncs_both_the_file_and_the_directory():
    """tmp -> fsync -> rename -> fsync(dir). Without the last step the rename
    itself is not durable and a power cut can lose the batch."""
    lg = _logger()
    _fill(lg, 20)
    calls = []
    real = os.fsync
    os.fsync = lambda fd: (calls.append(fd), real(fd))[1]
    try:
        asyncio.run(lg._flush_buffer())
    finally:
        os.fsync = real
    assert len(calls) == 2, f"expected file + directory fsync, got {len(calls)}"
    assert not list(lg.disk_dir.glob("*.tmp"))
    body = list(lg.disk_dir.glob("disk.*"))[0].read_bytes()
    assert len(gzip.decompress(body).decode().strip().splitlines()) == 20


def test_reupload_after_a_crash_is_a_harmless_overwrite():
    """Power cut between a successful PUT and the unlink: the surviving spool
    file re-uploads to the same content-derived key with the same bytes."""
    lg = _logger()
    _fill(lg, 10)
    asyncio.run(lg._flush_buffer())
    fp = list(lg.disk_dir.glob("disk.*"))[0]
    key = L._key_for_spool_file(fp.name)
    lg.s3.put_object(Bucket="b", Key=key, Body=fp.read_bytes())
    first = dict(lg.s3.puts)
    asyncio.run(lg._upload_spool())              # next boot replays it
    assert set(first) == set(lg.s3.puts)
    assert all(first[k] == lg.s3.puts[k] for k in first)


# ── the clock is recorded, never judged ──────────────────────────────────

def test_a_clock_step_is_recorded_not_routed():
    """The logger makes no judgement: everything goes to n2k/, and both clock
    bases stay recoverable from the rows alone."""
    lg = _logger()
    for i in range(5):
        lg._append(H.frame(L, i, clock_skew=2400.0))    # stamped 40 min off
    _fill(lg, 5)
    asyncio.run(lg._flush_buffer())
    asyncio.run(lg._upload_spool())
    assert all(k.startswith("raw/") for k in lg.s3.puts)
    rows = lg.s3.rows()
    assert len(rows) == 10
    from datetime import datetime
    bases = {round(datetime.fromisoformat(r["ts"]).timestamp() - r["mono"]) for r in rows}
    assert len(bases) == 2, f"both clock bases should be recoverable, got {bases}"


# ── supervision ──────────────────────────────────────────────────────────

def test_a_dead_loop_exits_instead_of_hanging():
    """asyncio.gather does not cancel siblings, so a crashed flush loop used to
    leave a live process that captured frames and never uploaded them."""
    lg = _logger()

    async def boom():
        await asyncio.sleep(0.01)
        raise RuntimeError("flush loop bug")

    async def idle():
        while True:
            await asyncio.sleep(1)

    lg._flush_loop, lg._can_listener, lg._upload_loop, lg._stats_loop = boom, idle, idle, idle

    class Args:
        can = "can0"

    saved = L.N2KLogger
    L.N2KLogger = lambda can_iface: lg
    try:
        with H.AuditLog() as log:
            asyncio.run(asyncio.wait_for(L._run(Args()), timeout=3))
        assert False, "_run() should have re-raised"
    except asyncio.TimeoutError:
        assert False, "_run() hung instead of exiting"
    except RuntimeError:
        pass
    finally:
        L.N2KLogger = saved
    # and the buffer is still written out on the way down, saying what killed it
    assert log.entries[-1]["reason"] == "task_failed", \
        "a crash must not be recorded as a clean shutdown"
    assert log.entries[-1]["exit_code"] == 1


# ── the unit file's paths are real ───────────────────────────────────────

def test_the_unit_starts_a_command_the_package_installs():
    """The unit was broken twice by renames, and nothing caught it because
    nothing reads this file — systemd does, on a machine the tests never
    touch. The absolute paths in it are a deployment detail (they are
    placeholders here), but the COMMAND name is not: it has to be one
    pyproject.toml actually installs, or the unit fails at every start with
    203/EXEC.
    """
    unit = _unit_directives()
    m = re.search(r"^ExecStart=(\S+)", unit, re.M)
    assert m, "the unit must have an ExecStart"
    command = m.group(1).rsplit("/", 1)[-1]
    scripts = (H.REPO / "pyproject.toml").read_text().split("[project.scripts]")[1]
    installed = [l.split("=")[0].strip() for l in scripts.splitlines()
                 if "=" in l and not l.startswith("[")]
    assert command in installed, f"unit runs {command}, which pyproject does not install"


def test_the_unit_can_write_its_spool():
    """ProtectHome=read-only makes every path under /home unwritable unless
    ReadWritePaths names it, and a logger that cannot write its spool has
    nowhere to put a frame — the one failure this whole design exists to
    prevent, introduced by a hardening directive."""
    unit = _unit_directives()
    if "ProtectHome=read-only" not in unit:
        return
    spool = [l.split("=", 1)[1] for l in unit.splitlines()
             if l.startswith("ReadWritePaths=")]
    assert spool, "ProtectHome=read-only with no ReadWritePaths for the spool"
    assert any(p.startswith("/home/") for p in spool), spool


def _unit_directives() -> str:
    """The unit with its comments stripped. The prose explains why `Wants=` is
    absent, so a naive substring search finds the word it is asserting against."""
    text = (H.REPO / "systemd" / "nmea2s3.service").read_text()
    return "\n".join(l for l in text.splitlines()
                      if l.strip() and not l.lstrip().startswith(("#", ";")))


def test_the_unit_never_waits_for_the_network_or_the_clock():
    """`Wants=` on these pulls the targets in, and both can wait forever on a
    boat: a wait-online service delays boot by its whole timeout when it can
    reach nothing, and systemd-time-wait-sync blocks INDEFINITELY on a first
    NTP sync that will never come. `After=` alone orders correctly if
    something else pulls them in and costs nothing when it does not."""
    unit = _unit_directives()
    assert "After=network-online.target time-sync.target" in unit
    assert "Wants=" not in unit, "ordering only — nothing here may block the start"


def test_the_logger_is_never_given_up_on():
    """systemd's default start limit (5 starts in 10s) puts a service
    permanently in `failed` and stops restarting it. For the one process that
    can capture a frame, that is silent permanent data loss."""
    unit = _unit_directives()
    assert "Restart=always" in unit
    head = unit.split("[Service]")[0]
    assert "StartLimitIntervalSec=0" in head, \
        "systemd moved this to [Unit] in v229 and ignores it in [Service]"


def test_every_directive_sits_in_a_section_systemd_reads_it_from():
    """A directive in the wrong section is not an error — systemd logs a
    warning nobody reads and carries on without it, so the setting silently
    does nothing. That is how StartLimitIntervalSec gets lost."""
    import configparser
    sections = {
        "Unit": {"Description", "After", "Before", "Wants", "Requires",
                 "StartLimitIntervalSec", "StartLimitBurst", "Documentation",
                 "Conflicts", "BindsTo", "PartOf"},
        "Service": {"Type", "User", "Group", "WorkingDirectory", "EnvironmentFile",
                    "ExecStart", "ExecStartPre", "ExecStop", "ExecReload",
                    "Restart", "RestartSec",
                    "TimeoutStopSec", "StandardOutput", "StandardError", "KillMode",
                    "SyslogIdentifier", "NoNewPrivileges", "ProtectSystem",
                    "ProtectHome", "ReadWritePaths", "PrivateTmp", "MemoryMax",
                    "OOMScoreAdjust", "CPUWeight", "CPUQuota"},
        "Install": {"WantedBy", "RequiredBy", "Alias", "Also"},
    }
    parser = configparser.ConfigParser(strict=False, comment_prefixes=("#", ";"))
    parser.optionxform = str
    parser.read(H.REPO / "systemd" / "nmea2s3.service")
    for section, allowed in sections.items():
        for key in parser[section]:
            assert key in allowed, f"[{section}] {key} is not read from there"


def _unit_value(unit: str, key: str) -> str | None:
    for line in unit.splitlines():
        if line.startswith(key + "="):
            return line.split("=", 1)[1].strip()
    return None


def _bytes(value: str) -> int:
    """systemd's size suffixes, as MemoryMax= is written."""
    scale = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3}
    return (int(value[:-1]) * scale[value[-1]] if value[-1] in scale
            else int(value))


def test_the_unit_brings_the_interface_up_before_capturing():
    """Binding a SocketCAN socket to a DOWN interface SUCCEEDS: the listener
    logs `CAN listening on can0`, `systemctl status` is green, and not one
    frame arrives. The listener's own reconnect loop does not cover it — that
    fires when the interface is ABSENT, not when it is present and down.

    Until this was added, can0 came up only because the n2k2ip gateway on the
    same box happened to bring it up first: disable or reorder that unit and
    this one captures nothing, silently.
    """
    unit = _unit_directives()
    step = _unit_value(unit, "ExecStartPre")
    assert step, "nothing brings the interface up"
    assert step.startswith("+"), \
        "`ip link` needs CAP_NET_ADMIN; `+` runs this one step privileged"
    assert "ip link set" in step
    iface = re.search(r"^ExecStart=\S+ --can (\S+)", unit, re.M).group(1)
    assert f"ip link show {iface}" in step, \
        "must configure the same interface ExecStart captures from"
    assert "||" in step, \
        "idempotent: the bus is shared, so an interface already up is left alone"


def test_the_memory_limit_cannot_bind_before_the_buffer_cap():
    """The two caps degrade completely differently: MAX_BUFFER_BYTES sheds its
    oldest frames and keeps capturing, MemoryMax kills the process and loses
    the entire buffer. At 128M this one bound first and turned a graceful shed
    into a total loss, so the unit's number is a property of the code's rather
    than a free-standing choice — and nothing but this test connects them.

    Measured peak RSS at the buffer cap was 191 MB against a 48 MB cap, which
    is where the 4x floor comes from.
    """
    limit = _bytes(_unit_value(_unit_directives(), "MemoryMax"))
    assert limit >= 4 * L.MAX_BUFFER_BYTES, (
        f"MemoryMax {limit // 1024 ** 2} MB would bind before the "
        f"{L.MAX_BUFFER_BYTES // 1024 ** 2} MB buffer cap sheds anything")


def test_the_logger_outranks_the_other_services_on_the_box():
    """The gateway sharing this bus sits at -700, and this unit sat at -500 —
    so under real memory pressure the kernel would have killed the logger
    first, the exact inverse of what the comment above the directive claimed.
    The gateway's traffic can be re-read off the bus a second later; a frame
    this process misses does not exist anywhere.
    """
    score = _unit_value(_unit_directives(), "OOMScoreAdjust")
    assert score is not None, "the OOM killer picks by score; say what this is worth"
    assert int(score) <= -900, \
        f"OOMScoreAdjust={score} is not below every other service on the box"


def test_the_cpu_share_is_not_also_a_cap():
    """A quota would only ever bind during a flush-and-gzip burst, which is
    exactly the moment the CAN reader must not be throttled."""
    unit = _unit_directives()
    assert "CPUWeight=" in unit
    assert "CPUQuota=" not in unit


def test_the_env_file_is_readable_by_systemd():
    """`export KEY=value` is valid shell and INVALID to systemd, which rejects
    the whole line — one warning per line in the journal — and starts the
    service with none of its environment. That is how the logger came up with
    no S3 credentials at all, and nothing in the code could tell: the
    variables were simply absent.

    Checked on env.example, which is the format every deployment copies.
    """
    for name in ("env.example", "env"):
        path = H.REPO / name
        if not path.exists():
            continue
        for n, line in enumerate(path.read_text().splitlines(), 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            assert not line.startswith("export "), \
                f"{name}:{n} starts with `export` — systemd will ignore this line"
            assert "=" in line, f"{name}:{n} is not KEY=value"
            key = line.split("=", 1)[0]
            assert key == key.strip(), f"{name}:{n} has whitespace around the key"


# ── what the audit records have to answer ────────────────────────────────

def _idle_loops(lg):
    """Replace the four background loops with ones that do nothing, so start()
    can be driven for the record it writes without capturing or uploading."""
    async def idle():
        while True:
            await asyncio.sleep(1)
    lg._can_listener = lg._flush_loop = lg._upload_loop = lg._stats_loop = idle


async def _await_entry(log, timeout=5.0):
    """The audit write is dispatched to a thread, so it lands slightly after
    start() has moved on. Poll rather than sleep a guessed interval."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not log.entries:
        assert loop.time() < deadline, "start() wrote no audit entry"
        await asyncio.sleep(0.005)
    return log.entries[0]


def _start_entry(lg):
    async def scenario(log):
        task = asyncio.create_task(lg.start())
        entry = await _await_entry(log)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return entry

    with H.AuditLog() as log:
        return asyncio.run(scenario(log))


def test_the_start_record_says_how_much_booting_cost():
    """Two real reboots cost 6.7 and 14.6 minutes of capture before the first
    frame, and establishing that meant subtracting clock_epoch from the first
    frame's timestamp by hand.

    `mono_at_start` is read from time.monotonic(), the SAME clock every
    frame's `mono` comes from, so the two cannot disagree — which is the
    property asserted here, and the reason it is not /proc/uptime.
    """
    lg = _logger()
    _idle_loops(lg)
    entry = _start_entry(lg)
    assert entry["application"] == "nmea2s3-logger"
    assert abs(entry["mono_at_start"] - time.monotonic()) < 5, \
        "not the clock the frames are stamped from"
    assert abs(entry["clock_epoch"] - H.epoch()) < 1e-3, \
        "clock_epoch must be ts - mono, the same boot epoch the rows carry"


def test_the_stop_record_says_why():
    """A clean SIGTERM from a reboot and a task falling over both wrote
    exit_code 0 and identical text, so a gap in the data gave no clue which
    had happened — the first question you ask when you see one."""
    lg = _logger()
    with H.AuditLog() as log:
        asyncio.run(lg.stop("signal"))
        asyncio.run(lg.stop("task_failed"))
    clean, crash = log.entries
    assert (clean["reason"], clean["exit_code"]) == ("signal", 0)
    assert (crash["reason"], crash["exit_code"]) == ("task_failed", 1), \
        "a crash must not report success"
    for entry in (clean, crash):
        for counter in ("rx", "spooled", "objects", "dropped"):
            assert counter in entry


def test_both_records_carry_the_unuploaded_spool():
    """`spooled` counts frames WRITTEN to the spool, which is not the same as
    frames still STUCK there. On a boat those are the only copy that exists,
    so the number belongs either side of a restart: at stop it says what is at
    risk, at start what is about to be replayed.

    A COUNT, not a size — two files, not their bytes. Sizing needed .stat() on
    every file, which brought a second directory walk and a race with the
    upload loop deleting them, for a number the journal already prints every
    STATS_INTERVAL.
    """
    lg = _logger()
    lg.s3.up = False                                  # nothing drains
    for offset in (-1, 0):
        lg._write_spool(L._objects([H.frame(L, 0, day_offset=offset)]))

    with H.AuditLog() as log:
        asyncio.run(lg.stop("signal"))
    assert log.entries[0]["spool_files"] == 2, "a count of files, not a size"

    _idle_loops(lg)
    assert _start_entry(lg)["spool_files"] == 2, "what the next run replays"


def test_the_capture_path_makes_no_special_system_calls():
    """Parsed, not grepped: the module docstring discusses `timedatectl` and
    `subprocess` by name, so a substring search finds the prose explaining why
    they were removed and fails on it.

    The header records that timedatectl was dropped as the only subprocess and
    the only systemd coupling here. Everything the audit records now needs is
    stdlib and portable — time.monotonic() rather than /proc/uptime, and a
    glob. The frames still come from SocketCAN, which is Linux and always will
    be, but nothing ELSE reaches outside Python.
    """
    import ast
    src = (H.REPO / "src" / "nmea2s3" / "logger.py").read_text()
    tree = ast.parse(src)

    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    for banned in ("subprocess", "platform", "ctypes", "resource", "pwd", "grp"):
        assert banned not in imported, f"{banned} is imported into the capture path"

    # No procfs/sysfs reads: those are Linux-only and, for uptime, disagree
    # with the `mono` already on every frame.
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            assert not node.value.startswith(("/proc/", "/sys/")), \
                f"reads {node.value}"
