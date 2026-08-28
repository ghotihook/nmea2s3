"""The Postgres side: a wide table that grows its own columns.

Every decoded field gets its own column, named exactly as the decoder named
it (`n2k_sog`, `mwv_wind_angle_r`) — the `proto_field` shape. Nothing here
holds a list of expected columns, because there isn't one: a new instrument
on the bus produces a field nobody has seen, and the table gains a column
for it on the next run.

That is only safe because this database is DERIVED and disposable. A column
that appears is NULL for rows written before it existed, which is the honest
state rather than a wrong one, and a rebuild fills it. Never point this at a
database holding anything that cannot be regenerated from the archive.

Two things this deliberately does not do:

  - It does not drop or narrow anything, ever. A field that stops appearing
    leaves its column in place, holding the history it has.
  - It does not type-guess. Every value out of the decoders is a float, so
    every generated column is DOUBLE PRECISION. A decoder that one day emits
    a string needs a decision here, not a silent cast.
"""

import logging
import re

log = logging.getLogger("nmea2s3.pg")

# Column names come from decoder field ids, which come from wire formats and
# ultimately from a device on a bus. They are interpolated into DDL, so they
# are checked rather than trusted — `psycopg.sql.Identifier` would quote them
# safely, but a field id that needs quoting is a decoder bug worth failing on,
# not a column to create with a name nobody can type.
SAFE_NAME = re.compile(r"\A[a-z][a-z0-9_]{0,62}\Z")

VALUE_TYPE = "DOUBLE PRECISION"


def check_name(name: str) -> str:
    if not SAFE_NAME.match(name):
        raise ValueError(
            f"unusable column name {name!r} from a decoder: expected lowercase "
            f"letters, digits and underscores, starting with a letter, at most "
            f"63 characters (Postgres truncates beyond that, which would "
            f"silently merge two fields into one column)")
    return name


def ensure(con, table: str, fields) -> list[str]:
    """Create the table if absent, add any column that is missing, and
    return the names of the columns added.

    CREATE TABLE IF NOT EXISTS on its own is not enough, and the way it
    fails is nasty: the table already exists so the CREATE is a no-op, the
    staging table is built LIKE it and inherits the old shape, and the COPY
    fails with "column does not exist" — an error naming the new column but
    saying nothing about the missing migration. Gaining an instrument is
    the most likely change here, so it must not read as a bug.
    """
    check_name(table)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} "
                f"(ts TIMESTAMPTZ NOT NULL, PRIMARY KEY (ts))")
    have = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = %s", (table,))}
    added = []
    for field in sorted(fields):
        if check_name(field) not in have:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {field} {VALUE_TYPE}")
            added.append(field)
    if added:
        log.warning("%s gained %d column(s): %s — NULL for existing rows "
                    "until a rebuild covers them", table, len(added),
                    ", ".join(added))
    return added


def write(con, table: str, rows: list[dict]) -> int:
    """Upsert wide rows, keyed on ts. Returns the number written.

    COPY into a staging table, then one INSERT ... ON CONFLICT DO UPDATE —
    a single statement for the merge, so it is atomic on its own and safe to
    call repeatedly over overlapping ranges.

    Only the columns PRESENT in this batch are updated. That distinction
    matters: a run covering a narrow window must not blank out columns it
    knows nothing about, which is exactly what listing every column and
    writing NULL for the absent ones would do. A field that genuinely
    stopped reporting keeps its previous value in the row rather than
    being overwritten with NULL by an unrelated re-run.
    """
    if not rows:
        return 0
    cols = ["ts"] + sorted({c for r in rows for c in r} - {"ts"})
    for c in cols[1:]:
        check_name(c)
    ensure(con, table, cols[1:])

    quoted = ", ".join(cols)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols[1:])

    # No ON COMMIT DROP: the connection is autocommit, so the CREATE would
    # commit immediately and the table would be gone before the COPY.
    con.execute(f"CREATE TEMP TABLE stage (LIKE {table})")
    try:
        with con.cursor().copy(
                f"COPY stage ({quoted}) FROM STDIN (FORMAT csv, NULL '')") as cp:
            for row in rows:
                cp.write(",".join(_csv(row.get(c)) for c in cols) + "\n")
        if updates:
            con.execute(f"INSERT INTO {table} ({quoted}) "
                        f"SELECT {quoted} FROM stage "
                        f"ON CONFLICT (ts) DO UPDATE SET {updates}")
        else:
            con.execute(f"INSERT INTO {table} ({quoted}) "
                        f"SELECT {quoted} FROM stage ON CONFLICT (ts) DO NOTHING")
    finally:
        con.execute("DROP TABLE stage")
    return len(rows)


def _csv(value) -> str:
    """One CSV field for the COPY above. An empty field is NULL — which is
    what `NULL ''` in the COPY declares — so a missing measurement stays
    missing rather than becoming zero."""
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return repr(float(value))
