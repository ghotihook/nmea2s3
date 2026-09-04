# Archive Utility rejects valid gzip files produced by macOS's own `gzip(1)`

**Summary.** Archive Utility refuses to expand certain valid RFC 1952 gzip
files with *"Error 79 - Inappropriate file type or format"*. The files are
produced by macOS's own `gzip(1)`, pass `gzip -t`, and decompress correctly
with `gzip -d`. Whether a file is accepted depends only on its **decompressed
payload** — two files whose payloads differ by a single byte get different
verdicts.

| | |
|---|---|
| macOS | 26.4.1 (25E253) |
| Archive Utility | 10.15 |
| gzip | Apple gzip 479 |
| Reproduced on | two machines, one a clean install |

---

## Reproduction

Two commands. Double-click each resulting file in Finder.

```sh
printf 'hello world\n' | gzip -9 > a.txt.gz    # expands to a.txt
printf 'ABCDE\n'       | gzip -9 > b.txt.gz    # Error 79
```

**Expected:** both expand. `gzip(1)` wrote both, Archive Utility is the
registered default handler for `.gz`, and both files are valid.

**Actual:** the second raises *"Unable to expand … (Error 79 - Inappropriate
file type or format.)"* — see `error-79-dialog.png`.

### The sharpest pair — one byte apart

Both payloads are 12 bytes. They differ in exactly one byte: `0x20` (space)
versus `0x0a` (newline).

```sh
printf 'ABCDE ABCDE\n'  | gzip -9 > pass.txt.gz    # expands
printf 'ABCDE\nABCDE\n' | gzip -9 > fail.txt.gz    # Error 79
```

```
pass payload: 41 42 43 44 45 20 41 42 43 44 45 0a    ABCDE ABCDE.
fail payload: 41 42 43 44 45 0a 41 42 43 44 45 0a    ABCDE.ABCDE.
```

Written by the same tool in the same second, so the gzip **headers are
byte-identical** — magic, CM, FLG, MTIME, XFL and OS all match. Only the
deflate payload differs.

---

## Why this looks like a defect rather than a deliberate guard

1. **Both files come from Apple tooling and are valid.** `gzip -t` passes on
   every sample here; `gzip -d` decompresses every one correctly.
2. **The diagnostic is wrong.** Error 79 is `EFTYPE`. The type is gzip, which
   is exactly the type Archive Utility is registered to handle.
3. **There is no rule a user could learn.** Acceptance is not monotonic in
   either payload size or compressed size, and adding one byte can flip the
   verdict in either direction (see the table below).
4. **It is not about the file name.** The same payload was tested as
   `.ndjson.gz`, `.txt.gz`, `.json.gz` and bare `.gz`. All four behave
   identically, so the inner extension is not involved.
5. **It is not about how the file was produced.** Files written by Python's
   `gzip` module (`mtime=0`, level 9) and by `gzip(1)` (current mtime,
   level 9) behave the same for the same payload.
6. **It is deterministic.** Every verdict below repeated identically across
   two passes, and the real-world cases reproduced on a second machine with a
   clean install of macOS.

### Verdicts observed

Payload is the decompressed content; `gz` is the resulting file size.

| payload (bytes) | gz | content | Archive Utility |
|---:|---:|---|---|
| 2 | 22 | `$` | expands |
| 3 | 23 | `GP` | expands |
| 5 | 25 | `$ABC` | expands |
| 5 | 25 | `ABCD` | expands |
| **6** | **26** | **`ABCDE`** | **Error 79** |
| 6 | 26 | `GPGLL` | Error 79 |
| 6 | 26 | `XYZQW` | Error 79 |
| 6 | 26 | `gpgll` | Error 79 |
| 6 | 26 | `hello` | Error 79 |
| 7 | 27 | `ABCDEF` | Error 79 |
| **12** | **29** | **`ABCDE ABCDE`** | **expands** |
| **12** | **28** | **`ABCDE\nABCDE`** | **Error 79** |
| 12 | 32 | `hello world` | expands |
| 26 | 32 | `11f40064#0000000000000000` | expands |
| 44 | 64 | `GPGLL,3351.059,S,15113.580,E,004056.00,A*1C` | Error 79 |
| 45 | 65 | `$GPGLL,3351.059,S,15113.580,E,004056.00,A*1C` | Error 79 |
| 69 | 84 | `$GPRMC,123519,A,4807.038,…*6A` | Error 79 |
| 141 | 138 | one line of JSON | expands |
| 168 | 163 | one line of JSON | Error 79 |
| 4,307,407 | 442,584 | newline-delimited JSON | expands |
| 5,956,011 | 225,617 | newline-delimited JSON | Error 79 |

Note the non-monotonicity in both directions: a 5-byte payload expands, 6 and
7 fail, 12 and 26 expand, 44–69 fail, 141 expands, 168 fails, 4.3 MB expands,
5.9 MB fails.

### Ruled out

- Compression ratio — the same payload at level 1 (17.8:1) and level 9
  (26.4:1) both fail.
- Payload size — 5 bytes expands, 6 fails, 12 expands.
- Compressed size — 25 expands, 26 fails, 29 expands, 32 expands, 27 fails.
- `ISIZE` overflow — every sample is far below 4 GiB.
- Multi-member or trailing data — zero bytes follow the first member in every
  sample (checked with `zlib.decompressobj(31).unused_data`).
- Deflate block type — fixed- and dynamic-Huffman blocks appear on both sides.
- gzip `MTIME`, compression level, file name and inner extension — all
  varied without changing the verdict.
- Extended attributes, quarantine, and the containing directory.

We could not reduce the accepted/rejected split to any rule.

---

## Files in this folder

| file | what |
|---|---|
| `poc.sh` | regenerates the four minimal samples; `--test` drives Archive Utility over each |
| `samples/poc-pass-01-hello-world.txt.gz` | expands |
| `samples/poc-fail-01-ABCDE.txt.gz` | Error 79 |
| `samples/poc-pass-02-space-separated.txt.gz` | expands — 12-byte payload |
| `samples/poc-fail-02-newline-separated.txt.gz` | Error 79 — same 12 bytes, one byte different |
| `samples/real-fail-004056-n0183.ndjson.gz` | real case: 5.9 MB of newline-delimited JSON, Error 79 |
| `samples/real-pass-025038-n2k.ndjson.gz` | real case: 4.3 MB of the same JSON shape, expands |
| `error-79-dialog.png` | the dialog |

The two real files come from a data-logging archive. Both are written by one
function with one set of parameters, and each is byte-for-byte identical to
what that function produces from its own payload — verified by decompressing
each and re-compressing it. They differ only in the text they contain.

## Testing note

Archive Utility must be driven **one file at a time**. `open -a "Archive
Utility" a.gz b.gz` does not reliably process every argument, and each failure
leaves a modal dialog that blocks the next file, so batching yields false
failures. `poc.sh --test` gives each case its own directory and kills Archive
Utility between runs.

## Impact

Low. Every other consumer reads these files correctly — `gzip -d`, `zlib`,
Python's `gzip`, and any library-based reader. The effect is that
double-clicking some valid `.gz` files in Finder fails with a misleading
error, and which ones cannot be predicted.
