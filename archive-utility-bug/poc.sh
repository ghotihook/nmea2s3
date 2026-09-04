#!/bin/zsh
# Archive Utility rejects valid gzip files produced by macOS's own gzip(1).
#
# Regenerates the four minimal samples and, with --test, drives Archive
# Utility over each one and reports which expand.
#
#   ./poc.sh           regenerate samples/ only
#   ./poc.sh --test    regenerate, then test each sample (needs a GUI session)
#
# The test harness matters. Archive Utility must be driven ONE FILE AT A TIME:
# `open -a "Archive Utility" a.gz b.gz` does not reliably process every
# argument, and each failure leaves a modal dialog that blocks the next file.
# Batching produces false failures — it did for us before we noticed. Each
# case therefore gets its own directory, Archive Utility is killed between
# runs, and the verdict is whether the expanded file appeared.

set -u
here=${0:a:h}
samples=$here/samples
mkdir -p $samples

# ── the four minimal cases, built with the system gzip ───────────────────
# 01: differ in payload only. Same tool, same flags, same second, so the
#     gzip headers are byte-identical including MTIME.
printf 'hello world\n' | gzip -9 > $samples/poc-pass-01-hello-world.txt.gz
printf 'ABCDE\n'       | gzip -9 > $samples/poc-fail-01-ABCDE.txt.gz

# 02: the sharpest pair. Twelve bytes each, identical except ONE byte —
#     0x20 (space) in the passing file, 0x0a (newline) in the failing one.
printf 'ABCDE ABCDE\n'  | gzip -9 > $samples/poc-pass-02-space-separated.txt.gz
printf 'ABCDE\nABCDE\n' | gzip -9 > $samples/poc-fail-02-newline-separated.txt.gz

print "Samples in $samples:"
for f in $samples/poc-*.gz; do
    printf '  %-42s %4d bytes gz, %2d bytes payload, gzip -t: ' \
        ${f:t} $(stat -f%z $f) $(gzip -dc $f | wc -c | tr -d ' ')
    gzip -t $f 2>/dev/null && print 'ok' || print 'FAILED'
done

[[ ${1:-} == --test ]] || { print "\nRe-run with --test to drive Archive Utility."; exit 0 }

# ── drive Archive Utility, one file at a time ────────────────────────────
work=$(mktemp -d)
trap 'killall "Archive Utility" 2>/dev/null; rm -rf $work' EXIT
print "\nTesting (each file isolated; ~10s each):"
for f in $samples/poc-*.gz; do
    name=${f:t:r}                       # strip .gz -> what Archive Utility should create
    mkdir -p $work/$name
    cp $f $work/$name/
    killall "Archive Utility" 2>/dev/null
    perl -e 'select(undef,undef,undef,2)'
    open -a "Archive Utility" $work/$name/${f:t}
    perl -e 'select(undef,undef,undef,8)'
    if [[ -f $work/$name/$name ]]; then
        print "  expands           ${f:t}"
    else
        print "  ERROR 79          ${f:t}"
    fi
done
killall "Archive Utility" 2>/dev/null

print "\nExpected: the two poc-pass-* files expand, the two poc-fail-* files"
print "raise \"Error 79 - Inappropriate file type or format\"."
