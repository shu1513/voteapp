#!/usr/bin/env python3
"""Compare every live roll-call description against the committed judgment for its roll.

Run from the repository root against local `voteapp`:

    python3 backend/evidence/rollcall/audits/description_drift_audit.py

Three things this gets right that a first attempt gets wrong:

  * A record is compared against BOTH the yea and the nay description of its roll, and
    counts as drifting only if it matches neither. Some descriptions open "Voted to
    accept the Senate's changes…" rather than "Voted for…", so deciding the side from
    the opening words misfiles them and invents tens of thousands of false differences.
  * Where a roll appears in more than one committed judgments file, the later
    correction or rejudge directory wins over the batch that first judged it; between
    two ordinary batches the later commit wins.
  * "Committed" means committed: the judgments are read from HEAD, not from the working
    tree, so an edited or untracked file cannot move the baseline.

Drift is then dated row by row, to the second: a row updated BEFORE its evidence file
was last committed is serving superseded text, which is a defect. A row updated AFTER it
is the reverse — the file has fallen behind, usually because the plain-language backfill
rewrote the row.
"""
from __future__ import annotations

import collections
import datetime as dt
import difflib
import json
import os
import subprocess

DB = os.environ.get("DATABASE_URL", "postgresql://localhost:5432/voteapp")
REL_ROOT = "backend/evidence/rollcall/"
LATER_DIRS = ("rejudge-", "hb1247-correction", "tx-sb2024-corrupt", "us-nay-repair")


def git(*args: str) -> str:
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout


def committed_judgments() -> tuple[dict, dict, dict]:
    """(jurisdiction, roll) -> {description: side}; the file/measure it came from; and the
    last-commit time (unix seconds) of every judgments file tracked at HEAD."""
    files = git("ls-files", "--", REL_ROOT + "**/judgments.json").split()
    commit_at: dict[str, int] = {}
    when = 0
    for line in git("log", "--format=%x00%ct", "--name-only", "--", *files).splitlines():
        if line.startswith("\x00"):
            when = int(line[1:])
        elif line and line not in commit_at:
            commit_at[line] = when

    def precedence(path: str) -> tuple[int, int]:
        rel = os.path.relpath(path, REL_ROOT)
        return (1 if rel.startswith(LATER_DIRS) else 0, commit_at[path])

    texts: dict[tuple[str, int], dict[str, str]] = {}
    source: dict[tuple[str, int], tuple[str, str]] = {}
    for path in sorted(files, key=precedence):
        doc = json.loads(git("show", f"HEAD:{path}"))
        for entry in doc.get("judgments") or []:
            if not entry.get("jurisdiction") or entry.get("roll") is None:
                continue
            key = (entry["jurisdiction"], int(entry["roll"]))
            texts[key] = {
                entry[field]: side
                for side, field in (("yea", "yea_description"), ("nay", "nay_description"))
                if entry.get(field)
            }
            source[key] = (path, entry.get("measure_id"))
    return texts, source, commit_at


def live_rows():
    sql = """
    select split_part(r.origin_run_id,':',2),
           split_part(r.origin_run_id,':',5)::bigint,
           r.description, extract(epoch from r.updated_at)::bigint
    from candidate_records r
    where r.origin_run_id like 'rollcall:%'
      and r.retired_at is null
      and split_part(r.origin_run_id,':',5) ~ '^[0-9]+$'
    """
    done = subprocess.run(["psql", DB, "-v", "ON_ERROR_STOP=1", "-tAF", "\x1f", "-c", sql],
                          capture_output=True, text=True, check=True)
    rows = [line.split("\x1f") for line in done.stdout.strip().split("\n") if line]
    if not rows:
        raise SystemExit("query returned no roll-call records; nothing to audit")
    for jur, roll, desc, updated in rows:
        yield jur, int(roll), desc, int(updated)


def stamp(seconds: int) -> str:
    return dt.datetime.fromtimestamp(seconds, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def main() -> None:
    texts, source, commit_at = committed_judgments()
    print(f"judgments read from HEAD {git('rev-parse', '--short', 'HEAD').strip()}: "
          f"{len(commit_at)} files, {len(texts)} rolls\n")

    matched = unjudged = 0
    by_class = collections.Counter()
    texts_by_class = collections.defaultdict(set)
    by_state = collections.Counter()
    texts_by_state = collections.defaultdict(set)
    examples = collections.defaultdict(list)

    for jur, roll, desc, updated in live_rows():
        key = (jur, roll)
        if key not in texts:
            unjudged += 1
            continue
        if desc in texts[key]:
            matched += 1
            continue
        src, measure = source[key]
        committed = commit_at[src]
        if updated < committed:
            cls = "row is STALE: file committed after the row was last touched"
        elif updated > committed:
            cls = "row is NEWER than the file: something rewrote it afterwards"
        else:
            cls = "same second: cannot be ordered"
        by_class[cls] += 1
        texts_by_class[cls].add((key, desc))
        by_state[jur] += 1
        texts_by_state[jur].add((key, desc))
        if len(examples[cls]) < 3 and all(e[2] != roll for e in examples[cls]):
            closest = max(texts[key], key=lambda t: difflib.SequenceMatcher(None, t, desc).ratio())
            examples[cls].append((jur, measure, roll, src, committed, updated, closest, desc))

    drift = sum(by_class.values())
    print(f"matched a committed description exactly : {matched}")
    print(f"matched neither the yea nor the nay text: {drift}")
    print(f"roll has no committed judgment          : {unjudged}\n")

    for cls, n in by_class.most_common():
        print(f"  {cls:62s} {n:6d} records / {len(texts_by_class[cls]):3d} texts")

    print("\nby state:")
    for jur in sorted(by_state, key=lambda j: -by_state[j]):
        print(f"  {jur:4s} {by_state[jur]:6d} records / {len(texts_by_state[jur]):3d} texts")

    for cls, rows in examples.items():
        print(f"\n===== {cls}")
        for jur, measure, roll, src, committed, updated, want, got in rows:
            ratio = difflib.SequenceMatcher(None, want.split(), got.split())
            print(f"  {jur} {measure} roll {roll} — {os.path.relpath(src, REL_ROOT)} committed "
                  f"{stamp(committed)}, row updated {stamp(updated)}, similarity {ratio.ratio():.2f}")
            for tag, i1, i2, j1, j2 in ratio.get_opcodes():
                if tag != "equal":
                    print("     FILE:", " ".join(want.split()[i1:i2])[:120] or "(nothing)")
                    print("     ROW :", " ".join(got.split()[j1:j2])[:120] or "(nothing)")
                    break


if __name__ == "__main__":
    main()
