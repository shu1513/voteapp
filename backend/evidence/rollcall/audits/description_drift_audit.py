#!/usr/bin/env python3
"""Compare every live roll-call description against the committed judgment for its roll.

Run from the repository root against local `voteapp`:

    python3 backend/evidence/rollcall/audits/description_drift_audit.py

Two things this gets right that a first attempt gets wrong:

  * A record is compared against BOTH the yea and the nay description of its roll, and
    counts as drifting only if it matches neither. Some descriptions open "Voted to
    accept the Senate's changes…" rather than "Voted for…", so deciding the side from
    the opening words misfiles them and invents tens of thousands of false differences.
  * Where a roll appears in more than one committed judgments file, the later
    correction or rejudge directory wins over the batch that first judged it.

Drift is then dated: a row updated BEFORE its evidence file was last committed is
serving superseded text, which is a defect. A row updated AFTER it is the reverse — the
file has fallen behind, usually because the plain-language backfill rewrote the row.
"""
from __future__ import annotations

import collections
import difflib
import glob
import json
import os
import subprocess

DB = os.environ.get("DATABASE_URL", "postgresql://localhost:5432/voteapp")
REL_ROOT = "backend/evidence/rollcall/"
ROOT = os.path.abspath(REL_ROOT) + "/"
LATER_DIRS = ("rejudge-", "hb1247-correction", "tx-sb2024-corrupt", "us-nay-repair")


def precedence(path: str) -> tuple:
    """A correction or rejudge directory supersedes the batch that first judged a roll."""
    rel = os.path.relpath(path, ROOT)
    return (1 if rel.startswith(LATER_DIRS) else 0, os.path.getmtime(path))


def committed_judgments() -> tuple[dict, dict]:
    """(jurisdiction, roll) -> {description: side}, and the file/measure it came from."""
    texts: dict[tuple[str, int], dict[str, str]] = {}
    source: dict[tuple[str, int], tuple[str, str]] = {}
    for path in sorted(glob.glob(ROOT + "**/judgments.json", recursive=True), key=precedence):
        try:
            doc = json.load(open(path))
        except (OSError, ValueError):
            continue
        for entry in doc.get("judgments") or []:
            if not entry.get("jurisdiction") or entry.get("roll") is None:
                continue
            key = (entry["jurisdiction"], int(entry["roll"]))
            texts[key] = {
                entry[field]: side
                for side, field in (("yea", "yea_description"), ("nay", "nay_description"))
                if entry.get(field)
            }
            source[key] = (os.path.relpath(path, ROOT), entry.get("measure_id"))
    return texts, source


def file_commit_dates(files: set[str]) -> dict[str, str]:
    dates = {}
    for rel in files:
        dates[rel] = subprocess.run(
            ["git", "log", "-1", "--format=%cs", "--", REL_ROOT + rel],
            capture_output=True, text=True).stdout.strip()
    return dates


def live_rows():
    sql = """
    select split_part(r.origin_run_id,':',2),
           split_part(r.origin_run_id,':',5)::bigint,
           r.description, count(*), max(r.updated_at)::date
    from candidate_records r
    where r.origin_run_id like 'rollcall:%'
      and r.retired_at is null
      and split_part(r.origin_run_id,':',5) ~ '^[0-9]+$'
    group by 1,2,3
    """
    out = subprocess.run(["psql", DB, "-tAF", "\x1f", "-c", sql],
                         capture_output=True, text=True).stdout
    for line in out.strip().split("\n"):
        parts = line.split("\x1f")
        if len(parts) == 5:
            jur, roll, desc, n, updated = parts
            yield jur, int(roll), desc, int(n), updated


def main() -> None:
    texts, source = committed_judgments()
    dates = file_commit_dates({src for src, _ in source.values()})

    matched = unjudged = 0
    by_class = collections.Counter()
    texts_by_class = collections.Counter()
    by_state = collections.Counter()
    groups_by_state = collections.Counter()
    examples = collections.defaultdict(list)

    for jur, roll, desc, n, updated in live_rows():
        key = (jur, roll)
        if key not in texts:
            unjudged += n
            continue
        if desc in texts[key]:
            matched += n
            continue
        src, measure = source[key]
        committed = dates.get(src, "")
        if committed and updated < committed:
            cls = "row is STALE: file committed after the row was last touched"
        elif committed and updated > committed:
            cls = "row is NEWER than the file: something rewrote it afterwards"
        else:
            cls = "same day: cannot be ordered"
        by_class[cls] += n
        texts_by_class[cls] += 1
        by_state[jur] += n
        groups_by_state[jur] += 1
        if len(examples[cls]) < 3:
            closest = max(texts[key], key=lambda t: difflib.SequenceMatcher(None, t, desc).ratio())
            examples[cls].append((jur, measure, roll, src, committed, updated, closest, desc))

    drift = sum(by_class.values())
    print(f"matched a committed description exactly : {matched}")
    print(f"matched neither the yea nor the nay text: {drift}")
    print(f"roll has no committed judgment          : {unjudged}\n")

    for cls, n in by_class.most_common():
        print(f"  {cls:62s} {n:6d} records / {texts_by_class[cls]:3d} texts")

    print("\nby state:")
    for jur in sorted(by_state, key=lambda j: -by_state[j]):
        print(f"  {jur:4s} {by_state[jur]:6d} records / {groups_by_state[jur]:3d} texts")

    for cls, rows in examples.items():
        print(f"\n===== {cls}")
        for jur, measure, roll, src, committed, updated, want, got in rows:
            ratio = difflib.SequenceMatcher(None, want.split(), got.split())
            print(f"  {jur} {measure} roll {roll} — {src} committed {committed}, "
                  f"row updated {updated}, similarity {ratio.ratio():.2f}")
            for tag, i1, i2, j1, j2 in ratio.get_opcodes():
                if tag != "equal":
                    print("     FILE:", " ".join(want.split()[i1:i2])[:120] or "(nothing)")
                    print("     ROW :", " ".join(got.split()[j1:j2])[:120] or "(nothing)")
                    break


if __name__ == "__main__":
    main()
