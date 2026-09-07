#!/usr/bin/env python3
"""Account for every LegiScan session dataset on disk: is there any batch left in it?

Run from the repository root:

    python3 backend/evidence/rollcall/audits/session_coverage_audit.py

Why this exists. Twice now the campaign has claimed a state was finished while a
whole session sat unlooked-at — Alabama's 2262, hidden by a stale config comment,
and later a set of datasets downloaded but never registered. A prose note goes
stale; this does not. It enumerates the datasets, applies the shipped config, and
prints what is actually left.

What it measures, and why each step matters:

  * **The last kept roll per chamber, not every divided roll.** A divided vote on
    a text the other chamber then rewrote is not a vote on the law. Arkansas's
    2026 fiscal session is the extreme case: appropriation bills need a
    three-quarters majority there, so a bill is voted again and again until it
    passes near-unanimously. Twelve of its rolls look divided; ten are superseded
    by a lopsided final vote, and only two survive.
  * **Candidate reach, not roll count.** A roll is only worth reading if the
    people who cast it are on a ballot we publish. Alaska's 30th Legislature
    (2017-2018) offers 33 divided rolls over 26 measures, and exactly one of its
    63 voters is a current candidate — 26 acts to read for one person's record.
  * **Sessions with no config are reported, not skipped**, so an unregistered
    dataset cannot hide.

It cannot judge selection filter 5 — whether a measure has a defensible for and
against stance. That needs a human read. Money-only measures routinely pass every
mechanical gate here and are then dropped: Arkansas HB 1100 and SB 75 are the
Revenue Stabilization Law, and Missouri SB 1 is a capital-improvements omnibus.
Treat a non-zero count as "worth reading", never as "worth importing".
"""
from __future__ import annotations

import collections
import glob
import json
import os
import re
import subprocess
import sys

DATA = os.environ.get("LEGISCAN_DATA", "/Users/shu/legiscan-data")
CONFIG = "backend/src/pipeline/rollcall/legiscanStateConfigs.ts"
EVIDENCE = "backend/evidence/rollcall"


def config_text() -> str:
    return open(CONFIG).read()


def shared_lists(text: str) -> dict[str, str]:
    return dict(
        re.findall(r"const ([A-Z_]+_(?:KEPT|EXCLUDED)_QUESTIONS)[^=]*=\s*\[([\s\S]*?)\n\];", text)
    )


def patterns(block: str) -> list[re.Pattern[str]]:
    """Every `pattern:` regex in a config block. Multi-line entries included — a
    one-line regex silently drops Missouri's House third-reading rule."""
    out = []
    for m in re.finditer(r"pattern:\s*/(.+?)/[a-z]*\s*,", block):
        body = m.group(1)
        out.append(re.compile("^" + body.lstrip("^"), re.I) if not body.endswith("$")
                   else re.compile("^" + body.lstrip("^").rstrip("$") + "$", re.I))
    return out


def kept_patterns(text: str, key: str):
    m = re.search(r'"?%s"?:\s*\{([\s\S]*?)\n  \},' % re.escape(key), text)
    if not m:
        return None
    body = m.group(1)
    named = re.search(r"keptQuestions:\s*([A-Z_]+)\s*,", body)
    return patterns(shared_lists(text)[named.group(1)]) if named else patterns(body)


def config_keys(text: str) -> dict[tuple[str, int], str]:
    """(jurisdiction, sessionId) -> the config key that serves it."""
    out = {}
    for m in re.finditer(r'\n  "?([A-Z]{2}(?:-\d+)?)"?:\s*\{([\s\S]*?)\n  \},', text):
        key, body = m.group(1), m.group(2)
        jm = re.search(r'jurisdiction:\s*"([A-Z]{2})"', body)
        sm = re.search(r"sessionId:\s*(\d+)", body)
        if jm and sm:
            out[(jm.group(1), int(sm.group(1)))] = key
    return out


def is_divided(yea: int, nay: int) -> bool:
    low, high = min(yea, nay), max(yea, nay)
    return high > 0 and low >= high / 4.0


def crosswalk_people(jur: str) -> set[int]:
    """people_ids already mapped to a candidate, from any committed crosswalk for
    the state. LegiScan people_ids are stable across a state's sessions."""
    ids: set[int] = set()
    for path in glob.glob(f"{EVIDENCE}/legiscan-{jur.lower()}-*/crosswalk.json"):
        try:
            doc = json.load(open(path))
        except (OSError, ValueError):
            continue
        rows = doc if isinstance(doc, list) else doc.get("entries") or doc.get("crosswalk") or []
        for row in rows:
            if isinstance(row, dict) and row.get("people_id") and row.get("candidate_id"):
                ids.add(int(row["people_id"]))
    return ids


def worked(jur: str, sid: int) -> bool:
    """Has this session been judged? Evidence may live in a sibling session's
    directory — Minnesota's 2217 sits inside legiscan-mn-2151 — so look for the
    session id in any of the state's roll evidence filenames, not for a dir."""
    if glob.glob(f"{EVIDENCE}/legiscan-{jur.lower()}-{sid}/*"):
        return True
    return bool(glob.glob(f"{EVIDENCE}/legiscan-{jur.lower()}-*/*/ls-{jur.lower()}-*-{sid}-roll*.json"))


def main() -> None:
    text = config_text()
    keys = config_keys(text)
    rows = []
    for path in sorted(glob.glob(f"{DATA}/??-[0-9]*")):
        name = os.path.basename(path)
        if not os.path.isdir(path) or not re.fullmatch(r"[a-z]{2}-\d+", name):
            continue
        jur, sid = name.split("-")[0].upper(), int(name.split("-")[1])
        # An UNREGISTERED session must still be measured, or it reports zero and
        # looks finished — the exact failure that hid Alabama's 2262. Fall back to
        # the state's base config key and say so.
        key = keys.get((jur, sid))
        borrowed = False
        if key is None:
            key = jur if kept_patterns(text, jur) else None
            borrowed = key is not None
        bills, session_name = {}, ""
        for p in glob.glob(f"{path}/*/*/bill/*.json"):
            try:
                bill = json.load(open(p))["bill"]
            except (OSError, ValueError):
                continue
            bills[bill["bill_id"]] = bill
            session_name = session_name or bill.get("session", {}).get("session_name", "")
        if not bills:
            continue
        pats = kept_patterns(text, key) if key else None
        registered = (jur, sid) in keys
        by_chamber = collections.defaultdict(list)
        if pats:
            for p in glob.glob(f"{path}/*/*/vote/*.json"):
                try:
                    rc = json.load(open(p))["roll_call"]
                except (OSError, ValueError):
                    continue
                bill = bills.get(rc["bill_id"])
                if not bill or bill["status"] != 4:
                    continue
                if not any(x.match((rc.get("desc") or "").strip()) for x in pats):
                    continue
                by_chamber[(bill["bill_number"], rc["chamber"])].append(rc)
        keep, kept_measures = [], set()
        for (bill_number, _chamber), rolls in by_chamber.items():
            last = max(rolls, key=lambda r: (r["date"], r["roll_call_id"]))
            if is_divided(last["yea"], last["nay"]):
                keep.append(last)
                kept_measures.add(bill_number)
        mapped = crosswalk_people(jur)
        voters = {int(v["people_id"]) for r in keep for v in (r.get("votes") or [])}
        rows.append({
            "session": f"{jur}-{sid}", "name": session_name[:34],
            "registered": registered, "borrowed": borrowed,
            "worked": worked(jur, sid), "unmeasurable": pats is None,
            "rolls": len(keep),
            "measures": len(kept_measures),
            "reach": len(voters & mapped), "voters": len(voters),
        })

    print(f"{'session':10s} {'cfg':>3s} {'done':>4s} {'rolls':>5s} {'reach':>5s}  name")
    todo = []
    for r in rows:
        flag = "" if (r["worked"] or not r["rolls"]) else "  <-- unworked"
        if r["unmeasurable"]:
            flag = "  <-- NO CONFIG, NOT MEASURED"
        elif r["borrowed"]:
            flag += "  (unregistered; measured with the state's patterns)"
        cfg = "y" if r["registered"] else ("~" if r["borrowed"] else "-")
        print(f"{r['session']:10s} {cfg:>3s} "
              f"{'y' if r['worked'] else '-':>4s} {r['rolls']:5d} "
              f"{r['reach']:>3d}/{r['voters']:<3d} {r['name']}{flag}")
        if (r["rolls"] and not r["worked"]) or r["unmeasurable"]:
            todo.append(r)

    print("\nsessions with rolls left to read:")
    if not todo:
        print("  (none)")
    for r in sorted(todo, key=lambda r: -r["reach"]):
        if r["unmeasurable"]:
            print(f"  {r['session']:10s} NO CONFIG for this state at all — measure by hand")
            continue
        note = " (unregistered)" if r["borrowed"] else ""
        print(f"  {r['session']:10s} {r['rolls']:3d} rolls, {r['reach']} of {r['voters']} "
              f"voters are current candidates{note}")
    print("\nA count here means 'worth reading', not 'worth importing'. Filter 5 —"
          "\nwhether the measure carries a defensible for-and-against stance — is a"
          "\nhuman read, and money-only measures pass every gate above it.")


if __name__ == "__main__":
    main()
