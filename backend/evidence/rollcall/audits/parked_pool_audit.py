#!/usr/bin/env python3
"""Report the divided roll calls the campaign cannot import because their bill is unsigned.

Run from the repository root, against the extracted LegiScan datasets:

    python3 backend/evidence/rollcall/audits/parked_pool_audit.py

The campaign imports a roll call only when its measure became law. A bill that has
cleared both chambers but has not been signed sits at LegiScan status 3 ("enrolled"),
and its divided rolls wait. This sizes that pool per session and, more usefully, says
which part of it can ever move.

Three distinctions this makes, because they change what you should do:

  * A session that has adjourned (`sine_die` 1) will not sign anything more. Its parked
    rolls are dead weight, not a queue.
  * A concurrent or joint resolution is never presented to a governor, so its status can
    never reach enacted no matter how long you wait. Alaska's SCR 28/201/202 and
    California's SJR 7 are parked permanently.
  * A vetoed bill can keep status 3 in the dataset. Alaska HB 10 and HB 93 both read as
    "awaiting the governor" long after they were vetoed.

And one warning the Delaware run earned (CODE-FINDINGS §7 of legiscan-de-2163): a
non-zero live count here is NOT a reason to re-download the dataset. The status field
can trail the state's own record by more than a cut interval — two Delaware bills were
signed four days BEFORE the cut that still shows them unsigned. Check the state's own
action log instead, and note the rolls are already pending in `legislative_votes`, so
nothing needs re-fetching.
"""
from __future__ import annotations

import collections
import glob
import json
import os
import re

DATA = os.environ.get("LEGISCAN_DATA", "/Users/shu/legiscan-data")
CONFIG = "backend/src/pipeline/rollcall/legiscanStateConfigs.ts"

ENROLLED, ENACTED, VETOED = 3, 4, 5
# LegiScan bill_type codes for instruments a governor never signs.
UNPRESENTED_TYPES = {"CR", "JR", "R"}


def configured_sessions() -> set[str]:
    text = open(CONFIG).read()
    return {
        f"{jur}-{sid}"
        for jur, sid in re.findall(
            r'jurisdiction:\s*"([A-Z]{2})"[\s\S]{0,200}?sessionId:\s*(\d+)', text
        )
    }


def kept_question_patterns() -> dict[str, list[re.Pattern[str]]]:
    """jurisdiction -> the configured floor-question regexes."""
    text = open(CONFIG).read()
    out: dict[str, list[re.Pattern[str]]] = {}
    for jur, body in re.findall(r'jurisdiction:\s*"([A-Z]{2})"([\s\S]*?)\n  \},', text):
        pats = re.findall(r"pattern:\s*/\^?(.*?)\$?/[a-z]*\s*,\s*questionClass", body)
        out.setdefault(jur, [re.compile(f"^{p}$", re.I) for p in pats])
    return out


def is_divided(yea: int, nay: int) -> bool:
    low, high = min(yea, nay), max(yea, nay)
    return high > 0 and low >= high / 4.0


def main() -> None:
    configured = configured_sessions()
    kept = kept_question_patterns()
    live: list[tuple] = []
    adjourned: list[tuple] = []

    for path in sorted(glob.glob(f"{DATA}/??-[0-9]*")):
        name = os.path.basename(path)
        if not os.path.isdir(path) or not re.fullmatch(r"[a-z]{2}-\d+", name):
            continue
        jur, sid = name.split("-")
        jur = jur.upper()
        if f"{jur}-{sid}" not in configured:
            continue

        bills, sine = {}, None
        for p in glob.glob(f"{path}/*/*/bill/*.json"):
            try:
                bill = json.load(open(p))["bill"]
            except (OSError, ValueError):
                continue
            bills[bill["bill_id"]] = bill
            if sine is None:
                sine = bill.get("session", {}).get("sine_die")
        if not bills:
            continue

        patterns = kept.get(jur) or []
        rolls = collections.Counter()
        for p in glob.glob(f"{path}/*/*/vote/*.json"):
            try:
                rc = json.load(open(p))["roll_call"]
            except (OSError, ValueError):
                continue
            bill = bills.get(rc["bill_id"])
            if not bill or bill["status"] != ENROLLED:
                continue
            if not is_divided(rc["yea"], rc["nay"]):
                continue
            desc = (rc.get("desc") or "").strip()
            if patterns and not any(x.match(desc) for x in patterns):
                continue
            rolls[bill["bill_number"]] += 1
        if not rolls:
            continue

        never = sorted(b for b in rolls if bills_type(bills, b) in UNPRESENTED_TYPES)
        can_move = sorted(b for b in rolls if b not in never)
        row = (f"{jur}-{sid}", sum(rolls.values()), rolls, can_move, never)
        (live if sine == 0 else adjourned).append(row)

    report("STILL SITTING — these can still be signed", live, actionable=True)
    report("ADJOURNED — nothing more will be signed here", adjourned, actionable=False)


def bills_type(bills: dict, number: str) -> str:
    for bill in bills.values():
        if bill["bill_number"] == number:
            return bill.get("bill_type") or ""
    return ""


def report(title: str, rows: list[tuple], actionable: bool) -> None:
    print(f"\n{title}")
    if not rows:
        print("  (none)")
        return
    for session, total, rolls, can_move, never in sorted(rows, key=lambda r: -r[1]):
        movable = sum(rolls[b] for b in can_move)
        tail = (f"  — {movable} on bills a governor can still sign" if actionable
                else "  — the signing window has closed")
        print(f"  {session:10s} {total:3d} parked rolls / {len(rolls):2d} bills{tail}")
        if can_move and actionable:
            print(f"       check the state's action log for: {', '.join(can_move)}")
        elif can_move:
            print(f"       enrolled but never signed: {', '.join(can_move)}")
        if never:
            print(f"       never presented to a governor: {', '.join(never)}")
    print(f"  subtotal {sum(r[1] for r in rows)} rolls")
    if actionable:
        print("  Read the state's own action log for the bills above — do NOT assume a"
              "\n  re-download will show the signature. The rolls are already pending in"
              "\n  legislative_votes, so only the enactment fact is missing.")


if __name__ == "__main__":
    main()
