"""Build the divided-and-enacted worklist for Indiana's 2026 session (LegiScan 2234).

One row per roll that clears the campaign's first three selection filters:

  1. divided   -- the losing side is at least a quarter of the winning side;
  2. enacted   -- the bill history carries a `Public Law <n>` line;
  3. floor     -- the roll is one the IN-2234 config keeps (third reading, concurrence
                  or conference report), so procedural rolls never reach the list.

Filter 4 (one roll per measure per chamber, preferring the final action) and filter 5
(a defensible for/against stance) are judgment calls and are left to each batch's PLAN.md.

The `journal` column is Indiana's own roll-call number, taken from the history line that
matches the roll's date, chamber and tally.  When no history line matches the tally exactly
the row is flagged, which is the LegiScan member-list defect recorded in
../legiscan-in-2143/CODE-FINDINGS.md section 2, not a missing journal number.

    python3 worklist.py > ../survey/divided-enacted-worklist.tsv
"""
import json, glob, re, sys

BASE = "/Users/shu/legiscan-data/in-2234/IN/2026-2026_Regular_Session"
KEPT_BILL_TYPES = {"B", "JR"}
DIVIDED_RATIO = 0.25

KEPT = [re.compile(p) for p in (
    r"^(?:house|senate) - third reading$",
    r"^(?:house|senate) - (?:rules suspended\. )?(?:house|senate) concurred with (?:house|senate) amendments$",
    r"^(?:house|senate) - (?:rules suspended\. )?conference committee report \d+$",
)]

# The final action ranks last so a bill's most recent kept roll sorts to the top of its
# chamber group; filter 4 picks from there.
RANK = {"conference": 0, "concurrence": 1, "passage": 2}


def question_class(desc):
    d = desc.lower().strip()
    if not any(p.match(d) for p in KEPT):
        return None
    if "conference committee report" in d:
        return "conference"
    if "concurred with" in d:
        return "concurrence"
    return "passage"


def journal_number(hist, vote):
    """Indiana's roll-call number for this vote, plus a flag when the tallies disagree."""
    marks = []
    for h in hist:
        if h["date"] != vote["date"] or h["chamber"] != vote["chamber"]:
            continue
        if "Roll Call" not in h["action"] or "Amendment" in h["action"]:
            continue
        m = re.search(r"Roll Call (\d+): yeas (\d+), nays (\d+)", h["action"])
        if m:
            marks.append((m.group(1), int(m.group(2)), int(m.group(3))))
    exact = [c for c in marks if c[1] == vote["yea"] and c[2] == vote["nay"]]
    if exact:
        return exact[0][0], ""
    if len(marks) == 1:
        return marks[0][0], "needs member-list check: LegiScan tally has no exact match in the official history"
    return "?", "needs member-list check: no unambiguous journal line for this roll"


def main():
    rows = []
    for f in sorted(glob.glob(BASE + "/bill/*.json")):
        b = json.load(open(f))["bill"]
        if b["bill_type"] not in KEPT_BILL_TYPES:
            continue
        if not any(re.search(r"\bPublic Law \d+", h["action"]) for h in b.get("history", [])):
            continue
        for v in b.get("votes", []):
            qc = question_class(v["desc"])
            if qc is None:
                continue
            win, lose = max(v["yea"], v["nay"]), min(v["yea"], v["nay"])
            if win == 0 or lose < win * DIVIDED_RATIO:
                continue
            journal, flag = journal_number(b.get("history", []), v)
            rows.append({
                "bill": b["bill_number"], "type": b["bill_type"], "status": str(b["status"]),
                "chamber": "house" if v["chamber"] == "H" else "senate",
                "date": v["date"], "roll": str(v["roll_call_id"]), "journal": journal,
                "yea": str(v["yea"]), "nay": str(v["nay"]), "desc": v["desc"],
                "title": b["title"], "rank": RANK[qc],
                "disposition": flag or "candidate: unbatched",
            })
    rows.sort(key=lambda r: (r["bill"], r["chamber"], r["rank"], r["date"]))
    cols = ["bill", "type", "status", "chamber", "date", "roll", "journal", "yea", "nay",
            "desc", "title", "disposition"]
    out = sys.stdout
    out.write("\t".join(cols) + "\n")
    for r in rows:
        out.write("\t".join(r[c] for c in cols) + "\n")


if __name__ == "__main__":
    main()
