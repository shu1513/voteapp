"""Match each selected LegiScan roll to Indiana's own journal roll-call number.

The worklist flags a roll when its LegiScan tally has no exact match in the bill history.
That flag does NOT mean the journal number is unknown: the history line for the same date
and the same action still identifies the roll.  It means LegiScan and the journal disagree
about the count, which is the defect recorded in CODE-FINDINGS.md section 2, and the reason
every selected roll's member list is checked against the official PDF.
"""
import json, os, re, sys
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))
from session import BILLDIR, VOTEDIR
SEL=json.load(open(sys.argv[1]))
for bill, rolls in SEL.items():
    hist=json.load(open(f"{BILLDIR}/{bill}.json"))["bill"]["history"]
    for roll_id in rolls:
        v=json.load(open(f"{VOTEDIR}/{roll_id}.json"))["roll_call"]
        cands=[h for h in hist if h["date"]==v["date"] and "Roll Call" in h["action"]
               and h["chamber"]==v["chamber"] and "Amendment" not in h["action"]]
        marks=[]
        for h in cands:
            m=re.search(r'Roll Call (\d+): yeas (\d+), nays (\d+)', h["action"])
            if m: marks.append((m.group(1), int(m.group(2)), int(m.group(3)), h["action"][:60]))
        exact=[c for c in marks if c[1]==v["yea"] and c[2]==v["nay"]]
        pick = exact[0] if exact else (marks[0] if len(marks)==1 else None)
        flag = "" if exact else ("  <== TALLY DISAGREES" if pick else "  <== AMBIGUOUS")
        print(f'{bill:8} {v["chamber"]} {v["date"]} roll{roll_id} legiscan {v["yea"]}-{v["nay"]:<3}'
              f' journal {pick[0] if pick else "?"} {pick[1] if pick else "?"}-{pick[2] if pick else "?"}{flag}')
        if not exact and len(marks)>1:
            for c in marks: print("      candidate:", c)
