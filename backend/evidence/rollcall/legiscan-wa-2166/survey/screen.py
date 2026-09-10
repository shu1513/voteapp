#!/usr/bin/env python3
"""Screen the remaining Washington measures from their descriptions.

This decides WHICH measures are worth reading, not what any of them says. A measure
is only ever DROPPED here when its own description puts it squarely inside a standing
campaign exclusion (appropriations, a study, a fee schedule, pure administration) or
inside the campaign's known research-area gaps. Everything else stays a candidate and
must still be read in full before it is judged.
"""
import csv
import collections
import pathlib
import re
import sys

P = pathlib.Path(__file__).resolve().with_name("divided-enacted-worklist.tsv")

# (regex over the description, disposition, reason)
RULES = [
    # Standing exclusion: appropriations and pure fiscal machinery.
    (r"\b(operating appropriations|supplemental (operating|transportation) appropriations"
     r"|fiscal biennium.*appropriations|authorizing bonds|bond authority|interfund loans"
     r"|fund transfers|statutory fund transfers|actuarial funding of pension"
     r"|transportation fiscal matters|transportation resources)\b",
     "dropped", "appropriations or fiscal machinery, excluded by standing campaign rule"),

    # Standing exclusion: a study, a task force, a report, an advisory body.
    (r"\b(conducting a study|task force|advisory committee|point-in-time count"
     r"|report recommendations|performance review process)\b",
     "dropped", "study, task force or advisory body only; no operative policy change"),

    # Standing exclusion: fee schedules and licence-fee levels.
    (r"\b(clerk fees|permit fees|license, permit, and endorsement fees|fee authority"
     r"|foreclosure prevention fee|connection charge waivers|concession fees"
     r"|predesign thresholds|license fees)\b",
     "dropped", "fee schedule or charge level; no area carries a direction on the rate"),

    # ⚠ The campaign has NO labour research area. A mandate on a PRIVATE employer can go
    # to corporate_accountability (the Illinois workaround), but bargaining rights and
    # public-employer duties cannot be reached at all.
    (r"\b(collective bargaining|interest arbitration|prevailing wage|call center retention"
     r"|cost-of-living adjustments for community and technical college"
     r"|salaries of ferry system|misclassification in the finishing trades"
     r"|employee ownership program)\b",
     "dropped", "no labor or union research area exists; the corporate_accountability "
                "workaround does not reach bargaining rights or a public employer"),

    # Board and committee membership, and naming or definition tidying.
    (r"\b(membership on|adding a student member|adding two voting members"
     r"|modernizing terminology|explicitly listing the department"
     r"|tribal membership on local boards)\b",
     "dropped", "board membership or terminology change; administrative"),
]


# ⚠ Measures a rule matched but that must still be READ. Each is a case where the
# keyword is in the description but the measure is substantive. Reviewing the screen's
# own output is what caught these.
KEEP_ANYWAY = {
    # A waiver of connection charges for certain properties is an affordable-housing
    # measure, not a fee schedule. Same subject as SB 5662.
    "HB1302",
    # It matched "performance review process", but the act ELIMINATES tax preferences,
    # which is a substantive tax change. It is also partially vetoed.
    "SB5794",
    # Ending a state program is a government or corporate question, not a bargaining one.
    "HB2047",
    # Terminology about non-citizens may still carry a civil rights direction, and the
    # chamber divided over it.
    "HB2632",
    # Tribal representation on a health board may be a civil rights question.
    "HB1946",
}


SCREENED_IN = "screened in: needs a full read"
# Only rows this script wrote are screened again. Imported batches and hand-written
# dispositions carry their own reasons and are final.
SCRIPT_REASONS = {why for _, _, why in RULES} | {SCREENED_IN}


def main():
    rows = list(csv.DictReader(P.open(), delimiter="\t"))
    counts = collections.Counter()
    touched = collections.Counter()
    for r in rows:
        if r["reason"] not in SCRIPT_REASONS:
            counts[r["disposition"]] += 1
            continue
        desc = (r["description"] or "").lower()
        hit = None
        if r["bill"] not in KEEP_ANYWAY:
            hit = next(((d, why) for rx, d, why in RULES if re.search(rx, desc)), None)
        if hit:
            r["disposition"], r["reason"] = hit[0], hit[1]
            touched[r["bill"]] = 1
        else:
            r["disposition"], r["reason"] = "candidate", SCREENED_IN
        counts[r["disposition"]] += 1

    if "--write" in sys.argv:
        with P.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0]), delimiter="\t",
                               lineterminator="\n")
            w.writeheader()
            for r in rows:
                w.writerow({k: str(v).replace("\t", " ").replace("\n", " ").rstrip()
                            for k, v in r.items()})
        print("worklist rewritten")

    print("slot dispositions:", dict(counts))
    bills = collections.defaultdict(set)
    for r in rows:
        bills[r["bill"]].add(r["disposition"])
    print("measures screened OUT:", sum(1 for b, d in bills.items() if d == {"dropped"}))
    print("measures still to read:", sum(1 for b, d in bills.items() if "candidate" in d))


if __name__ == "__main__":
    main()
