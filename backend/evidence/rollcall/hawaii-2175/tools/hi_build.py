#!/usr/bin/env python3
"""Build the Hawaii batch-01 judgments files, refusing to write bad prose.

One BODY per measure; the yes and no descriptions are generated from it behind
different opening clauses so they cannot drift apart (the Oregon/ND pattern).
Every check runs BEFORE anything is written. Roll numbers, dates and tallies
come from pool.json (built from the stored legislative_votes rows), never typed.

Usage: hi_build.py <pool.json> <out dir>   -> writes <out dir>/judgments-<session>.json
"""
import json, re, sys

CHAMBER = {"house": "Hawaii House", "senate": "Hawaii Senate"}

BRITISH = ["licence", "programme", "centre", "labour", "favour", "honour", "colour", "defence",
           "offence", "offences", "misdemeanour", "analyse", "recognise", "authorise", "penalise",
           "utilise", "organisation", "sterilisation", "itemise", "neighbour", "neighbouring",
           "practising", "pressurised", "counselling", "travelling", "enrol(?!l)", "fulfil(?!l)",
           "modelling", "labelling", "normalised", "disfavoured", "remodelling", "maths",
           "licences", "behaviour", "harbour", "harbours", "vapour", "cancelled", "totalling",
           "authorised", "recognised", "programmes", "centres", "flavour", "aluminium", "grey"]

# Hawaii's journal names only the members who voted no or were excused, so the
# fetcher counts everyone else present as a yes. Said once, in the yes record.
INFERENCE = "Hawaii's journal names only members voting no or excused, so this yes vote is inferred from that list."

from hi_measures import MEASURES, DROPPED  # noqa: E402

# Act lines (from each bill's own history) that decide the tail wording.
ENACTED_WITHOUT_SIGNATURE = set()  # none among the kept measures; HB 1194 (dropped) became law unsigned


def sentences(t):
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+", t.strip()) if s.strip()]


def fk_grade(t):
    ss = sentences(t)
    words = re.findall(r"[A-Za-z']+", t)

    def syl(w):
        w = w.lower(); v = "aeiouy"; n = 0; prev = False
        for ch in w:
            cur = ch in v
            if cur and not prev:
                n += 1
            prev = cur
        if w.endswith("e") and n > 1:
            n -= 1
        return max(n, 1)

    S = sum(syl(w) for w in words)
    if not ss or not words:
        return 0.0
    return 0.39 * (len(words) / len(ss)) + 11.8 * (S / len(words)) - 15.59


def build(pool):
    out = {"2175": [], "2245": []}
    problems = []
    seen = set()
    for r in pool:
        key = (r["session"], r["measure"])
        if key in DROPPED:
            continue
        m = MEASURES.get(key)
        if m is None:
            problems.append(f"{key}: in the pool but neither judged nor dropped"); continue
        seen.add(key)
        tally = f"{r['yeas']}-{r['nays']}"
        chamber = CHAMBER[r["chamber"]]
        passed = "passed it" if m["verb"] == "passed" else "gave it final approval"
        law = "became law without the governor's signature" if key in ENACTED_WITHOUT_SIGNATURE else "became law"
        tail = f"The {chamber} {passed} {tally}, and it {law}."
        body = m["body"].strip()
        if not body.endswith("."):
            problems.append(f"{key}: body does not end with a period"); continue
        yea = f"Voted for {body} {tail} {INFERENCE}"
        nay = f"Voted against {body} {tail}"
        for name, d in (("yea", yea), ("nay", nay)):
            if ", The " in d:
                problems.append(f"{key} {name}: comma splice before the tail")
            if re.search(r"[a-z0-9]\.[A-Z]", d):
                problems.append(f"{key} {name}: sentences joined with no space")
            if tally not in d:
                problems.append(f"{key} {name}: tally {tally} missing")
            for w in BRITISH:
                if re.search(r"\b" + w + r"\b", d, re.I):
                    problems.append(f"{key} {name}: British spelling {w!r}")
            for s in sentences(d):
                n = len(s.split())
                if n > 45:
                    problems.append(f"{key} {name}: {n}-word sentence: {s[:70]}...")
        for lab in m["labels"]:
            if "nay" not in lab:
                problems.append(f"{key}: label {lab['slug']} has no explicit nay")
        out[r["session"]].append({
            "jurisdiction": "HI", "chamber": r["chamber"], "session": r["session"],
            "roll": r["roll"], "measure_id": r["measure"], "vote_date": r["date"],
            "review_status": "approved",
            "yea_description": yea, "nay_description": nay,
            "labels": m["labels"],
        })
    for key in MEASURES:
        if key not in seen:
            problems.append(f"{key}: judged but not in the pool")
    return out, problems


if __name__ == "__main__":
    pool = json.load(open(sys.argv[1]))
    out_dir = sys.argv[2]
    out, problems = build(pool)
    if problems:
        print("REFUSING TO WRITE — %d problems:" % len(problems))
        for p in problems:
            print("  ", p)
        sys.exit(1)
    # Verify the British-spelling checker actually fires (a checker that never fires is worthless).
    assert any(re.search(r"\b" + w + r"\b", "a licence to labour", re.I) for w in BRITISH)
    grades = []
    for sid, entries in out.items():
        dst = f"{out_dir}/judgments-{sid}.json"
        json.dump({"judgments": entries}, open(dst, "w"), indent=2, ensure_ascii=False)
        open(dst, "a").write("\n")
        print(f"wrote {dst}: {len(entries)} entries over {len({e['measure_id'] for e in entries})} measures")
        for e in entries:
            grades.append((round(fk_grade(e["nay_description"]), 1), sid, e["measure_id"]))
    g = sorted(x[0] for x in grades)
    print(f"Flesch-Kincaid grade (no-side text): median {g[len(g)//2]}, worst {g[-1]}")
    longest = max((len(s.split()), s) for sid in out for e in out[sid] for s in sentences(e["yea_description"]))
    print(f"longest sentence: {longest[0]} words")
    for gr, sid, mid in sorted(grades, reverse=True)[:5]:
        print(f"   grade {gr}  {sid} {mid}")
