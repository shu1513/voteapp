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


def kept_bill_types() -> set[str]:
    """The fetcher's own instrument filter, read from source rather than copied.
    LegiScan marks an ADOPTED resolution status 4, the same as an enacted bill,
    so filtering on status alone counts every commendation and memorial as law.
    That is what the first run of this audit did: nearly all of its residue was
    Alaska concurrent resolutions, Arizona memorials and Tennessee House
    resolutions."""
    src = open("backend/src/pipeline/rollcall/legiscanRollCall.ts").read()
    m = re.search(r"LEGISCAN_KEPT_BILL_TYPES[^=]*=\s*\[([^\]]*)\]", src)
    if not m:
        raise SystemExit("LEGISCAN_KEPT_BILL_TYPES not found in legiscanRollCall.ts")
    return set(re.findall(r'"([A-Z]+)"', m.group(1)))


def shared_lists(text: str) -> dict[str, str]:
    return dict(
        re.findall(r"const ([A-Z][A-Z0-9_]*_(?:KEPT|EXCLUDED)_QUESTIONS)[^=]*=\s*\[([\s\S]*?)\n\];", text)
    )


def patterns(block: str) -> list[re.Pattern[str]]:
    """Every `pattern:` regex in a config block, exactly as shipped. Multi-line
    entries included — a one-line regex silently drops Missouri's House
    third-reading rule. Anchors are NOT added: many shipped rules are deliberately
    unanchored (`\\bconcur in\\b`, `floor:.*final passage$`) because a description
    may carry a sponsor prefix, and forcing `^` dropped Alabama concurrences."""
    return [re.compile(m.group(1)) for m in re.finditer(r"pattern:\s*/(.+?)/[a-z]*\s*,", block)]


def normalize(desc: str) -> str:
    """The classifier's own normalization: lower-case, whitespace collapsed, trimmed."""
    return re.sub(r"\s+", " ", (desc or "").lower()).strip()


def matches(pats, desc: str) -> bool:
    """JavaScript `RegExp.test` semantics — a search, not an anchored match."""
    d = normalize(desc)
    return any(x.search(d) for x in pats)


def kept_patterns(text: str, key: str):
    m = re.search(r'"?%s"?:\s*\{([\s\S]*?)\n  \},' % re.escape(key), text)
    if not m:
        return None
    body = m.group(1)
    named = re.search(r"keptQuestions:\s*([A-Z][A-Z0-9_]*)\s*,", body)
    if named:
        lists = shared_lists(text)
        if named.group(1) not in lists:
            raise SystemExit(f"{key}: keptQuestions names {named.group(1)}, which was not found")
        return patterns(lists[named.group(1)])
    return patterns(body)


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


_ACCOUNTED: dict[str, set[int]] = {}


def accounted_rolls(jur: str) -> set[int]:
    """Cached per state: New Mexico has fifteen sessions and California a hundred
    report files, so reading the evidence once per session is what made a run
    take minutes."""
    if jur not in _ACCOUNTED:
        _ACCOUNTED[jur] = _read_accounted_rolls(jur)
    return _ACCOUNTED[jur]


_LEDGER_TEXT: dict[str, str] = {}


def ledger_text(jur: str) -> str:
    """Every ledger, worklist, README and report for the state, as one string —
    everything except the per-roll evidence files. Cached per state."""
    if jur not in _LEDGER_TEXT:
        parts = []
        for path in glob.glob(f"{EVIDENCE}/legiscan-{jur.lower()}-*/**/*", recursive=True):
            if (os.path.isfile(path) and path.endswith((".tsv", ".md", ".json", ".txt", ".csv"))
                    and not os.path.basename(path).startswith("ls-")):
                try:
                    parts.append(open(path).read())
                except (OSError, UnicodeDecodeError):
                    pass
        _LEDGER_TEXT[jur] = "\n".join(parts)
    return _LEDGER_TEXT[jur]


_MEASURE_TOKENS: dict[str, set[str]] = {}


def ledger_measures(jur: str) -> set[str]:
    """Every bill-number-shaped token in the state's ledgers, normalised to
    `PREFIX NUMBER` with leading zeros dropped, computed once per state."""
    if jur not in _MEASURE_TOKENS:
        found = re.findall(r"\b([A-Z]{1,4}) ?0*(\d{1,5})\b", ledger_text(jur))
        _MEASURE_TOKENS[jur] = {f"{p} {n}" for p, n in found}
    return _MEASURE_TOKENS[jur]


def measure_accounted(jur: str, bill_number: str) -> bool:
    """Most states screened by MEASURE, not by roll: a synopsis read drops a bill and
    the ledger records the bill number, never its roll ids. New York, Maryland,
    Kansas and Georgia name every one of their leftover measures this way.
    Caveat: short numbers like `SB 1` can match a passing mention, so this can
    over-credit a little — in the direction of hiding work, which is why the
    roll-id check runs first and this is only a fallback."""
    m = re.match(r"([A-Z]+)0*(\d+)$", bill_number)
    key = f"{m.group(1)} {m.group(2)}" if m else bill_number
    return key in ledger_measures(jur)


def _read_accounted_rolls(jur: str) -> set[int]:
    """Every roll id the state's committed evidence knows about: an imported roll
    has an `ls-*-roll<id>.json` file, and a dropped or superseded one is listed by
    id in a worklist, ledger or report — TSV, Markdown or JSON; Maryland keeps no
    TSV at all. Ledgers come in a dozen layouts, so the ids are read as bare
    7-digit tokens from every evidence file rather than by column. Evidence may
    live in a sibling session's directory — Minnesota's 2217 sits inside
    legiscan-mn-2151 — so every directory of the state is read. A README alone
    accounts for nothing, which is the point: "started" is not "finished"."""
    ids: set[int] = set()
    for path in glob.glob(f"{EVIDENCE}/legiscan-{jur.lower()}-*/**/*", recursive=True):
        if not os.path.isfile(path):
            continue
        m = re.search(r"roll(\d+)\.json$", path)
        if m:
            ids.add(int(m.group(1)))
        if path.endswith((".tsv", ".md", ".json", ".txt", ".csv")):
            try:
                ids.update(int(x) for x in re.findall(r"\b(\d{7})\b", open(path).read()))
            except (OSError, UnicodeDecodeError):
                pass
    return ids


def main() -> None:
    text = config_text()
    keys = config_keys(text)
    kept_types = kept_bill_types()
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
                if not bill or bill["status"] != 4 or bill.get("bill_type") not in kept_types:
                    continue
                if not matches(pats, rc.get("desc")):
                    continue
                by_chamber[(bill["bill_number"], rc["chamber"])].append(rc)
        # The chamber's LAST kept roll decides. Same-day rolls cannot be ordered:
        # LegiScan has no sequence field and roll ids run backwards in some states
        # (Connecticut SB 1506: Vote 111, 11-24, has a HIGHER id than Vote 112,
        # 35-0, the unanimous passage that followed it). So when the same-day
        # rolls disagree about being divided, the answer is unknown and is counted
        # separately rather than guessed.
        keep, ambiguous, kept_measures = [], [], set()
        for (bill_number, _chamber), rolls in by_chamber.items():
            last_day = max(r["date"] for r in rolls)
            same_day = [r for r in rolls if r["date"] == last_day]
            verdicts = {is_divided(r["yea"], r["nay"]) for r in same_day}
            if verdicts == {True}:
                keep.append(same_day[0])
                kept_measures.add(bill_number)
            elif verdicts == {True, False}:
                ambiguous.append(same_day[0])
        mapped = crosswalk_people(jur)
        known = accounted_rolls(jur)
        bill_of = {r["roll_call_id"]: bills[r["bill_id"]]["bill_number"] for r in keep}
        # The measure fallback only applies to a REGISTERED session: an unregistered
        # one has no ledger, and its bill numbers collide with sibling sessions'
        # ("SB 1" exists in every Missouri session), which silently credited
        # Arkansas 2242 and Missouri 2216 to ledgers that never mention them.
        unaccounted = [r for r in keep
                       if r["roll_call_id"] not in known
                       and not (registered and measure_accounted(jur, bill_of[r["roll_call_id"]]))]
        if os.environ.get("SHOW_LEFT") and unaccounted:
            print(f"  {jur}-{sid} left: " + ", ".join(sorted({bill_of[r['roll_call_id']] for r in unaccounted})))
        voters = {int(v["people_id"]) for r in unaccounted for v in (r.get("votes") or [])}
        rows.append({
            "session": f"{jur}-{sid}", "name": session_name[:34],
            "registered": registered, "borrowed": borrowed,
            "unmeasurable": pats is None,
            "rolls": len(keep), "measures": len(kept_measures),
            "unaccounted": len(unaccounted), "ambiguous": len(ambiguous),
            "reach": len(voters & mapped), "voters": len(voters),
        })

    print(f"{'session':10s} {'cfg':>3s} {'rolls':>5s} {'left':>5s} {'tie?':>5s} {'reach':>7s}  name")
    todo = []
    for r in rows:
        flag = ""
        if r["unmeasurable"]:
            flag = "  <-- NO CONFIG, NOT MEASURED"
        elif r["unaccounted"]:
            flag = "  <-- rolls not in any evidence"
        if r["borrowed"]:
            flag += "  (unregistered; measured with the state's patterns)"
        cfg = "y" if r["registered"] else ("~" if r["borrowed"] else "-")
        print(f"{r['session']:10s} {cfg:>3s} {r['rolls']:5d} {r['unaccounted']:5d} "
              f"{r['ambiguous']:5d} {r['reach']:>3d}/{r['voters']:<3d} {r['name']}{flag}")
        if r["unaccounted"] or r["unmeasurable"]:
            todo.append(r)

    print("\nrolls    = the chamber's last kept roll on an enacted measure is divided")
    print("left     = of those, whose roll id AND bill number appear nowhere in the state's evidence")
    print("tie?     = same-day rolls disagree on being divided; order unknown, not counted")
    print("\nsessions with rolls left to read:")
    if not todo:
        print("  (none)")
    for r in sorted(todo, key=lambda r: -r["reach"]):
        if r["unmeasurable"]:
            print(f"  {r['session']:10s} NO CONFIG for this state at all — measure by hand")
            continue
        note = " (unregistered)" if r["borrowed"] else ""
        print(f"  {r['session']:10s} {r['unaccounted']:3d} rolls, {r['reach']} of {r['voters']} "
              f"voters are current candidates{note}")
    print("\nA count here means 'worth reading', not 'worth importing'. Filter 5 —"
          "\nwhether the measure carries a defensible for-and-against stance — is a"
          "\nhuman read, and money-only measures pass every gate above it.")


if __name__ == "__main__":
    main()
