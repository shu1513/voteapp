# How this batch was judged

Every measure was judged from its **enrolled act**. No claim here comes from a
bill summary, and none was carried over from the 2025 version of the same idea.
That discipline is the direct result of the review of Colorado batch-11, which
found ten of eighteen descriptions needed correcting because they rested on
summary text or quietly widened a bill's scope.

It paid for itself immediately. **Four claims I was ready to write turned out to
be wrong**, and each was caught by reading the operative text:

| I was going to say | the act says |
|---|---|
| HB 1001 covers land owned by faith groups and schools | school districts, colleges, boards of cooperative services, housing authorities, transit districts and affordable-housing nonprofits — no faith organizations |
| HB 1001 protects buildings up to 45 feet | **38 feet** |
| HB 1134 caps municipal jail terms at the state equivalent | it does not; that was the vetoed 2025 bill. This one sets 48-hour hearings, counsel for defendants who cannot pay, and flat-fee limits |
| HB 1424 requires fingerprint background checks | it does not; that was the vetoed 2025 bill. This one moves the existing check to the company, every six months |

## HB 1322 says outright that it creates nothing new

Subsection (6) reads: this section does not create a new cause of action. So the
description says the act "sets out" that a person harmed by conversion therapy
can sue and removes the time limit — not that it creates a new right to sue.
Getting that wrong is the same class of error the batch-11 review caught on
SB 185.

## SB 124 was dropped: the vehicle-bill trap, arriving through the title

SB 124 is titled **"Colorado Survivor Justice Act"** in the feed. Its enrolled
act is titled *Concerning information related to the automated protection order
notification system*, and the whole act amends what that system records — adding
whether a background check denied the restrained person a firearm, and repealing
one existing item.

The `description` field matches the act; only `title` is stale. The measure is
administrative and pulls in both directions, so it is dropped rather than given
a stance it does not have. **The reusable rule: triage Colorado 2026 on
`description`, never on `title`.**

## The version check had to be rebuilt for this session

96% of this session's bill texts carry the epoch date `1969-12-31`, so the
print-in-force-on-the-vote-date method is impossible. The action history is
correctly dated, so the check asks instead whether any text-changing action
follows the chamber's selected roll. Concurring in the other chamber's
amendments **accepts** text and is not a change; refusing to concur, adhering,
passing with amendments and conference reports are. All 21 rolls came back clean.

## Seven rolls were the wrong kind until the judge said so

Seven of the House rolls are `Senate Amendments Repass` votes with a same-day
`Concur` peer, not plain third readings. The first judge run refused the batch
and named the peer. All seven were corrected to repassage wording and the
same-day concurrence acknowledged.

## A hand-written record that was wrong about a member's vote

The dry run flagged five existing records as related. Four are the familiar
same-date, different-bill false positive. The fifth is not:

> "Voted Yes on House concurrence for HB26-1144 … the vote passed 40-23."

**Dusty Johnson voted Nay on both House votes on that bill** — the concurrence,
which carried 44-19, and the repassage, which carried 40-23. The record also
attaches the word "concurrence" to the repassage tally. It is retired with that
reason in `retirements.json`, and the imported record cites the roll call itself.

## Wording

First draft measured a Flesch-Kincaid median of 7.7; after rewriting the four
hardest measures it is 7.5. The plain-language lint reports 0 warnings across
all 42 descriptions. Reading grade is computed separately, because the lint only
counts words per sentence.

## Review correction, 2026-09-07: three descriptions were wrong and are rewritten

A review of the merged-pending PR found three claims that the enrolled acts do
not support. Each was checked against the act before anything was changed, and
each was confirmed.

- **HB 1126.** The description said the permit extends to "anyone moving
  firearms into or out of the state." Section 18-12-401.5 (1)(a) begins "Every
  **dealer** must obtain a state permit" — the extension is to a dealer's
  transfers across the state line, not to ordinary gun owners. 52 records
  rewritten.
- **HB 1139.** Two errors, one of them mine to own twice over. The description
  said insurers "cannot be paid for therapy given by a machine"; subsection (6)
  bars insurers from **covering** AI-delivered psychotherapy, and new 25.5-1-209
  bars Medicaid and the children's health plan from paying for it. Worse, this
  file's table of "claims I caught" said the act only requires an insurer to
  disclose whether a human approves a denial. That was subsection (4)(c). I
  missed subsection (5)(b), which requires that a medical-necessity denial not
  issue "solely on the output of an artificial intelligence system without
  human review and approval" by a licensed clinician. The act does require human
  approval; my catch was the error. That row is removed from the table and the
  count above is now four. 39 records rewritten.
- **HB 1335.** The description said a college cannot disclose named health
  records unless federal law or a court order requires it. Subsection (5) is
  narrower: the bar applies to "a request from another state seeking to impose
  liability for the legally protected health-care activity." Ordinary
  authorized disclosures are untouched. 55 records rewritten.

The judge updated the five affected roll-call rows; the importer's dry run
planned exactly 146 rewrites and 433 unchanged; the real run did the same, in
place, keeping every row id and tag. Totals are unchanged at 7,442 records, 55
candidates and 5,173 tags, and no row carries the old wording. The original
`import-report.json` is preserved; the re-run wrote `import-rerun-report.json`.
