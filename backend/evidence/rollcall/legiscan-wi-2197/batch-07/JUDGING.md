# Wisconsin batch-07 — how each measure was judged

Judged 2026-09-10. Local database only; production holds no Wisconsin roll-call
records.

Every description was written from the enrolled print.

## Result

**6 measures, 8 roll calls, 388 candidate records, 99 candidates, 305 area
tags.** Eleven measures were dropped.

## ⚠ A lint failure reached the database, and how it was repaired

The first import of this batch carried a 58-word sentence in AB 211's
description, over the 45-word limit. The lint had flagged it. The chain went
ahead anyway because the small wrapper script I use to run the repository's
`listPlainLanguageWarnings` printed its warnings but exited with success, so a
shell `&&` did not stop.

The repair:

1. The wrapper now exits with failure on any warning, so the same mistake cannot
   pass silently again.
2. The first `import-report.json` was preserved before any re-run.
3. The sentence was split, the judgments rebuilt and linted clean (longest
   sentence 44 words), and the batch re-judged: `updated 2` — the two AB 211
   slots — and `unchanged 6`.
4. The batch was re-imported, which rewrote the stored descriptions in place.
   A database check afterwards found **97 AB 211 records with the new wording and
   0 with the old**, and the Wisconsin total unchanged at 5,036.

**A second mistake followed, and it is recorded rather than hidden.** The
importer names its own report `import-rerun-report.json` when an
`import-report.json` already exists. Not knowing that, I moved files around after
the re-run and overwrote that re-run report with a copy of the first one. The
report of the re-run that rewrote the 97 records is therefore lost. The evidence
that the rewrite happened is the judge output and the database check above.

The batch was then imported once more to produce a genuine re-run report. That
report, stamped `2026-09-10T06:47:45.407Z`, shows **all 388 records unchanged**,
which confirms the stored state matches `judgments.json` exactly.

## ⭐ Three stated no-side positions, each on the same test

AB 211, SB 184 and SB 420 each carry `nay: for`. That is more than any other
batch, so the test is stated once: a no vote evidences a position for the area
only when the act is single-subject and its whole content is the area's own
mechanism. Each of these three is exactly that.

- **AB 211** would have exempted some cigar and pipe bars from the indoor smoking
  ban. Its whole content is a community health standard, and a no vote is a vote
  to keep the standard whole.
- **SB 184** would have barred every state agency and local government from
  restricting a vehicle or device because of its energy source — ruling out a
  local ban on new gas cars or gas stoves. Its whole content is removing a class
  of environmental regulation.
- **SB 420** would have barred local rights of nature ordinances. Its whole
  content is removing a local environmental protection tool.

All three are `environment_and_public_health`, yes = against.

## The other three keeps

- **AB 385** would have required any online platform passing card payments to
  political committees to verify the security code and a United States billing
  address. `election_integrity` yes = for: it is a control against fraudulent
  and foreign contributions.
- **AB 793** would have created an internal audit office at the Department of
  Employee Trust Funds, reporting to the board rather than to management.
  `government_efficiency` yes = for.
- **SB 16** would have barred school districts from belonging to a high school
  athletic association that does not follow the open records and open meetings
  laws. `anti_corruption` yes = for. The association is a private nonprofit, which
  is why the area's "public office" wording was weighed: the act extends the
  area's own mechanism, transparency law, to a body that governs public school
  activity, so the direction is clear.

## ⚠ AB 105: the Senate removed the provision that made it famous

AB 105 would have required websites with a substantial amount of sexual material
to verify visitors' ages. The Assembly passed it 69-22 in March 2025 with a
further requirement: such a website **must block visitors using a virtual private
network**. The Senate's Amendment 2, adopted in February 2026, deleted that
requirement and instead added virtual private network providers to the list of
companies that cannot be held liable. The Assembly accepted the change without a
recorded vote.

The Assembly's only recorded vote is on a bill with a VPN-blocking mandate that
did not survive. Under the Wisconsin version rule — drop when a later amendment
removed an operative provision the chamber voted on — the slot is dropped, and
AB 105 has no other divided roll.

The deleted text was confirmed by reading page 5, lines 3 to 8, of the
introduced print against the amendment.

## The administrative-rules package: four drops on one reason

SB 275, SB 276, SB 277 and SB 289 would have, in turn: made statements of scope
for agency rules expire sooner and limited each to one rule; awarded attorney
fees to anyone who successfully challenges a rule; made every chapter of the
administrative code expire every seven years unless readopted; and made an
agency stop work on any proposed rule with net costs, where today the line is
$10 million over two years.

**Re-examined 2026-09-11 at the operator's request, from the full enrolled
text. The drop stands, and the reason is sharper than the first draft's.**

The four bills govern how every agency writes rules, on every subject at once.
None names a policy field. The research areas each describe an outcome in a
field: cleaner water, safer workplaces, fair treatment. A label would claim the
bill moves one of those outcomes, when the bill never says which rules it will
touch. Two areas come close, and neither fits:

- `government_efficiency` is about the government's own operations: service
  delivery, waste and modernization. These bills are about the cost that rules
  put on businesses, local governments and people outside the government. SB 277
  also adds paperwork for agencies: a readoption notice for each chapter, a
  legislative staff certification and a committee review every seven years.
  So even inside that area, "more efficient" is not a safe reading.
- `environment_and_public_health`, `labor_rights` and `corporate_accountability`
  would each read a yes vote as against, since SB 289 and SB 277 make new or
  stricter standards harder to adopt. SB 277 even bars a readopted chapter from
  adding any cost or stricter standard, including one needed to meet a change in
  federal law. But the bills reach those fields only through whatever rules an
  agency later proposes. Tagging a yes vote as against clean water would put a
  claim on a record that the bill's text does not make.

SB 275 and SB 276 are the clearest drops: one is a filing deadline, the other a
fee rule for lawsuits. SB 277 has a second, independent problem: after the Senate's 18-15 vote,
the Assembly replaced the whole bill with a substitute, and the Senate accepted
it without a recorded vote. Only the Assembly's 53-45 vote is on the final text.

**The ruling for other states:** a bill that changes rulemaking procedure for
all agencies, without naming a policy field, is dropped. If the operator wants
these votes on the site, the fix is a new research area about the amount of
regulation, not a stretch of an existing one.

## AB 595: imported in batch-09

AB 595 was dropped here pending the campaign-wide decision on Wyoming HB0318.
Wyoming batch-04 made that decision, and AB 595 was imported as
`election_integrity`, a yes vote is for. See `../batch-09/JUDGING.md`.

## The other drops

- **AB 39** would have required state employees to work at the office for at
  least 80 percent of their hours. Productivity and cost arguments run both ways
  inside `government_efficiency`; no direction is defensible.
- **AB 248** would have ended a state appointee's term on its expiry date rather
  than letting the appointee hold over until a successor is confirmed. A contest
  over appointment power that no area describes.
- **AB 450** would have let a building follow the commercial building code in
  effect on 1 August 2025 if its plans were filed by 1 April 2026. A code
  transition rule with no area.
- **SB 270** would have let anyone whose complaint against an election official
  was dismissed by the Elections Commission appeal to court, without showing an
  injury. It reads two ways inside `election_integrity`: more review of election
  administration on one side, more litigation against election officials on the
  other. Both claims are about public trust in elections.
- **SB 622** sets fees for animal markets, dealers, truckers and animal health
  registrations. A fee schedule with no area.

## The labels

| measure | area | yes means | no means |
| --- | --- | --- | --- |
| AB 211 | environment_and_public_health | against | **for** |
| AB 385 | election_integrity | for | — |
| AB 793 | government_efficiency | for | — |
| SB 16 | anti_corruption | for | — |
| SB 184 | environment_and_public_health | against | **for** |
| SB 420 | environment_and_public_health | against | **for** |

## Quality

- Plain-language lint over all 16 descriptions: **0 warnings** after the repair
  above. Longest sentence 44 words.
- Every roll number, chamber, date and tally checked against `legislative_votes`
  before judging.
- British-spelling scan over the descriptions and these documents: clean.

## Reconciliation

- Plan: 388 inserts over 8 rolls.
- First real run: `outcomes {imported: 8}`, `actions {insert: 388}`, 0 errors,
  0 notified. Run stamp `2026-09-10T06:45:22.934Z`.
- Run-stamp predicate: 388. All Wisconsin roll-call records: 5,036, which is
  4,648 before this batch plus 388.
- Tags predicted independently and confirmed: 215 records on the yes side plus
  90 records on the no side of AB 211, SB 184 and SB 420, giving 305. The only
  tagged no-side records are those three measures'.
- 99 distinct candidates.
- **Duplicate sweep: nothing to retire.**
