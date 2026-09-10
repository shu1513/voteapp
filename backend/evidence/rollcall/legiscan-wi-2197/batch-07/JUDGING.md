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
administrative code expire every seven years unless readopted; and required the
Legislature's approval for any rule with net costs.

They read two ways. Clearing out stale rules and requiring a cost count is what
`government_efficiency` describes; making it harder for agencies to write and
keep rules weakens the enforcement that `corporate_accountability` and
`environment_and_public_health` depend on. Neither side of that is foreign to the
research areas, so no single direction is defensible. The underlying contest is
between the Legislature and executive agencies over who writes rules, and no
area describes separation of powers.

The campaign had no earlier ruling on this class. This is the first, and it is
written here so other states can follow it or the operator can overrule it.

SB 277's Senate slot would also have fallen to the version rule: the Assembly
replaced the whole bill with a substitute after the Senate voted.

## AB 595: an open operator question

AB 595 would have changed how Wisconsin removes ineligible voters from its
registration list, added a citizenship audit and data sharing, and set fees for
obtaining the list. It is the same structure as Wyoming HB0318 — `election_integrity`
reads it as accurate rolls, `civil_rights` as a risk of cancelling eligible
voters — which is still an open campaign-wide operator question. It is dropped on
the same basis as AB 100 and AB 102 in batch-05, and ready to add.

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
