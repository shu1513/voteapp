# Wisconsin batch-03 — how each measure was judged

Judged 2026-09-10. Local database only; production holds no Wisconsin roll-call
records.

Every description was written from the enrolled print. No description was
written from a title or a summary.

## Result

**10 measures, 18 roll calls, 878 candidate records, 99 candidates, 566 area
tags.** Nothing was dropped.

## ⭐ Why nothing was dropped, when four of thirteen fell in batch-02

Criminal law bills state one mechanism and state it plainly: who may be charged,
what the penalty is, who must report what to whom. The research area
`public_safety_and_crime_control` names enforcement, prevention, **accountability**
and justice system performance in its own definition, so both directions inside
it are available and a measure rarely reads two ways at once.

The education strand had the opposite problem. Measures there were about who
governs a university or how a scholarship credit works, and the area's words —
teaching, standards, funding, accountability — did not reach them.

This is worth carrying to the remaining strands: the drop rate is a property of
how well the area's definition matches the measure's mechanism, not of how
contested the measure was.

## ⚠ The version check found nothing

All 18 slots voted text that nothing changed afterwards. Wisconsin's usual
hazard — the first chamber accepting the second chamber's amendment without a
recorded vote — did not arise anywhere in this batch.

## Every measure died the same way

All ten passed both chambers, were vetoed, and failed to pass again over the
governor's objection on **13 May 2026** in the chamber where the bill started.
Every tail says so.

## The two measures with two labels

The standing rule is one label per policy strand. Two measures here have two
strands, and in both cases the second strand is not a footnote.

- **AB 73** creates two unrelated specialized court dockets in one act. The
  treatment court docket is criminal justice, so `public_safety_and_crime_control`
  yes = for. The commercial court docket is about how fast business disputes get
  decided, which is `government_efficiency` — the Legislature's own finding in
  the act is that the pilot cut the time to a decision. Yes = for on both.
- **AB 87** pairs immediate restitution for trafficking and child exploitation
  victims with a new condition on getting the vote back after a felony. The
  first is `public_safety_and_crime_control` yes = for. The second is
  `civil_rights` yes = **against**: under the law as it stands, finishing the
  sentence restores the vote, and the measure would have added every unpaid
  fine, cost, fee, surcharge and restitution order plus unfinished community
  service. A restoration that depends on ability to pay is narrower than one
  that does not, and `civil_rights` names equal rights and fair treatment under
  law. Both labels sit on the yes side of the same vote, which is correct: a
  member voting yes voted for both halves.

## SB 25 is the one measure where yes counts against the area

It would have stopped a judge from issuing a criminal complaint against an
officer in a death the officer was involved in once the district attorney
declined, unless new evidence appeared. Wisconsin's citizen complaint route is
the only independent check on that decision, and the act would have closed it.

`public_safety_and_crime_control` names accountability, so removing the check is
against the area. `nay: null`, because a no vote could mean "keep the check" or
"this is the wrong way to stop repetitive filings", and those are not the same
position.

## The two hardest keeps, with the objection stated

- **AB 85** would have required the corrections department to recommend
  revocation whenever a person on supervision, parole or probation is *charged*
  with a crime. The objection is real: a charge is an accusation, and mandatory
  recommendation on an accusation puts pressure on a process meant to weigh
  evidence. It does not change the label. What the act does on its own terms is
  tighten enforcement of supervision conditions, which is what the area
  describes, and the hearing that decides revocation is untouched. The
  description says plainly that a charge is not a conviction so a reader can
  weigh it.
- **SB 146** would have barred anyone with a violent felony conviction from ever
  changing their name, by any route, with no end date and no exceptions —
  including resuming a surname after a divorce and changing a birth record after
  gender-confirming surgery. There is a genuine `civil_rights` objection to a
  permanent, exception-free ban. It is not labeled that way, because the act's
  operative content is a restriction keyed to a criminal record, which is the
  `public_safety_and_crime_control` mechanism, and adding a second label would
  assert a position the vote does not establish. The description names every
  route the ban would have closed so the reader can see its reach.

Two other keeps deserve one line each. **SB 610** adds homeless shelters to an
existing drug penalty enhancer; the campaign treated Wisconsin AB 89's
repeat-theft escalation the same way in batch-01, and consistency matters more
than a fresh argument each time. **AB 672** creates a penalty enhancer and a new
felony aimed at foreign governments that intimidate dissidents here; it was
considered for `civil_rights` and kept in `public_safety_and_crime_control`,
because everything it does is criminal law and law enforcement training.

## The labels

| measure | area | yes means |
| --- | --- | --- |
| AB 73 | public_safety_and_crime_control | for |
| AB 73 | government_efficiency | for |
| AB 85 | public_safety_and_crime_control | for |
| AB 87 | public_safety_and_crime_control | for |
| AB 87 | civil_rights | against |
| AB 629 | public_safety_and_crime_control | for |
| AB 672 | public_safety_and_crime_control | for |
| SB 25 | public_safety_and_crime_control | **against** |
| SB 76 | public_safety_and_crime_control | for |
| SB 146 | public_safety_and_crime_control | for |
| SB 432 | public_safety_and_crime_control | for |
| SB 610 | public_safety_and_crime_control | for |

Every label is `nay: null`. No measure in this batch is single-subject in the
way SB 389 and SB 825 were, where the act's whole content is the area's own
mechanism, so no stated no-side position is defensible here.

## Quality, measured before importing

- Plain-language lint over all 36 descriptions with the repository's own
  `listPlainLanguageWarnings`: **0 warnings**. Longest sentence 41 words. Two
  sentences measured 46 words on the first run and were split.
- Every roll number, chamber, date and tally checked against `legislative_votes`
  before judging.
- British-spelling scan over the descriptions and these documents: clean.
- Each description cites its own roll's tally, and the yes and no versions are
  generated from a single body so they cannot drift apart.

## Reconciliation

- Plan: 878 inserts over 18 rolls.
- Real run: `outcomes {imported: 18}`, `actions {insert: 878}`, 0 errors,
  0 notified. Run stamp `2026-09-10T06:24:49.351Z`.
- Run-stamp predicate: 878. All Wisconsin roll-call records: 2,423, which is
  706 from batch-01 plus 839 from batch-02 plus 878 here.
- Tags predicted independently and confirmed: 466 records on the yes side, of
  which the 100 on AB 73 and AB 87 carry two tags each, giving 566. The database
  agreed at 566.
- 99 distinct candidates, the same 99 the earlier batches reached.
- **Duplicate sweep: nothing to retire.** No record outside the roll-call runs
  mentions any of these ten Wisconsin measures.

## Review fixes, after the first import

Two findings from external review, both checked against the enrolled print and
both real.

- **SB 76.** The covered-crime list named "strangulation" and "hit and run
  causing death". Neither is in the bill. Item (c) is s. 940.198 and s. 940.285,
  physical abuse of an elder person and abuse of an individual at risk; item (g)
  is s. 346.62 (4), reckless driving causing great bodily harm. Strangulation
  (s. 940.235) and hit and run (s. 346.67) appear nowhere in the text. Now
  "physical abuse of an elder or at-risk adult" and "reckless driving that causes
  great bodily harm".
- **SB 610.** The description presented the enhancer as methamphetamine-only. It
  is not. Section 1 creates s. 961.49 (1m) (e), which adds shelters to the
  general enhancer for delivering, or possessing with intent to deliver, any of
  the substances in s. 961.41 (1) (cm) to (h): cocaine, heroin, fentanyl,
  carfentanil, methamphetamine, LSD, psilocybin and THC. Section 2 then folds the
  new place into the separate methamphetamine-materials enhancer in s. 961.49
  (2m). Now "dealing drugs", with one sentence naming the main substances. The
  existing-law sentence was split to stay under the 45-word limit.

Fix path: `build-judgments.py` -> `judgments.json` -> `rollcall:judge` ->
`rollcall:legiscan:import`. Lint over all 36 descriptions: 0 warnings, longest
sentence 41 words. Dry run predicted `rewrite 194, unchanged 684`; the real run
at `2026-09-10T06:44:55.044Z` did exactly that (85 + 11 records on SB 76, 87 +
11 on SB 610), 0 errors, 0 notified (`import-rerun-report.json`). Convergence
dry run afterwards: 878 unchanged (`import-dry-run-rerun-report.json`). No
record with the old wording remains.

## ⚠ Correction after the review fixes: AB 87 misnamed one crime

The review of this batch caught two descriptions that named statute sections
from memory rather than from the text (SB 76 and SB 610, fixed in commit
`4d6b20905`). A sweep of the other descriptions for the same mistake found a
third.

AB 87's description said the immediate-restitution rule covered a person
convicted of human trafficking "or of sexually exploiting a child". The act
cites s. 940.302 (2) and s. 948.051. Section 948.051 is **trafficking of a
child**, not sexual exploitation of a child, which is a different section. The
Legislative Reference Bureau's analysis printed on the introduced bill confirms
it: the rule applies to "human trafficking".

The description now reads "human trafficking, including trafficking of a
child". Re-judged `updated 2` (both AB 87 slots), re-imported with **98 records
rewritten** and 780 unchanged; a database check found 98 records with the new
wording and none with the old. The review session's own re-run reports were set
aside before this run and restored byte-identical afterwards; this run's reports
are `import-dry-run-rerun-2-report.json` and `import-rerun-2-report.json`.

**The lesson, stated plainly: never describe what a cross-referenced statute
section covers without reading it or an official summary of it.** Three
descriptions in this batch broke that rule.
