# Arizona batch-02 — judging and import

## Sources

Same contract as batch-01. Every description was written from the **enacted text**, cross-read
against the version-stamped staff analysis for the text that became law — the House Bill
Summary headed `Signed`, or the Senate Fact Sheet headed `As Passed House` where no House
summary exists. Neither carries a sponsor statement of intent.

All 77 candidate measures were read through their analyses before any of the 21 was judged, so
the drops in `PLAN.md` rest on a read rather than on a title.

## The official summary overstated an act, and the chaptered text caught it

**SB 1378.** The House Summary's Overview says the act alters the definition of a political sign
"by including certain flags **and allowing signs without regard to if the person supported or
opposed on the sign is on the ballot of the upcoming election**." The second half is not in the
act. Arizona marks its edits in machine-readable CSS, and the extracted insertions for SB 1378
are exactly two occurrences of `or flag` and one `an` to `a`. Reading the enacted definition
confirms it: a political sign is "a sign **or flag** that attempts to influence the outcome of
an election, including supporting or opposing the recall of a public officer or supporting or
opposing the circulation of a petition for a ballot measure". The ballot-timing condition the
Overview describes was never there to remove.

The description therefore says only that a flag now counts as a political sign an association
may not ban. This is the Georgia HBRO and Kentucky LRC pattern — an official summary that
misstates its own act — and it is the third state where the enacted text has had to overrule
the summary.

Three other stance-critical measures were verified the same way and all three matched their
summaries: SB 1060 (the 24-hour rule and its three exceptions), SB 1461 (may not dismiss, may
demote, with the recruit and lateral-transfer carve-outs) and HB 2880 (the encampment
definition, the enforcement steps and the liability for repair costs).

**One wording correction came out of that check.** SB 1060's summary says the employer must
provide "all relevant materials"; the statute says "any relevant **and readily available**
materials". The description carries the statutory limit.

## Vote checks

**HB 2447's Senate vote is a reconsidered pair.** The third reading failed 15-12 on 2025-03-26,
the Senate moved to reconsider, and the bill passed 19-8 the same day. Arizona stores both under
the plain `Senate - Third Reading` caption, so only the `passed` flag and the ascending roll id
tell them apart — the third instance of this trap in Arizona, after HB 2518 and SB 1661 in
batch-01. The passing roll is judged; the failed roll is listed in `acknowledge_later_rolls`
with a note explaining the order, because the superseded-stage gate scans by date and cannot
order two votes taken on one day.

A single query over all 26 selected rolls found that pair and nothing else, so it is the only
acknowledgement in the batch. All 26 roll dates match Arizona's own history, so no
`official_vote_date` override was needed.

## Duplicate sweep, and two records retired

The importer reported **0 related flags**, and as in batch-01 that scan is weak on state
measures, so a wider sweep ran over every live Arizona record not written by this pipeline,
matching each batch measure's number in both spellings.

It found nine records. Seven are not duplicates: sponsorship claims, and two false positives on
bill numbers reused in earlier sessions (a 2013 HB 2611 about school boards, a 2020 HB 2880
committee vote).

**Two are real duplicates and were retired.** Vote Smart rows for Senators Analise Ortiz and
Mark Finchem state how each voted on HB 2611 — "Ortiz voted against", "Finchem voted for" —
which matches their positions on the Senate roll this batch imports exactly. Both rows are dated
2025-05-06, the day the House took its **final reading**, which Arizona publishes without a
member list; Vote Smart dates the claim to the bill's last action rather than to the vote. The
retirement reasons name the replacing record ids. The file is
`duplicate-retirements.json`, kept for a production run.

The two representatives in the same sweep, Kiana Sears and Julie Willoughby, were **not**
retired: their rows describe the House final reading on HB 2611, which is a real vote that this
pipeline cannot import and does not duplicate.

## Labels

Eight areas, three new to Arizona: `data_privacy`, `public_infrastructure` and
`social_programs_and_welfare`. Every stance label states the nay side and every nay is `null`.

Three measures score `against`, and two of them make the same argument from opposite ends of
the batch: SB 1060 and SB 1461 both strengthen an officer's hand against their own employer,
and `public_safety_and_crime_control` names accountability among its goals. They sit beside
seven measures in the same area scoring `for`.

Arizona still records nothing without a stance, so a measure with no honest direction is
dropped. That cost 56 measures this batch, each with a written reason on its worklist row.

## Writing checks, run before importing

- Plain-language lint: **52 descriptions, 0 warnings.**
- Reading level measured separately: **Flesch-Kincaid median 9.4, best 7.2, worst 10.7**, mean
  sentence 16.5 words, longest 38. A first draft measured **median 10.8 and worst 13.8**, and
  ten bodies were rewritten into shorter sentences before anything was imported. The remaining
  high scores are driven by terms that cannot be replaced without losing the statute's meaning
  (licensing board, community college, transportation, impersonation).
- The builder's assertions caught two defects before the file was written: a 46-word sentence in
  SB 1462, and a British spelling of the word license in SB 1537.

## Import

| run | stamp | result |
| --- | --- | --- |
| dry run | `2026-09-05T03:43:24.220Z` | 682 planned inserts, 0 errors |
| real run | `2026-09-05T03:43:38.688Z` | **682 inserts**, 0 errors, 0 notified |
| convergence re-run | — | all 682 `unchanged` |

Reconciled three ways:

1. `candidate_records` with `origin='rollcall_import'` moved 128,872 to 129,554, a delta of 682.
   (The baseline is not batch-01's 126,505 because a parallel session wrote to the shared local
   database in between — the run stamp, never the table delta, is the authority.)
2. The run-stamp predicate returns **682 records across 54 candidates**.
3. The dry run's own stamp matches **zero** rows.

**Arizona now holds 1,090 live records across 54 candidates and 741 tags** (408 from batch-01,
682 here). Tags reconcile by side arithmetic: 741 tags on 741 yes-side records, with the no-side
records carrying none because every nay is `null`.

**Production is untouched.** It holds no Arizona roll-call records.
