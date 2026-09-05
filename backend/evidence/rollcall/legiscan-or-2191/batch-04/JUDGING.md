# Oregon batch-04 — plan and judging

Four measures, eight roll calls, **228 records across 61 candidates**. Oregon
totals after this batch: **1,200 records / 913 tags / 43 approved rolls.**
Production still has zero Oregon roll-call records.

| Measure | House | Senate | Area | Yea | Nay |
| --- | --- | --- | --- | --- | --- |
| SB 916 unemployment pay in a labour dispute | 35-22 | 16-12 | social_programs_and_welfare | for | null |
| SB 951 investor control of medical practices | 41-16 | 21-8 | corporate_accountability | for | null |
| SB 1154 ground water quality concern areas | 39-11 | 18-12 | environment_and_public_health | for | null |
| HB 3365 climate change in school standards | 32-21 | 18-10 | environment_and_public_health | for | null |

## SB 916 is the campaign's first Oregon conference-report measure

It is also the only measure so far that needed an acknowledgment. The bill
went to a conference committee after the Senate refused to concur in the
House's amendments, and Oregon's House then took **two rolls on the same day**:
adopting the conference report (roll 1590863, 35-22) and repassing the bill
(roll 1590864, 35-22). Identical tallies, two different questions.

The repass is the chamber's final action on the enacted text, so it is the
judged roll, and the adopt roll goes in `acknowledge_later_rolls` with a note
saying why. The Senate folds both steps into one `Senate Repassed` roll, which
is what the config comment predicted.

Both chambers' earlier passage rolls (Senate 16-12 in March, House 33-23 in
June) are on superseded drafts and were correctly not chosen.

## Area choices worth recording

**SB 916 went to `social_programs_and_welfare`, not to a labour area, because
there is no labour area.** The California campaign recorded this gap: ten of
its drops were measures about union or wage-standard questions that no
research area covers. Extending unemployment benefits to locked-out and
striking workers is a safety-net expansion, which is what that area describes,
so the mapping is honest even though it is not the most natural label a
newspaper would use.

**SB 951 went to `corporate_accountability` rather than
`healthcare_affordability`.** The Act's whole operative content binds
management companies, hospitals and the contracts they write. It does nothing
directly about what patients pay.

## What the enacted text changed

- **SB 916.** The strike rule is not a simple entitlement. A striking worker
  is disqualified for the **first week**, and only then may draw benefits, for
  up to ten further weeks — or eight, if the unemployment fund sits on one of
  its stronger tax schedules when the strike begins. The description carries
  both the first-week bar and the two caps. Lockouts carry no such limit.
- **SB 951** runs to 448 lines of new language, most of it definitions. The
  description states the four things a management company may no longer do and
  the two kinds of agreement the Act voids, and says plainly that both bans
  carry exceptions — chiefly for clinicians holding a real ownership stake and
  for terms agreed in settling a dispute. Flattening those into an
  unqualified ban would be the Texas SB 2972 error.
- **SB 1154** adds a **new, earlier tier** rather than tightening the old one.
  The description says so, and keeps the three-part test: many wells affected,
  sources needing more study, or limits likely to be broken within 20 years.
- **HB 3365** binds only **future revisions** of the standards, in six named
  subjects. It does not rewrite anything now, and the description says that.

## Labels

All four score `for` with `nay: null`. The objections run on different axes
from their areas: unemployment fund solvency and neutrality between employer
and union (SB 916), practice-ownership structure and physician supply
(SB 951), regulatory burden on farms (SB 1154), and local control over
curriculum (HB 3365).

## Checks

- Version check on all 8 rolls: each is on the text that became law.
- Superseded check run up front. One acknowledgment, on SB 916's House repass.
- **One `related` flag, read before writing this file** (the North Carolina
  rule). It is a false positive: on HB 3365's House roll the scan matched two
  hand-written records on Christine Drazan and Virgle Osborne — a 2020 walkout
  over cap-and-trade, a same-day motion on a different bill (SB 83), and a
  committee-membership row. None is a vote claim about HB 3365. This is the
  known state-measure noise recorded in the Kentucky findings: the duplicate
  test only understands federal bill spellings, so on a state bill it falls
  back to matching the word "vote". Nothing retired.
- Errors 0, `ambiguous` 0, notifications 0.
- Dry run matched the real run at 228 inserts; convergence reports all 228
  `unchanged`.
- Reading level measured: first drafts scored grade 11.3 to 13.1 and were
  rewritten before importing to **median 9.1, worst 9.8**.

## A gap the builder had, found here

The British-spelling check missed **"practising"** in an SB 951 draft. The
word list had `practise` and `practised` but not the participle. It has been
widened, and batches 01 through 03 were rebuilt against the new list as a
regression check — all three still pass. This is the second defect the
builder's own rules have had, after the suffix-versus-word-list problem in
batch-02, and both were found by using it rather than by review.

## Ledger

`import-report.json` — the insert run, 228 records.
