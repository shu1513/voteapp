# Wisconsin batch-09: AB 595

Judged 2026-09-11. Local database only; production holds no Wisconsin roll-call
records. No AI calls.

## Why this batch exists

Batch-07 dropped AB 595 as the Wisconsin twin of Wyoming HB0318, which was then
an open campaign-wide question. Wyoming batch-04 settled it: a voter-list act
that adds data sources and keeps a notice before removal is
`election_integrity`, and a yes vote is **for**. The same line already covers
Alabama HB 233, Idaho HB 339 and Kansas HB 2437. Montana HB 423 was dropped
because it cut the notice.

## Does it qualify?

Yes. AB 595 passed both chambers. The governor vetoed it on 8 April 2026, and
the Assembly failed to override on 13 May 2026. Wisconsin's pool covers vetoed
measures (see batch-02 PLAN.md). The prose is conditional ("would have") and
says how the bill died.

The Assembly passed it on a voice vote, so the Senate's 18-15 concurrence on
17 March 2026 is the only roll call. The Senate adopted no amendment, so that
vote is on the enrolled text. The later "LRB correction" is a drafting fix by
the Legislative Reference Bureau, not a vote.

## What the bill would have done

Read from the enrolled print with the markup resolver, and checked against the
bureau's analysis on the introduced print.

- **Daily data checks.** The transportation department, the vital records
  office and the corrections department would each match the voter list every
  day. Transportation data would be used to check citizenship.
- **Removal instead of an "ineligible" status.** Today an ineligible voter stays
  on the list, marked ineligible. The bill removes them and keeps a permanent
  record of the date and reason.
- **A citizenship audit.** In each odd-numbered year, and once by 30 June 2026,
  the Legislative Audit Bureau compares the list with that data and confirms
  each possible noncitizen in SAVE, a federal immigration database. The
  commission then mails a notice. A voter who does not show a birth
  certificate, naturalization certificate or passport within 30 days is removed.
  The bureau must report how many people it flagged wrongly.
- **Complaints against the commission.** The elections commission must decide
  a complaint that it is itself breaking the federal Help America Vote Act.
  Hearings are open and recorded, and the complainant can go to court.
- **A fee cap.** An electronic copy of the voter list costs $1,000 or less.

## The label, and why it is for

The Wyoming test asks two questions: does the act add ways to find ineligible
entries, and does it keep a notice before removal? AB 595 does both. The
citizenship removal comes only after SAVE confirms the match, a mailed notice,
and 30 days to answer.

The strongest civil-rights worry is a citizen who misses the notice or has no
document ready. Two facts in the act limit that harm. First, a removed voter
may register again under s. 6.50 (10), which includes registering at the polls
on election day under s. 6.55 (2). Second, the audit must report its wrong
flags, which is itself an accuracy check. The act also adds open, recorded
hearings and court review for complaints. That is the "auditable, and trusted"
half of the area.

| measure | area | yes means | no means |
| --- | --- | --- | --- |
| AB 595 | election_integrity | for | — |

The no side is `null`, as with HB0318: a no vote could rest on privacy, cost or
the fee cap.

## Roll

| measure | chamber | roll | date | tally | records |
| --- | --- | --- | --- | --- | --- |
| AB 595 | Senate | 1664414 | 2026-03-17 | 18-15 | 11 |

## How it was run

From `backend/`, the same commands as the other Wisconsin batches:

1. The roll file was copied from the earlier fetch (`wi-2197-fetch2`). It
   matches the first fetch except for the fetch time.
2. `python3 build-judgments.py`, then the plain-language lint (0 warnings,
   longest sentence 36 words), then `npm run rollcall:judge -- --judgments-file
   evidence/rollcall/legiscan-wi-2197/batch-09/judgments.json`, dry run first.
3. `npm run rollcall:legiscan:import -- --state WI --evidence-dir
   evidence/rollcall/legiscan-wi-2197/batch-09 --crosswalk-file
   evidence/rollcall/legiscan-wi-2197/crosswalk.json --people-file
   evidence/rollcall/legiscan-wi-2197/legiscan-people-wi-2197.json`, dry run
   first.

## Reconciliation

- Dry run: 11 inserts. Real run at `2026-09-11T05:42:48.261Z`: `outcomes
  {imported: 1}`, `actions {insert: 11}`, 0 errors, 0 notified.
- 6 yes-side records, each with one `election_integrity` tag. 5 no-side
  records, no tags. Only 11 of 33 senators are on the 2026 ballot, because only
  odd-numbered Senate districts are up.
- Duplicate sweep: no live hand-written record mentions AB 595.
- All live Wisconsin roll-call records: 5,236 over 103 rolls and 99 candidates,
  with 3,291 tags. That count includes batch-08 (AB 100 and AB 102), which is
  imported locally but still on its own open pull request.
