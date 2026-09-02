# Kentucky 2025 batch-03 — how these judgments were made

## Sources

Every judgment was written here from Kentucky's own documents. No AI provider
was called and no reading was delegated, for the reason recorded in batch-02: a
batch-01 research agent reported sections it had not actually read.

- The **enrolled Act** at
  `apps.legislature.ky.gov/recorddocuments/bill/25RS/<bill>/bill.pdf`.
- The **official vote record** at
  `apps.legislature.ky.gov/record/25rs/<bill>/vote_history.pdf`, which gives
  each roll its sequence number, its question in plain words, and its tally.
- The **bill page** at `apps.legislature.ky.gov/record/25rs/<bill>.html`, for
  the action history and the summaries of the original and enacted versions.

All three need a browser user agent.

## The Legislative Research Commission summary was used only as a starting point

The Commission's Summary of Enacted Version misstated the Act in six of
batch-01's twelve measures, so every claim in a description below was checked
against the Act text itself. Two checks changed what was written:

- **HB 606.** The summary says the taxing district must comply with the
  petition requirements of KRS 65.182 to 65.190. The Act adds "but not the
  percentage of registered voter signature requirements under KRS
  65.182(1)(a)", so the description says the voter signatures are waived.
- **HB 664.** The summary says the Act "lowers the age" for motor vehicle
  offenses without saying from what to what. The Act's bracketed deletions show
  "fifteen (15)[sixteen (16)]" in three places, so the description gives both
  numbers.

## Version checks, roll by roll

Only SB 28 was an override, and Kentucky takes an override vote on the enrolled
Act with no amendment possible, so that roll needs no further check. The other
eight were traced through the bill history:

- **HB 241 Senate, 13 March, 31-6.** RSN# 3543 adopts the free conference
  committee report, which is the enrolled text.
- **HB 606 House, 28 March, 82-17.** RCS# 361 adopts the free conference
  committee report, which is the enrolled text.
- **HB 664, both chambers, 28 March.** The Senate passed Senate Committee
  Substitute 1 with floor amendment 2 (RSN# 3673) and the House concurred the
  same day (RCS# 365). Both rolls are on the enrolled text.
- **HJR 15 Senate, 13 March, 32-6.** RSN# 3537 adopts the resolution as the
  House sent it, with no Senate change.
- **SB 202, both chambers.** The House passed House Committee Substitute 1
  (RCS# 226, 12 March) and the Senate concurred in that substitute the next day
  (RSN# 3523). Both rolls are on the enrolled text.
- **SB 9 Senate, 5 March, 31-7.** This one is not on the enrolled text — see
  below.

Every roll was matched to Kentucky's own vote record by sequence number, and
all nine tallies agree.

## SB 9 is described as the Senate passed it, not as it was enacted

The Senate's divided vote (RSN# 3435, 31-7) was on the Senate's own bill with
its two floor amendments. The House then replaced it with a 72-page committee
substitute rewriting much of the Teachers' Retirement System, and the Senate
concurred in that substitute 38-0 — a vote with no division to record.

So the description for this roll says "Senate Bill 9, as the Senate passed it"
and covers only the two provisions that were in the text the Senate voted on:
the paid maternity leave mandate and the shift of sick leave retirement cost
from the state to local districts. Both appear in the Commission's summary of
the original version and both survived into the Act, with the yearly sick leave
limit moving from 12 days to 13.

Nothing from the House rewrite is described. That matters most for one clause
the House added, which says the 30-day deadline to appeal a retirement decision
"shall not be subject to the jurisdiction of any court or appeal process, nor
shall it otherwise be tolled or waived". No senator voted on that clause in the
31-7 roll, so no senator is credited or charged with it.

## One acknowledged later roll

Because roll 1506432 is not the Senate's last kept floor vote on SB 9, the
judgment lists roll 1530424, the 38-0 concurrence of 28 March, in
`acknowledge_later_rolls`. That later roll is unanimous, so it never entered the
divided pool and could not have been selected in its place.

## Direction calls worth recording

**HJR 15 is `civil_rights` / against.** The resolution orders a religious
monument back to permanent display on the grounds of the state Capitol. The
area is the one batch-01 weighed for SB 19, the moment-of-silence bill, and
then dropped because that bill ran both ways. HJR 15 does not run both ways,
because returning the monument is its whole content.

**SB 9 is `social_programs_and_welfare` / for.** The measure creates a new paid
leave benefit that every school district must offer. The sick leave cost shift
in the same bill moves money between the state and district budgets without
reducing anyone's benefit, so it does not pull against the leave mandate.

**SB 202 is `environment_and_public_health` / for.** Before the Act,
intoxicating hemp drinks were sold in Kentucky under Department for Public
Health rules. The Act moves them into the alcohol licensing system, caps them at
5 milligrams of intoxicating cannabinoids per 12-ounce serving, bars anyone
under 21 from buying or drinking them, and confines retail sale to licensed
package stores in wet territory. Every one of those is a tightening, so the
direction is not in doubt even though thirteen senators voted no.

## Filter 4 removed three rolls

Three pool rolls are the same chamber's earlier vote on a text that later
changed: HB 241 Senate RSN# 3445 of 6 March, HB 664 House RCS# 191 of 7 March,
and SB 202 Senate RSN# 3447 of 7 March. Each is marked
`batch-03:not-selected` in the worklist.
