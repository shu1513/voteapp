# South Dakota roll-call judging notes

Covers batches 01 to 05, across the 2025 Regular Session (2170), the 2026
Regular Session (2231) and the 2025 First Special Session (2222). **The enacted
pool is closed: every measure with a closely divided final vote now carries a
disposition, and none is deferred.**

Everything here was written from South Dakota's own published record: the bill
history, the adopted amendments and the enrolled act on `sdlegislature.gov`.
Committee testimony was not used. Sponsor material was not used. No AI provider
was called at any point.

## What was imported

    Batch 01  2170 + 2222   12 measures   18 roll calls     601 records
    Batch 02  2170            9 measures   12 roll calls     381 records
    Batch 03  2231            7 measures   10 roll calls     341 records
    Batch 04  2231            5 measures    5 roll calls     171 records
    Batch 05  2170 + 2231     14 measures   18 roll calls     636 records
    Total                    47 measures   63 roll calls   2,130 records

68 candidates, 1,579 research area tags. Production holds zero South Dakota
records.

## The rules that decided each vote

**The roll call is the chamber's last recorded passage on the text that became
law.** Where a chamber passed a bill, the other chamber amended it, and the
first chamber then agreed, the concurrence vote is the one imported and the
earlier vote is not. House Bill 1218 is imported on the conference committee
report rather than on either chamber's earlier vote.

**Whether a vote carried comes from South Dakota's own history line, never from
LegiScan's `passed` flag.** The flag is wrong 31 times in these two sessions, in
both directions: 29 votes it calls passes were defeats under a two-thirds
requirement, and 2 votes it calls failures passed on the Lieutenant Governor's
tie-breaking vote in the Senate.

**Every signing date is read out of the state's action log by the builder,** not
typed by hand. Two dates in a first draft were wrong and this caught both.

**Descriptions come from the enrolled act.** South Dakota's enrolled act carries
no markup — it is the clean final law — so what a bill *changed* was read from
the last marked version instead, where new language is underlined and deleted
language is struck through. A plain text dump loses both marks, so the marks are
read from the page.

## The hoghouse check

A hoghouse amendment strikes a bill's whole text and replaces it, sometimes on a
different subject, while the number and often the title stay the same. Every
measure here had its amendment list read.

**House Bill 1239 is the case that proves the check.** It started as a repeal of
the affirmative defense that protects school and public library staff from
obscenity charges. The Senate struck the entire bill and put in something else
altogether: schools and libraries must let a person appeal to the school board or
library governing body over whether material is obscene, with court review.
The House's first vote (38-32) and its final vote (36-34) were on two different
bills. **Dropped**, because on the text that actually became law neither
direction is honest — a formal appeal path with judicial review reads as a
safeguard and as a book-challenge mechanism at the same time.

**Senate Bill 12 is a hoghouse that undid itself.** The House added a cap on
transfers from a candidate's federal committee and then removed it again before
passage, so the Senate's earlier vote and the House's vote are on the same loan
rule after all. Both are imported.

**Senate Bill 216 needed an acknowledgment.** The House rejected it 35-34, voted
to reconsider, and passed it 53-16 the same day. Same-day votes cannot be
ordered by date, so the successful vote is judged and the failed one is listed in
`acknowledge_later_rolls`.

## Labels worth explaining

**House Bill 1052, carbon pipeline eminent domain, went to
`corporate_accountability`, not `environment_and_public_health`.** The act does
not change any health or emissions standard. It changes who may force a
landowner to sell. On the environment the direction is genuinely arguable,
because the pipeline exists to capture carbon; on a company's power over private
land it is not.

**The prison bill went to `government_spending_reduction`, yea against.** A
$650 million appropriation is the clearest single fiscal decision in either
session. `public_safety_and_crime_control` was considered and rejected: whether
more prison capacity improves safety is exactly the argument the vote was about.

**Senate Bill 44 carries two labels pointing opposite ways.** A new
attorney general subpoena for business records is an investigative tool
(`public_safety_and_crime_control`, for) and an expansion of government access to
electronic records (`data_privacy`, against). Both are true, and the judicial
approval step is stated in the description so a reader can weigh it.

**Voter registration measures went to `election_integrity`, not `civil_rights`.**
House Bill 1066, House Bill 1208, House Bill 1127 and Senate Bill 214 all bear on
whether the rolls describe people who actually live where they vote, or on what
the public can check. The class affected by the mailbox rule is not a protected
class and the mechanism is roll accuracy. Senate Bill 214 was also weighed under
`data_privacy`, since it publishes the statewide file weekly; the fields state
law keeps private are excluded, and the file was already open to inspection, so
only `election_integrity` was used.

**Senate Bill 83 is `public_safety_and_crime_control`, yea for, because the area
names justice system reform.** Cutting a first drug ingestion offense from a
felony to a misdemeanor with mandatory evaluation and supervised probation is
that. Senate Bill 179, which lets a court commit a juvenile weapons offender to
the Department of Corrections, was **dropped** rather than labeled the same way:
the area names both accountability and reform, and that measure is read as one by
its supporters and the other by its opponents.

## Drops worth remembering

**Senate Bill 96** lets a county levy a new local gross receipts tax to cut
property tax on owner-occupied homes. It raises one household tax to lower
another, so on cost of living neither direction is honest.

**House Bill 1256 and House Bill 1184**, on petition signature information and
the initiative filing deadline, are dropped under the rule that a measure about
how hard it is to make law by petition carries no honest direction.

**Two joint resolutions** — House Joint Resolution 5002, applying for a federal
constitutional convention, and Senate Joint Resolution 501, approving a future
water permit — never went to the Governor and neither history carries an
enactment action, so both fail the "became law" filter.

**Routine fee schedules, internal state paperwork, single-parcel land bills and
plain spending bills** are dropped on subject, with the reason recorded per
measure in `dispositions.json`.

## Where the state stands

The full ledger is `dispositions.json` in this directory: one row per measure,
with its roll calls and a written disposition.

    99 measures with a closely divided final vote
    47 imported over five batches
    52 dropped with a written reason
     0 deferred
     0 open

**Nothing in the enacted pool is unread.** What remains for South Dakota is the
scope this campaign has not opened here — the closely divided votes on measures
that did NOT become law — and production promotion.

## Batch 05: what closed the pool

The last fourteen measures were the ones that needed the most reading, because
their titles say least. Worth recording:

**Senate Bill 21 was dropped after the act was read in full, not before.** Its
title says it modifies tax refunds for elderly and disabled people. It repeals
chapter 10-18A, the property tax refund, and folds the benefit into the sales
tax refund, where a claimant now receives a pro rata share of a fixed
appropriation capped at $500 for one person and $1,000 for a larger household.
Whether a claimant ends up better or worse off depends on what is appropriated
each year, so the direction cannot be established from the act itself.

**House Bill 1062 shows why a renumbering is not a change.** Most of the marked
text is subsections being renumbered and two subsections repealed years ago being
deleted. The single real addition is that aggravated assault now covers menacing
someone with "a physical object realistically simulating a deadly weapon". The
description says that and nothing more.

**Senate Bill 6 and Senate Bill 76 are the same measure a year apart** — both let
the housing infrastructure fund, created to pay for water lines, streets and
sidewalks serving housing projects, lend for something else: a school building
next to a military installation in 2025, airport infrastructure in 2026. Both go
to `housing_affordability`, yea against, because on that axis the direction is
plain.

**House Bill 1084 was the one measure carried over undecided from batch 04,** and
reading the marked text settled it. It widens what is withheld from the public
voter file, from judges' home addresses alone to the home address, phone and
personal email of current and retired state and federal judges and of law
enforcement officers. The introduced bill also covered statewide, legislative and
federal officeholders; House Judiciary struck them before the floor vote, and the
enrolled act does not have them. City and county stay public. `data_privacy`,
yea for.

**House Bill 1093 was dropped for the shape that is easiest to miss.** It moves
school bond votes onto the primary or general ballot, which puts them before more
voters, and it also lets a district put a question rejected at the primary back on
the general ballot. More voters decide, and the district gets two tries. Those
pull opposite ways on the same axis.

## One gap worth raising

South Dakota has no research area for labor or workplace rights, the same gap
that has cost measures in seven other states in this campaign. Senate Bill 63,
the state office of apprenticeship, was placed under `reduce_wealth_gap` because
that area names economic mobility, which an apprenticeship route into skilled
work plainly is. A labor area would have been the better home.

## Review round

Seven review findings on pull request #1272, all checked against the enrolled
act and all real. Corrected in the judgments, re-judged, and re-imported on the
same record ids: 272 records rewritten, counts unchanged at 45 roll calls and
1,494 records. Each batch keeps its ledger in
`import-review-fixes-report.json`; `import-report.json` is untouched.

House Bill 1220 had vapes on the wrong side of the line. The new chapter's
"nicotine product" is an alternative nicotine product or a vapor product; only
cigarettes and tobacco products are outside it. The description also now says
that distributors and wholesalers already licensed under chapter 10-50 do not
need the new license.

Senate Bill 44 was described as a general criminal-subpoena power. Section 2
limits it to Division of Criminal Investigation cases involving internet crimes
against children or human trafficking, and section 5 limits what can be demanded
to subscriber and account details. Both limits are now in the text.

House Bill 1219 said an agency "may now obtain" an interpreter. Section 2 says
the agency shall procure, appoint and pay for one. The description now reads as
the duty it is.

Senate Bill 159 omitted two conditions on the under-eighteen exception: the
applicants may be no more than four years apart in age, and the license may not
issue until thirty days after the court approves.

House Bill 1092 caps a grant at $5,000 per school district or accredited school,
not per school.

Senate Bill 164 defines a deepfake as an image, audio or video made or altered
with artificial intelligence or other digital technology; the description had
narrowed it to AI audio and video.

House Bill 1238 does not reach representatives accredited or regulated by the
United States Department of Veterans Affairs (section 6); both chambers'
descriptions now carry that exception.

## Review round, batch 05

Four review findings on pull request #1277, all checked against the enrolled
act and the marked versions on sdlegislature.gov, and all real. A fifth error
turned up in the same reading. Corrected in the judgments, re-judged, and
re-imported on the same record ids; counts unchanged at 63 roll calls and 2,130
records. The ledger is `legiscan-sd-2231/batch-03/import-review-fixes-report.json`.

House Bill 1084 does not cover statewide, legislative or federal officeholders.
The introduced bill listed them; House Judiciary struck them before the floor
vote, so the version the House passed and the enrolled act protect only current
and retired state and federal judges and law enforcement officers. The
description and the batch 05 note both said officeholders. Both now match the act.

Senate Bill 106 has no inflation index. The introduced bill raised the set-aside
and the reserve ceiling by the index factor each year from July 2027; House
Appropriations removed both, and the enrolled act is a flat $5 million set-aside
with the $5.5 million ceiling. The fifth error was in the next sentence: the
description said the act drops the oversight board. It does not. "As provided in
§ 13-37-60" is the Extraordinary Cost Oversight Board statute, which still has
the board review requests and the secretary approve them.

House Bill 1064 is conditional. Section 2 makes the whole act effective only on
the date the attorney general certifies that federal law allows the sales, by
statute or a final court ruling. The description had the permission as operative.
It also had the sale venues wrong: the act lists the seller's residence, a
farmers market, a roadside stand or another temporary venue, and requires a label
saying the meat is uninspected and not for resale.

House Bill 1130 adds one use, not five. Transportation contracts and mileage (up
to fifteen percent), textbooks, instructional software and warranties on capital
assets were already in § 13-16-6 as separate paragraphs; the act folds them into
a numbered list. The only new item is premiums on a property insurance policy the
district holds. `public_education_quality`, yea for, stands on that narrower
change.
