# Montana batch-13 — safety, disclosure, tribal education, and a queue correction

Eight measures, nine roll calls, 417 candidate records. All eight became law.

The larger result of this batch is not the imports. **58 joint-resolution rolls
were sitting in the queue that never belonged there**, and they are now disposed.

| Measure | Chapter | Area | Yes vote means | House | Senate |
| --- | --- | --- | --- | --- | --- |
| SB 95 $300,000 for veteran suicide prevention | 552 | social_programs_and_welfare | **for** | — | 39-11 |
| SB 110 amusement rides need yearly inspection and insurance | 156 | corporate_accountability | **for** | — | 33-16 |
| SB 181 Indian Education for All money tied to reporting | 557 | public_education_quality | **for** | 69-28 | — |
| SB 182 more freedom in the Indian language programme | 558 | public_education_quality | **for** | — | 35-13 |
| SB 276 tighter voter identification, no impediment declaration | 381 | election_integrity | **for** | 57-42 | — |
| SB 457 criminal contempt for ignoring a legislative subpoena | 405 | anti_corruption | **for** | 55-44 | 31-19 |
| SB 492 officials disclose only holdings above 10 percent | 614 | anti_corruption | against | 56-43 | — |
| SB 560 nonprofit hospitals must match their tax break | 627 | corporate_accountability | **for** | 72-28 | — |

## The queue correction

A Montana joint resolution does not become law. Its action trail ends at "Filed
with Secretary of State" — no governor's signature, no chapter number, no statute
touched. Every one of the 58 remaining joint-resolution rolls therefore fails
**filter 2**, the "became law" test, and none should have been carried as
unworked. They are now marked `dropped:filter-2-not-law`.

That removes 58 rolls across 38 resolutions from the queue at a stroke, and it
means the remaining work is smaller than the campaign has been reporting.

## Two measures on legislative power, pointing opposite ways

SB 457 gives a chamber the power to hold someone in criminal contempt for
ignoring its subpoena, punishable by up to $1,000 and up to 12 months in county
jail. That is judged **for** `anti_corruption`, because it makes the
legislature's oversight of others enforceable.

It sits alongside HB 531 from batch-12, which bars courts from reviewing the
legislature's own rules and is judged **against** the same area. The two are
consistent: one strengthens scrutiny the legislature applies outward, the other
removes scrutiny applied to it.

SB 492 runs the other way from SB 457. Officials used to disclose every business
in which they held any interest at all; now only those above 10 percent, or above
1 percent for a publicly traded company, and the same floor applies to reportable
land. Less is disclosed, so it is judged **against**.

## Every roll was checked against Montana's own vote record

All nine imported rolls, and every other floor roll on the measures read, were
compared member by member against
`api.legmt.gov/bills/v1/votes/findByBillId`. **All agree exactly.**

## What was not judged

**SB 147 was deferred, not dropped.** Its only operative content is a repeal of
"Section 55, Chapter 716, Laws of 2023", and nothing in the enrolled text says
what that section did. The title points to a repealed sunset on the Montana
Indian Child Welfare Act, but the archive would not serve the 2023 chapter, so it
is unconfirmed and no record was written. **SB 430** on civil commitment was set
aside for the same reason: it is long, it supersedes the unfunded mandate laws,
and it deserves a full read rather than a judgment from its title.
