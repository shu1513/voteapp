# Oregon 2026 batch-02 — the immigration cluster

Six measures, twelve roll calls, **332 records**. Running total: 625 records
across 63 candidates and 681 tags.

| Measure | Rolls | Area | Yea | Nay |
| --- | --- | --- | --- | --- |
| HB 4079 schools notify families of immigration visits | House 35-22, Senate 18-10 | `immigration` | for | against |
| SB 1538 immigration status a protected class in schools | House 36-21, Senate 23-7 | `immigration` | for | against |
| SB 1570 hospitals plan for police arrivals | House 34-14, Senate 18-11 | `immigration` | for | against |
| SB 1594 immigration model policies written with OIRA | House 33-14, Senate 18-11 | `immigration` | for | against |
| SB 1587 no public data to brokers for immigration use | House 33-14, Senate 18-11 | `data_privacy`, `immigration` | for | —, against |
| HB 4138 officers must be identifiable | House 34-18, Senate 18-10 | `civil_rights` | for | — |

## ⚠ The minority report's staff summary describes a version that lost

This is the session's own version trap, and it is worth stating plainly.

Oregon publishes a staff measure summary for the **minority** committee report
as well as for the majority one. The minority summary describes the amendments
the minority wanted, and on the floor those amendments were **voted down**.
Nothing in the file name says which is which; the flag lives in the
legislature's document index.

**SB 1594 is the clear case.** Its minority summary says the measure "directs
state and local law enforcement to cooperate with any federal immigration
authority in carrying out any action concerning a person who is a convicted
felon under Oregon law." The word "felon" does not appear anywhere in the
enrolled Act. The Senate rejected the minority report 11-18, and what became
law is only the consultation and model-policy provisions.

Four measures in this session were first screened against a minority summary —
HB 4111, HB 4116, SB 1594 and SB 1598. **No imported record was affected**,
because every description is written from the enrolled Act and the summary is
used only as an index. HB 4111 was already imported in batch-01, and its
description matches the enrolled text. The screening list has been rebuilt to
exclude minority summaries.

## Version check, per roll

- **HB 4138** and **SB 1587**, **SB 1570** and **SB 1594** each have a
  concurrence as one chamber's final vote, and in every case it is the
  concurrence that is recorded, because that is the vote on the text that
  became law. SB 1587, SB 1570 and SB 1594 all had an earlier divided Senate
  roll on the A-Engrossed text; those are not used.
- HB 4079 and SB 1538 each have a single floor roll per chamber.

## Why these stances

Five measures carry an explicit **nay: against** under `immigration`. Each Act
is about immigration from beginning to end, so a no vote is a genuine position
within that area rather than an objection about something else.

**SB 1587 carries two labels** because it has two policy strands. The
mechanism is a data-privacy rule about what government may hand to data
brokers, and the purpose is immigration enforcement. The data-privacy label
takes `nay: null` — an objection there is usually about administrative burden,
which is not a data-privacy position.

**HB 4138 takes `civil_rights` with `nay: null`.** The Act is police
identifiability and limits on assisting other agencies. The strongest argument
against it is officer safety, which is a `public_safety_and_crime_control`
argument rather than a civil-rights one, so a no vote gets a record and no
stance tag. The Act was not labelled `immigration`: its text names federal and
out-of-state law enforcement generally and never immigration enforcement.

## Reading level

First drafts ran grade 9.2 to 13.3 and were rewritten before importing.
Final: **median grade 7.6, worst 9.3**, longest sentence 25 words.

## Verification

Dry run and real run agree — 22 approved rolls, 332 inserts, 0 notifications —
and a third run reports all 625 records `unchanged`. Database holds 625 rows
across 63 candidates, matching the sum of both batches' inserts.
