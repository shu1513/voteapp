# Retention titles repaired — 2026-09-13

The app recognises a judicial retention race by its `official_ballot_title`
(`isJudicialRetentionTitle`). A sweep of every one-candidate judicial race on
the 2026-11-03 ballot (1,434 rows) found two retention seats stored under a
bare office name, so they rendered as ordinary one-candidate races with a
"Make my pick" button and a wrong vote-power rating. Both were retitled to the
question the ballot prints (LOCAL DB; prod = same two UPDATEs after promote).

| election id | old title | new title | source |
| --- | --- | --- | --- |
| `7126fcd3-745a-4e1b-8c6b-4665e3ca5a7c` (Alaska, statewide) | State Supreme Court Justice | Shall Jude Pate be retained as justice of the supreme court for ten years? | https://www.ajc.state.ak.us/retention/docs/upcoming-retention-elections.pdf (Pate listed under "Judges Eligible to Stand for Retention Election in 2026", Supreme Court, appointed 1/20/23); statutory wording AS 15.35 |
| `d4d4730c-2b0f-4ef4-b5e8-db1c5a5eabb7` (Sarpy County, NE) | Judge of the Separate Juvenile Court District 2 - Sarpy County | Shall Judge Sarah M. Moore be retained in office? | https://sos.nebraska.gov/sites/default/files/doc/elections/2026/Final_List_of_Judges_Who_Filed_for_Retention_List_8.3.26.pdf (Ballot Question column, verbatim) |

`official_ballot_title_key` was recomputed with `normalizeElectionTitleKey`
(the upsert identity is district + key + date).

Also found and handled in code the same day: California prints one Court of
Appeal question without the office word ("Shall DAVID B. SAPP be elected to
the office for the term provided by law?"); the matcher now accepts the
prescribed phrase on its own.

Not a retention issue but noted: the Nebraska source lists ~40 judges filing
for retention statewide (Court of Appeals, Workers' Compensation Court,
district and county courts); the local DB holds only the Sarpy juvenile seat.
The audit queries now live in the manual-research skill
(`references/elections.md` → Retention Title Audit).
