# Texas 89R (LegiScan session 2160) — member crosswalk

The identity layer for the Texas roll-call import. LegiScan identifies a
member by `people_id`; our candidates have their own ids and share nothing
with it. `crosswalk.json` is the reviewed mapping between the two, and the
importer attaches votes to a candidate **only** where this file says so.
Nothing ever auto-attaches on a name.

## Files

| file | what it is |
|---|---|
| `crosswalk.json` | the reviewed mapping — 181 entries, one per session member |
| `legiscan-people-tx-2160.json` | the session's people list, verbatim from the dataset |
| `resolve-report.json` | the proposal pass (`rollcall:legiscan:resolve` with no `--crosswalk-file`) |

The dataset itself and the 6,824 per-roll evidence files live outside the
repo at `/Users/shu/legiscan-data/` (78 MB); the full-resolution report is
88 MB and is deliberately not committed. Re-download the dataset with
`getDatasetRaw&id=2160` if that directory is lost.

## What the crosswalk holds

181 entries = every member of the session.

- **136 mapped** to a candidate — house 123 of 150, senate 13 of 31.
- **45 explicitly null** — reviewed, no Nov-2026 candidacy on file. `null`
  means *reviewed and no candidate*, which is why the resolve run reports
  them as `unmatched_reviewed` rather than `no_crosswalk`.

The senate is thin on purpose: Texas senate terms are staggered, so only 14
of 31 districts are on the Nov-2026 ballot, and one of those (SD-22) has a
retiring incumbent.

## How it was built

`rollcall:legiscan:resolve` proposed 122 pairs by strict name matching
against the 313-candidate Nov-2026 state-legislative pool. All 122 were
accepted; 14 more were added by hand; the remaining 45 were set to null.

### The 122 proposals

116 matched on first *and* last name with the member's seat agreeing with
the candidacy's seat. The other six were checked individually:

| member | proposed candidate | why it needed eyes |
|---|---|---|
| William Metcalf (HD-016) | Will Metcalf | first-name prefix |
| Matthew Shaheen (HD-066) | Matt Shaheen | first-name prefix |
| Peter Flores (SD-024) | Pete Flores | first-name prefix |
| Brad Buckley (HD-054) | Brad Buckley | LegiScan `first_name` is "Bradley" |
| Dennis Paul (HD-129) | Dennis Paul → **SD-011** | seat disagrees |
| David Cook (HD-096) | David Cook → **SD-022** | seat disagrees |

Both seat disagreements are the documented non-veto case: a sitting
representative running for a senate seat. Confirmed against the candidate
rows — David Cook's `current_office` is "Texas State Representative,
District 96", matching LegiScan's HD-096 exactly, and Dennis Paul's row is
a sitting Texas representative. A `seatAgrees: false` is a flag to look
twice, never a rejection.

### The 14 hand additions

The proposer is deliberately conservative, so two classes of real member
fall outside it. Every one below was confirmed against the candidate row
before being added.

**Name variants the matcher cannot reach** (its rules require the last name
to be the tail of the candidate's tokens, and first names to agree exactly
or by prefix):

| member | LegiScan field | candidate |
|---|---|---|
| Eugene Wu (HD-137) | `first_name` "Eugene" | Gene Wu |
| Mike Schofield (HD-132) | `first_name` "Michael" | Mike Schofield |
| Terry Meza (HD-105) | `first_name` "Thresa" | Terry Meza |
| AJ Louderback (HD-030) | `first_name` "AJ" | A.J. Louderback |
| Ana-Maria Rodriguez Ramos (HD-102) | `last_name` "Rodriguez Ramos" | Ana-Maria Ramos |
| Claudia Ordaz Perez (HD-079) | `last_name` "Ordaz Perez" | Claudia Ordaz |
| Christian Manuel (HD-022) | `last_name` "Hayes" | Christian Manuel |

Note the shapes: a legal first name that is not a prefix of the common one
("Thresa"/"Terry", "Eugene"/"Gene"), a punctuated candidate name whose
first token is a single letter ("A.J." tokenizes to `a`, and the prefix
rule requires both sides to be at least two characters), and a dropped or
extra surname in a two-part Hispanic surname. Expect all three again in the
next state.

**Sitting legislators running statewide in Nov 2026.** The proposer's pool
is state-legislative candidacies only, so a member seeking another office is
invisible to it — the module comment calls this out and leaves it to the
reviewer:

| member | Nov-2026 candidacy |
|---|---|
| James Talarico (HD-050) | statewide |
| Gina Hinojosa (HD-049) | statewide |
| Vikki Goodwin (HD-047) | Lieutenant Governor |
| Jon Rosenthal (HD-135) | Railroad Commissioner |
| Mayes Middleton (SD-011) | statewide |
| Nathan Johnson (SD-016) | statewide |
| Sarah Eckhardt (SD-014) | Comptroller |

These are the highest-value entries in the file: they are the members whose
legislative record voters will be weighing in a race with no district.

### The 45 nulls

23 sit in a seat with no Nov-2026 candidates on file at all (mostly senate
districts not up this cycle); 22 sit in a seat that does have candidates on
file, none of whom is the incumbent — that is, the member is not seeking
re-election. Each entry carries which case it is in its `note`.

## Validation

Resolving all 6,824 stored roll calls through the committed crosswalk and
the committed people snapshot:

```
files 6824 | fileErrors 0
matched 491,131 | unmatched_reviewed 141,631
no_crosswalk 0 | out_of_scope 0
```

- **`no_crosswalk` is 0** — every people_id that cast a yea or nay anywhere
  in the session is in the file. There is no member the importer could meet
  and not recognize.
- **`out_of_scope` is 0** — every mapped candidate is on a Nov-2026-or-later
  office election.
- 194 roll calls match nobody. All 194 have between one and five recorded
  members — lone dissents and similar procedural fragments, not a coverage
  gap. No roll call with more than five members matches nobody.
- Median matched members per house roll call is 114 (max 122); the senate is
  13. That is the fan-out size: judging one divided house vote writes on the
  order of 114 candidate records.

The run also proves the committed artifacts stand alone — it read the
people snapshot from this directory, not the dataset.

## Gotchas for the next state

- `role` can disagree with `district`. Phil King is `role: "Rep"` with
  `district: "SD-010"`; he is a senator. Seat logic reads `district`, never
  `role` — keep it that way.
- `first_name` is the legal name, `name` is the working one. Match on
  `name` at your peril, but do not assume `first_name` is a prefix of it.
- LegiScan pads some name fields with stray spaces; the parser trims.
- Proposals require uniqueness in **both** directions, so a member with a
  common surname against a field of challengers proposes nothing. That is
  the intended failure mode — it produces work for a reviewer, never a
  wrong attachment.
