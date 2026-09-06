# Research-area label backfill for existing candidate records

Live candidate records with no row in `candidate_record_area_tags`, labeled by hand (no AI provider calls).

## Triage, 2026-09-06

Untagged live records by origin: `rollcall_import` 47,417, `manual` 3,530, NULL 1,907.

The `rollcall_import` rows are untagged **by design**. `rollCallFanOut.labelsForSide()` gives yea voters every label in the roll call's judgment, but gives nay voters a stance area only where the judgment explicitly stated a `nay` stance — an unstated nay side is silence, not the opposite claim. 47,415 of the 47,417 untagged rows are nay-side records.

Of those nay-side rows, 45,454 sit on judgments that explicitly wrote `nay: null`, and 2,250 sit on judgments made before the `nay` key existed, which default to null.

Two rows are a real gap:

- `60748f6d-5401-4c05-9875-01772fd4e02f` — Chuck Edwards (NC), US House roll 285, judgment label `general`; `updated_at > created_at`, so the tag looks hand-removed.
- `db4f5691-1e13-4853-b70e-97ae8188e39b` — Fredrick Love (AR), AR HB1752, judgment `reduce_wealth_gap: yea for`; never tagged.

## Import path

`npm run ai:candidate-records:relabel -- --election-date <date> --candidate-id <uuid> --labels-file <file> --out-file <jsonl>`

File mode makes **no AI call** (`ai_calls=0` in every run summary). It is additive: it inserts missing tags and never edits or deletes an existing one. It accepts stance-bearing areas only, so a record whose only honest label is `general` or `integrity_and_ethics` cannot be tagged and is logged as skipped instead.

## Per state

| state | records | labeled | skipped | retire candidates |
|---|---|---|---|---|
| MI | 576 | 351 | 184 | 41 |
| WA | 534 | 338 | 164 | 32 |
| AK | 390 | 237 | 124 | 16 |
| AL | 382 | 201 | 142 | 39 |
| FL | 271 | 124 | 71 | 74 |
| AZ | 264 | 124 | 100 | 34 |
| NC | 231 | 114 | 74 | 42 |
| AR | 195 | 76 | 108 | 11 |
| MD | 190 | 94 | 70 | 26 |
| IL | 174 | 92 | 53 | 28 |
| TN | 168 | 77 | 59 | 32 |
| GA | 165 | 67 | 84 | 14 |
| MO | 129 | 58 | 58 | 13 |
| TX | 119 | 52 | 44 | 23 |
| IN | 106 | 54 | 39 | 13 |
| CT | 97 | 45 | 22 | 30 |
| CA | 96 | 67 | 22 | 7 |
| MN | 95 | 37 | 33 | 25 |
| NV | 91 | 48 | 38 | 5 |
| KS | 91 | 39 | 25 | 5 |
| OH | 85 | 48 | 22 | 15 |
| KY | 81 | 43 | 21 | 17 |
| ME | 80 | 42 | 27 | 11 |
| NY | 53 | 23 | 20 | 10 |
| PA | 49 | 27 | 14 | 7 |
| MA | 47 | 16 | 10 | 21 |
| OR | 39 | 23 | 13 | 3 |
| WY | 38 | 13 | 14 | 11 |
| LA | 37 | 20 | 8 | 9 |
| SD | 34 | 25 | 9 | 0 |
| RI | 27 | 15 | 5 | 7 |
| UT | 26 | 10 | 11 | 5 |
| 17 smaller states (NM MT VA ND SC NE IA ID DE NJ OK NH VT WI HI WV DC) | 213 | 112 | 58 | 41 |
| **TOTAL** | **5,437** | **2866** | **1847** | **676** |
| CO | 264 | 154 | 101 | 9 |

`labels-<candidate_id>.json` is the payload sent to the wrapper. `batch<N>-output.json` holds the full proposal, including the skip reasons and the retire-candidate flags.

A record whose candidate has no office election at all cannot go through the wrapper — it takes `--election-date` and resolves the office allowlist from that election. Those records stay untagged and are listed per state below. AK has 13 across six candidates, FL has 2, AZ has 6: Rasmussen, Schuerch, Church, Gettys, Sumner, Hnilicka.

## Roll-call nay backfill, 2026-09-06

134 approved judgments predated the `nay` field, so nay voters on them got no tag. Each stance label was decided again: what does a NO vote on THIS measure actually evidence? 14 labels got a real stance, 120 rows were confirmed null. Applied through `rollcall:judge`, then the affected roll calls were re-imported so the tags come from the pipeline. Evidence: `backend/evidence/rollcall/nay-backfill-2026-09-06/`.

A separate, larger gap turned up while verifying: 294 records whose judgment DID state a nay stance had never had it applied (GA 170, ME 49, TX 44, US 118-1 roll 705 31). Re-importing those roll calls closed all of them.

Re-importing the federal evidence also INSERTS records, for approved roll calls
whose members are linked now but were not at first import. Those inserts were
reviewed and approved, and all 27 federal evidence directories were then run
(backfill-118-117, expansion-119-1, pilot-119-1, refetch-2026-08-29): 456 roll
calls, 6,609 records inserted, 5,291 of them tagged immediately. The rest are
nay-side rows whose judgment gives the no side no tag.

About 42 hand-written rows were absorbed in the process: when a live
hand-written record cites the same roll call, the importer rewrites it in place
so one vote is not told twice. That is the importer's dedupe, not data loss.

A hand-removed tag on a roll-call record does not survive a re-import either.
One record (Chuck Edwards, US House roll 285) had its `general` tag removed by
hand and got it back, because tags are re-derived from the roll call's own
labels every time.

After this pass there is no roll-call record left whose judgment gives it a tag
it does not have.

Do not hand-tag a `rollcall_import` record. `syncRollCallRecordTags` makes a record's tags exactly the roll call's labels for its side, so anything added by hand is deleted on the next import of that roll call. Fix the judgment instead.

## Web-research pass over the vague descriptions

628 records had been skipped for one reason: the description says a law was
"amended", "revised" or "changed" without saying which way. Eight agents
researched what each bill actually did — the record's own source first, then
the state legislature, LegiScan, Ballotpedia or news, escalating to a browser
when a site blocked plain fetching. 262 of 626 (41%) converted into real
labels. Rules and per-record findings are in `RESEARCHED-VAGUE/`; every label
and skip carries a `found` note saying what the bill did and where that came
from.

What the remaining 364 are, in order of size: bills that really are direction-
free once read (fee schedules, definition swaps, renamings, sunset extensions,
board membership counts); "voted against X" with no stated alternative; bills
that genuinely cut both ways; and a small tail whose text is simply not online
any more.

The research also surfaced defects no label pass would have caught:

- Maryland HB0094: the stored description states the opposite of what the bill
  did — it repealed inmate farm-labor authority.
- Record `370abb0e`: cites Maine LD 274 as an emergency-services bill; LD 274
  of the 131st Legislature is about municipal firefighting water costs.
- Arkansas Act 715 is titled as strengthening nursing-home staffing but removed
  the penalties for understaffing.
- Georgia HB 844 and HB 1274 never became law, which their descriptions omit.

Fixing those needs a description repair, which a label pass is not allowed to do.

## Label rules tightened during the run

- `personal_income_tax_reduction` covers personal income tax only. Sales, property, business and production tax records do not belong there.
- A no vote on a funding bill is not a spending cut unless the record says the member wanted less money.
- A single court ruling, or a lawsuit about the candidate's own eligibility, does not carry a stance on a whole area.

## Retirements applied 2026-09-06

The 676 retire candidates were approved and soft-retired with
`manual:records:retire --apply`. The file is `retirements-2026-09-06.json`.
Soft retirement keeps the row, its area tags and its notification history, and
keeps the identity slot so a later sweep folds into the tombstone instead of
resurrecting the claim. Reverse one with `--unretire`.

## Ohio SB 1: one bill, two answers, resolved

The House vote (roll 1742433648) and the Senate concurrence (roll 1743028529)
are the same bill. Its content is three separate restrictions: a ban on
university DEI offices, a ban on full-time faculty strikes, and post-tenure
reviews. Only the first touches civil rights.

The Senate judgment said a no vote evidences `civil_rights: for`; the House
judgment said it evidences nothing. A no vote here cannot be pinned to any one
of the three provisions, so reading it as a civil-rights position would render
a member who objected to the strike ban as "Supports Civil Rights". That is the
inversion `labelsForSide` exists to prevent, and the "the whole bill is a
repeal" exception does not apply to a three-part bill.

The Senate judgment was corrected to `nay: null` and the roll call re-imported.
Both chambers now read the same way: yes-voters tagged `civil_rights: against`,
no-voters untagged.
