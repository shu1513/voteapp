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

`labels-<candidate_id>.json` is the payload sent to the wrapper. `batch<N>-output.json` holds the full proposal, including the skip reasons and the retire-candidate flags.

A record whose candidate has no office election at all cannot go through the wrapper — it takes `--election-date` and resolves the office allowlist from that election. Those records stay untagged and are listed per state below. AK has 13 across six candidates, FL has 2, AZ has 6: Rasmussen, Schuerch, Church, Gettys, Sumner, Hnilicka.

## Label rules tightened during the run

- `personal_income_tax_reduction` covers personal income tax only. Sales, property, business and production tax records do not belong there.
- A no vote on a funding bill is not a spending cut unless the record says the member wanted less money.
- A single court ruling, or a lawsuit about the candidate's own eligibility, does not carry a stance on a whole area.

Retire candidates are flagged only. Nothing is retired without a separate decision.
