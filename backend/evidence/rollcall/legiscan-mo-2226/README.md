# Missouri — LegiScan session 2226 (2025 Second Extraordinary Session)

Missouri's September 2025 special session, registered under the compound key **`MO-2226`** because
the state has two sessions in scope at once. Run every script with `--state MO-2226`; every database
row, evidence filename and run id still says `MO`. Local `voteapp` only; production untouched.

The regular session's evidence — the crosswalk this session reuses, and the Missouri hazards that
apply here too — lives at [`../legiscan-mo-2169/`](../legiscan-mo-2169/).

## Why the registry key is not the postal code

`LegiscanStateConfig` pins one `sessionId` per entry, and every consumer resolves its config from the
`--state` CLI flag rather than from a row's jurisdiction. So a second entry keyed `MO-2226`, carrying
`jurisdiction: "MO"`, pins the second session without touching any code path: the two entries cannot
collide, and `legislative_votes` separates them on its `(jurisdiction, chamber, session, roll_number)`
key. The two sessions share one `keptQuestions` / `excludedQuestions` definition so they cannot drift.

One thing the compound key did require: the judge validated a judgment's `jurisdiction` against the
registry's **keys**, which would have accepted `MO-2226` as a jurisdiction and written rows no
importer looks for. It now validates against `LEGISCAN_RECORD_JURISDICTIONS`, the distinct
`jurisdiction` values — which is what that check always meant.

## Dataset and survey

13 bills / 8 roll calls / 194 people. Feed health is perfect: 0 repeated `roll_call_id`s, 0
summary-only rolls, 0 tally mismatches, 0 file errors, and **no `roll_call_id` collides with session
2169's**. House tallies run to 159 of 163 seats, the Senate to 34 of 34.

The vocabulary is a strict subset of the regular session's and needed no new pattern:

| desc | rolls | disposition |
|---|---|---|
| `House: HJRs FOR THIRD READING HCS HJR 3` | 2 | kept (passage) |
| `House: HBs FOR THIRD READING HB 1` | 1 | kept (passage) |
| `Senate: Third Reading` | 2 | kept (passage) |
| `House: HJRs FOR PERFECTION *HCS HJR 3, A.A.` | 2 | excluded (amend-and-engross stage) |
| `House: HBs FOR PERFECTION *HB 1` | 1 | excluded |

Fetch stored 8 rows: 5 kept floor + 3 excluded-question. Nothing surfaced.

## Crosswalk — reused unchanged

`../legiscan-mo-2169/crosswalk.json` covers this session as-is: **people_ids are session-stable**, and
all 194 of this session's people appear in the 2169 snapshot. Validation over all 8 rolls: **matched
664 / unmatched_reviewed 310 / `no_crosswalk` 0 / `out_of_scope` 8 / 0 file errors.** Three crosswalk
entries (20716, 23316, 24149) name members who are not in this session's people file — legislators
who left between May and September — and are reported, not an error.

Fan-out: **house 110-111 matched per roll, senate 4.** The 45-odd House members who voted but do not
match are sitting legislators with no November 2026 candidacy, and the thin Senate reach is the
seven Senate districts that still carry only their August primary rows (see the 2169 README).

## Judging source — one deviation from the regular session

The regular session's recipe holds, with one gap: **the House publishes no Truly Agreed (`T`) summary
for special-session bills.** `documents.house.mo.gov/billtracking/bills254/sumpdf/HB0001T.pdf` and
`HJR0003T.pdf` both answer 200 with a 793-byte error page. Both measures were therefore judged from
the **enrolled text** — `3344H.01T` and `3353H.03T` — with the Perfected (`P`) summary used only as an
index. Special-session documents live under `bills254`, and the bill page is
`house.mo.gov/BillContent.aspx?bill=HB1&year=2025&code=S2&style=new`.

The roll-call PDFs are under `bills254/rollcalls/` and were used exactly as in the regular session:
`006.002` carries the extra header line `PREVIOUS QUESTION`, `006.003` does not.

## Session 2216 is deliberately NOT registered

Missouri's *first* 2025 extraordinary session carries a data trap. Its only divided House rolls are
dated 2025-03-13 with the desc `House: SBs FOR THIRD READING SS#2 SB 4` and tallies 99-44 and 96-44 —
which are the **regular session's** SB 4 (utilities) votes, attached by LegiScan to the special
session's unrelated SB 4 (a housing trust fund bill). The `roll_call_id`s do not collide with 2169's,
so nothing would deduplicate them: importing 2216 would file utility votes under a housing bill.
Everything else there is an appropriation or a senate-only roll.
