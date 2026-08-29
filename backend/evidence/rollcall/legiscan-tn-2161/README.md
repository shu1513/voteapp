# Tennessee 114th General Assembly — LegiScan roll calls (session 2161)

Phase-4 LegiScan state #3, after Texas (2160) and Georgia (2167). Data run on the
local `voteapp` database only; **production was never touched.**

## Dataset

LegiScan bulk dataset `2161` (TN 114th General Assembly), downloaded 2026-08-27:
9,159 bills / 15,468 roll calls / 136 people. Both regular sessions (2025 and 2026)
ship in one dataset. The extracted dataset and the 8,648 evidence JSONs live outside
the repo at `/Users/shu/legiscan-data/tn-2161/` and `/Users/shu/legiscan-data/tn-2161-evidence/`
(Texas precedent — the repo keeps only the curated subset). The 14 roll JSONs the
batch-01 import consumed ARE committed, flat in `batch-01/` (also the Texas layout),
so the judged votes stay independently verifiable and re-importable after the
crosswalk grows.

## Data quality

- **0 duplicate `roll_call_id`s** — the Texas re-issue hazard (9.4% of TX rolls) is a
  verified no-op in Tennessee. Only 6 identity-collapse groups exist in the raw file;
  the fetcher collapsed 3 within the kept set.
- 0 summary-only tallies (every roll carries its member list), 0 rows where
  `total != yea + nay + nv + absent`.
- One data anomaly: roll `1550782` (HB 486, senate, 33-0) is dated **2023-03-06** in
  LegiScan, outside the 114th GA. It is not divided and is never judged.

## Vocabulary (see `survey/` and the config comment in `legiscanStateConfigs.ts`)

Tennessee is the first state whose feed labels floor votes: floor descs start
`FLOOR VOTE:`, committee descs start with the committee's name. The House prints the
calendar and every preceding motion into one desc, so the trailing
`PASSAGE ON THIRD CONSIDERATION` is calendar context, not the question — 296 amendment
rolls and 106 previous-question rolls carry it too and are excluded. Each bill's own
history is the ground truth (`Passed H., as am., Ayes 82, Nays 8`).

Fetch stored **8,648 rows**: 8,174 floor (house 4,015 / senate 4,159), 472
excluded-question, 2 surfaced; 743 resolution-typed measures and 6,074 committee votes
were rejected pre-queue, 3 duplicate-identity rolls dropped.

**316 divided floor votes** (`LEAST(yea,nay) >= GREATEST(yea,nay)/4`): house 260 /
senate 56. **289 of them on measures that became law, across 239 measures** —
`survey/divided-enacted-worklist.tsv` is the full list, with the batch-01 picks marked.

## Crosswalk

`crosswalk.json` — 136 entries: **99 mapped, 37 explicit null.** The current
proposer made 90 unique suggestions; every suggestion was manually accepted after
checking the physical House/Senate district (all `seatAgrees: true`, never the
member's role). The remaining nine reviewed mappings predate this pass, including:

- **Rusty Crowe (SD-003)** — the candidate files under his legal first name,
  `Dewey "Rusty" Crowe`, and LegiScan's `first_name` is the nickname, so neither
  direction is a prefix of the other.
- **Monty Fritts (HD-032)** — a sitting representative running **statewide for
  Governor**, outside the state-legislature candidate pool the proposer searches
  (the Texas Talarico class).

Resolution over all 8,648 rolls: matched 405,601 / unmatched_reviewed 134,755 /
**no_crosswalk 0 / out_of_scope 0**, 0 file errors. The full `resolve-report.json`
is ~100 MB and stays out of the repo.

**Scope note:** the crosswalk is built with `--scope-from 2026-08-01`, not the
pipeline default of 2026-11-01. Tennessee's state primary is **August 6, 2026**. After
the TN House roster repair, the August scope gives a local candidate pool of 214 and
the 99 mappings above. This is the user's explicit call.

**Fan-out is roster-bound and idempotent.** The 14 curated batch-01 rolls first
covered 199 candidate records. After the House roster repair and crosswalk review, the
same batch inserted 627 additional local records while retaining those 199; a final
dry re-run planned **826 unchanged** records.

## Judging sources (probed before use)

- **Fiscal Review Committee fiscal note** — the Ohio-LSC / Georgia-HBRO analog:
  official, nonpartisan, opens with `SUMMARY OF BILL`, and carries **no sponsor
  statement of intent**, so the Texas advocacy-preamble hazard does not recur. The
  URL uses the **House companion number**: `https://capitol.tn.gov/Bills/114/Fiscal/HB0923.pdf`.
  Amended versions are separate `FM####.pdf` memos headed `SUMMARY OF BILL AS AMENDED
  (<filing numbers>)`.
- **Public chapter (the enrolled act)**:
  `https://publications.tnsosfiles.com/acts/114/pub/pc0458.pdf` — answers 403 to a bare
  curl and needs a full browser User-Agent. These are scans: `pdftotext` renders `DEI`
  as `DEi` and `(B)` as `(8)`.
- **Bill page** `https://wapp.capitol.tn.gov/apps/BillInfo/Default?BillNumber=SB1084&ga=114`
  (follow the redirect) is server-rendered and carries the caption, the full history
  with `Ayes/Nays` per vote, every amendment marked adopted / tabled / withdrawn, and
  links to each amendment PDF.
- **Amendment PDFs** `https://capitol.tn.gov/Bills/114/Amend/SA0330.pdf` — delete-all
  substitutes are the norm; the six-digit **filing number** printed at the foot of the
  amendment is what the `FM####` memo names, which is how a fiscal memo is matched to
  the exact text a chamber voted.
