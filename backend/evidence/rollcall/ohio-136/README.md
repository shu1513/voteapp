# Ohio 136th General Assembly — phase 3 state pilot

Plan §5 step 3: the first state-legislature import. Local database only;
migrations 251 and 252 are still local-only, so nothing here is promotable
yet.

## Source

The official Ohio LIS API (`search-prod.lis.state.oh.us`, undocumented, no
auth), NOT LegiScan. The plan sketched LegiScan bulk datasets as the
50-state spine with the Ohio API as ground truth; the pilot uses the ground
truth directly because it needs no API key or account, the host is listed
by the record source policy (`state.oh.us` = government), and every fact a
record cites — the per-member `yeas[]`/`nays[]` — is in the cited feed
itself. LegiScan remains the plan of record for the phase-4 rollout, where
most states have no such API.

Pipeline (all new code, PR'd with this directory's first batch):

    rollcall:oh:fetch    per-bill actions feed → legislative_votes rows + per-vote evidence JSON
    rollcall:judge       same judgments.json flow as federal (state entries name jurisdiction + session "136")
    rollcall:oh:resolve  roster snapshot + crosswalk proposals/validation
    rollcall:oh:import   fan-out through the committed crosswalk

All commands need `DATABASE_URL=postgresql://localhost:5432/voteapp` inline;
the worktree has no `backend/.env`.

## What is different from the federal pipeline

- **No roll numbers.** Ohio identifies a vote only as an action on a bill.
  The surrogate `roll_number` is the action's `occurred` timestamp in epoch
  seconds — deterministic from the source, unique per chamber, int4-safe
  until 2038. The fetcher refuses collisions and refuses two kept floor
  votes of one chamber on one bill and day (the per-bill source URL could
  not tell them apart) — both members of a colliding pair are rejected
  before anything stores, and because the feed grows between fetches, a
  stored PENDING kept row for a newly colliding day is parked in place
  (`is_floor_vote` cleared, so it cannot be approved; the normal upsert
  restores it if the collision turns out to be a journal artifact), while
  an APPROVED one is reported for human re-review like a federal
  approved_conflict.
- **Committee-ness comes from the action code, never `cmte_name`.** A
  conference-report FLOOR vote carries the conference committee's name.
  `crpt_*`/`refer_*` are committee; `pass_300`, `msg_507`, `concur_606`,
  `confer_712/713`, `govern_858` are the kept floor classes; refused-concur
  (`msg_506`, `concur_608`) is floor but procedural; any other vote-bearing
  code classifies unknown (`is_floor_vote = null`, surfaced, never queued).
- **Identity = the committed crosswalk file** (`crosswalk.json`), the state
  analog of exact-FEC-id matching. Ohio's lpids are name slugs, so
  `rollcall:oh:resolve` PROPOSES lpid → candidate pairs (last name = token
  tail, first name exact or prefix, unique both directions) and a human
  reviews what enters the file. Nothing auto-attaches on a name at import
  time. Extending the crosswalk later and re-importing a roll adds the
  newly mapped members idempotently.
- **Evidence pins the action element**, not the whole feed response: a
  bill's actions array keeps growing after our fetch (a signing, a veto),
  and hashing the response would flag every approved vote as an
  approved_conflict each time. `source_sha256` = sha256 of the verbatim
  action object.
- **source_url** is the machine feed (`…/legislation/<bill>/actions/`,
  trailing slash canonical — the API 301s to it). The human bill page
  (`legislature.ohio.gov/legislation/136/<bill>`) is display_url/bill_url
  only: its TLS chain omits an intermediate, so the validator cannot fetch
  it (same class of problem the plan records for legislature.mi.gov).
- **Judgment grounding** = the Ohio LSC Final Analysis (the CRS-summary
  analog), fetched from the bill page's Documents tab
  (`legislature.ohio.gov/download?key=<n>`). No judgment is written without
  one. Vehicle-bill hygiene carries over from phase 2 unchanged.

## Files

- `oh-legislators-136.json` — roster snapshot (134 members after dropping
  4 vacant-seat placeholder rows), fetched 2026-08-23.
- `crosswalk.json` — 79 reviewed entries (74 same-district identities, 5
  known cross-chamber candidacies including the Gayle/Nathan Manning seat
  swap). Members without an entry are NOT yet reviewed: the remaining
  active-roster members need a hand sweep (name variants such as
  Mike/Michael Dovilla, members running for non-legislative offices)
  before the full run claims completeness.
- `CODE-FINDINGS.md` — two source changes the data run turned up, recorded
  rather than made. Read this before extending the pipeline.
- `survey/` — the full GA-136 fetch report (963 vote actions over all 1,477
  bills and joint resolutions) and `divided-worklist.tsv`, one row per
  divided kept floor vote with the batch that judged it or the reason it
  stays pending. Raw survey evidence is not committed: each action is
  re-fetchable and pinned by the `source_sha256` on its row, and the
  judged ones are copied into their batch directory.
- `batch-01/` — S.B. 1 (Advance Ohio Higher Education Act): House passage
  59-34 and Senate concurrence 20-11 judged and imported. The Senate's
  initial passage (Feb 12) stays pending under the phase-2 decisive-vote
  rule. One hand retirement: Adam Bird's press-release copy of the same
  vote (record `729b9fe5…`), superseded by his roll-call record
  `686cc780…`. Re-imported after the crosswalk sweep, which added exactly
  the 15 newly mapped members → **89 records / 89 candidates**.
- `batch-02/` — 11 rolls over 9 bills, each written from that bill's LSC
  analysis: S.B. 172 (immigration arrests), S.B. 293 (absentee deadline,
  House passage + Senate concurrence, enacted), H.B. 88 (drug
  trafficking), H.B. 485 (Baby Olivia Act), H.B. 492 (interfering with
  traffic arrests, both chambers, enacted), H.B. 249 (adult cabaret
  performance), H.B. 252 (burglary), H.B. 347 (SHE WINS Act), S.B. 278
  (damages against municipal gun controls) → **579 records**; re-run all
  `unchanged`. Latyna Humphrey's press release condemning S.B. 293 was
  flagged related and KEPT: it reports her public opposition, not how she
  voted, so it is a distinct claim (the phase-1 Grijalva rule).

Totals after batch-02: **668 live `rollcall_import` records across 94
Ohio candidates, 668 area tags, 13 approved roll calls** of 472 queued.

## The judging gate

Same as phase 2: a roll is judged only when the vote was **divided** (the
losing side at least a quarter of the winning side) **and** a research
area fits it without inventing a direction, with the bill's LSC analysis
on file to write from. Of 466 kept floor votes, 66 are divided; 13 are
judged, 8 are appropriations (H.B. 96, H.B. 730 — no research area maps
onto a vote to fund the government), and the rest are pending with their
reason in the worklist.

Ohio-specific hazards found while judging, all caught by reading the LSC
analysis rather than the title:

- **Titles go stale on vehicle bills.** H.B. 472 is listed as "Waive ID,
  birth certificate fees for homeless individuals"; its final analysis is
  a mail-voting photo-ID bill. Phase 2's rule carries over — judge the
  version the analysis on file describes, or leave it pending.
- **Titles mislead about subject.** H.B. 5, the "Repeat Offender Act", is
  a weapons-under-disability bill.
- **"Would have" means it did not take effect.** LSC writes final
  analyses of vetoed or unadopted provisions in the past conditional.

## Next

1. Judge the remaining 31 unjudged divided rolls, batch by batch; the
   analyses are re-fetchable from each bill's Documents tab (the
   `Analysis` section's first link is the newest version).
2. Act on `CODE-FINDINGS.md` — item 1 blocks a voter-ID constitutional
   amendment (S.J.R. 10) from ever being judged.
3. H.B. 184's two same-day House concurrence votes are stored nowhere by
   design; if that bill matters, it needs the action-specific identity
   the surrogate roll number cannot give.
