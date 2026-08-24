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
  not tell them apart).
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
- `batch-01/` — S.B. 1 (Advance Ohio Higher Education Act): House passage
  59-34 and Senate concurrence 20-11 judged and imported → **74 records /
  74 candidates / 74 tags**; re-run all `unchanged`. The Senate's initial
  passage (Feb 12) stays pending under the phase-2 decisive-vote rule.
  One hand retirement: Adam Bird's press-release copy of the same vote
  (record `729b9fe5…`), superseded by his roll-call record `686cc780…`.

## Next

1. Survey: `rollcall:oh:fetch --ga 136 --all-kept` (~1,477 hb/sb/hjr/sjr
   bills, ~8 minutes at the default delay) into `survey/`, then read the
   report's `unknownActions` before trusting the classifier vocabulary.
2. Crosswalk hand sweep for the unreviewed active members.
3. Judge divided floor votes batch by batch — same gate as phase 2
   (divided = loser ≥ ¼ of winner, a research area that fits without
   inventing a direction, LSC analysis on file).
