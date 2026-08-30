# Connecticut roll-call import — LegiScan session 2174

Phase-4 state #7 (after TX, FL, GA, IL, TN, CA). Plan: `docs/plans/roll-call-vote-import.md` §5.
Dataset: LegiScan bulk **session 2174, "2025 General Assembly"** (CT `state_id` = 7), downloaded
2026-08-29 — 4,073 bills / 2,625 roll calls / 211 people.

Connecticut files **one dataset per calendar year**, unlike the two-year assemblies of GA / IL / TN.
2174 therefore also carries the special sessions that ran under the 2025 session, including the
**November 2025 housing session** (HB 8002, HB 8003, both voted 2025-11-12/13). The 2026 session is a
separate dataset (**2244**), never surveyed here.

## Feed shape

| | |
|---|---|
| roll calls | 2,625 |
| joint-committee (`chamber: "J"`) | 1,774 — rejected pre-queue, see below |
| floor rolls | 851 (House 413, Senate 438) |
| stored by fetch | 842 (768 floor / 2 surfaced / 72 excluded-question) |
| excluded-measure rolls | 9 (type `R` resolutions) |
| duplicate `roll_call_id`s | **0** (the TX 9.4% fix is a verified no-op again) |
| summary-only rolls (no member list) | **0** |
| tally violations | **0** |
| dates | 2025-01-08 … 2025-11-13 |

The dataset holds **floor votes and joint-committee votes only** — there is no third tier.

### Connecticut is the joint-committee legislature

CT standing committees seat members of BOTH chambers, so a committee tally belongs to no chamber and
LegiScan prints `chamber: "J"` (every one of the 1,774 carries `chamber_id` 108 and a
`… Vote Tally Sheet (Joint Favorable…)` desc; the largest seats 54 against a 151-seat House).
`parseLegiscanRollCall` rightly refused all of them, which turned two thirds of the feed into parse
errors. Fixed in the config PR: `isLegiscanCommitteeChamberRollCall` rejects them pre-queue on the
chamber code — counted, never stored. `parseLegiscanRollCall` still throws on `J`, and a test pins that.

## ⚠ The Senate desc does not name the question

All 438 Senate rolls read `Senate Roll Call Vote <n>` and nothing else — passage and floor amendment
alike. HB 7042 alone carries **18 rejected Senate amendments, every one 11-25 and therefore
"divided"**. The `questionClass` stored on a CT Senate row is this pipeline's default, not
Connecticut's claim (the Florida caveat recurring).

The **House** does name its question: bare = passage, ` AS AMENDED`, ` CONSENT CALENDAR`,
` EMERGENCY CERTIFICATION`, and ` HOUSE AMD <letter>` = a vote on that amendment. The amendment
suffix can follow either of the others, and the question is always the amendment:
`House Roll Call Vote 225 AS AMENDED HOUSE AMD E` is SB 7's 48-99 rejection of Amendment Schedule E.

**Ground truth = the bill-status page's ordered action trail**
(`https://www.cga.ct.gov/asp/cgabillstatus/cgabillstatus.asp?selBillType=Bill&bill_num=<BILL>&which_year=2025`).
Its chamber actions run in the same order as that chamber's printed vote numbers, one-to-one — proved
on HB 7042, whose Senate rolls 266…283 are amendments A…R in order and roll 284 is
`Senate Passed as Amended by House Amendment Schedule A`. **Batch selection must read that trail for
every Senate roll it keeps.**

The feed's `passed` flag narrows the problem but does NOT solve it: 81 Senate rolls are `passed:0`
(failed amendments), and **17 bills carry two `passed:1` Senate rolls** because an ADOPTED amendment
also passes. SB 3 is the proof in the wild — Senate roll 182 (adopt Amendment A) and roll 184
(passage) are **both 25-10 on the same day**. Selecting on "divided and passed" alone would have
imported the amendment vote as if it were the bill.

The rule that works, applied to every roll in batch-01 and verified against the trail: **the decisive
passage roll is the LAST `passed:1` roll of that chamber by printed vote number.** It survives the
`Senate Passed → Senate Reconsidered → Senate Passed` shape too (HB 7066 rolls 37 and 38, both 25-9;
38 is the decisive one).

The state's own vote-record PDF (`.../VOTE/<H|S>/PDF/2025<H|S>V-<nnnnn>-R00<BILL>-<H|S>V.PDF`, whose
`Sequence Number` equals the printed vote number) gives the tally and the roll — **but not the
question**, so unlike Florida it cannot settle this on its own.

## Other measured facts

- **Consent calendars are not Georgia's en-bloc calendars.** No roll call in the dataset is attached
  to more than one bill (checked over all 2,625). All 29 `CONSENT CALENDAR` rolls are on joint
  resolutions and none is divided.
- **`roll_call_id` order is not time order here.** Within a same-day Senate batch LegiScan issues ids
  in reverse of the printed vote numbers (SB 3: roll 1572265 = vote 182, roll 1572263 = vote 184).
  Harmless for the fetcher (its identity collapse found 0 duplicates in CT), but order by the printed
  vote number, never by id.
- Five Senate rolls list only the members present (21, 21, 22, 25, 27 of 36); the two at 21 fall under
  the 0.6 floor ratio and surface.
- The people file carries **24 committee pseudo-persons** (`role: "Jnt"`, e.g. "Aging Committee") beside
  the 187 real members. The parser already ignores them; the crosswalk covers the 187 only.

## Yield

177 divided floor rolls (House 53 / Senate 124). On measures that became law, **50 divided decisive
passage rolls across 35 measures** — after the Senate-amendment rolls are removed. Dispositions for all
50 are in `survey/divided-enacted-worklist.tsv`.

## Crosswalk

**187 entries = 142 proposed (all accepted) + 17 hand-added + 1 cross-office hand-add + 27 explicit null.**
Validation over all 842 stored rolls: **matched 63,598 / unmatched_reviewed 10,635 / out_of_scope 409 /
no_crosswalk 0**, 0 file errors.

**Fan-out: House median 126 candidates per roll, Senate 29** (TX 114/13, GA 149/42, IL 92/33, CA 21/11).
Both chambers are fully up in Nov 2026 (House 151, Senate 36, all two-year terms), and our rosters cover
141 of 151 House districts and 33 of 36 Senate districts.

### The hand-add classes the proposer cannot reach

**LegiScan CT carries a `nickname` field the proposer does not read, and CT members are rostered under
the nickname while `first_name` holds the legal name.** That is 17 of the 18 hand-adds:

- legal first name vs working name — Christine/**Cara** Pavalock-D'Amato, Lucia/**Lucy** Dathan,
  Kathleen/**Kathy** Kennedy, Emmanuel/**Manny** Sanchez, Katherine/**Tina** Courpas,
  Mary/**Renee** LaMark Muir, Michael/**MJ** Shannon;
- not-a-prefix pairs, the Joe/Joseph gotcha recurring — **Joseph/Joe** Gresko, **Joseph/Joe** Canino,
  **Robert/Bob** Duff, **Stephen/Steve** Meskers, **Stephen/Steve** Weir, **Martin/Marty** Foncello, and
  twice **Nicholas/Nick** (Gauthier, Menapace). `nick` is not a string prefix of `nicholas` — the fourth
  letter differs (n-i-c-**k** vs n-i-c-**h**);
- multi-part surnames our roster shortens — Cristin **McCarthy Vahey** → Cristin Vahey, Heather **Bond
  Somers** → Heather Somers.

The 18th is the cross-office class: **Jillian Gilchrest**, sitting HD-018 representative, running for the
U.S. House in CD-1 (`current_office` confirms the seat), so she is outside the state-legislative pool the
proposer draws from.

**⚠ Gilchrest is the one deliberate `out_of_scope`** (all 409 of them). Her only 2026 candidacy in our DB
is the August primary, and the importer's scope gate is Nov-2026-or-later, so she is mapped but receives
no records until a general-election row exists. Identity is a fact; scope is derived from the roster, and
the mapping self-heals on a re-import once that row lands. Every other state reported `out_of_scope: 0`.

One proposal carried `seatAgrees: false` and was accepted after checking: **Alphonse Paolillo**, sitting
HD-097 representative, is running for SD-011 (Martin Looney's seat) and has no other 2026 candidacy.

One surname+seat pair was **rejected**: LegiScan's Robert "Bobby" Sanchez (HD-025) is not the roster's
**Iris Sanchez** (HD-025) — different people. He is an explicit null.

## Judging source ⭐

**The Office of Legislative Research (OLR) — the best source since the Ohio LSC, and better than Texas.**
Official, nonpartisan, section-by-section, and with **no sponsor statement of intent anywhere**, so the
Texas advocacy-preamble hazard does not recur.

- **Public Act Summary** — `https://www.cga.ct.gov/2025/SUM/PDF/2025SUM<nnnnn>-R<nn><TYPE>-<BILL>-SUM.PDF`.
  Describes what became law, marks vetoed sections ("but these sections were vetoed"), and opens with a
  table of contents that is an index, not a substitute for the body.
- **Bill Analysis** — `https://www.cga.ct.gov/2025/BA/PDF/2025<BILL>-R<nn>-BA.PDF`, and it is
  **version-specific**: the `R01` analysis of HB 7042 is headed `sHB 7042 (as amended by House "A")`.
  Connecticut builds the version check into the document, which no other state in this campaign does.
- Enrolled act text — `https://www.cga.ct.gov/2025/ACT/PA/PDF/2025PA-<nnnnn>-R00<BILL>-PA.PDF`.

**TLS:** `cga.ct.gov` omits its intermediate certificate (the Ohio / Illinois pattern). Fetch the GoDaddy
G2 intermediate from the leaf's AIA URI (`http://certificates.godaddy.com/repository/gdig2.crt`), append
it to a CA bundle, and pass `--cacert`. Never disable verification.

## Layout

- `crosswalk.json`, `legiscan-people-ct-2174.json` — the identity pair.
- `survey/` — the desc histogram, the fetch ledger for all 842 stored rolls, the crosswalk validation
  report (its 842 per-roll resolutions are omitted; the full report is 12 MB), and
  `divided-enacted-worklist.tsv`, one row per divided decisive passage roll with its disposition.
- `batch-01/` — PLAN.md, JUDGING.md, rolls.json, judgments.json, the 17 roll evidence JSONs, and the
  import ledgers.

The dataset itself and all 842 evidence JSONs live OUTSIDE the repo at `/Users/shu/legiscan-data/ct-2174*`.

**Local `voteapp` only. Prod holds no Connecticut roll-call records.**
