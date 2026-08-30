# Maine batch-01 — judging notes

Every judgment was written from the **enacted chapter text** on
legislature.maine.gov, with the neutral `SUMMARY` at the foot of the adopted
committee amendment used as an index into it. No AI provider was called.

## Result

- **24 rolls / 12 measures / 1,516 inserts, 0 errors, 131 candidates.**
- Dry-run plan 1,516 planned inserts → real run 1,516 inserts → 1,516 rows in
  the database at the run stamp. Reconciled three ways.
- Real run stamp: `rollcall:ME:<chamber>:2181:<roll>:2026-08-29T05:24:26.277Z`
  (one timestamp per run, shared by every roll — it is the batch key). After
  the LD 1016 review rewrite the batch spans two stamps: 1,415 records at
  `…T05:24:26.277Z` plus 101 at `…T06:30:26.398Z` (the rewrite re-stamps
  `origin_run_id`, the TX batch-02 mechanic).
- The dry run's own stamp `…T05:23:59.141Z` matches **zero** rows: positive
  proof `--dry-run` is inert.
- Idempotency: a dry re-run reports **1,516 unchanged**
  (`import-dry-run-rerun-report.json`, which does not overwrite the ledger).
- 1,516 area tags. 0 notifications (every vote is older than 30 days).
- **131 candidates, not 132**: Aaron Dana, the Passamaquoddy tribal
  representative, is mapped in the crosswalk but casts no recorded vote in
  Maine, so he never fans out. This is the TX-Burrows / GA-Burns shape with a
  different cause.
- **Production untouched.**

## Version check, per roll

Maine amends by committee report, and a chamber can vote before or after the
other chamber changes the text, so each roll was pinned to the version it was
actually cast on using the `history[]` action trail:

- **LD 1126** — enacted text is Committee Amendment "A" (H-660) as amended by
  Senate Amendment "B" (S-468). The House's 6/17 `Enactment` (76-72) predates
  S-468, so the House roll taken is its 6/25 **recede-and-concur** (73-67), cast
  on the final text; the Senate's 6/25 enactment (17-16) matches.
  **⚠ Senate Amendment "A" (S-403) FAILED adoption**, though LegiScan flags it
  `adopted: 1` — it was a full substitute that would have replaced the bill and
  its title, cutting it down to two new crimes. Judging off it would have
  described a different law to ~103 representatives.
- **LD 598** — enacted text is CA "A" (S-195) plus Senate Amendment "A"
  (S-301). The House's 6/3 report vote was on a different combination (CA "A"
  plus House Amendment H-471), so the House roll taken is the 6/10
  recede-and-concur.
- **LD 1016** — enacted text is CA "A" (S-247) as amended by House Amendment
  "A" (H-639). The Senate's 6/5 report vote predates H-639, so the Senate roll
  taken is its 6/11 recede-and-concur. **Review fix 2026-08-29:** the House's
  roll 425 ALSO predates H-639 — Maine accepts the report first (history line
  23), then reads the bill and takes amendments (H-639 adopted at line 26), so
  the report vote was cast on the S-247 text. H-639 changed only
  implementation details (affiliated-entity net worth, an attestation, the
  start date, which official credits the fund), none of which the description
  states, so the roll is KEPT and reframed: its descriptions now say the vote
  accepted the committee report that advanced the bill, and note the House
  amended implementation details afterwards. 101 records rewritten in place
  (`import-rewrite-report.json`); convergence dry run = 1,516 unchanged.
- **LD 1937** — House Amendment "A" (H-707) FAILED adoption; both chambers
  voted CA "A" (S-346), which is the enacted text.
- **LD 1868** — the Senate enacted the bill, reconsidered, and enacted it
  again the same day. Journal roll numbers order them: RC #610 (20-15) then RC
  #612 (19-16). The later, operative one is taken.
- **LD 556** — no amendments at all; every roll is on the bill as introduced.
- LD 1971, LD 538, LD 61, LD 517, LD 2231, LD 2176 — a single committee
  amendment, adopted in both chambers before either recorded vote.

No vehicle-bill or gut-and-replace substitution was found in the batch: every
title matches the enacted text. (The one substitute that would have created
that hazard, LD 1126's S-403, failed.)

## Date audit

Maine's feed dates each roll on the day the journal records it. All 24 rolls
were checked against the journal lines LegiScan reproduces in `history[]`
("Roll Call Number 654 Yeas 17 - Nays 16" / "ROLL CALL NO. 757 (Yeas 78 - Nays
67…)"), matching on roll number AND tally:

- **21 of 24 matched exactly, on the same date. Zero skews.**
- 3 rolls have no matching history line at all (LD 1971 House enactment, LD
  2231 Senate RC #915, LD 1016 Senate RC #461) — the journal action is present
  but LegiScan's rendering of it omits or garbles the roll number. Nothing
  contradicts the feed's date, so nothing was overridden.
- One line is affirmatively wrong in a way worth remembering: LD 2231's
  2026-04-08 Senate history line repeats **RC #752, 18-13** from a March
  action, while the vote record is RC #915, 20-14. The `roll_call` records are
  the reliable side.

`official_vote_date` (migration 257) was therefore **not** used anywhere in
this batch.

## Labels

Nine areas. Directions follow the **area description**, not the bill's own
framing, per the standing rule:

- `gun_control` / **for** — LD 1126. Serial-number and undetectable-firearm
  rules are firearm-access regulation.
- `immigration` / **for** — LD 1971. The area reads "Welcome immigration…
  humane", so limiting local participation in federal civil enforcement is
  *for*. This is the exact mirror of Texas SB 8, which scored *against*.
- `womens_reproductive_rights` / for — LD 538.
- `environment_and_public_health` / **against** — LD 556, which preempts
  municipal ordinances restricting a heating or energy system. The description
  carries the statute's own qualification: municipalities may still *encourage*
  a system and spend money supporting one.
- `environment_and_public_health` / for — LD 1868, a new clean-energy portfolio
  requirement rising to 10% by 2040. Recorded counter-reading: the Class III
  definition counts existing nuclear and qualifying hydro, so a supplier may be
  able to comply without new build; the enacted text answers this with a
  biennial review (§3210 sub-§3-D ¶B) and a bar on double-counting Class I/IA
  credits against the older classes. Direction stands.
- `data_privacy` / for — LD 61 (employer surveillance) and LD 2176 (landlord
  disclosure of tenant personal information).
- `election_integrity` / for — LD 517.
- `healthcare_affordability` / for — LD 1937.
- `corporate_accountability` / for — LD 598, following the Georgia SB 50 and
  Texas CHOICE Act labor precedent.
- `housing_affordability` / for — LD 2231 and LD 1016. Counter-reading recorded
  for LD 1016: a $10,000-per-lot transfer fee is a cost on purchasers. It is
  scored *for* because resident-owned cooperatives and housing authorities are
  exempt and the proceeds fund park preservation, so the fee falls on outside
  buyers and finances resident purchase.

## Qualifications carried into the descriptions

Per the SB 2972 rule — when a statute qualifies a ban, the description carries
the qualifier:

- **LD 2176** bans landlord disclosure only "with the intent to harass,
  intimidate or otherwise cause a person to vacate… and without a legitimate
  business purpose". The committee amendment's own summary omits that intent
  clause; the enacted §6025-B(2) has it, and the description follows the
  statute.
- **LD 1126** excepts antiques, curios, permanently inoperable guns and guns
  made **before 1968** — not 1969, which is the figure in the *failed* S-403.
- **LD 598** applies only to employers with at least 10 workers, and excepts
  weather, disaster, illness, injury and a documented good-faith attempt to
  tell the worker not to come in.
- **LD 61** exempts personal-care settings and job-required monitoring.
- **LD 556** preserves municipal authority to encourage and to spend.

## Duplicate scan

One `related` flag, reviewed and left alone: Sen. **Anne Carney** has a
hand-written record dated 2025-05-21 (the same day as the LD 538 Senate vote)
about sponsoring **LD 1022**, a different bill that died. Not a duplicate, so
nothing was retired. No hand-written record cites any of the 24 rolls.

## Style

Descriptions were built with the body-and-tail join **terminated by a period**
(the mandatory rule after the comma-splice incidents in Illinois batches 01 and
02), and `", The "` was asserted absent before the judgments file was written.
The repo's `candidateRecordPlainLanguageLint` was run over all 48 descriptions
**before** importing: **0 warnings** (longest sentence 44 words, limit 45).
Tails say "became law", not "was signed into law" — LD 1126 and LD 61 both
became law WITHOUT the governor's signature.

## Plain-language rewrite, 2026-08-30

Every description in this batch was rewritten for an ordinary reader at roughly
a 7th-grade level. **Wording only** — no fact, date, tally, chamber, stance
direction, or label changed, and nothing was re-researched. The rewrite works
from the same evidence the original judgments were built on.

What changed in the writing: each description now opens by naming what the bill
is about in everyday words ("a gun bill", "a bill about bosses watching
workers"), every term of art is explained in the sentence that uses it (a
serial number is "the maker's ID number that lets police trace a gun"; a
frame is "the core part a gun is built on"; an immigration detainer is a
federal request "to keep someone locked up past their release time"), and
Maine's procedural stages are stated in plain terms ("This was the final
passage vote", "The two chambers had passed different versions. This vote
accepted the Senate's wording"). Sentences are short. Every description still
closes with the roll call's own tally.

**Ledger:** 1,516 `rewrite`, 0 errors, row count unchanged at 1,516; the
importer preserved the original insert ledger as `import-report.json` on its
own and wrote this run to `import-rerun-report.json` (stamp
`2026-08-30T05:31:14.491Z`). Convergence dry run: all 1,516 `unchanged`.

**Two things this re-judge surfaced, both from gates added after batch-01
shipped:**

1. **`nay` is now required on every label.** Set to `null` throughout, matching
   batch-02 and the PA precedent. For the 20 rolls this session had written,
   that is a no-op — the pre-gate importer already tagged only the yea side
   (LD 556's 70 `against` tags are its YEA voters, since that measure scores
   `environment_and_public_health`/against).
2. **Two same-day roll pairs needed `acknowledge_later_rolls`**, the same shape
   as batch-02: LD 1868 senate 1591936 acknowledges 1591935 (the Senate
   enacted, reconsidered, then re-enacted; RC #612 is the final one), and
   LD 2176 house 1678710 acknowledges 1678709 (the report vote that preceded
   enactment the same day).

## ⚠ A parallel session had rewritten four of these rolls

Between batch-02's import and this rewrite, **another session working the same
local database** rewrote the descriptions on LD 1126 house (1594835), LD 598
house (1587268), and LD 1016 house + senate (1587988, 1588941) — 327 records,
stamp `2026-08-30T03:57:07.640Z`. Their judgments file was never committed, so
that text existed **only in the local database**, and this rewrite overwrote
it. It is not lost: the eight distinct descriptions are captured in
`parallel-session-descriptions-2026-08-30.json` beside this file.

Their versions carried two facts these do not — the chapter number of the
resulting Public Law and the date penalties take effect. Worth folding in
deliberately if the campaign wants them, for all 46 rolls rather than four.
Their labels also set `nay` to the inverted stance, which is why Maine's tag
count fell from 1,578 to 1,529: the 49 `against` tags on LD 1126 house nay
voters are gone, and every other tag is untouched.
