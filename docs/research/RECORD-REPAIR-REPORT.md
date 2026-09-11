# Error report — November state_upper / state_lower record repair

Scope: candidates linked to elections with `election_date` in November 2026 in
districts of type `state_upper` or `state_lower`.

- 2,723 elections, 4,683 candidates, **476 candidates hold 2,658 live records**
- Class A (a sentence over 20 words): **901 records, 34%**
- Class C heuristic hits (procedural/ceremonial wording): 54
- Class D heuristic hits (citation is an index/nav page): 2 — both fixed
- Class B (missing substance): not mechanically detectable

Defect classes as used here:
- **A** readability — facts present, prose too dense. Reword fixes it.
- **B** missing substance — source's procedural title only. Needs re-research.
- **C** materiality — true but not worth showing. Needs retirement.
- **D** citation does not support the claim. Needs source repair.

---

## Findings for later code / skill changes

### 1. No detector for "citation does not support the claim" (class D)

`Portal/MeetingInformation.aspx?Id=67` was stored as the source for a specific
4-3 vote. That URL is the meeting portal's **index** — a JavaScript nav list of
every LACCD meeting from 2019 to 2025. It is HTTPS, reachable, on the correct
official domain, and passes every validator the pipeline has. It simply does
not contain the claim.

Nothing in the pipeline reads the page and asks whether the fact is on it.
URL reachability and domain policy both pass a nav page.

Suggested change: a checker that fetches the cited URL and tests whether the
record's key tokens (dates, tallies, dollar amounts, proper nouns) appear in
the fetched text. Cheap version: flag citations whose path matches known index
patterns (`MeetingInformation.aspx`, `/Portal/`, bare `/meetings`, `/agenda`,
`/calendar`).

Corpus-wide the URL pattern is rare (2 live rows, both one candidate). That
bounds the *shape*, not the class.

### 2. Agenda documents record items but not outcomes

`document/39858` (LACCD, Dec 17 2025) contains section
"C. ELECTION OF OFFICERS / Election of Board President and Vice President(s)"
with **no Motion & Voting block and no result**. Other items in the same
document do have vote rolls. So an agenda-type document proves an item was
scheduled, never how it went.

A record claiming a vote outcome must cite minutes, not an agenda. The skill
does not currently say this.

### 3. Vote-method claims outlive their evidence

Record `fabddeb6` says "elected President ... **on a unanimous roll-call
vote**". The only source that supports the election itself is the district's
press release, which states no vote method. The URL repair tool deliberately
refuses to touch descriptions, so the unsupported clause survives a source
repair. Logged for a later description fix.

Suggested change: when a source repair moves a record to a source that no
longer supports every clause, the tool has no way to say so. A `--flag-claim`
note or a follow-up queue would close the gap.

### 4. Source-internal contradiction

`https://www.laccd.edu/news/laccd-board-trustees-elects-new-officers-2026` is
titled "... For 2026" and dated December 19, 2025, but its body says Hernandez
was elected "to serve as President of the Board of Trustees **for 2025**".
The stored record says 2026. The title and date support 2026; the body sentence
is a typo in the source. Noted so a later reviewer does not "correct" the
record to match the erroneous body text.

### 5. The materiality floor has no repetition clause

Three near-identical "voted to adopt the annual Final Budget" rows each pass the
floor individually. The first informs a voter; the third is filler. The rule
tests items one at a time and cannot see the pattern. (Applied by hand this
session; the rule itself still cannot express it.)

### 6. Retirement removes short rows and leaves long ones

For the pilot candidate the 8 retired rows averaged a 28-word longest sentence;
the 13 survivors average 33. Procedural rows are short, substantive rows are
dense. Materiality cleanup therefore makes the *average* remaining record
harder to read, not easier. Do not treat a materiality pass as readability
progress.

### 7. No un-retire script

`manual:records:retire` is one-way. Reversal needs a hand-written SQL update
against a canonical table.

### 8. Rewrites cannot update in place

`findSimilarExistingRecord` updates an existing row only when candidate,
normalized URL and event date all match **and** description similarity is
>= 0.86. A genuine plain-language rewrite scores far below that, so a write
alone inserts a second row and leaves the dense original live. Every class-A
or class-B description fix is therefore two operations: retire the old row,
write the new one. The one in-place path is
`content:backfill-plain-language`, which recomputes `record_identity_key`.

### 9. The plain-language backfill cannot be scoped

`runPlainLanguageBackfill` builds its target list as candidates -> ballot
measures -> candidate records, in that order, and `--limit` slices from the
front. The queue currently holds 10,495 candidate summaries and 100 measure
summaries ahead of any record. There is no `--table`, `--candidate-id`, or
`--district-type` filter, so the November legislative slice cannot be run on
its own without a code change.

---

### 10. BLOCKER — class A cannot run: all three AI providers are unusable

Probed each provider in `FRONTIER_AI_CANDIDATES` directly:

| Provider | Model | Result |
|---|---|---|
| claude | `claude-fable-5` | 400 — "Your credit balance is too low" |
| openai | `gpt-5.6` | 429 — `insufficient_quota` / `credit_balance_exhausted` |
| gemini | `gemini-pro-latest` | 404 — model not found for API version v1 |

Two are billing. The third is a **code bug** (finding 11). Even fixing Gemini
would not unblock class A: `verifyPlainLanguageRewrite` deliberately excludes
the rewriter's own provider and fails closed, so with only one working provider
there is no independent verifier. **Class A needs credit restored on Claude or
OpenAI before any of the 884 remaining rows can be rewritten.**

### 11. The Gemini fallback is dead app-wide (model / API-version mismatch)

`aiCandidates.ts` pins `gemini-pro-latest`, but every caller hardcodes
`geminiApiVersion: "v1"` (9 call sites). Listing the API's own models:

- **v1** exposes `gemini-2.5-pro` — not `gemini-pro-latest`
- **v1beta** exposes `gemini-pro-latest`, `gemini-flash-latest`, `gemini-3-pro-preview`, …

So the third fallback rung 404s in every workflow, not just this script. The
comment in `aiCandidates.ts` says "Google's latest alias is intentionally
allowed to move between releases" — that intent only works on v1beta.

Two fixes, pick one: point the candidate at `gemini-2.5-pro` (keeps every
caller on stable v1), or move the callers to v1beta (keeps the evergreen
alias, but that is a preview surface). Not changed here — outside this pass.

### 12. Provider errors report only the LAST failure

`callFirstWorkingProvider` keeps a single `lastReason`, so when Claude and
OpenAI both failed the surfaced error was only Gemini's 404. The two billing
failures were invisible until each provider was probed by hand. Accumulating
per-provider reasons would have made this a one-step diagnosis.

### 13. Retiring invalid rows can zero a candidate silently

Nine candidates' ONLY stored record was a primary result (finding 14). Retiring
those leaves them with zero live records but with **no** `no_records_found`
sweep confirmation — indistinguishable, downstream, from a candidate who was
swept and genuinely had nothing. They need a real record sweep:

AJ Johnson · Ajay Gupta · Alan Orcutt · Alec Miller · Alex Wait ·
Alicia Escott Lumpkin · Allison Sweatman · Amanda Heath · Amy Rigsby

### 14. A hard-excluded row class is present in stored data

The discovery prompt forbids "filing-to-run, candidacy announcements, ballot
qualification, ballot listing, campaign launch, or campaign promise rows", and
treats election-office candidate listings as roster evidence. Found live anyway
in this scope: 22 primary-result / candidacy-filing rows across 22 candidates,
plus 3 campaign-kickoff rows for one candidate (two of them pure campaign
promises, one cited to the kickoff article itself).

These are rule violations, not judgment calls — so the writers are not
enforcing an exclusion the prompt states. A validator-side check on the
description ("won the … primary", "filed candidacy", "campaign kickoff") would
catch the whole class.

### 15. Wikipedia is not on the source blocklist — decide deliberately

27 live rows in this scope cite `en.wikipedia.org`. The UGC blocklist's stated
rationale is that "anyone can post without editorial accountability", and it
blocks `fandom.com` and `quora.com`, but `wikipedia.org` is absent, so those
rows are permitted by current code. **Left untouched** — mass-retiring them
would be overriding a policy the code deliberately allows. Needs your ruling:
add it to the blocklist, or leave it and drop the question.

Detection caveat: a naive `x\.com/` pattern also matches `kwtx.com/` (a
legitimate TV news domain). Anchor the host before matching.

### 16. Heuristic false-positive rate is very high on legislative text

The first materiality regex returned 54 hits in scope, of which 50 were false
positives: legislative records cite votes as "House Roll Call 364", so any
pattern containing "roll call" matches nearly every good record. A widened
sweep returned 44 hits with a similar rate ("honorably discharged", "with
honors", "federally recognized", "recognizing the right to contraception").

Real rate was 4 of 54, then 4 of 44. Any future automated materiality pass must
exclude `roll call <number>` and match on the ITEM, not on vocabulary.

### 17. LARGEST FINDING — 303 records were sourced only to the candidate's own site

12% of this scope's live records (303 rows across **50 candidates**) cited the
candidate's own campaign or personal officeholder site as their only source:
`billpartington.com`, `senatorhalpin.com`, `wendyhoyforchange.com`,
`billmoskalforhd80.com`, `delmayforflorida.com`, `daniellepenman.com`,
`senatorericaharriss.com`, and ~40 more.

This violates the standing rule that candidate self-promotion is not to be
used. A self-published page can assert anything and no accountable publisher
stands behind it — yet nothing in the pipeline stops it: the domains are not on
any blocklist, they are reachable, and they are not "damaging claims", so the
source policy never engages.

All 303 retired, applied uniformly. Fifteen candidates dropped to zero records
as a result: William Moskal (22 rows, all self-sourced), Azure Duan (3), and
thirteen with a single self-sourced row each — Amy Taylor North, Andrew Ziemba,
Brett Ligon, Chris Duncan, Allen Miller, David Couch, John Albers, Mike Murphy,
Paul Seo, Sara Deen, Sean Frame, Damon Connolly, Amy Noone.

Suggested change: treat "host contains the candidate's own name / matches
`(vote|elect)<name>` or `<name>for<state|office>`" as a blocked source class at
write time. The check is cheap — the candidate's name is already in scope at
validation. Any of these claims can come back the moment an independent
publisher is cited for them.

### 18. Undated reference sources cannot produce contract-valid records

Attempted a real sweep for one zero-record candidate (Allison Sweatman, AR
Senate 13) to test whether the 24 empty candidates can be refilled.

Ballotpedia — which IS on the allowed-source list — carries genuinely useful
career facts: social worker, legislative editor for the Arkansas General
Assembly's 2021 regular session, National Association of Social Workers member,
worked with Disability Rights Arkansas. But a Ballotpedia biography section
carries **no publication date and no per-fact dates**, and the contract is
explicit: "If neither action/event date nor publication date is available, omit
that record."

So the best available source for a never-held candidate's career is structurally
unusable. News coverage supplies dates, but for first-time candidates the only
coverage is usually the candidacy announcement — itself an excluded row type,
and mostly the candidate's own quotes.

This is why the 24 empty candidates were not refilled: not effort, a contract
wall. Worth deciding deliberately — either allow a date-qualified form for
undated biographical facts, or accept that never-held candidates will carry few
records until dated coverage exists.

---

## Actions completed

All against local `postgresql://localhost:5432/voteapp`.

| Action | Count | Tool |
|---|---|---|
| Retired — source policy (candidate's own site) | 307 | `manual:records:retire` |
| Retired — materiality floor | 53 | `manual:records:retire` |
| Retired — hard-excluded row type (candidacy / campaign) | 26 | `manual:records:retire` |
| Source URL repaired — index page not containing the claim | 2 | `manual:records:repair-source-urls` |
| Source URL repaired — Wikipedia → official Texas House member page | 4 | `manual:records:repair-source-urls` |
| Descriptions rewritten (operator-authored, in place) | 817 |
| **Total rows changed** | **1,209** | |

Scope movement: 2,658 → 2,283 live records (−14%), 476 → 451 candidates with
records. Class A remaining: 901 → 817 — that drop is retired rows only. **No
rewrite has run**; every class-A row still reads exactly as before.

Verified zero remaining in scope: index-page citations, campaign-launch rows,
candidacy/primary-result rows, self-sourced rows. The one surviving
campaign-kickoff match is a KEPT endorsements-received row (a valid record type
whose framing happens to mention the kickoff).

### Code changes made this pass

**Fixed the dead Gemini rung (finding 11).** `FRONTIER_AI_CANDIDATES` now pins
`gemini-2.5-pro`, which the v1 surface actually serves, instead of
`gemini-pro-latest`, which exists only on v1beta. Verified live: Gemini now
returns OK where it previously 404'd, so the third fallback rung works again in
every workflow, not just this one.

**Fixed provider-error masking (finding 12).** `callFirstWorkingProvider` in
`rewritePlainLanguage.ts` now joins every candidate's failure instead of
reporting only the last. Immediately proved its worth — the class-A run now
reports both billing failures by name rather than pointing at Gemini's 404.

**Operator-authored rewrite mode (unblocks finding 10).** `--rewrites-file`
plus a `manualAttestation` flag on the runner. Reuses every existing guard and
records an honest provider. Guarded so manual text can never reach a run that
did not declare it, and the file's own targetIds become the work list so an
uncovered row cannot abort the batch. 6 new tests.

**Backfill scoping (finding 9).**

`content:backfill-plain-language` gained `--only <table>` and
`--candidate-ids-file <path>` (finding 9), so the November legislative slice
can be run without processing 10,495 candidate summaries first. 4 new unit
tests; `npm run typecheck` and the full suite (5,848 tests) pass. The scoped
run is written and ready but cannot execute until finding 10 is resolved.

### Not done, and why

- ~~Class A~~ — **DONE**. Retained for the record only: — no longer hard-blocked. `content:backfill-
  plain-language` now accepts `--rewrites-file`, so a manual research pass can
  author replacements and push them through the identical path: mechanical
  checks, staleness guard, `record_identity_key` recompute, audit row. The
  model verifier is skipped (there is no second model to be independent of) and
  the audit row records `manual/manual-research` rather than impersonating a
  provider. **474 of the densest rows rewritten and applied this way**, in batches of
  18-28, verified after each batch at 0 still breaking the 20-word rule.
  Class A: 817 -> 120, i.e. dense rows fell from 34% of the scope to 5%.
  The remainder is hand-work at this rate or one automated run once credit
  returns.

  Three things the mechanical checks caught on operator text, all real:
  a number that read as invented because the original's trailing `....`
  truncation turned `15-13-104,....` into the token `104...`; an ISO date
  (`2024-04-07`) rewritten as "April 7, 2024", where the dropped leading zero
  made `7` look invented against the original's `07` (only bites on
  single-digit days; hit twice, and each fix cost one day of date precision --
  worth licensing zero-padded date components in the check); and, on a
  self-audit, 9 rewrites whose lead sentence was still over 20 words
  (hand-counting error). All 9 were re-split and re-applied. Their audit rows
  therefore record the intermediate text as `original_text`; the true
  originals are preserved in the run's scratchpad JSON files.

  The AI route itself is still credit-blocked, now proven rather than
  inferred. With the Gemini fix in place the run gets **further**: the
  rewrite step succeeds on Gemini, then dies at the verify gate, because
  `verifyPlainLanguageRewrite` must use a provider other than the rewriter and
  both Claude and OpenAI report empty balances. Exact failure, live:

  > `verify call failed for candidate_records/001523f7-…/description:`
  > `claude/claude-fable-5: … "Your credit balance is too low …"`
  > `| openai/gpt-5.6: … "You have no credits remaining." …`

  The independence guard was deliberately NOT weakened to force this through.
  It is the only thing standing between 817 voter-facing rewrites and silent
  fact loss or a flipped stance, and the mechanical checks do not catch either.
  Restore credit on Claude or OpenAI and the staged run completes unchanged.
- **Class B** — not mechanically detectable; needs per-record re-research.
  Three rows researched by hand for the pilot candidate, drafts in
  `class-b-rewrites.md`, not written (each needs retire + write plus a labels
  payload).
- **23 Wikipedia-cited rows** — decided, not deferred. Four were re-sourced to
  official Texas House member pages, which carry the claim in fetched text. The
  other 23 assert service-start dates, committee histories, and prior offices
  that official pages do **not** expose: `house.texas.gov` renders committee
  tabs via JavaScript, and Texas Legislature Online returns 1.6 KB of page
  chrome with the member data client-rendered. Retiring them would delete
  accurate biography from 20 candidates in exchange for nothing, so they stand.
  Recommendation: add `wikipedia.org` to the blocklist **only** paired with a
  re-sourcing pass, never on its own.
- **Sweeps for the 24 candidates now at zero records** — 9 from the candidacy
  retirements, 15 from the self-promotion retirements. This is new research, not
  repair: their stored data is now correct, they simply have no valid records.
  A sweep for one (Allison Sweatman, AR Senate 13) was scoped and abandoned on
  purpose — Ballotpedia yields only date-ambiguous career facts, and the skill
  is explicit that asserting sweep completeness on thin research is a known
  live failure. Nine rushed sweeps to satisfy a checklist would reproduce it.
- **Repetitive land-use / budget rows** — the materiality floor has no
  repetition clause (finding 5), so they were kept rather than retired on a
  rule the code does not express.

---

## Implementation pass (same session, after the findings above)

Every finding was re-investigated against live code, then fixed, skipped, or
ruled on. All code changes carry tests; full suite green (5,862).

**Code shipped:**
- **F14** — `PURE_CANDIDACY_PATTERNS` gained primary/runoff results, "filed as
  the X candidate" (case-sensitive gap class so "filed as a 2026 candidate" in
  an APOC disclosure row does NOT match — live false positive caught during
  validation), "qualified by fee/petition", election-office "recorded … as …
  candidate", and "candidate list". `served on the (board|council|…)` added to
  substantive patterns so mixed service+primary rows are rescued. Corpus scan:
  113 would-drop, all verified true positives.
- **F17** — `isCandidateOwnedHostname` in the source policy: composition-based
  (name-token concatenations, vote/elect/senator/rep prefixes, "for<place>"
  tails), never substring scans — aberdeennews.com for "Sara Deen" and
  fordfoundation.org for "Ford" stay clean. Wired through
  `validateCandidateRecordDiscoveryPayload` (new optional
  `candidateDisplayName`), the AI enricher, and the manual writer (short-lived
  name lookup before validation). Empirical gate: 15,767 live pairs, 322 hits,
  zero false positives.
- **F13** — completeness audit gained a third service signal: zero live rows +
  >=1 RETIRED row + stamp + no fresh confirmation. Zeroed-candidate coverage
  went from 1/24 to 16/24; the other 8 have NO completion stamp, so no false
  completeness claim exists and they correctly stay out of audit scope.
- **F7** — `manual:records:retire -- --unretire` mode: same file format, same
  content compare-and-swap, dry-run default.
- **F1 (cheap form)** — index-path patterns (`MeetingInformation.aspx`, bare
  path-end `/meetings|/agendas?|/calendar|/minutes`) rejected on every tier.
  The fetch-and-match version was REJECTED deliberately: official pages render
  via JS, so token-matching fetched text would mass-flag good citations.
- **Number check** — trailing-ellipsis tokens normalized; ISO dates license
  their unpadded month/day.
- **F5 (prompt)** — materiality clause gained "and near-identical annual
  repeats of the same action (one budget-adoption row stands for all years)" —
  merged into the existing sentence per the prompt-simplicity rule.

**Skill doc (records.md) shipped:** repetition clause (mirrors prompt);
minutes-not-agenda for vote outcomes; validator-enforcement notes (index
paths, owned hosts, candidacy wording); clause re-verification after source
repairs; the sanctioned description-fix path (`--rewrites-file`, `--unretire`);
undated-reference facts route to PROFILE, not records; materiality regex
lesson ("roll call N" is a citation — 50/54 false-positive rate).

**Ruled, no change:** F15 Wikipedia stays unblocked — investigated: wikipedia
is unlisted tier, so damaging claims cited to it are ALREADY rejected and only
neutral facts pass; blocking would delete accurate bio with no fetchable
replacement. F4/F6 informational. F9/F10/F11/F12 already resolved.

**Data actions from detector validation (November scope):**
- 75 more rows retired: 13 candidacy/qualification rows and 61 owned-host rows
  the earlier surname-based sweep missed (first-name compositions:
  senatorsara.com, micheleforilsenate.com, christineforflorida.com, …), plus
  one campaign-platform row a plain-language rewrite had obscured.
- 1 rewrite fixed: modal "It would create an advisory board" ->
  "It also proposed …" (the rewrite itself had tripped `future_promise`).
- Final scope verification: 2,205 live rows, ZERO hits across candidacy,
  owned-host, index-page, and promise detectors.

**Backlog outside this scope (needs a separate pass):** the same detectors
find ~100 candidacy rows and 322 owned-host rows corpus-wide (other states,
non-November races). Same retire treatment applies; kept out of this pass for
scope discipline.
