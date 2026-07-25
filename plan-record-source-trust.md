# Candidate-Record Source Trust Plan

Protects candidate records from evidence poisoning: astroturfed social/UGC content,
SEO'd smear pages, fake "local news" domains, and coordinated (including foreign)
influence campaigns. Records are the only public claim surface an outsider could try
to plant content into — not by writing to the app (no public write path exists), but
by publishing convincing material elsewhere and letting the research pipeline import
it as evidence.

## The rule that shapes everything

**A reachable URL is not evidence. Where a claim comes from decides whether it can
become a record, and damaging claims need sources that carry editorial or official
accountability.**

Today a source is accepted when it parses as http(s) and responds (even 403 —
deliberate, WAF-friendly). Nothing distinguishes `sos.ca.gov` from
`reddit.com/r/anything` from a domain registered last week. The discovery prompt asks
for "official/legal sources or reputable news" on damaging claims
(`candidateRecordDiscoveryPrompt.ts:112`), but prompt rules alone are proven
insufficient in this repo (route-enforcement precedent: PRs #350/#352 exist because
doc/prompt discipline failed). This plan moves source trust into code contracts the
same way.

## Decisions already made (user)

- **No human review queues.** Single-operator app; nothing gates on a human approving
  individual records. Enforcement is structural (contracts/writers) or advisory
  (audit detectors the operator runs manually).
- **One accept tier.** Government + courts + legislatures + curated established news +
  civic-data sites form a single allowlist; no A/B distinction.
- **Not over-restrictive.** Unlisted domains (small local papers, niche outlets) are
  ACCEPTED, not blocked — they only become reviewable via audit detectors, and the
  allowlist grows as legit domains show up. The only hard rejections are (a) UGC/social
  platforms and (b) damaging claims whose only source is unlisted.
- **No over-engineering.** No publication state machine, no separate
  fetcher/verifier services, no C2PA/signed datasets, no proof-of-humanity. Rejected
  explicitly after reviewing an external writeup that proposed them.
- Stance vocabulary expansion (`mixed`/`unclear`) — PARKED. `general` and
  `integrity_and_ethics` already omit stance, so the "forced binary" concern is
  overstated; this is quality polish, not a poisoning defense.

## Threat model (what we defend against)

An attacker cannot write records directly (verified: public API has auth, follows,
preferences, and flag-only rate-limited content reports — zero record-write
endpoints). The attack is indirect:

1. Publish false/spun material on the open web — astroturfed Reddit/X threads, a
   credible-looking "local news" or "watchdog" site, SEO'd pages, Wikipedia edits.
2. Wait for research (AI provider web search, or Claude manual research) to find it.
3. The pipeline imports it: URL parses, URL responds → record published, possibly
   pushed to followers.

Foreign-interference variants use the same funnel at scale (proxy media masquerading
as local outlets, persona networks amplifying), typed to election timing. IP/geo
blocking is useless against it (domestic infrastructure, PR firms); the defense is
evaluating evidence, not inferring nationality.

Prompt injection (a page carrying instructions aimed at the researching model) is a
second-order variant: its payoff is still a poisoned payload, which the same
contracts see. Skill docs already treat report text as untrusted; discovery-prompt
hardening rides along in PR 1's docs step.

## What already defends (verified in code)

- No public record-write path; content reports are flag-only + rate-limited.
- `validateCandidateRecordDiscoveryPayload` (`backend/src/ai/enrichCandidateRecords.ts:192`)
  is the single validation funnel for ALL four write paths: AI enricher
  (`candidateRecordEnricher.ts`), manual district writer
  (`writeManualCandidateRecords.ts`), manual presidential writer
  (`writeManualPresidentialCandidateRecords.ts`), live probe
  (`liveCandidateRecordAiProbe.ts`). It runs, in order: schema parse → since-date
  window → quality classifier → source reachability.
- `verifyHttpUrlReachability` (`urlReachability.ts`) blocks private networks /
  DNS-rebinding; SSRF-safe.
- `candidateRecordQuality.ts` drops thin/candidacy/promise rows (shape, not truth).
- Sweep-route coverage gates + `manual:records:audit` detectors (PRs #350/#352)
  catch template-style mass fabrication and route gaps.
- `aiCallGuard.ts` default-denies AI calls → no unattended runs to exploit.

## Gaps this plan closes

1. **No source-domain policy at all** — `reddit.com` is as valid as `courts.ca.gov`.
2. **Damaging-claim source rule is prompt-only** — unenforced.
3. **Repair pass bypass** — model-suggested replacement URLs are re-verified for
   reachability only (`candidateRecordEnricher.ts:611`), so a "repair" could swap in
   another UGC link even if discovery enforced policy.
4. **Redirect hole** — the stored URL is `finalUrl` after redirects; a shortener
   passing the pre-fetch check could land on a blocked domain.
5. **No per-record provenance** — the 07-15 poisoning cleanup needed days of
   forensics to find 5,461 rows; origin columns make a poisoned cohort a WHERE clause.
6. **No campaign-shaped detectors** — nothing flags one domain feeding many
   candidates, or a negative burst right before an election.
7. **Notification-before-validation** — the enricher persists records AND emits
   notification events (`upsertCandidateRecordsWithNotificationEvents`,
   `candidateRecordEnricher.ts:525`) BEFORE label validation; a poisoned record can
   reach followers before checks finish.

## PR 1 — Source-domain policy (contract enforcement)

The core PR. No migration, no new infra, no new AI calls.

### New module: `backend/src/pipeline/candidates/candidateRecordSourcePolicy.ts`

(Same directory as `candidateRecordQuality.ts`; `ai/` already imports from
`pipeline/candidates/`, so the dependency direction is established.)

- `classifyCandidateRecordSourceDomain(sourceUrl)` →
  `{ tier: "blocked" | "listed" | "unlisted"; hostname }`
  - **blocked** — social/UGC/self-publishing platforms where anyone can post without
    editorial accountability: reddit, x/twitter (+t.co/redd.it shorteners), facebook,
    instagram, threads, tiktok, youtube (+youtu.be), medium, substack, blogspot,
    wordpress.com, tumblr, quora, pinterest, linkedin, telegram (t.me), discord,
    truthsocial, gab, parler, rumble, bitchute, 4chan/8kun, fandom. Matching:
    hostname equals the entry or ends with `.` + entry (covers `old.reddit.com`).
  - **listed** — single accept tier:
    - any `.gov` or `.mil` hostname;
    - legacy state-government pattern `*.state.XX.us` (e.g. `courts.state.mn.us`);
    - curated civic-data set: ballotpedia.org, votesmart.org, opensecrets.org,
      followthemoney.org, govtrack.us, openstates.org, courtlistener.com,
      justia.com, oyez.org, c-span.org;
    - curated established-news set: wires + national outlets + one-or-two major
      papers per state (~70 domains as a starter). The list is a plain sorted array
      in the module — growable by trivial PR whenever research hits a legit domain
      that isn't on it.
  - **unlisted** — everything else. ACCEPTED (see decisions). Wikipedia lands here
    deliberately: user-editable (poisoning surface) but too widely legitimate to
    block; audit detectors (PR 3) surface unlisted-source records for later review.
- `matchesDamagingClaimPattern(description)` — narrow regex for
  accusation/enforcement-against-the-candidate content: indicted, convicted,
  arrested, charged with, pleaded guilty, fined, sanctioned, censured, embezzle-,
  bribery, corruption, fraud, misconduct, ethics violation, falsified, concealed,
  resigned amid, sexual harassment/assault/abuse, accused of, allegedly.
  Deliberately narrower than the quality classifier's misconduct verbs
  ("investigated", "audited" excluded — those describe legitimate actions BY a
  candidate, e.g. a comptroller auditing agencies, and would over-drop).
- `evaluateCandidateRecordSourcePolicy({ description, sourceUrl })` →
  `{ ok: true; tier }` or `{ ok: false; reason }`:
  - blocked tier → fail ("user-generated/social platform; cite official, news, or
    research-grade sources");
  - unlisted tier + damaging pattern → fail ("damaging claim requires a .gov/court
    or listed news source");
  - otherwise ok.

False-positive safety valve: a policy failure is a permanent DROP with a clear
reason, which flows into the existing repair machinery — the AI path's source-repair
pass asks the model for a replacement URL with the failure reason attached, and the
manual path's repair report tells the operator exactly what to fix. A real scandal
always has official/news coverage, so the worst case is "find the better source that
exists", not lost data.

### Wiring (three call sites)

1. `validateCandidateRecordDiscoveryPayload` — policy check between the quality gate
   and reachability (cheap check first; blocked domains never get fetched). Failures
   become `droppedRecords` entries with `failureKind: "source_url"`,
   `failureType: "permanent"` — reusing `source_url` routes them into the existing
   repair pass (AI path) / repair report (manual path) with zero new plumbing.
2. Post-reachability finalUrl re-check — when `verifyCandidateRecordSources` returns
   a `finalUrl` whose hostname differs from the submitted URL, re-run the policy on
   the final URL (closes the redirect hole).
3. Enricher repair loop (`candidateRecordEnricher.ts` repair-suggestion verification,
   and the equivalent block in `liveCandidateRecordAiProbe.ts`) — same policy check
   on each suggested replacement URL before reachability, and on its finalUrl after.
   Policy failures → `unresolvedDetails`, permanent.

### Tests

- New `backend/tests/pipeline/candidateRecordSourcePolicy.test.ts` — tier
  classification (subdomains, shorteners, `.gov`, `state.XX.us`, case), damaging
  regex accept/reject table (incl. the "audited the agencies" non-match).
- Extend `backend/tests/ai/enrichCandidateRecords.test.ts` — blocked-domain row
  dropped pre-fetch (reachability mock NOT called for it); unlisted+damaging dropped;
  unlisted+neutral accepted; redirect-to-blocked finalUrl dropped.
- Extend manual-writer validation test (`manualCandidatePayloadValidation.test.ts`)
  if it exercises drops — confirm policy drop blocks import with repair report.

### Docs (same PR or immediately after merge)

- `candidateRecordDiscoveryPrompt.ts`: one added line stating blocked platforms and
  the damaging-source rule as fact (keep prompt-simplicity: merge into the existing
  damaging-claims line, don't add a rule list).
- Skill doc `~/.claude/skills/voteapp-manual-research/references/records.md`: source
  policy paragraph (outside repo; after merge).

## PR 2 — Provenance columns (implemented)

Migration `197_add_candidate_records_provenance.sql`: `candidate_records` gains
`origin` (`ai_enricher` | `repair` | `manual`, CHECK-constrained, NULL = written
before provenance existed) and `origin_run_id` (nullable text: enricher
staging-stream `run_id`, or the manual writer's manual key). All writers stamp
them — the fields are REQUIRED on `CandidateRecordUpsertInput`, so the compiler
forces any future writer to declare provenance. Semantics: origin is the writer
that INTRODUCED the record's normalized content — identical re-imports preserve
the original attribution (identity-key comparison in the similar-record UPDATE;
the ON CONFLICT clause never touches the columns), so periodic reruns cannot
rotate a poisoned cohort out of its `WHERE origin_run_id = ...` cleanup query.
Manual writers suffix their manual key with a per-import timestamp so one bad
import is isolable. Partial index on `origin_run_id` serves the cleanup lookup.

`source_content_sha256` was planned on the premise that the verification body
was already fetched. Verified false: `verifyHttpUrlReachability` is HEAD-first
and cancels every response body without reading it, so no body exists anywhere
in the record pipeline. Populating the hash would mean a new GET + full
download per record — exactly the extra fetch this plan forbids — and an
unpopulated column is schema noise. Dropped; a future content re-verification
job can add the column alongside its own fetcher if ever built.

## PR 3 — Audit detectors (advisory, rides `manual:records:audit`)

- Same source domain across ≥N candidates within a time window.
- Negative-pattern (damaging-regex) record burst for one candidate < 30 days before
  its election (replaces the rejected human-review gate — alerts, blocks nothing).
- Newly-seen domain concentration: domains first appearing in a window and feeding
  multiple records.
- Unlisted-source sweep: list records whose domain classifies `unlisted`, grouped by
  domain, count-sorted — the operator's periodic review feed and the allowlist's
  growth mechanism. Tier is a pure function of the stored URL, so this needs no
  schema change and retro-covers all existing rows.

## PR 4 — Notification ordering

Move notification-event emission after label validation succeeds in
`candidateRecordEnricher.ts` (records may still persist first; the notification
events are the follower-facing blast radius). Same-transaction if cheap; ordering
alone is the win. Presidential path skips notifications already
(`contextType !== "presidential_cycle"` guard) — district path only.

## Explicitly rejected (do not resurrect without new evidence)

- Human approval queues / publication state machine (user decision: no human review).
- Multi-model voting (models would agree on the same poisoned source).
- Independent-source graph analysis (ownership/authorship dedup) — not automatable
  at this scale; the damaging-needs-listed-source rule is the practical substitute.
- Separate researcher/fetcher/verifier/publisher services; content snapshots/archive
  integration; C2PA; signed datasets; kill switch (feature flags + PR 2 rollback
  cover it); durable Redis rate limiting for reports (single-instance deploy);
  IP/geo attribution of interference.
