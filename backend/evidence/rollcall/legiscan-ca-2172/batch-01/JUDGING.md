# California batch-01 — judging notes

Every judgment in `judgments.json` was written from the **chaptered text and its Legislative
Counsel's Digest** on leginfo (`billTextClient.xhtml?bill_id=202520260<BILL>`), never from a title,
a caption, or a sponsor's description.

## Why leginfo is a better source than Texas's analyses

The Legislative Counsel's Digest is written by a nonpartisan drafting office, sits at the top of the
enrolled text itself, and states what the bill changes clause by clause against existing law. There
is **no author's statement of intent anywhere in it** — the Texas hazard (a sponsor's advocacy
opening the official analysis, with numbers that contradict the enacted sections) does not recur.
The page also serves as its own version list, which is what filter 4's version check reads, and it
answers plain `curl` — no Cloudflare, unlike legiscan.com.

Where the digest says "except as specified", that is a pointer, not a summary: the exemption list
was read out of the statute text itself. SB 627 is the case in point — see below.

## Label calls

| measure | label | why this direction |
| --- | --- | --- |
| SB 79 | `housing_affordability`/for | Requires housing near transit to be an allowed use, with density and height floors. "Increase housing supply and reduce cost burdens" is the area's own description. |
| SB 627 | `public_safety_and_crime_control`/for | The area names **accountability** as part of public safety. The statute is a transparency mandate on law enforcement, and its findings section is about public perception and accountability, not immigration. |
| AB 495 | `immigration`/for | Keeps a child's schooling, medical consent and guardianship intact when a parent is detained. The area reads "welcome immigration … lawful, orderly, and humane"; this is the humane side. |
| SB 580 | `immigration`/for | Limits how far state and local systems assist immigration enforcement. Same reading. |
| SB 704 | `gun_control`/for | Routes firearm-barrel sales through licensed dealers with eligibility checks — "regulate firearm access through background checks". |
| AB 325 | `corporate_accountability`/for | Lowers the pleading bar for antitrust suits and bans coercive common pricing algorithms. |
| AB 692 | `corporate_accountability`/for | Voids stay-or-pay employment terms and gives workers a civil action. |
| AB 1415 | `healthcare_affordability`/for | Extends cost-oversight and transaction notice to management services organizations and investor owners. |
| AB 1319 | `environment_and_public_health`/for | Bans trade in wildlife taken in violation of the law as it stood on 2025-01-19 and provisionally lists species whose federal protection is cut. |
| SB 42 | `anti_corruption`/for | Allows public campaign financing with spending limits, bars public money for legal defense or repaying a candidate's own loans, and raises the fine for foreign-government contributions — "prevent abuse of public office through transparency, ethics rules, and enforcement". |

**Direction follows the AREA DESCRIPTION, not the bill.** California's divided-and-enacted set
mirrors Texas's: `gun_control` and `immigration` come out **for** here where Texas's came out
against, because the area text ("welcome immigration…", "regulate firearm access…") is fixed and it
is the bills that flipped.

Only `general` and `integrity_and_ethics` may carry no stance; nothing in this batch needed either,
because filter 5 dropped the measures that would have.

## Traps caught while reading, and how the descriptions answer them

- **SB 627 is not a flat mask ban.** The statute exempts undercover and tactical operations,
  occupational health and safety law, protecting an identity during prosecution, and disability
  accommodations; a translucent shield is not a "facial covering"; and Section 185.5(f) exempts the
  personnel of any agency that posts a compliant policy by 2026-07-01 from the criminal penalty
  entirely. The Texas SB 2972 lesson — **when a statute qualifies a ban, the description must carry
  the qualification** — so the descriptions name the operational exemptions AND the agency-policy
  safe harbor (the first pass carried only the former; fixed on review, see below).
- **SB 42 is conditional.** It was chaptered, but because it amends the voter-approved Political
  Reform Act its provisions only take effect if voters approve them on 2026-11-03. Saying it
  "became law" alone would be misleading, so both descriptions state the condition.
- **AB 1319 sunsets.** The provisions go inoperative 2031-12-31 and are repealed 2032-01-01; the
  descriptions say so. It also shields entities operating under a federal biological opinion.
- **SB 704's dealer checks start later than the rest.** SB 704 has no urgency clause, so as an
  ordinary 2025 statute its transfer rule took effect 2026-01-01; only the eligibility check and
  record-keeping wait until 2027-07-01. The descriptions carry the 2027 date and do not claim an
  effective date for the rest.
- **AB 692 applies prospectively** — contracts entered into on or after 2026-01-01, not existing
  ones.
- **AB 325 has two distinct changes** (pleading standard; pricing algorithms), and the algorithm ban
  has two branches (use as part of a conspiracy; coercing another to adopt the recommended price).
  Both are stated; neither is flattened into "banned algorithmic pricing", which the statute does
  not do.
- **The Assembly is called the Assembly.** The pipeline's chamber key is `house`, but every
  description names "the California State Assembly" or "the California State Senate", because that
  is what a voter sees on the record.

## Concurrence wording

Ten of the twenty rolls are concurrence votes. Their descriptions say "Voted to accept the
Senate's/Assembly's amendments to …" and close with "The California State X agreed to the amendments
N-M, sending the bill to the governor, and it became law" — naming the OTHER chamber's amendments,
which is what the vote was on.

## Runs

| step | result |
| --- | --- |
| `rollcall:judge --dry-run` | 20 judgments, all `dry_run` |
| `rollcall:judge` | 20 `updated` → queue 20 approved / 5,308 pending |
| `rollcall:legiscan:import --dry-run` | 20 rolls, **298 planned inserts**, 0 errors, 0 notified |
| `rollcall:legiscan:import` | 20 `imported`, **298 inserts**, 0 errors, 0 notified, 33 candidates |
| re-run `--dry-run` after the import | 298 `unchanged` |

**Reconciled three ways.** `candidate_records` went 68,172 → 68,470 (+298); the run's own predicate
`origin_run_id LIKE 'rollcall:CA:%:2172:%:2026-08-27T01:50:49.334Z'` returns 298 across 33 distinct
candidates; and the DRY RUN's stamp `2026-08-27T01:50:25.430Z` matches **zero** rows, which is the
positive proof that `--dry-run` writes nothing.

**33 of 33 crosswalk-mapped members got records**, at most 10 each (one per measure, in their own
chamber). The Texas/Georgia Speaker gap does not arise: Speaker Robert Rivas is not one of the 33 at
all — AD-29 has no Nov-2026 election in our data, so he is one of the 88 explicit nulls.

0 notified: every vote is from September 2025, far outside the 30-day notification window. Worth
watching in a later batch — this session is still live, with rolls as recent as 2026-08-20.

Prod untouched.

## Review response (2026-08-27) — 4 descriptions tightened, 119 records rewritten in place

External review caught four descriptions that flattened enacted qualifications — the same failure
class as Texas SB 2972. All four verified against the chaptered text and fixed:

1. **SB 627** (P1): the first pass called the exemptions "narrow" and omitted the biggest one —
   Section 185.5(f)'s safe harbor exempting the personnel of any agency that posts a compliant
   facial-covering policy by 2026-07-01 from the criminal penalty. 30 records rewritten.
2. **AB 495** (P2): "schools and child care providers must keep their immigration-enforcement
   policies current" overstated the child-care half. The keep-policies-current duty binds local
   educational agencies; licensed child daycare facilities instead get an immigration-status
   information-collection ban plus duties to report law-enforcement requests and maintain emergency
   contacts (digest item (4)). 32 records rewritten.
3. **AB 692** (P2): the ban on stay-or-pay terms has five enacted exception classes — government
   loan-repayment/forgiveness programs, qualifying transferable-credential tuition contracts,
   approved apprenticeships, qualifying discretionary signing bonuses, and residential-property
   contracts. The descriptions now say so. 26 records rewritten.
4. **SB 704** (P2): the dealer-transfer rule has eight enacted exemption classes; the descriptions
   now name the principal ones (law enforcement, military, licensed dealers and curio collectors,
   estates, attached barrels, same-transaction firearm purchases). 31 records rewritten. The P3
   note-only error ("applies on enactment" — no urgency clause, so 2026-01-01) is fixed above.

**Rewrite ledger:** `rollcall:judge` 8 `updated` / 12 `unchanged`; import dry run planned 119
rewrites / 179 unchanged; the real run rewrote all 119 (one transient `citation URL fetch timed
out` on roll 1602473 made the first attempt report 19 imported / 1 error with all 119 rewrites
already applied; the immediate re-run was 20 `imported` / 298 `unchanged` / 0 errors). Row count
unchanged at 298 / 33 candidates. That run split the batch across two `origin_run_id` stamps (a
rewrite re-stamps, the TX batch-02 mechanic); the second review pass below then rewrote all 298
rows and collapsed the batch back to ONE stamp, so this run's ledger was superseded before it was
ever committed. Verified per-fix record counts:
SB 627 = 30, AB 495 = 32, AB 692 = 26, SB 704 = 31 (sum 119).

## Review response 2 (2026-08-28, on the re-targeted PR) — plain language, audit ledgers

Four more findings, all accepted:

1. **16 of 20 rolls (32 of 40 descriptions) broke the repository's plain-language line** —
   `PLAIN_LANGUAGE_MAX_SENTENCE_WORDS = 45` in `candidateRecordPlainLanguageLint.ts`, a warn-only
   lint the importer does not run. Worst sentences were 75 and 79 words (SB 704). Every body is now
   rewritten as short sentences — lead sentence, one qualification per sentence, closing tally
   sentence — with every enacted qualification kept. The repo lint now reports **0 warnings over
   all 40 judgment descriptions**, and a direct sweep of the 298 live records shows 0 sentences
   over 45 words (longest now 41).
2. **AB 692's audit note claimed five exception classes while the description named four.** Fixed
   in the description (residential-property contracts added, verified in the chaptered text's
   exception (E)), so the note above is now accurate as written.
3. **The rewrite runs were not machine-verifiable from the repo.** This directory now commits the
   full chain: `import-report.json` (original 298 inserts), `import-rewrite-report.json` (this
   pass's rewrite of all 298 rows, stamp `2026-08-28T00:34:44.806Z`), and
   `import-rerun-report.json` (the idempotency check: 298 `unchanged`, 0 errors). Because this
   pass rewrote every row, the batch is back to a SINGLE stamp — the batch predicate is
   `origin_run_id LIKE 'rollcall:CA:%:2172:%:2026-08-28T00:34:44.806Z'`, which returns 298 rows
   across 33 candidates.
4. Trailing blank line at EOF removed (`git diff --check` clean).

Run chain for this pass: judge dry 20 `dry_run` → judge 20 `updated` → import dry 298 planned
rewrites → import real 20 `imported` / 298 `rewrite` / 0 errors → dry re-run 298 `unchanged`.
Row count unchanged at 298 / 33 candidates. Prod untouched.

## Roster-completion re-import (2026-08-28) — +47 members, 431 new records, batch now 729 / 80

The Nov-2026 CA Assembly rosters were completed (80/80 districts) and the crosswalk extended from
33 to **80 mapped / 41 explicit null** (`crosswalk-review-2026-08-28.md`; 47 new mappings, all
exact or approved first-prefix with `seatAgrees: true`). This supersedes the run numbers above,
which were correct for the 33-member crosswalk they ran under. In particular, **Speaker Robert
Rivas is no longer a null** — AD-29 now has a Nov-2026 election on file, so he is one of the 80
mapped and holds records like any other member.

| step | result |
| --- | --- |
| `rollcall:legiscan:import --dry-run` | 20 rolls, **431 planned inserts / 298 unchanged**, 0 errors, 0 notified |
| `rollcall:legiscan:import` | 20 `imported`, **431 inserts / 298 unchanged**, 0 errors, 0 notified |

**Reconciled the same three ways.** `candidate_records` now holds **729 CA roll-call rows across
80 distinct candidates**: 431 rows on the re-import's stamp
(`origin_run_id LIKE 'rollcall:CA:%:2172:%:2026-08-28T02:20:12.960Z'`, 47 candidates — exactly the
newly mapped members) plus the 298 pre-existing rows still on the review-2 stamp
`2026-08-28T00:34:44.806Z` (33 candidates), untouched by the re-run. The dry-run's stamp
(`02:19:21.497Z`) matches zero rows.

Report files: `import-report.json` is now this re-import's report; the original 2026-08-27
298-insert report referenced in Review response 2 above is preserved as
`import-report-pre-roster-rerun.json`, and the dry-run as `import-dry-run-rerun-report.json`.

Prod untouched.
