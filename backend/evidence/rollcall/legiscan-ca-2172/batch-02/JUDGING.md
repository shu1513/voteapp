# California batch-02 — judging notes

Same source as batch-01: the **chaptered text and its Legislative Counsel's Digest** on leginfo
(`billTextClient.xhtml?bill_id=202520260<BILL>`), never a title or caption. The digest carries no
sponsor's statement of intent, and the same page is the version list filter 4's check reads.

## Label calls

| measure | label | why this direction |
| --- | --- | --- |
| AB 572 | `public_safety_and_crime_control`/for | The area names **accountability** as part of public safety; this is a disclosure duty on officers interviewing a family whose relative police killed or seriously injured. Same reading as batch-01's SB 627. |
| AB 1071 | `civil_rights`/for | "Anti-discrimination enforcement" is the area's own wording, and the bill's whole subject is proving race, ethnicity, or national-origin discrimination in a conviction or sentence. |
| AB 2624 | `immigration`/for | Shields people who help immigrants from doxxing and address exposure — the humane side of "welcome immigration … lawful, orderly, and humane". |
| AB 1318 | `immigration`/for | Keeps immigrant- and refugee-serving nonprofits eligible for the state grants that fund immigration legal services and resettlement help. |
| SB 30 | `environment_and_public_health`/for | Stops decommissioned diesel rail equipment being resold into continued service, with emissions-tier exemptions. |
| AB 1037 | `environment_and_public_health`/for | Overdose prevention is community health prevention, which the area names. |
| AB 858 | `corporate_accountability`/for | An employer recall-and-retention duty enforceable against the employer, the AB 692 reading. |
| SB 763 | `corporate_accountability`/for | Raises antitrust fines sixfold and adds a civil penalty; pairs with batch-01's AB 325. |
| SB 82 | `corporate_accountability`/for | Consumer protection: it stops a consumer use agreement's dispute-resolution terms reaching disputes unrelated to that agreement. |
| SB 596 | `healthcare_affordability`/for | The area reads "affordable, **quality** care"; nurse-to-patient ratio enforcement is the quality half. |
| AB 1061 | `housing_affordability`/for | Narrows a historic-district exclusion that blocked ministerial approval, so more lot splits and small developments qualify. |
| SB 73 | `election_integrity`/for | "Secure, accurate, auditable, and trusted" — the bill protects election administration from interference and locks down rosters, voter lists, and voting technology. |

**Why SB 73 is kept where batch-01 dropped two election bills.** AB 930 and AB 1249 were access
expansions (a longer mail-ballot receipt window, more early-voting sites), and expanded access is
not the claim `election_integrity` makes. SB 73 is about the security and custody of the election
itself, which is exactly that claim. The distinction is the area description, not the topic.

## Traps caught while reading, and how the descriptions answer them

- **AB 572's duty is not absolute, and the digest misstates the exception.** The chaptered
  digest says the advisements do not apply to a family member "under custodial interrogation"; the
  enacted text (Gov. Code 7287(c)(2)) instead excepts a family member who has already received
  **substantially equivalent or Miranda advisements**. The descriptions follow the statute, carry
  the evidence/imminent-threat exception too, state that the duties run through a mandatory agency
  policy due 2027-01-01, and include the separate flat ban on threats or deception (7287(b)).
- **AB 1037 removes a training requirement.** Expanding naloxone access and dropping the training
  precondition are the same provision's two halves, and the descriptions say both rather than only
  the appealing half.
- **AB 2624 starts later than enactment** (October 1, 2027) and its posting ban turns on a specific
  intent that someone imminently commit a violent crime — an intent clause the description carries,
  not a flat ban on posting.
- **AB 1061 narrows an exclusion; it does not abolish historic protection.** A contributing
  structure in a listed district and an individually listed landmark are still excluded, local
  agencies may still adopt objective standards for district character, and a lot split still may not
  require demolishing the named structures.
- **SB 30's exemption is conjunctive.** A transfer needs BOTH a qualifying condition (federal
  emissions tier, equivalent emissions, or engine removed) AND authorization at a public hearing
  (Pub. Util. Code 99153.5(c) requires "both of the following criteria"). It also binds any "public
  entity" that owns the equipment, not only transit agencies. Both fixed on review, see below.
- **SB 596 is an enforcement-mechanics bill**, not a new ratio. The descriptions say what changed:
  the on-call list definition, what does not count as exhausting it, and separate days as separate
  violations.
- **AB 858 is an extension, not a new right** — the recall duty already existed and would have
  expired at the end of 2025; the bill runs it to January 1, 2027.
- **AB 1318 is an urgency statute**, so it took effect immediately; the description says so.
- **The Assembly is called the Assembly** in every description, as in batch-01, though the
  pipeline's chamber key is `house`.

## Plain language checked BEFORE the import

The batch-01 lesson (its review round 2) is now standard practice here: descriptions were written as
short sentences and run through the repository's own
`listPlainLanguageWarnings` (`PLAIN_LANGUAGE_MAX_SENTENCE_WORDS = 45`) before any judge or import
run. First pass flagged exactly one description (AB 1037's Assembly concurrence, 49 words); it was
split, and the committed file reports **0 warnings across all 48 descriptions**.

## Runs

| step | result |
| --- | --- |
| plain-language lint | 48 descriptions, **0 warnings** |
| `rollcall:judge --dry-run` | 24 `dry_run` |
| `rollcall:judge` | 24 `updated` → queue 44 approved / 5,284 pending |
| `rollcall:legiscan:import --dry-run` | **859 planned inserts**, 729 unchanged (batch-01), 0 errors, 0 notified |
| `rollcall:legiscan:import` | 44 `imported`, **859 inserts**, 729 unchanged, 0 errors, 0 notified |
| re-run `--dry-run` after the import | **1,588 unchanged**, 0 errors |

**Reconciled three ways.** `candidate_records` went 74,203 → 75,062 (+859); the run's predicate
`origin_run_id LIKE 'rollcall:CA:%:2172:%:2026-08-29T05:00:09.638Z'` returns 859 rows across 80
candidates; and the DRY RUN's stamp `2026-08-29T04:59:49.673Z` matches **zero** rows, the positive
proof that `--dry-run` writes nothing. California now holds **1,588 roll-call records across 80
candidates** (batch-01 729 + batch-02 859).

**One transient failure worth recording.** The first idempotency re-run reported 1,528 unchanged
instead of 1,588 and one `error` on roll 1601634 (batch-01's SB 627 Assembly vote): `citation URL
fetch timed out` — legiscan.com sitting behind Cloudflare, the same flake batch-01 hit. Its 60
records were never at risk; the dry run simply could not validate the citation and reported 0
candidates for that roll. An immediate re-run returned 1,588 unchanged with 0 errors, and that clean
run is what `import-rerun-report.json` holds. **A single `citation URL fetch timed out` is a network
flake, not a data problem: re-run before investigating.**

Prod untouched.

## Review response (2026-08-29) — 5 descriptions corrected, 358 records rewritten in place

External review on PR #933 raised five findings. Four verified fully true, one partially; every
defect was re-verified against the chaptered text before fixing. 10 rolls re-judged, **358 records
rewritten** (SB 30 = 76, SB 73 = 76, AB 572 = 64, AB 858 = 68, SB 763 = 74), re-run 1,588
`unchanged` / 0 errors. Row count unchanged at 1,588 / 80 candidates; the batch's other 14 rolls
kept their `2026-08-29T05:00:09.638Z` stamp, the rewritten 10 now carry
`2026-08-29T05:22:44.691Z`. `import-rewrite-report.json` and the refreshed
`import-rerun-report.json` are the machine ledgers.

1. **SB 30** (P1, true): the exemption requires a qualifying condition AND a public hearing —
   §99153.5(c) says "both of the following criteria" — and binds any public entity. The first pass
   presented qualifying equipment as automatically exempt and said "public transit agency".
2. **SB 73** (P1, partially true): two real defects fixed — the voting-fraud exception belongs only
   to the rosters/voter-lists rule (§15553), not to certified voting technology (§19230(b), court
   order only), and both rules carry a written-agreement carve-out for logistical, transportation,
   or security support that the first pass omitted. **The reviewer's third sub-claim did not
   verify**: nothing in chaptered SB 73 creates a separate federal-agency rule or requires a
   *federal* court order — both sections say "a court order", and federal agencies appear only in
   the shared definition of "law enforcement agency". The descriptions state the two rules
   separately and add the carve-out; they say nothing about federal courts.
3. **AB 572** (P1, true): the enacted exception is "substantially equivalent or Miranda
   advisements", not "custodial interrogation" — the first pass copied the CHAPTERED DIGEST's loose
   paraphrase, which is a new hazard for the state file: **even the digest attached to the chaptered
   text can misstate an operative clause; exceptions must be read from the statute**. The omitted
   §7287(b) threats-and-deception ban and the agency-policy mechanism (due 2027-01-01) are now in.
4. **AB 858** (P2, true): the recall law covers specified enterprises — hotels, private clubs,
   event centers, airport hospitality and service providers, building services for commercial
   buildings — and pandemic-related layoffs of employees with six months' service. The first pass
   read as if it covered all employers.
5. **SB 763** (P2, true): every amount is a maximum — corporate fine "not more than" $6 million,
   individual "not more than" $1 million (each with a gain/loss alternative if greater), and the
   civil penalty "not more than" $1 million per violation, assessed in an action by the Attorney
   General or a district attorney (§16755.1). The first pass stated fixed amounts.

Plain-language lint re-run after the fixes: **0 warnings across all 48 descriptions**.
