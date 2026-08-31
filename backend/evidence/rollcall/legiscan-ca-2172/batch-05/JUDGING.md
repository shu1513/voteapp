# California batch-05 — judging notes

Same method as batch-04: chaptered text on leginfo, every qualification read from the enacted
section rather than the digest, plain English in the first draft, `nay` stated on purpose.

## Label calls

| measure | label | why this direction |
| --- | --- | --- |
| AB 1079 | `civil_rights`/for | The California Voting Rights Act is anti-discrimination law about vote dilution of protected classes; this stops an appeal automatically freezing the remedy. "Anti-discrimination enforcement" is the area's own wording. |
| SB 22 | `corporate_accountability`/for | Consumer protection: a customer can take the money back instead of a small balance quietly staying with the retailer. |
| AB 931 | `corporate_accountability`/for | Disclosure, a cancellation right, and a bar on the funder steering the case — consumer protection against predatory lawsuit lending. |
| AB 1487 | `social_programs_and_welfare`/for | "Support vulnerable populations through effective safety-net" — job training, resettlement, and youth diversion grants for a specific population. |
| SB 635 | `data_privacy`/for + `immigration`/for | "Clear limits on collection, sharing, and misuse": the mechanism is a bar on handing over identifying records without a subpoena or warrant. |
| SB 805 | `public_safety_and_crime_control`/for + `immigration`/for | Accountability, the SB 627 / AB 572 / AB 847 reading — this is the identification companion to SB 627's mask rule. |
| SB 358 | `housing_affordability`/for | Impact fees are a direct cost of building; this makes it harder to deny the lower rate to housing designed to generate fewer car trips. |

## Traps caught while reading

- **SB 805 has TWO exemption lists and a safe harbor** (corrected on review, see below). The
  crime's own exemptions (Penal Code section, subdivision (b)) are six: undercover/investigative
  work, listed plainclothes state and federal regulatory roles, protective equipment, exigent
  circumstances, **SWAT and tactical operations, and dignitary protection details**. The
  officer-danger exception lives in the separate Gov. Code 7288 agency-policy list. And subdivision
  (e) exempts an agency AND its personnel from the crime entirely once the agency publicly posts the
  required policy — the SB 627 Section 185.5(f) pattern, again.
- **AB 931's economics are the point.** What the funder is owed is a set amount fixed by how long
  the case runs, explicitly **not** a percentage of the recovery, and the company may not pay
  referral fees to attorneys. Describing it only as "disclosure rules" would have missed the
  substance.
- **AB 1079 is not absolute.** The trial court may still pause its own order, and must consider it
  when the Secretary of State certifies a pause is needed for orderly elections; cases begun on or
  before 2026-01-01 are untouched.
- **AB 1487 is contingent on an appropriation** — it widens what the fund *may* pay for, and the
  fund makes grants only when the Legislature sets money aside.
- **SB 635 yields to law.** The bar on disclosure gives way where state or federal law requires
  disclosure; it stops *voluntary* cooperation, not compelled process.
- **SB 22's donation exemption is conditional** — a donated card escapes the cash-out rule only if
  nothing was given in exchange for it AND it carries a printed notice that it is not redeemable
  for cash (corrected on review, see below). The change begins 2026-04-01.

## A self-check worth recording

The first draft slipped into British spellings — "misdemeanour", "itemised", "programmes",
"licence", "organisation" — which no lint or jargon check looks for, and which would have been the
only five such spellings in a 3,548-record American-spelled corpus. Caught before the import by
diffing the new vocabulary against the existing records. **A spelling-register check belongs
alongside the jargon check when writing for an established corpus.**

## Runs

| step | result |
| --- | --- |
| plain-language lint | 26 descriptions, **0 warnings** (one 52-word sentence split pre-import) |
| `rollcall:judge --dry-run` | 13 `dry_run` |
| `rollcall:judge` | 13 `updated` → queue 96 approved / 5,232 pending |
| `rollcall:legiscan:import --dry-run` | **509 planned inserts**, 3,039 unchanged, 0 errors, 0 notified |
| `rollcall:legiscan:import` | 96 `imported`, **509 inserts**, 3,039 unchanged, 0 errors, 0 notified |
| re-run `--dry-run` after the import | **3,548 unchanged**, 0 errors |

**Reconciled three ways.** `candidate_records` went 110,517 → 111,026 (+509); the run's predicate
`origin_run_id LIKE 'rollcall:CA:%:2172:%:2026-08-30T05:34:14.517Z'` returns 509 rows across 80
candidates; and the DRY RUN's stamp `2026-08-30T05:33:40.939Z` matches **zero** rows.

California now holds **3,548 roll-call records across 80 candidates** (729 + 859 + 645 + 806 + 509)
across 49 measures. All runs clean first time. Prod untouched.

## Review response (2026-08-30) — five measures corrected, 360 records rewritten, two labels added

External review found material omissions in five of this batch's seven measures. All five verified
true against the full chaptered texts. **The honest root cause: this batch read digests and targeted
excerpts, not the full enacted texts** — the "every qualification read from the enacted section"
claim above was not true for these five. Batch-05 was the last and fastest batch of a long campaign,
and it shows. The fix below is written from the full texts.

1. **SB 805 (P1).** Three defects: (a) "breaking it is a misdemeanor" omitted subdivision (e)'s safe
   harbor — the crime does not apply to an agency or its personnel once the agency posts the
   required policy (the same SB 627 pattern this campaign documented in batch-01 and still missed);
   (b) the exemptions listed were the Gov. Code 7288 policy list, not the crime's own subdivision
   (b) list, which additionally exempts SWAT/tactical operations and dignitary protection details;
   (c) the bill's bail-recovery-agent sections were omitted entirely — bail agents may not
   impersonate law enforcement and may not hand over a bail fugitive's personally identifiable
   information for immigration enforcement without a judicial warrant or court order. Descriptions
   corrected and an **`immigration`/for label added** (the SB 580 direction precedent).
2. **SB 635 (P1).** The first pass described only the records-disclosure rule. The enacted bill also
   bans collecting immigration/citizenship status, place of birth, criminal history, fingerprints,
   and background checks in vending permit programs; orders pre-2026 records of that kind destroyed
   by 2026-03-01; binds the contractors who run permit programs; bars local money and personnel from
   vendor enforcement beyond the vending rules; extends the protections to compact mobile food
   operations; and keeps the 8 U.S.C. 1373 information-exchange carve-out. Descriptions corrected
   and an **`immigration`/for label added** — users following `immigration` would otherwise never
   see these votes.
3. **AB 931 (P2).** The bill's second half was omitted: until 2030-01-01, for contracts made on or
   after 2026-01-01, California attorneys may not share legal fees with out-of-state "alternative
   business structures" (legal businesses allowing non-attorney ownership or control) except under
   three conditions, on pain of State Bar discipline and damages of $10,000 per violation or treble
   actual damages; nonprofits are excluded from the definition.
4. **SB 358 (P2).** Only one of three operative changes was described. Added: the revised
   qualifying criteria (transit priority area; the parking caps; within half a mile of three or more
   listed destinations, replacing the old nearby-retail test) and the deletion of the express
   authorization to charge non-qualifying projects a trip-proportional fee.
5. **SB 22 (P2).** "Cards donated to a nonprofit or charity are exempt" dropped both statutory
   conditions: nothing given in exchange AND a printed not-redeemable-for-cash disclaimer
   (Civ. Code 1749.5(c)).

**Runs:** judge 9 `updated` / 4 `unchanged`; import **360 rewrites** across exactly the nine rolls of
the five measures (SB 22's Senate roll is not in the batch), 0 errors; re-run **3,548 `unchanged`**.
Tag sync verified in the database: SB 805's 61 yea-voters and SB 635's 59 yea-voters now carry
`immigration`/for beside their original tag; nay voters carry no stance tags, per `nay: null`.
Row count unchanged at 3,548 / 80 candidates. Lint 0 warnings. Prod untouched.

**Lesson, recorded plainly: excerpts are not the text.** Batches 02-04 earned their accuracy by
reading enacted sections in full; batch-05 substituted digest-plus-grep and paid for it in five of
seven measures. A measure is not judged until the whole enacted text has been read top to bottom.

## Completeness audit (2026-08-31) — the corrections were themselves incomplete

After the review response above, I audited **all seven** batch-05 measures the way the batch should
have been done in the first place: pull each bill's full chaptered text, extract every change the
Legislative Counsel's Digest enumerates, and check the description against that list item by item.
The reviewer had found five measures; the root cause — judging from excerpts — applied to all seven.

**Result: seven digest-enumerated changes were still missing from four measures**, even after the
five corrections. Fixed here, 303 records rewritten across eight rolls:

- **AB 1487** — the bill also revises the definition of "health care" the fund works from to include
  **mental health services**. 75 records.
- **AB 931** — two gaps: charges may not run **beyond 36 months from the funding date**, and a
  separate provision bars an attorney from **promising or giving anything of value** to secure
  clients. 77 records.
- **SB 805** — two gaps, both about how the safe harbor actually works: a posted policy stands
  **unless challenged** by a member of the public, an oversight body, or a local governing
  authority, and if the agency does not cure within **90 days** the challenger may go to court; and
  a peace officer who suspects impersonation **may demand identification**. 77 records.
- **SB 635** — two gaps: the protected information is **exempt from the California Public Records
  Act**, and the same collection, destruction, and disclosure limits bind the **health enforcement
  agencies** that inspect food sellers, not only permit-issuing local authorities. 74 records.

**AB 1079, SB 22, and SB 358 audited clean** — every change their digests enumerate was already in
their descriptions.

### Two lessons, and they pull in opposite directions

1. **Fixing what a review names is not the same as making the measure correct.** The five
   corrections were accurate but scoped to the findings. A review is a sample, not an audit; after
   acting on findings, re-derive completeness independently.
2. **The full text over-reports change, so it cannot be the only source either.** SB 358's section
   restates Government Code 66005.1 in full, including a land-dedication ban that reads like a major
   new provision. The digest never mentions it, because California's "amended to read" sections
   reprint the entire code section — most of what you read is pre-existing law. Reading the full
   text alone would have added a change SB 358 never made.

**The method that actually works is both together**: the digest enumerates the delta, the enacted
text gives the delta its exact terms, and the description must cover every digest item with statute
wording. That is what this audit did, and it is what the next batch should do from the start.

**Runs:** judge 8 `updated` / 5 `unchanged`; import **303 rewrites** across exactly the eight rolls
of the four measures, 0 errors; re-run **3,548 `unchanged`**. Row count unchanged at 3,548 / 80
candidates. Lint 0 warnings. Prod untouched.
