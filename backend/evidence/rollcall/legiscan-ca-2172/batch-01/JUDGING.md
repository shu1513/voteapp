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
  accommodations; a translucent shield is not a "facial covering"; and an agency that posts a
  compliant policy by 2026-07-01 exempts its personnel from the criminal penalty. The Texas SB 2972
  lesson — **when a statute qualifies a ban, the description must carry the qualification** — so
  the descriptions say "the exemptions are narrow" and name them, rather than "banned masks".
- **SB 42 is conditional.** It was chaptered, but because it amends the voter-approved Political
  Reform Act its provisions only take effect if voters approve them on 2026-11-03. Saying it
  "became law" alone would be misleading, so both descriptions state the condition.
- **AB 1319 sunsets.** The provisions go inoperative 2031-12-31 and are repealed 2032-01-01; the
  descriptions say so. It also shields entities operating under a federal biological opinion.
- **SB 704's dealer checks start later than the bill.** The transfer rule applies on enactment, the
  eligibility check and record-keeping from 2027-07-01. The descriptions carry both dates.
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
