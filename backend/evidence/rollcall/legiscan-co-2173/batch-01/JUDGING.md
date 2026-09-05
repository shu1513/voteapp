# Colorado batch-01 — judging notes

Every description was written from the **enrolled act, read top to bottom**, and
then checked against the Legislative Council Staff **final fiscal note**. No AI
provider was called at any point.

## Result

| | |
|---|---|
| rolls / measures | 13 / 7 |
| records inserted | 343 |
| candidates | 52 — every member the crosswalk maps |
| tags | 258 |
| errors, notifications, duplicate flags | 0 |
| run stamp | `2026-09-04T20:35:47.349Z` |

Reconciled three ways: the ledger reports 343 inserts, `candidate_records` moved
155,956 → 156,299, and the run-stamp predicate returns 343 rows over 52
candidates. The dry run's own stamp (`…20:35:30.388Z`) matches **zero** rows,
which is the positive proof that `--dry-run` writes nothing. A convergence run
afterwards reported all 343 `unchanged`.

The tag count was predicted from the ledger before it was checked: every record
on the two measures whose nay side carries a stance, plus the yes-voting records
on the other five, comes to 258, and the database holds exactly 258.

## Two source traps, both caught before anything was written

**1. The dataset's list of fiscal notes can omit the final one, and the note it
does list describes the introduced bill.** For SB 25-004 the listed note claims
the act caps wait-list and application fees at $25. The enacted act contains no
cap at all: it makes those fees refundable after six months, requires the
deposit to count toward tuition, and adds an enforcement path. For HB 25-1090
the listed note says a violator owes treble damages up to $1,000; the enacted
act says the money back plus 18% yearly interest.

The final notes for both exist on Colorado's site — the `_f1` URL is
constructible even when `supplements[]` does not name it — and both confirm the
enacted reading. **Rule for later batches: build the `_f1` URL and check the
note's own `Version:` and `Fiscal note status:` lines before reading a word of
it.**

**2. A title can promise more than the act delivers.** SB 25-030 is titled
"Increase Transportation Mode Choice Reduce Emissions" and its early note
describes mandatory targets, but the enacted act requires inventories and
reports and leaves target-setting to local option. It was dropped rather than
described as an emissions measure.

## Version check

Colorado's dated version stack makes this mechanical. For each selected roll the
last print in force on the vote date was diffed against the enrolled act, after
stripping cover pages, page furniture and margin line numbers.

All seven measures came back identical apart from typography, with one
substantive-looking difference that is not one: **HB 25-1133's new section was
renumbered from 18-12-116 to 18-12-117 at enrollment**, because SB 25-003 took
18-12-116. The descriptions cite no section number.

Because Colorado repasses a bill after concurring, every roll in this batch
sits on the text that became law. The problem other states have — a chamber
voting a draft that was later replaced — does not arise when the last roll is
the one selected.

## SB 25-004 carries only its House roll

Colorado's own bill history records one concur-and-repass event for SB 25-004,
on 2025-03-18. LegiScan carries three Senate rolls around it: a concurrence
34-0 and a repassage 23-11 dated 03-17, and a second repassage 21-12 dated
03-18. The tallies differ, so these are not duplicate copies of one vote, and
nothing in the dataset settles which roll the Senate's final action actually
was.

Rather than attribute a tally that cannot be verified, the Senate side was
dropped and only the House roll imported. That costs 11 records and follows the
precedent set by Maryland SB 255 and California SB 707: when the right roll
cannot be established, nobody is credited with the vote.

## Same-day acknowledgements

Six rolls needed `acknowledge_later_rolls`, and all six are the same Colorado
pattern: the concurrence vote sits beside the repassage on the same day, so the
gate that guards against judging a superseded vote cannot order them. Each
judgment names the concurrence roll it acknowledges and says why. None is a
genuine supersession — the acknowledged roll comes first.

## Labels

| measure | area | yes | no |
|---|---|---|---|
| SB 25-003 | gun_control | for | against |
| HB 25-1133 | gun_control | for | against |
| SB 25-001 | civil_rights | for | not stated |
| HB 25-1312 | civil_rights | for | not stated |
| HB 25-1249 | housing_affordability | for | not stated |
| HB 25-1090 | corporate_accountability | for | not stated |
| SB 25-004 | cost_of_living_reduction | for | not stated |

The two firearms measures state a no-side stance because each act is
single-subject, its whole operative content is the area's own mechanism —
regulating who may buy a firearm or ammunition, which is what the area
description names — and the ordinary objection is to that mechanism. This
follows the federal repair, which authored a no-side stance on all five federal
gun measures for the same reason.

The other five leave the no side unstated, because the ordinary objection runs
on a different axis from the area being scored: litigation cost and local
burden on SB 25-001, parental and religious objections on HB 25-1312, landlord
liability on HB 25-1249, compliance cost on HB 25-1090 and HB 25-1249, and
provider cost on SB 25-004. A no vote there is not evidence that the member
opposes the area's goal, so those voters carry no tag.

**SB 25-001 is scored `civil_rights`, not `election_integrity`.** The area
description for election integrity is about elections being secure, accurate and
auditable; this act is about protecting minority voters from suppression and
vote dilution, which is anti-discrimination. That follows the California rule
that voting-access measures go to civil rights.

## Wording

- The plain-language lint was run over all 26 descriptions **before** the
  import: 0 warnings, longest sentence 40 words.
- Reading level was measured separately, because the lint only counts words per
  sentence: median Flesch-Kincaid grade **8.3**, worst 9.9 (SB 25-001, driven by
  words like discrimination and orientation that the act is about). A first
  draft measured 10.8 and was rewritten before anything was imported.
- **The descriptions run 9 to 10 sentences, not the 2 to 4 the house style
  asks for, and that is a deliberate trade.** Reaching a 7th-grade reading level
  means short sentences, and keeping the statute's own limits — the exemptions,
  the effective dates, the notice a tenant must give before suing — means
  keeping the facts. Cutting sentences would have meant dropping limits, which
  is the single most common cause of correction rounds in this campaign.
- The builder asserts, before writing the file: both sentences carry the roll's
  own tally, the body and tail join with a period, no comma splice, no British
  spellings, and no sentence over 45 words.

## A shortening pass dropped four qualifiers, all restored before the import

Cutting the first draft's sentence length quietly lost four things, each found
by re-reading the act against the new text: the sheriff's power to refuse or
revoke a firearms safety card (SB 25-003); that a first ammunition violation is
a civil penalty rather than a crime (HB 25-1133); the tenant's duty to demand
the money and give seven days' notice before suing (HB 25-1249); and that the
excluded charges are government charges generally, not only taxes (HB 25-1090).
This is the failure shape earlier states hit repeatedly: simplification is where
precision dies.

## Review corrections, 2026-09-04 — two more qualifiers restored

PR review flagged two descriptions as overstating their acts. Both were re-read
against the signed act and both held up:

- **HB 25-1133.** The description listed four exceptions for 18-to-20-year-olds
  and said delivery recipients must be 21, but the act carries a fifth,
  time-limited exception for both sales (p. 2) and deliveries (p. 5): a person
  "born on or before January 28, 2007", repealed January 28, 2028 — the day the
  youngest such person turns 21. It is the act's phase-in, so a 20-year-old in
  2026 is not barred as the old text implied. Added as one sentence; the delivery
  sentence now says the deliverer checks age with a photo ID rather than
  restating 21.
- **HB 25-1090.** "Cannot charge more for utilities than the utility bills them"
  omitted the cross-reference to 38-12-801 (3)(a)(VI) (p. 8, p. 10): a lease may
  carry a markup of up to 2% of the bill or up to $10 a month, "but not both".
  And the 2% cap on fee increases during a lease of a year or less applies
  "except for the cost of utilities". Both exceptions added.

Re-run: judge **4 updated**, import (`import-rerun-report.json`) **100 rewrite,
243 unchanged, 0 notified** (HB 25-1133 House 39, Senate 11; HB 25-1090 House
39, Senate 11 — rewritten in place, ids and tags kept), convergence dry run
(`import-dry-run-rerun-report.json`) **343 unchanged**. Still 343 records / 52
candidates. Longest sentence still 40 words.
