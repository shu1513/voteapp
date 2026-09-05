# Colorado batch-02 — judging notes

Every description was written from the **enrolled act, read top to bottom**, and
checked against the Legislative Council Staff **final fiscal note**, whose own
header was read first to confirm it describes the enacted bill. No AI provider
was called.

## Result

| | |
|---|---|
| rolls / measures | 10 / 5 |
| records inserted | 251 |
| candidates | 52 |
| tags added | 173 |
| errors, notifications | 0 |
| run stamp | `2026-09-05T03:20:52.370Z` |

Reconciled: the ledger reports 251 inserts, the run-stamp predicate returns 251
rows over 52 candidates, and Colorado's total moved to 594 records with 431
tags — 258 from batch-01 plus exactly the 173 predicted here (every label
leaves the no side unstated, so only yes-voting records are tagged). The dry
run's stamp matches zero rows, and a convergence run reported all 251
`unchanged`.

⚠ The raw `candidate_records` count is not a usable check: a parallel session
added rows between the baseline and the import. Reconcile by run stamp.

## The final fiscal note described a duty the act does not contain

**HB 25-1018.** The final note — stamped "the final fiscal note reflects the
enacted bill" — says that beginning in 2026 the labour department must report
data on people served, staffing and expenditure at its SMART Act hearing. **The
enrolled act contains no reporting duty at all**; the words "report", "data" and
"SMART" do not appear in it. The act amends one statute, 8-84-106, and sets an
effective date.

This is the same family as Georgia's session report inverting an act and
Kentucky's summaries misstating six of twelve. The description was written from
the act.

## Colorado hides deletions the way Georgia and Montana do

HB 25-1018 is almost entirely a set of **removals**, and Colorado prints those as
struck-through text that `pdftotext` renders as ordinary text — so an extracted
read shows repealed law as if it were still live. Pages 1 to 3 of the signed act
were **rendered and looked at**, which confirmed all three removals:

- the requirement that an applicant be present in Colorado when applying;
- the screen asking whether the department judged the person able to achieve
  rehabilitation; and
- the duty on the person, or a relative who claims them as a dependent, to
  contribute toward the cost according to a state assessment of ability to pay.

**Rule for later Colorado batches: new text is printed in CAPITALS, so lowercase
text is either untouched law or struck. Whenever a claim rests on something the
act removed, render the page.**

## Version check

Each roll's last print in force on the vote date was diffed against the enrolled
act. All five came back identical apart from page furniture, one typo repair
(`DESGINATED` → `DESIGNATED` in HB 25-1225) and the deceptive-trade-practice
paragraph letters being renumbered at enrollment (HB 25-1161 `(1)(iiii)` →
`(1)(kkkk)`, HB 25-1117 `(1)(iiii)` → `(1)(mmmm)`). That is the same
renumbering-at-enrollment pattern batch-01 saw on HB 25-1133, and no description
cites a paragraph letter.

## Labels

All five score `for` on the yes side and leave the no side unstated. In each
case the ordinary objection runs on a different axis from the area being scored:
retailer burden and consumer choice on HB 25-1161, cost on HB 25-1018, gun
rights and speech on HB 25-1225, local control on HB 25-1093, and business
burden on HB 25-1117. A no vote there is not evidence that the member opposes
the area's goal.

`election_integrity` fits HB 25-1225 on the area's own words — elections
"trusted by the public" — because the act protects voters and election workers
from intimidation rather than changing who may vote. That is the distinction
batch-01 drew when it scored the Colorado Voting Rights Act as civil rights
instead.

## Same-day acknowledgements

Five rolls carry `acknowledge_later_rolls`, all the standard Colorado pattern:
the concurrence vote sits beside the repassage on the same day, so the gate
cannot order them. None is a real supersession.

## One duplicate flag, read before importing

The importer flagged an existing record for Naquetta Ricks against HB 25-1117's
House roll. It is a false positive: that record describes **Senate Bill 25-070**,
a different measure that happens to share the 2025-04-23 date. Nothing was
retired. The related test falls back to a bare "mentions voting" check on state
measures, so this noise is expected.

## Wording

- Plain-language lint over all 20 descriptions before the import: **0 warnings**.
- Reading level measured separately: median Flesch-Kincaid grade **7.9**, worst
  8.8. A first draft measured 10.0 and was rewritten before anything was judged.
- Descriptions run 8 to 12 sentences rather than the house style's 2 to 4, the
  same deliberate trade batch-01 documented: short sentences are what buys the
  reading level, and the statute's limits are what keep the text true.

### The shortening pass dropped four qualifiers again, all restored pre-import

Re-reading each act against the shortened text found four losses, exactly the
failure shape this campaign keeps hitting:

- HB 25-1225: "does not cover police" lost that the exemption is for officers
  **on duty** and guards **working under a contract** at the site.
- HB 25-1225: "need not prove they meant to" dropped the act's exception —
  intent still matters when the claim is an **attempt**.
- HB 25-1093: "how much housing land can hold" dropped **uses** alongside
  density, and the act's own narrowing of the older ban to rules that apply
  **across the board**.
- HB 25-1117: the **24-hour written windshield warning** before booting in a
  parking space or shared lot had been compressed away entirely.
