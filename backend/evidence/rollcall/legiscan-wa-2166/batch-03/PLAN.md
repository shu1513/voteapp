# Washington batch-03

Seven measures, twelve roll calls, 637 records across 108 candidates. Local database
only. Production holds no Washington records.

| measure | chapter | area | direction |
| --- | --- | --- | --- |
| House Bill 1232 — private detention facility standards | 235 L 25 | corporate_accountability | for |
| House Bill 1710 — Voting Rights Act preclearance | 211 L 26 | civil_rights | for |
| House Bill 1750 — voting rights abridgment standard | 215 L 26 | civil_rights | for |
| House Bill 2225 — artificial intelligence companion chatbots | 168 L 26 | ai_regulation | for |
| Senate Bill 6002 — automated license plate readers | 239 L 26 | data_privacy | for |
| Senate Bill 6026 — homes on commercial land | 236 L 26 | housing_affordability | for |
| Senate Bill 6081 — sex designation records | 56 L 26 | data_privacy | for |

**`data_privacy` gets its first Washington coverage.** No measure in this batch carries
an authored nay, so no new `against` tags were created.

Two measures are single-chamber, and in both cases that is the divided gate working
rather than a gap. The House passed Senate Bill 6002 by 84-10 and the Senate passed
House Bill 2225 by 43-5; neither is close enough to count.

## Roll selection

Filter 4 takes each chamber's last kept floor vote, ordered by date. Every pick was then
confirmed against the Final Bill Report's own "Votes on Final Passage" list, and every
tally matches exactly.

- **Concurrences** (1710, 1750, 2225, 6002, 6026): the second chamber amended, and the
  originating chamber then voted on the amended text.
- **House Bill 1232 is the batch's one odd shape.** The Senate amended it, the House
  refused to concur and asked the Senate to recede, and the Senate then receded and
  passed the House's own text 29-19. So the House's only floor vote, 56-38, is a vote on
  the text that became law, and no version problem arises. The Senate roll is captioned
  "Senate Final Passage without Senate Amendments", and its description says the Senate
  passed the bill after dropping its own changes.
- **Senate Bill 6081** took one vote in each chamber with no amendment.

## ⚠ The duplicate sweep was wrong in both directions, and had to be done by hand

The tally sweep proposed nine retirements. Only five survived review, one real duplicate
it had missed was added by hand, and six were retired in total.

**House Bill 1710 has two House votes with identical tallies.** The House passed it
57-39 with two excused on 12 February 2026, the Senate amended it, and the House
concurred 57-39 with two excused on 11 March 2026. Bill number plus tally cannot tell
them apart, so each hand-written record had to be read:

- Three name "third reading and final passage", which is the February vote, and one of
  those states the February date. **Left alone** — a different vote is a different claim.
- Two say only "the House passed it 57-39 with two excused" and cannot be pinned to
  either vote. **Left alone**, because a retirement needs a precise match and an
  ambiguous record does not give one.
- One names "final passage after Senate amendments", which is unambiguously the
  concurrence. It quotes no tally, so the sweep missed it. **Retired by hand.**

## ⚠ One bill number, three different Washington laws

The wider sweep over every live hand-written record naming these bill numbers returned
fourteen rows, and **none of them needed retiring**. Five are the House Bill 1710 rows
above. Three are prime-sponsorship records, which are not votes — the same finding the
batch-02 review made about Senate Bill 5917. One describes the undivided House vote on
Senate Bill 6002.

The remaining five all cite "ESSB 6002" and none of them is this act. Washington reused
that number for the 2014 supplemental operating budget and again for the 2018 Washington
Voting Rights Act. **A sweep on bill number alone would have retired five correct
records about two unrelated laws.**

## Reconciliation

- Dry run planned 637, the real run inserted 637, and the database holds 637 under the
  run stamp `2026-09-10T06:21:35.381Z`. Reconciled three ways.
- The dry run's own stamp matches **zero** rows.
- Convergence re-run: all 637 `unchanged`.
- 108 candidates, every member the crosswalk maps. 0 errors, 0 notified.
- Washington now holds **2,092 records over 39 approved rolls**.

## Writing

Reading level was measured BEFORE the import. A first draft measured **median grade 9.4,
worst 12.5**, so the two worst bodies were rewritten to **median 9.4, worst 10.4**,
longest sentence 33 words. The pipeline's own plain-language lint reported **0 warnings**
over all 24 descriptions.
