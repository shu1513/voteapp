# Maryland — code findings (recorded, NOT fixed)

## 1. The crosswalk proposer's one-to-one rule rejects a candidate who holds two candidacies

`proposeLegiscanCrosswalk` proposes a pair only when the person matches exactly
one candidate **and** that candidate matches exactly one person. Its candidate
pool comes from `loadLegiscanStateLegCandidates`, which selects
`DISTINCT candidate_id, name, scope, district_name` — so **one candidate running
for two seats appears as two pool rows**, the person then matches two rows, and
the pair is dropped.

Proven in Maryland by **Nicholaus Kipke** (people_id 4606): a sitting Delegate
in HD-031 who is also on the 2026 ballot for Senate District 31, so he holds a
`state_lower` and a `state_upper` candidacy under one candidate id. Exact name
match on both rows, and the proposer declined. Hand-added.

This will recur in any state where the whole legislature is up at once and
sitting members run for the other chamber — exactly Maryland's shape. It is not
a correctness bug (the human review catches it, and the conservative rule is the
right default), but the report gives no hint why the pair was dropped. A cheap
improvement would be to collapse pool rows by `candidate_id` before counting, or
to surface a `reason` on `unmatchedPeople`.

Scope check: only 2 Maryland candidates have two pool rows (Kipke and Brent
Mulrooney, who has no LegiScan person), so this cost exactly one hand-add here.

## 2. Three exact-name proposer misses left undiagnosed

`Bob Long` (17424, HD-006), `Tom Hutchinson` (24535, HD-037) and `Jim Rosapepe`
(4731, SD-021) each have a single, exactly-matching, otherwise-unclaimed pool row
and were still left unmatched — and unlike Kipke, none of them has a second pool
row, so §1 does not explain them. They were resolved by hand and the crosswalk
notes say so. Recorded here rather than guessed at: reproducing the three is a
few minutes of work against `proposeLegiscanCrosswalk` and would probably explain
a handful of hand-adds in every past state too.

## 3. LegiScan's Maryland people file carries 27 sponsor pseudo-people

`MD/2025-2025_Regular_Session/people/` holds 216 records, but 27 of them are not
legislators: they are sponsor placeholders for standing committees and county
Senate delegations (`Ways and Means`, `Baltimore City Senators`, `Pensions`),
each with a blank `district` and a `role` of Rep / Sen / Jnt. `resolveLegiscan…`
already filters them (`peopleMembers` = 189), which is correct.

The only rough edge: a crosswalk that lists them — the natural thing to write if
you enumerate the people file — has all 27 reported back as
`crosswalkPeopleNotInSnapshot`. That list is informational and the run still
succeeds, but the committed `crosswalk.json` here deliberately excludes them, so
the field reads 0. Worth a sentence in the resolver's docs; no code change.
