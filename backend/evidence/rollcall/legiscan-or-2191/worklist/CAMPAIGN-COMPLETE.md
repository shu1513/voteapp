# Oregon 2025 — campaign complete

Every measure in the gated pool now carries a disposition. **Nothing is left
open, and nothing is parked.**

## Final state, local database

- **3,091 live records** across **61 candidates** — every candidate the
  crosswalk maps.
- **2,328 research-area tags.**
- **107 approved roll calls** over **65 measures**, in twelve batches.
- **Production holds zero Oregon roll-call records.** Promotion is the only
  remaining Oregon work for this session.

## The ledger, all 225 measures

| Disposition | Measures |
| --- | ---: |
| Judged and imported (batches 01-12) | 65 |
| Excluded, appropriations and fee ratification | 47 |
| Screened out on the summary, with reasons | 72 |
| Dropped under filter 5 after a full read | 41 |
| **Total** | **225** |

## HB 3908, the last measure to be settled

HB 3908 was held back for a direction call and has now been dropped under
filter 5. The reason is worth writing down, because the first reading of it
was wrong.

The Act changes one number in one statute. ORS 248.006 says an affiliation of
voters becomes a **major** political party when at least five percent of
registered voters are members; the Act raises that to ten percent. Nothing
else changes.

The first reading treated this as ballot access, which would point to
`civil_rights` under the rule that sends voter-access questions there. Reading
the enrolled Act and both neutral staff summaries shows that is not what the
Act does. Major and minor is a **nominating method**, not a place on the
ballot: a major party must nominate at the state-run primary, a minor party
nominates through its own process, and both summaries say a party keeps ballot
access separately from that classification. No party is removed from any
ballot.

So neither area fits. `election_integrity` is defined as a secure, accurate
and auditable count. `civil_rights` covers equal rights, anti-discrimination
and fair treatment, and the access rule that carried Colorado's SB 1 and
Arizona's HB 2017 into it is about who may vote and how easily, not about how
a party picks its nominees.

Two further facts show there is no defensible for-or-against side to write
down. The neutral summaries record **other minor parties supporting the
increase** — the group a restriction would be said to harm asked for it. And
the vote split both caucuses rather than dividing them: 31-26 in the House and
19-9 in the Senate, with Democrats and Republicans on each side.

A stance tag here would tell voters that 31 House members and 19 Senators took
a side in a policy fight that the record does not show. Filter 5 exists for
exactly that, so HB 3908 joins the other 40 drops.

## Five roll calls held, and why

LegiScan places one member on the wrong side of five Oregon rolls. All 393
divided-and-enacted rolls were audited against the tally Oregon's own bill
history prints: **388 match exactly, five are off by one.** Because the
journal names the nay voters, the misplaced member is identifiable — Girod on
SB 906's Senate roll, Boice on HB 2957's House roll.

All five sit in the config's `heldRollCallIds`, so they are surfaced and can
never be queued or approved. SB 906 was therefore imported House-only and
HB 2957 Senate-only. The other three were already out of scope.

## Areas covered

Fifteen of the twenty-seven research areas, led by
`environment_and_public_health` and `corporate_accountability`, then
`healthcare_affordability`, `housing_affordability`,
`public_safety_and_crime_control`, `civil_rights`, `public_education_quality`,
`cost_of_living_reduction`, `social_programs_and_welfare`, `immigration`,
`public_infrastructure`, `gun_control`, `reduce_wealth_gap`.

**Sixty-three of sixty-five measures score `for`.** Oregon was a Democratic
trifecta in 2025, so the divided-and-enacted set is the majority's agenda, and
the directions are the mirror image of the Republican-trifecta states. The two
exceptions both narrow a protection: HB 3550 takes minor league baseball
players out of wage law, and SB 1173 shields health facilities from product
liability.

## Recurring gap worth fixing upstream

**No research area covers labour or union questions.** It cost four measures
here — HB 2688 prevailing wage, HB 2944 collective bargaining penalties,
HB 3194 farmworker camp liability and HB 3789 union misrepresentation — and
the California campaign recorded ten drops for the same reason. SB 916 was
labelled `social_programs_and_welfare` and SB 426 `corporate_accountability`
only because each had a second, honest reading.

## What is left for Oregon

1. **Promotion to production**, the only remaining step for this session.
2. **HB 3908**, waiting on a direction call.
3. **The 2026 session**, a separate LegiScan dataset (2252) that would need
   its own `OR-2252` registry entry. Nothing here covers it.
