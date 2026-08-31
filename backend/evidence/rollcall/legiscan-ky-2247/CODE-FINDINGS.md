# Kentucky 2026 session — findings recorded, not fixed

## 1. Two duplicate rolls survive the exclusion rules

The 2026 dataset repeats 31 roll calls: one chamber and sequence number under
two different `roll_call_id`s, with the same bill, date and tally. The shared
identity key in `fetchLegiscanRollCallVotes.ts` includes the description, so
two copies with different descriptions do not collapse, and each surviving id
would write its own record on every member.

Each pair names `House: Veto Override` alongside one of:

| Partner description | Pairs |
| --- | --- |
| `House: Third Reading` | 29 |
| `House: Adopt HFA 1` | 1 |
| `House: Co-Sponsor` | 1 |

Excluding the three partner spellings resolves 29 of the 31, because the
excluded copy is the one that is genuinely redundant. **It does not resolve the
other two.** For those, the copy the configuration keeps is labelled `House:
Veto Override` but is really a floor amendment and a vote to add co-sponsors.

This is not fixed in code, for the same reason the Texas duplicate fix was
scoped the way it was: relaxing the identity key to ignore the description would
re-open the double-record hazard everywhere else. The two rolls are instead
caught by the check every selected roll already gets against Kentucky's official
vote record, which names the real question. Neither is divided and enacted, so
neither is reachable by the current selection filters.

If a future change makes the identity key description-insensitive, re-measure
Kentucky 2026 first: it is the only session in the registry where two rolls
differ in nothing but their description and their id.

## 2. The 2025 rules and the 2026 rules are opposites, by measurement

`House: Third Reading` is a kept floor vote in session 2179 and an excluded
duplicate in session 2247. `House: Veto Override` is the reverse. This is
pinned by a test so that a later edit cannot quietly align the two entries on
the assumption that one state should have one vocabulary.
