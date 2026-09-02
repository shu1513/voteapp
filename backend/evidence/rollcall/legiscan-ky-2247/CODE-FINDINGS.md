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
excluded copy is the one that is genuinely redundant. The other two turned out
to need no code change either, once each was looked at on its own:

- The **co-sponsor** twin sits on HR 21, a simple resolution. The shared
  kept-types list drops simple resolutions before this configuration is
  consulted, so neither copy is ever stored.
- The **floor amendment** twin is HB 84's RCS# 40. Kentucky's own record says
  RCS# 40 is `Adopt` — the adoption of House Floor Amendment 1, 81-8 — and the
  passage is the neighbouring RCS# 41. The `House: Veto Override RCS# 40` copy
  is therefore excluded by its exact sequence number, and a test pins both that
  exclusion and that RCS# 41 stays kept. Found by review of the config PR; the
  first draft of this file had wrongly described it as unreachable by rule and
  left it to the per-roll check at selection time.

Relaxing the identity key to ignore the description would have been the wrong
fix: it would re-open the double-record hazard everywhere else. If a future
change makes the identity key description-insensitive, re-measure Kentucky
2026 first: it is the only session in the registry where two rolls differ in
nothing but their description and their id.

## 2. The 2025 rules and the 2026 rules are opposites, by measurement

`House: Third Reading` is a kept floor vote in session 2179 and an excluded
duplicate in session 2247. `House: Veto Override` is the reverse. This is
pinned by a test so that a later edit cannot quietly align the two entries on
the assumption that one state should have one vocabulary.
