# California batch-08 — judging notes

Source and method unchanged from batch-07: digest plus operative sections, every "as specified"
chased into the statute, completeness audit before judging.

## The pre-flight audit — 75 items, four real gaps

Every "This bill would…" clause listed **untruncated** and checked against the draft. Four gaps
were found and closed before any database write:

| measure | what the draft had left out |
| --- | --- |
| AB 1263 | a **private right of action** — a person harmed may sue for damages and an injunction |
| SB 53 | incident reports and risk assessments sent to the state are **exempt from public records requests** |
| AB 45 | the **research-records shield** against out-of-state abortion subpoenas; and the carve-out leaving police use of electronic monitoring data untouched |
| SB 766 | used cars sold **at auction** are outside the disclosure rule |

The SB 53 gap is the one that mattered most. A transparency act whose safety reports are themselves
closed to the public is making a real trade, and a voter should see both halves. It sat inside a
clause the digest wrote as a bare list.

## Label calls

| measure | label | why this direction |
| --- | --- | --- |
| AB 1263 | `gun_control`/for | The area is "regulate firearm access … to reduce gun violence". This closes the untraceable-manufacture route. |
| SB 1220 | `gun_control`/for | Adds serial-number tampering to the 10-year firearm prohibition. |
| SB 53 | `corporate_accountability`/for | "Hold companies accountable for legal compliance … and public impact" — published safety frameworks, incident reporting, whistleblower protection, AG penalties. |
| AB 45 | `data_privacy`/for | "Clear limits on collection, sharing, and misuse." `womens_reproductive_rights` was considered: the bill serves it, but the operative mechanism is a location-data rule, and the reproductive-rights area is already covered by SB 497. |
| SB 1418 | `election_integrity`/for | "Secure, accurate, auditable" — this is the auditable half: material must survive until any contest or investigation ends. |
| AB 1411 | `election_integrity`/for | Registration outreach is the "trusted by the public" half. Judged `for` on the ground that it replaces a state-designed fallback with a published county plan and a state template. |
| AB 1167 | `anti_corruption`/for | "Prevent abuse of public office through transparency" — ratepayers stop funding utility political influence, and every public message must say who paid. |
| SB 766 | `corporate_accountability`/for | Consumer protection in plain form: no misrepresented prices, no useless add-ons, a three-day cancellation right. |
| AB 1261 | `immigration`/for | The area calls for a "lawful, orderly, and humane system". Counsel for immigrant youth in removal proceedings is procedural fairness within that system. |
| SB 771 | `corporate_accountability`/for | Extends existing civil rights liability to a platform whose algorithm relays the violating content. |

## Traps caught while reading

- **AB 1411 repeals as well as creates.** The Secretary of State no longer sets minimum program
  requirements or designs a county's program when it falls short; counties write their own plans
  instead. The description says both halves, because "requires counties to do outreach" alone would
  hide that a state backstop was removed.
- **AB 1261 is funding-conditional**, and the department must weigh federal money already received
  when sizing awards. Both stated.
- **SB 53's thresholds are technical and load-bearing.** A "frontier model" is one trained with more
  than 10^26 operations; the heavier duties apply only to developers above $500 million in annual
  revenue. Without those, the description would read as regulating all AI.
- **SB 53 preempts local rules** on frontier catastrophic risk adopted on or after January 1, 2025.
  A bill that takes power away from cities while granting it to the state is doing two things, and
  the description says so.
- **SB 1220 is prospective**, applying to convictions on or after January 1, 2027.
- **SB 766 starts on October 1, 2026** — it is law but not yet in force.
- **SB 771 was written as an addition to existing civil rights law**, not a new speech rule; the
  description keeps that framing and gives both penalty tiers.

## The `nay` side

Every label states `"nay": null`. Voting against one firearm-manufacture bill is not evidence of
opposing gun control as a goal, and the same logic holds across the batch.

## Runs

| step | result |
| --- | --- |
| plain-language lint | 20 descriptions, **0 warnings**, longest sentence 44 words |
| `rollcall:judge --dry-run` | 10 `dry_run` |
| `rollcall:judge` | 10 `updated` |
| `rollcall:legiscan:import --dry-run` | **107 planned inserts**, 4,366 unchanged, 0 errors |
| `rollcall:legiscan:import` | **107 inserts**, 4,366 unchanged, 0 errors, 0 notified |
| re-run `--dry-run` | **4,473 unchanged**, 0 errors |

**Reconciled three ways.** `candidate_records` went 120,802 → 120,909 (+107); the run's predicate
`origin_run_id LIKE 'rollcall:CA:%:2172:%:2026-08-31T18:20:21.664Z'` returns 107 rows across 11
candidates; the DRY RUN's stamp `2026-08-31T18:19:56.659Z` matches **zero** rows.

**A run that did nothing, recorded because it looked like one that did.** The first import attempt
failed on shell quoting — the flags reached the script as a single argument and it exited on an
unknown flag. Nothing was written and the row count was unchanged, which is exactly what a
fail-closed CLI should do. Noted because "the import errored" and "the import half-ran" look alike
in a scrollback, and only the row count tells them apart.

California now holds **4,473 roll-call records across 80 candidates**, 72 measures, **17 of 27
research areas**. Prod untouched.
