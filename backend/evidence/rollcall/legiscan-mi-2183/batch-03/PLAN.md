# Michigan batch-03 — selection

LegiScan session 2183, the 103rd Legislature, 2025-2026.

**16 roll calls over 16 measures. 715 candidate records across 113 candidates.**
Six are House votes, which is where the reach is: a Michigan House roll fans out
to 93 candidates and a Senate roll to 16.

## This is the first batch of the not-enacted scope

Michigan's enacted pool closed in batch-02. This batch opens the scope used in
Pennsylvania and later in North Dakota and West Virginia: **measures one chamber
passed that the other chamber never voted on.**

The gate was checked per measure, not assumed. For every measure here:
- LegiScan status is not 4, and no history line records approval by the Governor
- the chamber's final floor roll is itself closely divided and is not a rejection
- **the other chamber held no floor vote at all**

The pool after that gate is **348 measures**, 202 House slots and 146 Senate. It
is much larger than the enacted pool was, and it is where Michigan's real
disagreements live. Divided government means the enacted pool held the things
both parties could agree on; this pool holds the rest.

## ⚠ Every description here is conditional, and none claims the bill is dead

**The 103rd Legislature is still sitting.** It runs through the end of 2026 and
the dataset's last recorded vote is 2026-07-15. A bill that passed one chamber in
2025 can still pass the other.

So every description says what the bill **would** do, and every tail is in the
present tense: "The Senate has not voted on it, so it is not law." None says the
bill died or failed. A builder guard enforced this: it refuses any description
containing "became law", "the act", "was enacted" or "it died", requires the word
"would", and requires the present-tense tail. It ran clean over all 16.

## The 16 roll calls

### Guns — the Senate's package, all four passed 2025-06-25
| measure | tally | area | a yes vote is |
| --- | --- | --- | --- |
| SB 224, bump stocks and multiburst trigger activators banned | 22-14 | gun_control | for |
| SB 225, no concealed pistols in the Capitol and the two legislative office buildings | 19-17 | gun_control | for |
| SB 226, no firearms at all in those same three buildings | 19-17 | gun_control | for |
| SB 331, serial numbers required on guns, frames and receivers | 19-17 | gun_control | for |

### Elections — the House's bills
| measure | tally | area | a yes vote is |
| --- | --- | --- | --- |
| HB 4765, citizenship checked before registering, federal-only voters otherwise | 58-46 | civil_rights | against |
| HB 4720, voting systems may not contain parts from firms on the federal security list | 63-41 | election_integrity | for |
| HB 5845, poll book flash drive data kept for 22 months | 62-45 | election_integrity | for |

### Elections — the Senate's bills
| measure | tally | area | a yes vote is |
| --- | --- | --- | --- |
| SB 961, the State Voting Rights Act | 20-17 | civil_rights | for |
| SB 963, the Language Assistance for Elections Act | 20-17 | civil_rights | for |
| SB 964, local governments must report election changes to the state, plus curbside voting, chosen language helpers and food in line | 20-17 | election_integrity, civil_rights | for |
| SB 533, civil fines for lies told to stop someone voting | 19-14 | election_integrity | for |

### Civil rights
| measure | tally | area | a yes vote is |
| --- | --- | --- | --- |
| HB 4024, school restrooms and changing areas used by birth-certificate sex | 58-46 | civil_rights | against |
| HB 4469, school sports eligibility by birth-certificate sex | 59-45 | civil_rights | against |
| HB 4664, a crime to block a highway while taking part in an assembly | 61-42 | civil_rights | against |
| SB 34, lactating status added to the meaning of sex in the civil rights law | 22-14 | civil_rights | for |
| SB 30, the state to collect reports of biased perinatal care | 22-14 | civil_rights | for |

No label carries a direction for a no vote.

## Where the areas fall

`election_integrity` is used where the measure is about a secure, accurate or
auditable count: banned equipment parts, record retention, reporting election
changes, and punishing lies about how to vote. `civil_rights` is used where the
measure is about **who may take part and on what terms**: the citizenship
requirement, the Voting Rights Act, language assistance. That split follows the
standing rule that voting-access measures are civil rights, not election
integrity.

## Dropped from the slice worked here

- **HB 4707, banning ranked choice voting.** A method of election has no honest
  direction under either elections area. This follows the Oregon HB 3908 call on
  nominating methods and the North Dakota rule that a measure about how lawmaking
  or elections are structured is not a for-or-against question.
- **HB 4980**, letting a person apply for a concealed pistol license in any
  county rather than their own. It changes where you apply, not who may carry.
- **HB 5515**, the definition of a dangerous weapon. It adds a six-inch floor to
  "any other dangerous weapon" and widens the hunting-knife exception, narrowing
  when concealed carry of a blade is a felony. That reads as weaker enforcement
  or as ending overcriminalization, and the text supports both.
- **HB 4584**, moving school millage elections to November; **HB 4736** and
  **HB 5467**, candidate replacement deadlines; **SB 332**, sentencing guidelines
  for SB 331; **SB 129, SB 288, SB 420**, open meetings machinery. All fail the
  nameable-subject filter.
- **HB 4284, HB 5113, HB 5388** — concealed pistol license fees, a grace period
  for administrative-error denials, and the definition of self-defense spray. All
  narrow administrative changes.

## Deferred, not dropped
**HB 4356** and **HB 4602**. Both have only an "as Introduced" summary and
HB 4356 adopted a substitute on the floor, so the engrossed text has to be read
before either can be judged.

**HB 4066**, single-sex school sports teams, is in the pool and not yet worked.
See JUDGING.md — it is the companion to HB 4469 and passed the same day.
