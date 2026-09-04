# Alaska batch-03 — divided votes on bills that passed one chamber only

## Scope

These bills never reached the governor. Each passed the chamber that voted on it, on a split
vote, and then died because the other chamber never voted. They are not law, and no part of
them ever took effect.

This is a smaller and thinner pool than batch-02, and it is included because a vote on a bill
that died still tells a voter what their legislator wanted. The wording carries a heavier
burden here, so every description is conditional and every one ends by naming the chamber
that did not act.

## The pool

Twelve measures carried 17 divided floor rolls after identity duplicates were collapsed.
Eight measures were dropped, one was excluded as an appropriations bill, and three procedural
concurrent resolutions — authorizing a recess, carrying bills over to a special session, and
suspending the uniform rules — were excluded because they carry no policy subject at all.

## Selection

The same five filters as batch-02, with filter 2 read as "passed the chamber that voted"
rather than "became law". Filter 4 still matters: the roll must sit on the version that
chamber actually passed, not on a later committee substitute the other chamber produced.

**SB 111 is the case that shows why.** The Senate passed `CSSB 111(L&C) am` on 2026-05-11.
The House Labor and Commerce Committee then produced its own substitute, dated 2026-05-15,
which is the latest text LegiScan lists. The description here is written from the version the
Senate voted, confirmed against the journal line "and so, CS FOR SENATE BILL NO. 111(L&C) am
passed the Senate."

## Result

**4 measures, 4 rolls.** Three House rolls and one Senate roll.

| measure | chamber | research area |
| --- | --- | --- |
| HB 20 no extra fee for a paper bill | House | cost_of_living_reduction |
| HB 58 independent public advocate | House | anti_corruption |
| SB 111 consumer right to repair | Senate | corporate_accountability |
| SB 250 who pays for a data center's power | Senate | cost_of_living_reduction, environment_and_public_health |

SB 250 carries two labels because it has two strands that both run the same way: keeping the
cost of serving a data center off other utility customers, and requiring a decommissioning
plan, a water plan and limits on fossil-fuel backup power.

## Measures dropped, and why

| measure | reason |
| --- | --- |
| HB 261 education funding | Moves both ways on the same question. It adds a "greater of three counts" cushion and mid-year catch-up payments, and repeals the five-percent enrollment-decline hold-harmless and the year-end recomputation. |
| HB 381 gas project tax and AGDC | Eight strands, and openly opposite tax directions: property tax on the project is abated and replaced by a throughput tax, while a new income tax up to 9.4% lands on other oil and gas pass-through businesses. The House roll is also a failed conference report, 19-19. |
| HB 194 oil and gas income tax | Two unrelated halves: the new pass-through tax, and ratification of one named Marathon royalty-oil contract the bill does not reproduce. |
| SB 83 telehealth parity | Telehealth payment parity plus an unrelated section moving a 2022 session-law effective date from 2030 to 2040, which the bill never restates, so it cannot be described from the text. |
| SB 170 gaming and electronic pull-tabs | Deletes the 15% adjusted-gross-income floor while adding a new revocation ground, lifts the ban on broadcast promotion while capping marketing gifts, doubles prize caps while tightening game mechanics. |
| HB 125 Board of Fisheries | Two strands: mandating interest-group seats on the board, and deleting the rural-domicile requirement from the subsistence definitions. The second changes who gets subsistence priority statewide and does not run the same way as the first for rural users. |
| SJR 2 veto override threshold | A constitutional amendment on how many votes it takes to override a veto. No research area covers the balance between the legislature and the governor. |
| HB 56 supplemental appropriations | Appropriations bill; excluded by the campaign rule. |

## Ledgers in this directory

- `judgments.json` — the approved judgments. SB 250's carries
  `acknowledge_later_rolls: [1699327]`, because the Senate passed the bill 13-5, a senator
  gave notice of reconsideration, and the vote that stands is the 14-5 passage on
  reconsideration the same day.
- `judge-report.json`, `import-dry-run-report.json`, `import-report.json`,
  `import-rerun-report.json` — the run ledgers.
- `ls-ak-*.json` — the stored roll evidence.
