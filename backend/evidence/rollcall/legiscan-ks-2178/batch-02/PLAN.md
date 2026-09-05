# Kansas batch-02 — the first pass over the 76-roll pool

## Scope

Batch-01 imported 12 rolls. The survey worklist left **76 House rolls on 76 measures** marked
`candidate:batch-02` — a pool larger than some states' entire session. This batch works twelve
of them and leaves the rest, with their triage recorded, as `candidate:batch-03`.

**7 measures and 7 rolls are imported here. 5 are dropped. 64 remain for batch-03.**

Only the Kansas House is on the November 2026 ballot; Senate terms run to 2028. That is why
the worklist is House-only, and it is a fact about the Kansas calendar rather than a gap in our
rosters. The crosswalk maps 75 of the 125 representatives, so each roll reaches about 72
people — the largest fan-out per roll in this campaign.

## What is in this batch

| measure | area | vote |
| --- | --- | --- |
| HB 2088 fast-track permits act | housing_affordability | 83-40 |
| HB 2106 foreign money in constitutional amendment campaigns | anti_corruption | 94-25 |
| HB 2217 inspector general over cash and food assistance | anti_corruption | 87-38, veto override |
| HB 2304 local economic development incentive reporting | anti_corruption | 88-37 |
| HB 2593 contingent fee contracts for local governments | anti_corruption | 85-38, veto override |
| SB 299 supreme court nominating commission records | anti_corruption | 87-32 |
| SB 375 proxy advisor transparency act | corporate_accountability | 87-36, veto override |

## A check worth running on every Kansas measure

Kansas titles are unusually unreliable, and two different failures showed up.

**The title can describe a different bill entirely.** A mechanical comparison of each worklist
title against the enrolled act's own "AN ACT concerning…" line was run across all 76 measures.
Two came back with almost no shared content words, and one is a genuine swap: the worklist
lists **HB 2183** as "modifying elements in the crimes of sexual exploitation of a child", but
the enrolled HB 2183 is "concerning state agencies; relating to interpretation of statutes,
rules and regulations… prohibiting deference to the agency's interpretation." That is a
different subject. HB 2183 is dropped pending a check of which text the conference-report roll
actually voted. The other flagged measure, HB 2520, was a false positive — the title and act
agree, in different words.

**The title can describe a minor section and hide the bulk of the act.** That comparison passes
cleanly on **HB 2372**, whose title names "the crime of unlawful approach of a first responder"
— which is real, and is one section out of fourteen. The rest of the act is federal immigration
enforcement: authority for county jails to hold people on ICE detainers, state payment of
federal judgments against officers working under 287(g) agreements, state-court immunity for
them, a route for a sheriff to sign a 287(g) agreement without county commission approval, and
tort immunity for enforcing federal law or a federal executive order. **A title check alone
would not have caught this.** Reading the act did.

## Five measures dropped

| measure | reason |
| --- | --- |
| HB 2183 | Title and enrolled act describe different subjects. See above. |
| HB 2372 | The title names one of fourteen sections; the act is mostly federal immigration enforcement, a separate subject from the new crime. |
| SB 82 | Four subjects in one tax package, and the child care credit moves both ways — the rate rises from 30% to 75% while refundability is removed. |
| HB 2497 | Restates the home-loan prepayment penalty ban so it expressly covers consumer-purpose loans. Whether that broadens or narrows the old "any home loan" wording cannot be told from the extracted text. |
| HB 2652 | The title advertises only what the act adds. It also deletes the district court's 120-day decision deadline and the process that enforced it, replacing it with a publication duty for overdue appellate cases. |

HB 2652 is the one to look at twice. Publishing a monthly list of appellate cases undecided
after six months is real transparency. Deleting the district court deadline machinery removes
an accountability tool covering the courts that hear most Kansas cases. Those are opposite
directions of comparable weight inside the same subject.

## Getting the enrolled text

The Kansas LegiScan dataset often has no Enrolled text record. This URL pattern works and was
used for every act read here:

```
https://kslegislature.gov/li/b2025_26/measures/documents/<bill lowercase>_enrolled.pdf
```

## Ledgers

`judgments.json`, `judge-report.json`, `import-dry-run-report.json`, `import-report.json`,
`import-rerun-report.json`, and the stored roll evidence. Standard output was sent to
`/dev/null` on every import run, so each report is the importer's own full-form file.
