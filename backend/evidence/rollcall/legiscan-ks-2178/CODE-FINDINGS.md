# Kansas (LegiScan 2178) — findings recorded, not fixed

## 1. Kansas constitutional amendments cannot be queued

Kansas proposes amendments to its constitution as **concurrent resolutions**
(`HCR 5004`, `SCR 1611`), which LegiScan types `CR`. The shared
`LEGISCAN_KEPT_BILL_TYPES` list is `B`, `JR`, `JRCA`, `CA`, and it is applied
before the state config is read, so every one of them is dropped as
`excluded_measure:CR` and can never reach the queue.

Measured cost: the session holds 62 `CR` measures with 41 roll calls, 32 of them
divided, on 12 distinct measures. Two of those are adopted amendments that went
to Kansas voters:

- **HCR 5004** — requiring voters to present photo identification (House
  adopted 90-28 on 2025-02-05, `by required 2/3 Majority`).
- **SCR 1611** — providing for the direct election of Kansas Supreme Court
  justices (Senate adopted 27-13 on 2025-03-06).

**Keeping `CR` wholesale would be wrong**, and this is the same shape as the
Georgia finding. The same type carries ceremonial resolutions (`SCR 1615`,
honoring Charlie Kirk, adopted 30-9) and applications to Congress for an
Article V convention (`SCR 1604`, `HCR 5022`), several of them divided. An
Article V application is not a law and never reaches a Kansas ballot, so
describing one as an amendment before the voters would be a false claim — the
Texas HJR 98 trap.

A real fix has to distinguish a proposed amendment from a ceremonial resolution
and from a convention application, which the bill type alone cannot do.

## 2. One roll's caption disagrees with its own tally

Because every Kansas description ends with ` - Yea: <n> Nay: <n>`, the caption
is a free checksum against the roll's structured `yea` and `nay` fields.
Comparing all 1,433 rolls that carry the suffix found exactly one disagreement:

- **roll 1661569**, House Substitute for SB 229, 2026-03-12. The caption reads
  `House Final Action - Substitute passed as amended - Yea: 85 Nay: 36`, while
  the structured fields and the member list both say **83** yeas. The member
  list is internally consistent (83 yea + 36 nay + 5 absent = 124 listed, of 125
  seats), so the caption is the odd one out.

SB 229 did not become law, so it sits outside the campaign gate and no record
was written from it. The check is cheap and should be run over any batch before
judging it — this is the North Carolina and Indiana lesson (audit tallies
against a second source), except that Kansas ships the second source inside the
same field.

## 3. Two descriptions are truncated at 250 characters

`House Motion to override line item veto failed; Line item veto Secs. a portion
of 46(a), 46(c), 47, …` and its `override selected line item veto prevailed`
sibling both stop mid-word at 250 characters, losing their tally suffix. Both are
line-item veto rolls, which this config excludes by rule, so nothing depends on
them. Worth knowing before anyone writes a parser that assumes the suffix is
always present.

## 4. Line-item veto overrides are excluded by rule, not by defect

Kansas votes separately on overriding individual line items of an appropriations
bill (`Motion to override line item veto prevailed; Line item veto 88(k), 88(m)
overridden`). These are votes on the vetoed **items**, not on the act, so
describing one as a vote on the bill would be a false claim. Every one sits on an
appropriations bill, which the campaign excludes anyway. This is the trap that
removed Kentucky's HB 2 from its 2026 pool.

## 5. ⚠⚠ Eleven rolls put a member on the wrong side

The tally the description embeds agrees with the roll's structured `yea` and `nay`
fields almost everywhere (finding 2), but **that pair can still both be wrong**.
Comparing every roll against the tally Kansas prints in its own bill history found
**11 disagreements out of 1,386 rolls that could be matched** (1,375 exact, 49 with no
matching history line).

The clearest one was checked against a third source, Kansas's published roll call:

- **SB 63, House motion to override the veto, 2025-02-18, roll 1491886.** LegiScan
  says 84-35 with 6 absent. The bill history says 85-34, and the state's own vote
  record at `/b2025_26/vote/?apn=…0078_SB63.odt` says **Yea 85, Nay 34, Absent 6**.
  Diffing the member lists name by name shows LegiScan lists **Rep. Bob Lewis as a
  nay when he voted yea**. Every other difference between the two lists is a first-name
  spelling variant.

**Never import a roll that fails this check.** `applyLegislativeVoteJudgment` requires
the description to contain the stored `<yeas>-<nays>`, so an import would publish the
wrong number and, worse, would write a record saying a named legislator voted the
opposite of how they did. This is the Indiana finding (CODE-FINDINGS §2 there) and the
North Carolina finding 4 recurring in a third state.

The eleven, with LegiScan's tally first and the bill history's second:

| bill | chamber | date | roll | LegiScan | Kansas |
| --- | --- | --- | --- | --- | --- |
| HB 2007 | Senate | 2025-03-18 | 1520565 | 28-12 | 27-13 |
| HB 2060 | Senate | 2025-03-20 | 1523869 | 36-4 | 35-5 |
| HB 2164 | Senate | 2025-03-20 | 1523332 | 37-3 | 38-2 |
| HB 2164 | House | 2026-04-09 | 1679592 | 38-85 | 40-83 |
| HB 2240 | House | 2025-04-10 | 1543581 | 87-38 | 88-37 |
| HB 2444 | Senate | 2026-03-19 | 1666167 | 35-5 | 34-6 |
| HB 2739 | Senate | 2026-03-19 | 1666842 | 39-1 | 38-2 |
| SB 63 | House | 2025-02-18 | 1491886 | 84-35 | 85-34 |
| SB 197 | House | 2026-03-27 | 1671816 | 74-49 | 75-48 |
| SB 254 | House | 2026-03-19 | 1666091 | 77-47 | 78-46 |
| SB 356 | House | 2026-03-26 | 1670877 | 105-19 | 99-25 |

SB 356 is off by six votes, the rest by one or two. Two are in the campaign gate
(SB 63 and HB 2240) and are held in the worklist.

**The check is cheap and should be part of every Kansas batch.** LegiScan's own
`history[]` array carries the state's tally, so it costs no network requests: match a
roll to the history line with the same date, chamber and question text, and compare.
