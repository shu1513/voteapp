# Alaska batch-02 — how these votes were judged

## Sources

The enrolled act is the only ground truth. Every enrolled text was downloaded from
`www.akleg.gov` using the `state_link` on the LegiScan text record, so the document read is
the state's own PDF and not a LegiScan rendering. `legis.state.ak.us` fails TLS verification;
`www.akleg.gov` serves the same files.

Sponsor statements are advocacy and were not used, for any measure, at any point.

Two limits of the extracted text are worth stating. Alaska prints deleted words in
`[BRACKETS AND CAPITALS]` and new words underlined. Underlining does not survive text
extraction, so deletions are reliable and additions are inferred from context. Where that
mattered, the whole surrounding subsection was read rather than the bracketed words alone.
That is the lesson from batch-01: a description of what a law changed *from* is only as good
as the reader's grasp of the rule the deleted words were sitting inside.

## The version check, per roll

Alaska amends by committee substitute, and the substitute is dated when the committee offers
it, not when the chamber adopts it. For each selected roll the bill history was read to find
which version that chamber had in front of it, and the roll was kept only if that version is
the enrolled text.

Two rolls failed this check and were dropped:

- **SB 21, Senate, 2026-04-22, 15-4.** The Senate passed `CSSB 21(FIN)`. The House then
  amended it, and the Senate's concurrence vote on the enrolled text was 17-3, which is not
  divided. The Senate roll is superseded; only the House roll is used.
- **SB 24, Senate, 2025-05-12, 15-5.** Same pattern a year earlier. The Senate's vote on the
  enrolled text is the 2026-05-20 concurrence, 15-5, and that is the roll used.

**HB 70 was dropped entirely for this reason.** Its two divided House rolls, 29-8 and 32-8,
were cast in April 2025 on a House committee substitute. The Senate later rewrote the bill
under a new title, and the House concurred in the enrolled text 34-6, which is not divided.
Describing HB 70 from the enrolled act would have described a text those divided rolls never
saw.

## The rule applied on measures that pull both ways

A measure is dropped when a reader who cares about the named research area could reasonably
want to vote yes on one part and no on another part **of comparable weight**. A narrow
exception inside an otherwise one-directional bill is described in the record instead of
disqualifying it.

That rule kept SB 24, whose one contrary section lets people smoke cigars inside cigar
stores — real, and named in the description, but narrow beside raising the age to 21
statewide and taxing every vape. It kept HB 280, which preserves the old sourcing test for
one industry while switching everyone else. It dropped the measures below.

## Measures dropped, and why

| measure | reason |
| --- | --- |
| HB 16 campaign finance | Raises contribution limits (an individual could give $2,000 an election cycle instead of $500 a year) and repeals AS 15.13.068(b) and (c) on foreign-influenced corporate money, while adding an in-state address rule and deadlines for the ethics commission. Opposite directions of comparable weight inside anti_corruption. |
| SB 64 elections | Narrows accepted voter ID — the entire non-photo proof-of-address category disappears at the polls — and collapses two inactivation notices into one, while adding postage-paid return envelopes, an online ballot tracker and a formal cure process. Opposite directions inside election_integrity. |
| SB 39 payday and small loans | Raises the monthly rate ceiling on the $850–$10,000 band from 2% to 3% and deletes the mandatory 18-month examination cycle, while capping the previously uncapped $10,000–$25,000 band and repealing the payday loan chapter. |
| SB 54 interior designers | Four unrelated strands: a new credential, an eight-year board sunset extension, a gas-pipeline engineering exemption that reverses itself between two consecutive sections, and board housekeeping. |
| HB 79 Vic Fischer naming | Renames a university research institute and a marine park. No policy stance to take. |
| HB 78 public employee pensions | One coherent subject, and the largest measure dropped here. No research area describes public employee retirement benefits. |
| HB 93 hunting and fishing residency | No research area describes who qualifies as a resident for a hunting or fishing license. Same reason as HB 75 in batch-01. |

### Why HB 78 is dropped but SB 21 is kept

Both are about saving for retirement, so the line needs stating plainly.
`social_programs_and_welfare` covers a state program that serves the public. SB 21 creates
one: Alaska Work and Save reaches private-sector workers whose employers offer no retirement
plan at all. HB 78 sets the pension terms of teachers and state employees, which is
government employment compensation. No research area covers that, so HB 78 is dropped for
want of an area rather than for any defect in the bill or the vote.

## Wording

Every description is conditional and every description ends by saying the bill did not become
law. Where the legislature attempted an override and fell short — HB 52, HB 69, SB 21, SB 41
and SB 113 — the description says so, because a reader deserves to know the vote was tested
twice.

Alaska's veto overrides happen in joint session. Batch-01 excluded that family of rolls from
the config after finding that two of the five override rolls are 60-member joint-session
rolls filed under the House, and that the other three report tallies which do not match the
official joint result. Nothing here changes that; the override outcome is stated in prose
rather than imported as a vote.

## Checks run before importing

| check | result |
| --- | --- |
| Plain-language lint, 45-word sentence cap | 44 descriptions, **0 warnings** |
| Flesch-Kincaid grade | median **7.2**, worst **8.1** |
| Longest sentence | 24 words |
| American spelling scan | 0 hits |
| Horse-race language scan | 0 hits |
| Banned areas (`general`, `impartiality`, `legal_competence`) | 0 used |
| Tally oracle: every roll's yea-nay against the bill history action line | **22 of 22 confirmed** |
| Journal audit: every member name against Alaska's own journal | **0 on the wrong side** |

The journal audit could not place seven names automatically, all of them the last name on a
side, where the parser glues the name to the prose that follows. Each of the seven was read
by hand in the journal text and confirmed on the side LegiScan reports: Robert Yundt on
HB 133, HB 69, SB 24 and SB 111; Cathy Tilton on HB 25 and HB 26; Mike Shower on SB 113.

## Reconciliation

Predicted independently from the crosswalk and the roll evidence, before touching the
database: **65 records and 48 area tags**.

| source | records | tags |
| --- | --- | --- |
| independent prediction | 65 | 48 |
| importer dry run | 65 insert | — |
| importer real run | 65 insert, 0 errors | — |
| database, this run's stamp | 65 | 48 |

The dry run's stamp `2026-09-04T20:28:50.206Z` matches zero rows. The real run's stamp is
`2026-09-04T20:29:35.072Z`. The convergence re-run reported all 65 unchanged.

## Related records

Three rolls flagged one pre-existing hand-researched record each: HB 133, HB 25 and SB 24.
All three are sponsorship records — a legislator introducing or leading the bill. Leading a
bill and voting on it are different facts about different people, so none was retired.
