# Arizona batch-03 — judging and import

## Sources

A vetoed bill has no chaptered law, so the two anchors change:

- **The staff analysis stamped `Vetoed`** (House Bill Summary) or **`As Vetoed`** (Senate Fact
  Sheet). Arizona publishes these for a vetoed bill exactly as it publishes `Signed` analyses
  for one that became law, so the version question is still answered in the document's own
  title. Neither carries a sponsor statement of intent.
- **The last engrossed print** as the final text — the version the Governor was sent. It carries
  the same machine-readable edit markup as a chaptered law (`<span class=O>` deleted,
  `<span class=UP>` new), so change extraction works unchanged.

`az_docs.py` was extended for both: the stage matcher now accepts `Vetoed` and `As Vetoed`, and
the final-text fallback takes the last engrossed print when no chaptered text exists.

**One fetch defect, worth recording.** Arizona publishes at least two documents whose filename
contains a literal space — `H.HB2576_020525_VETOED .DOCX.htm`. `curl` will not fetch that
unescaped, and the failure looks like a missing document. The fetcher now percent-encodes
spaces. Two documents were recovered that way, and one of them changed a decision: see below.

## A drop reason corrected in an earlier batch

HB 2112, the age-verification measure, was dropped in batch-02 with the reason "child
protection against the privacy cost of identity checks". Its analysis had failed to download
because of the space defect. Once read, that reason turned out to be wrong on its own terms:
the act **bars anyone performing age verification from retaining identifying information**, bars
transmitting it to any government entity, and gives a private right of action to a person whose
data is retained. The privacy objection is answered inside the act.

The measure stays dropped — no research area covers minors' access to sexual material — but the
recorded reason on `../survey/divided-signed-worklist.tsv` has been corrected. The campaign's
Kentucky HB 424 rule applies: when re-reading a measure, check the old drop reason, because a
drop is a judgment that can be wrong in its stated grounds even when right in its outcome.

## Vote and version checks

All 58 rolls were confirmed to be on the text the Governor was sent, using batch-01's rule: a
`Final Read` line in the bill history means the second chamber amended, and the originating
chamber's earlier roll is on a superseded draft.

**SB 1001 is a reconsidered pair with a new wrinkle.** The House third reading failed 29-26 on
2025-06-24, the House reconsidered, and the bill passed 31-25 the same day. LegiScan stores the
failed roll with `passed = 1`, because it checks a bare majority of the votes cast. Arizona
requires a majority of the whole chamber — 31 of 60 in the House, 16 of 30 in the Senate. The
passing roll is judged and the failed one is acknowledged with a note explaining the order.

**That defect was then measured across the whole session: 25 rolls carry `passed = 1` while
falling short of a constitutional majority, and Arizona's own history records every one of them
as FAILED.** None of the 149 rolls approved across all four Arizona batches is affected, and the
reason is structural rather than lucky: selection always takes the *last* divided roll in a
chamber, and a failed vote is always followed either by a reconsidered passing vote or by the
bill dying, in which case it never enters a signed or vetoed pool. Written up as finding 6 in
`../CODE-FINDINGS.md`.

## Duplicate sweep, and six records retired

The importer reported 0 related flags, so the wider sweep was run as usual — but this batch
needed a stricter match than batch-02's, because 32 measures produce a lot of loose text hits.
The query joins each imported record to any live hand-written record for **the same candidate on
the same event date** whose text asserts a vote and names the same bill number.

That found six, and each was checked against the roll evidence before retirement. In every case
the side stated in the hand-written row matches the member's recorded vote:

| member | measure | chamber | stated | roll |
| --- | --- | --- | --- | --- |
| Analise Ortiz | HB 2062 | Senate | against | Nay |
| Carine Werner | HB 2438 | Senate | for | Yea |
| Kiana Sears | HB 2438 | Senate | against | Nay |
| Shawnna Bolick | HB 2438 | Senate | for | Yea |
| Wendy Rogers | HB 2438 | Senate | for | Yea |
| Gail Griffin | SB 1705 | House | for | Yea |

Retired with reasons naming the replacing record ids; the file is `duplicate-retirements.json`,
kept for a production run. Sponsorship rows and veto-message rows in the same sweep were kept —
they are a distinct claim from a vote. One sweep hit was a *different* HB 2440, from the 2026
session, about a prisoner transition program.

## Labels

Six area-and-direction pairs across four areas. `gun_control` and `immigration` are new to
Arizona. Every nay is stated and every nay is `null`.

## Writing checks

- Plain-language lint: **116 descriptions, 0 warnings.**
- Reading level measured separately: **Flesch-Kincaid median 9.9, best 7.1, worst 11.5**, mean
  sentence 16.4 words, longest 41. A first draft measured **median 10.4 and worst 15.3**, and
  ten bodies were rewritten before anything was imported.
- The builder gained two assertions for this scope: every body must contain "would have", and no
  body may contain a phrase asserting the measure took effect.

**⚠ The British-spelling checker was silently broken and had to be repaired.** An earlier edit
rewrote its regex through a shell heredoc and double-escaped it, so the pattern required a
literal backslash and matched nothing. The check passed on everything for two batches. It was
caught only because a spelling sweep over the committed evidence found `programme` in a
batch-04 body. The regex is now a raw string literal, verified against three cases before use —
it matches `programme`, and leaves `program` and `enrollment` alone. Batch-03 came back clean;
batch-04 had two, fixed and re-imported in place. **A checker that never fires looks exactly
like a checker that passes: assert that it catches a known-bad string before trusting it.**

## Import

| run | stamp | result |
| --- | --- | --- |
| dry run | `2026-09-05T04:12:23.960Z` | 1,463 planned inserts, 0 errors |
| real run | `2026-09-05T04:12:46.557Z` | **1,463 inserts**, 0 errors, 0 notified |
| convergence re-run | — | all 1,463 `unchanged` |

Reconciled three ways: Arizona's live roll-call records moved 1,090 to 2,553, a delta of 1,463;
the run-stamp predicate returns 1,463 records across 54 candidates; the dry run's stamp matches
zero rows.

**Production is untouched.**
