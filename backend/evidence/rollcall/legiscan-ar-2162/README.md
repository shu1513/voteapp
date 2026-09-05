# Arkansas roll-call import — 2025 Regular Session (LegiScan session 2162)

Phase 4 of the roll-call vote import (`docs/plans/roll-call-vote-import.md` §5). Arkansas's
95th General Assembly met in regular session from January 13 to May 5, 2025. LegiScan's
dataset was cut on 2025-12-07, so the session is closed and the file is complete.

Everything here was written against the local development database only. Production holds
no Arkansas roll-call records.

## What the dataset holds

1,928 bills, 2,501 roll calls, 138 people (100 House seats, 35 Senate seats, plus
mid-session turnover and three committee sponsor placeholder rows).

Arkansas has the cleanest feed measured so far, tied with Georgia and Maryland:

- 0 repeated roll call ids and 0 rolls identical in every field but their id, so the
  duplicate-collapse fix built for Texas is a verified no-op here.
- 0 summary-only rolls: every roll carries a full member list.
- 0 tally mismatches: `total` equals yeas + nays + not voting + absent on all 2,501, and
  the member list length matches `total` on all 2,501.
- 0 committee votes. Every tally in the file is a whole-chamber tally, exactly 100 in the
  House and 35 in the Senate, so the floor-versus-committee check never has to decide.
- 0 parse errors, 0 file errors.

## The question vocabulary

2,501 roll calls collapse to 35 distinct descriptions, and 2,321 of them are the bare
string `Third Reading`. The full histogram is in `survey/`. The config entry in
`backend/src/pipeline/rollcall/legiscanStateConfigs.ts` carries the reasoning; the two
facts worth repeating here are:

**The chambers handle concurrence differently.** When the Senate amends a House bill, the
House takes a recorded vote worded `Senate amendment # 1 read and concurred in.` and stops.
When the House amends a Senate bill, the Senate concurs with no recorded vote and then takes
a SECOND `Third Reading` vote on the amended bill. Either way the chamber's last kept floor
vote is the one cast on the text that became law, which is what selection uses.

**The emergency clause is excluded.** Arkansas needs a two-thirds vote to attach an
emergency clause, and it is a separate question about when an act takes effect. Those rolls
sit right beside the passage roll on the same day with a similar tally, so mistaking one for
passage is easy. Missouri excludes its emergency clause for the same reason.

One roll is deliberately left unmatched so a human sees it: HJR 1004's failed adoption on
2025-04-01 (`Upon sounding of the ballot, the HJR failded of adoption`, Arkansas's own typo
included). It is a real vote but a one-off with no family behind it, and the measure failed,
so it falls outside the divided-and-enacted gate either way.

## The pool

Under the campaign's standard divided gate — the losing side at least a quarter of the
winning side — 2,449 kept floor votes give 204 divided rolls and 131 divided AND enacted
rolls on 95 measures. Two wider gates were measured first, because Arkansas has a
Republican supermajority the way Texas and Kentucky do: a fifth of the winning side gives
203 divided-and-enacted rolls on 136 measures, and 15 percent of the votes cast (the gate
Kentucky chose) gives 230 on 153. The standard gate already yields a healthy pool, so it
stands. Every gated roll is dispositioned in `survey/divided-enacted-worklist.tsv`.

## The crosswalk

`crosswalk.json`: 135 entries, 96 mapped, 39 explicitly null with a stated reason.
Validation over all 2,489 stored rolls: matched 113,692, no_crosswalk 0, out_of_scope 0,
0 rolls with no match at all, 0 file errors.

- 91 came from the proposer and all were accepted. Five of those are sitting House members
  running for a Senate seat in November 2026; each was checked for a party match and for
  other candidates now holding the House seat, so the seat is genuinely being vacated.
- 5 were added by hand. Four are the finding Pennsylvania, Connecticut, North Carolina and
  Indiana all recorded: LegiScan keeps the legal first name in `first_name` and the working
  name in `nickname`, and the proposer reads neither `name` nor `nickname`, so members whose
  LegiScan `name` is byte-identical to our candidate were still missed (`R. Scott` Richardson,
  `Justice Wayne` Long, Stephen/Steve Magie, Jimmie/Jimmy Gazaway). The fifth is the class
  the proposer can never reach: Fredrick Love, a sitting senator running statewide, so no
  state-legislative pool row exists for him.
- 39 are null. Most are structural rather than gaps: Arkansas staggers its Senate, so only
  17 of the 35 districts are on the November 2026 ballot.

**Fan-out: a House roll reaches a median of 80 candidates, a Senate roll 11.** All 100 House
districts are on the ballot; the Senate reach is the staggering, not a roster gap. House
rolls carry nearly all of the value, so batches are chosen House-first.

## Judging source

**The enrolled Act, fetched through LegiScan's bulk `getBillText` with the dataset's own
`doc_id` and verified against the returned `text_size`.** Arkansas publishes acts at
`arkleg.state.ar.us` (`/Acts/FTPDocument?...&file=<act>.pdf`, which redirects to
`/Home/FTPDocument?path=%2FACTS%2F2025R%2FPublic%2FACT<n>.pdf`), and that link is the
citation, but the site answers slowly enough to time out; the API route returns the same
bytes in seconds. Alabama's campaign learned the same lesson.

Neutrality was checked before anything was judged. An Arkansas act opens with its title, a
Subtitle written to state plainly what the act does, and the enacting clause. **There is no
sponsor statement of intent anywhere in the document**, so the advocacy hazard that makes
Texas's bill analyses dangerous does not arise here. Arkansas publishes no prose analysis
comparable to Ohio's LSC, Georgia's HBRO, Maryland's DLS or Connecticut's OLR, so the
enrolled Act is the source and it is read top to bottom.

## ⚠ The Arkansas hazard: amendments are printed in place

Every Arkansas act carries this line at the top of page 1:

> Stricken language would be deleted from and underlined language would be added to present law.

Deleted words are struck through and new words are underlined, and `pdftotext` renders both
as ordinary text. An extract therefore shows repealed law as though it were still in force,
and can invert an act. Act 116 makes the point in one sentence: the extract reads "shall
consider lack of diversity in ownership and financial interest in the geographic area at
issue in the permit application the benefit of competition to consumers", which is the old
rule and the new rule run together.

`/Users/shu/legiscan-data/ar-work/ar_text.py` resolves it. It asks the PDF where its drawn
lines sit relative to each character — a line about 0.54 of the glyph height above the
baseline is a strikethrough, one about 0.13 above it is an underline — and prints deletions
in `[[...]]` and additions in `<<...>>`. Text with no markup is existing law being reprinted,
not something the act changed. This is the same family as Kentucky's bold-font read, Georgia
and Maine's "render the page" rule, and Montana's renumbering tell.

**Read every act through the marked reader. Never write a description from a plain extract.**

## Layout

- `survey/` — the desc histogram, both fetch reports, the dispositioned worklist, and
  `DISPOSITIONS.md`, which accounts for every gated measure in the session.
- `crosswalk.json`, `legiscan-people-ar-2162.json`, `crosswalk-proposals-report.json`.
- `tools/ar_text.py` — the strikethrough reader described above, and `tools/READER-BRIEF.md`,
  the instructions every act reader worked from.
- `batch-01/` through `batch-04/` — PLAN.md, JUDGING.md, judgments.json, the roll evidence
  files and the import ledgers. Batches 02, 03 and 04 were one run and share a JUDGING.md.

The dataset and the full 2,489-file evidence directory live outside the repository at
`/Users/shu/legiscan-data/ar-2162` and `ar-2162-evidence`, following the precedent set for
Texas; only the curated subset is committed.

## The session is finished

All 131 divided-and-enacted roll calls are dispositioned, and nothing is left open:

| Outcome | Rolls |
| --- | --- |
| Judged and imported (batches 01-04) | 61 |
| Dropped after the act was read in full | 42 |
| Superseded by a later vote in the same chamber | 28 |

**Arkansas holds 2,708 records across 96 candidates and 2,131 tags, on 49 measures. Production
holds none.** 96 is every member the crosswalk maps, because Arkansas's Speaker votes.

Two records carry an expiry. SJR 11 and SJR 15 are on the November 2026 ballot, and their
descriptions are written in the conditional. **Both must be revisited once Arkansas votes**, the
rule Missouri's HJR 3 established.

## Sessions not yet worked

Arkansas's other two sessions in scope, both downloaded and neither surveyed:

- **LegiScan 2242**, the 2026 Fiscal Session (279 bills, 387 rolls).
- **LegiScan 2261**, the 2026 First Special Session (2 bills, 4 rolls).

Each would need only its own registry key (`AR-2242`, `AR-2261`) if its vocabulary turns out
to be a subset of this one, following Missouri, Maryland and Alabama. Neither is claimed to
be out of scope: they are simply not done, and Alabama's campaign showed what a stale
"not registered" note costs.
