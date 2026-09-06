# Tennessee run summary, LegiScan session 2161

Complete as of 6 September 2026. Batches 01 through 09.

| batch | rolls | measures | records |
| --- | --- | --- | --- |
| 01 | 14 | 12 | 826 |
| 02 | 10 | 7 | 646 |
| 03 | 13 | 11 | 864 |
| 04 | 12 | 11 | 861 |
| 05 | 11 | 10 | 774 |
| 06 | 8 | 7 | 496 |
| 07 | 7 | 7 | 463 |
| 08 | 10 | 9 | 454 |
| 09 | 4 | 4 | 244 |
| **total** | **89** | **78** | **5,628** |

Scope for every batch is `--scope-from 2026-08-01`, Tennessee's primary date,
per the scope note in `README.md`. Every batch reconciles three ways — report
total, run-stamp predicate, table delta — and sweeps clean for duplicates.

## Where the 289 divided-and-enacted rolls went

| disposition | rolls |
| --- | --- |
| imported across batches 01-09 | 89 |
| superseded by a later roll on the same measure and chamber | 7 |
| dropped on the version check | 2 |
| left open: ceremonial joint resolutions | 56 |
| left open: failed filter 3 or filter 5 | 135 |

## Findings recorded during the run

1. **Tennessee bill titles carry no subject.** Every caption reads "AN ACT to
   amend Tennessee Code Annotated, Title 49, relative to education." Triage runs
   on the dataset's per-bill `description`, which is written from the act as
   passed and begins "As enacted".
2. **But never label from that summary.** HB 612's summary says it "expands from
   wetlands to all areas that an aquatic resource alteration permit may apply";
   the act waives compensatory mitigation, which is the opposite direction.
3. **PDF byte size is not content size.** HB 500 grows from 10,826 to 74,708
   bytes between introduction and enrollment and is the same 1,930-character
   bill. Compare extracted character counts.
4. **Roll ids are not chronological, even inside one chamber.** On SB 229 the
   Senate's conference report vote is roll 1556906 and its earlier passage vote
   is 1556907. Read the bill history, not the ids.
5. **The scope flag is state-specific and silent when wrong.** Importing at the
   2026-11-01 default fanned both Senate rolls in batch-02 out to zero
   candidates. Read the session README's scope note first.
6. **56 of the divided-and-enacted rolls are ceremonial** joint resolutions
   honoring individuals. They pass the kept-bill-type and divided checks and
   carry no stance.
7. **Tennessee's divided education record mostly cannot be labeled.** It is
   school-choice financing and administrative machinery, plus curriculum
   mandates. Roughly 25 education measures were read and four were imported.

## Open for the operator

**HB 7003** (redraws the congressional districts, H 64-25) and **HB 7002**
(deletes the sentence barring changes between apportionments, H 66-24 / S 22-8)
are dropped under filter 5 and need a decision on direction. The Georgia maps
in `../legiscan-ga-2114/` could carry `civil_rights / for` because a federal
court had ruled the prior maps unlawful and these were the remedy. Tennessee's
was a mid-decade redraw with no court order, and the act is 141,000 characters
of census-block tables, so any direction would be an assertion about who the map
favors rather than something the text settles.
