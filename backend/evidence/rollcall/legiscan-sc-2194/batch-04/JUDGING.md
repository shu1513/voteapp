# South Carolina batch-04 — H 5683, the congressional map the Senate did not take up

One measure, one roll. This is the last South Carolina roll left from the non-enacted
scope; batch-03 took S 933, S 508 and H 3558 and left this one behind.

## What the bill would have done

H 5683 is a mid-decade redraw of South Carolina's seven U.S. House districts. Read from
the text the House actually voted, the print dated 2026-05-20:

- **Section 1** adds § 7-19-35, listing the seven districts by county and voting district.
- **Section 2** repeals § 7-19-45, the map already in state law.
- **Section 3** keeps state boards, commissions, committees and authorities whose members
  are elected or appointed by congressional district on the **existing** lines.
- **Section 4** reopens candidate filing for the U.S. House for 2026 only, from noon on
  June 1 to noon on June 5, sets a special primary for August 18, 2026, and a runoff for
  September 1.
- **Section 5** takes effect on the Governor's approval.

## Why it is recorded with no stance

`general`, `yea: null`, `nay: null` — the standing user decision for mid-decade
congressional redistricting, matching Missouri HB 1 and the Tennessee HB 7002 / HB 7003
records already in the database. No research area describes redistricting, and Georgia's
maps only carried `civil_rights` because a court had ruled the prior maps unlawful, which
is not the case here.

## The roll, and why this one

The House recorded two votes both spelled `House: Passage Of Bill`, and **LegiScan dates
them both 2026-05-20**, so the judge could not order them and its superseded-stage gate
fired. The bill history separates them:

    2026-05-19  Read second time        Roll call Yeas-74 Nays-36   -> roll 1700318
    2026-05-20  Read third time         Roll call Yeas-74 Nays-37   -> roll 1700319

Roll 1700319 is third reading, the vote that sent the bill to the Senate, so it is the
decisive one. Roll 1700318 is listed in `acknowledge_later_rolls` for that reason. The
tallies, not the dates, are what tie each roll to its reading.

## The tail states no finality, on purpose

The 126th General Assembly is still sitting (`sine_die` 0). The Senate read the bill a
second time and then **continued** it on 2026-05-26, which carries it over rather than
killing it. The description says the bill "has not passed and is not law" and stops
there. It does not say the bill died.

## Checks

- `candidateRecordPlainLanguageLint`: 0 warnings over both descriptions.
- Flesch-Kincaid grade 7.1, longest sentence 34 words.
- No British spellings; no comma splice before the tally sentence.
- 105 records: the dry run planned 105 inserts, the run made 105, the re-run reported 105
  unchanged, and the live row count for roll 1700319 is 105.
