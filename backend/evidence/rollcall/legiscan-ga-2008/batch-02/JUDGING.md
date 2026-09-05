# Judging notes, Georgia 2023-2024, batch 02

Ten roll calls on seven measures, every one enacted.

## The version check, and a correction to how it is run

Every text each roll actually voted was downloaded through the LegiScan API and
verified by byte length and MD5 against the dataset manifest.

**PDF byte size is not a measure of content.** Five measures in this batch look
alarming by byte size and are fine, and one looks fine and is not. HB 500 goes
from a 10,826-byte introduced print to a 74,708-byte enrolled print, which reads
like a bill that grew sevenfold; extracted to text it is 1,928 characters
against 1,963, the same bill. The difference is embedded fonts. Every version
comparison in this batch was therefore redone on extracted character counts.

On that measure:

- **HB 500** — introduced 1,928 characters, enrolled 1,963. Both chambers voted
  the same bill, a year apart. Kept, both rolls.
- **SB 159** — the House passed the 15 March 2023 committee substitute, 7,422
  characters, and the enrolled Act is 7,448. The Senate's divided vote, on
  27 February 2023, was on the 15 February substitute, 3,687 characters, which
  is barely half the Act. **The Senate roll is dropped and only the House roll
  is imported.** The Senate's later vote on the final text, receding and
  agreeing on 22 January 2024, was 43-7 and not divided, so there is no Senate
  record for SB 159 at all.
- **HB 1409** — the Senate's divided vote on 28 March 2024 was on a 9,319
  character substitute; the House then amended it down to the 8,272 character
  Act, and the Senate agreed 53-2, which is not divided. The only divided roll
  is therefore on a text that is not the law. **Dropped.**
- **SB 362** (6,001 / 6,018 / 6,011), **SB 517** (3,156 / 3,173 / 3,166),
  **SB 414** (9,492 voted against 9,536 enrolled), **HB 557** (24,678
  conference print against 24,947 enrolled) and **HB 1339** (87,490 Senate
  substitute against 86,432 enrolled) all sit within normal enrollment drift.
  Kept.

## The vehicle-bill catch

**SB 344** was in the batch until its text was read. LegiScan titles it "Sales
and Use Taxes; firearms, ammunition, gun safes, and related accessories during
an 11 day period each year; exempt", and that is what the Senate passed 30-22 on
6 February 2024. The enacted Act does something unrelated: it excludes broadband
investment grants from corporate taxable income. The bill was gutted and used as
a vehicle. Dropped, exactly as SB 33 was in the 2025-2026 run.

## Supersession

No roll in this batch tripped the gate.

## Label reasoning

Every label uses `nay: null`.

- **SB 362**, `reduce_wealth_gap`, yea against. To keep state economic
  development incentives an employer must refuse to recognize a union on signed
  authorization cards alone, must withhold employees' personal contact details,
  and must repay every incentive for the project if it does either. Making union
  recognition harder cuts against workers' bargaining power, which is what this
  area measures.
- **HB 1339**, `healthcare_affordability`, yea for, and **HB 557** the same.
  One lets more facilities open by loosening certificate of need and raises the
  rural hospital tax credit; the other widens who may prescribe, which is a
  supply-side change in a state with prescriber shortages.
- **SB 517**, `civil_rights`, yea against. It converts what was a defense at
  trial into immunity from the case itself, criminal and civil, for officers
  whose force was justified or otherwise lawful.
- **SB 414**, `data_privacy`, yea for. It bars public agencies from collecting,
  requiring or releasing any list identifying a person as a member, volunteer,
  supporter or donor of a nonprofit, and makes improper disclosure a crime.
- **SB 159** and **HB 500**, `public_safety_and_crime_control`, yea for. Both
  raise criminal penalties.

## Descriptions

Each cites its own roll call's tally. Plain-language lint: 20 descriptions, 0
warnings, median Flesch-Kincaid grade 8.6, worst 10.

## Duplicates

Swept the 181 candidates who received records for any non-roll-call record on
the same measure and date. 0 found.
