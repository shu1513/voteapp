# batch-11 — what was decided and why

## AB 1743 is titled "Firearms" and is not a gun-control vote

The bill does not touch who may buy, own or carry a firearm. It widens who can request the crime-gun
tracing data the Department of Justice already collects. Tagging it `gun_control` would have
repeated the AB 1078 mistake in reverse: a label the text does not support. It is tagged
`public_safety_and_crime_control`, and the description says plainly that the bill does not change
firearm access.

## Two bills carry a provision that runs against their own thrust, stated in the description

- **AB 1336** creates a heat-illness presumption triggered by an employer breaking heat rules, then
  bars that finding from the state workplace safety appeals board. The description says so.
- **AB 1661** pays cash to households near the Inglewood oil field but makes community projects
  wait behind those payments. The description says so.

Neither reverses the bill's direction, so both keep a label. The test applied is the one from
AB 1078: a provision pushing the opposite way **on the same dimension** kills the label; a limit,
carve-out or delay is described instead.

## AB 1758 is small but real

A fee ceiling on travel sellers, raised to fund the body that repays defrauded customers. The
subject is narrow, but it is nameable and the stance is defensible under
`corporate_accountability`. It was kept rather than dropped for being unglamorous.

## Lint found a real defect in my own screen

The British-spelling screen caught "offence" in AB 1201. Checking the rest of the batch by hand
turned up "neighbouring", "neighbourhood" and "summarise" that the screen did not know about, so
the screen was widened and 16 spellings were corrected. The widened screen then produced a false
positive on "analysis" in batch-12, which is correct American English, so the pattern was narrowed
to the British verb forms only. Final: 44 descriptions, 0 warnings, longest sentence 38 words
against the 45-word limit.

## Reconciliation — three ways, all agreeing

| check | result |
| --- | --- |
| row-count delta | 4,794 -> 5,618 = **824**, the predicted insert |
| import report | `insert: 824`, `unchanged: 4794` |
| re-run dry-run | `unchanged: 5618`, zero inserts |

## Owed later

Ten of the eleven are `enrolled` and written in the conditional. They join the governor watch;
the last day to act is 2026-09-30. AB 1336 was vetoed and is written in the past tense already.
