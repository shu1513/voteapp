# What is left in the roll-call campaign, measured 2026-09-07

`session_coverage_audit.py` enumerates every LegiScan dataset on disk, applies the
shipped question config, keeps only each chamber's **last** kept roll per measure, and
reports how many of that roll's voters are current candidates. Its run for this date is
`session-coverage-2026-09-07.txt`.

Four sessions still hold rolls nobody has read. Three of them were read by hand for this
note and are closed out below; the fourth is being worked in another session.

## Arkansas 2026 Fiscal Session (AR-2242) — nothing importable

Twelve rolls look divided and enacted. **Ten are superseded**, and the reason is
structural: an Arkansas appropriation bill needs a three-quarters majority, so it is put
to the floor again and again until it clears. The early divided votes are the failed
attempts; the vote that made the law is near-unanimous.

    HB1023  68-19, 71-20  ->  final 92-4        SB41  21-58 (failed)  ->  final 75-18
    HB1053  71-19, 60-21, 60-27  ->  final 93-4 SB56  30-52 (failed)  ->  final 81-14
    HB1035  64-20  ->  final 83-13              SB59  72-19  ->  final 89-7
    HB1066  66-25  ->  final 80-14

The two survivors, **HB 1100 (Act 143) and SB 75 (Act 144)**, are the Revenue
Stabilization Law — the same act passed as House and Senate twins. Read in full: they
amend the general-revenue allocation code and move money into set-asides ($100M Medicaid
sustainability, $70M Children's Educational Freedom Accounts, $43.7M discretionary, $5M
motor vehicle, up to $150M economic-development closing fund).

**Dropped under filter 5.** A vote against the Revenue Stabilization Act is a vote
against funding state government for the year, not a position on any one of those
set-asides. The voucher money is the most contested line in the act, but the program was
created by the LEARNS Act in 2023; the RSA only refills its account, so a no vote here
does not record a member's position on vouchers. This matches the Delaware money-only
drops and Oregon's exclusion of 47 appropriations.

## Missouri 2025 First Extraordinary Session (MO-2216) — nothing importable

One roll survives: **SB 1**, Senate 23-10. LegiScan's title names only one line of it
("Department of Economic Development for the Missouri Housing Development Commission").
The enrolled act is a **capital-improvements omnibus** — $50M for a radioisotope center
at the University of Missouri research reactor, $55M for barns at the State Fair, state
park works, and more. **Dropped under filter 5**, as a bundle of unrelated
capital items with no single defensible stance.

## Alaska 30th Legislature, 2017-2018 (AK-1397) — not worth reading

33 divided rolls over 26 measures, and **one of its 63 voters is a current candidate**
(Neal Foster). Twenty-six acts read for one person's records. Left unworked on value,
not on principle; if Alaska's roster fills out for old incumbents, re-run the audit.

## Colorado 2026 Regular Session (CO-2243) — in progress elsewhere

328 rolls, 52 of 101 voters are current candidates. By far the largest pool left. A
parallel session was judging it while this note was written, so it is untouched here.
**The local database is shared between worktrees: check `legislative_votes` before
judging anything in Colorado.**

## Sessions that report zero, and why that is trustworthy now

The audit used to report zero for any session with no config entry, which is how a whole
Alabama session once hid in plain sight. It now falls back to the state's base patterns
and marks the row `~`, so an unregistered dataset is measured rather than skipped.
AR-2261, AL-1661, AL-1972, AL-2048, NM-2150 and NM-2232 measure a genuine zero.

## What that leaves

Nothing importable outside Colorado. The other standing items are not batches: Delaware
has 20 divided rolls on 11 bills still sitting with the Governor (re-checked 2026-09-07,
none had moved), and production promotion is untouched.
