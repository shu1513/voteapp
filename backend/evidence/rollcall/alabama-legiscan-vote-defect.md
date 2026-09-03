# Alabama: LegiScan swaps two pairs of same-surname members

Found by running the Montana audit method against Alabama. **This one reached
live records**, which the Montana defect did not.

## What is wrong

LegiScan's Alabama feed swaps the votes of two House members who share a
surname:

- **Mary Moore** (Democrat, House District 59) and **Parker Moore** (Republican,
  House District 4) — on **22** audited rolls.
- **Randy Wood** and **Debbie Wood** — on **2** audited rolls.

The swap is complete, not sporadic. On every audited roll in the 2023 regular
session, the 2023 second special session and the 2024 regular session where the
two Moores voted differently, LegiScan has them the wrong way round. Outside
those sessions LegiScan is right: the two differ on 28 other audited rolls with
no errors.

The likely cause is visible in Alabama's own data. Alabama prints the name as
**"M. Moore"** on these roll calls, and LegiScan appears to resolve that
abbreviation to the wrong member.

## Worked example, verified by hand

2023 regular session, House, Alabama roll call 125, SB 1, 11 April 2023. Both
sources agree the tally was 79-24.

Alabama's own record, from
`https://gql.api.alison.legislature.state.al.us/graphql`:

    Parker Moore = Y
    M. Moore     = N

LegiScan's stored evidence for the same vote:

    Mary Moore   = Yea
    Parker Moore = Nay

Exactly swapped.

## Which source is right

Alabama's. On the 24 disputed rolls, Alabama's coding puts Mary Moore with her
own caucus 22 times out of 24 and Parker Moore with his 24 out of 24. LegiScan's
coding puts each of them against their own caucus on essentially every one. A
member voting against their caucus on almost every contested bill in two
consecutive sessions, and never outside them, is not a plausible voting record.

The Wood swap has the same shape but both members are Republicans, so the
caucus test cannot separate them. That pair rests on the pattern alone and is
recorded as inferred.

## What was done

**46 live candidate records were retired** with
`npm run manual:records:retire`, which is a soft retirement — it sets
`retired_at` and `retired_reason` and is reversible with `--unretire`. The
breakdown:

| Member | Records retired |
| --- | --- |
| Mary Moore | 22 |
| Parker Moore | 22 |
| Randy Wood | 2 |

Each retirement reason names the session, bill, date and Alabama roll-call
number, states what Alabama's record says and what LegiScan says, and cites the
endpoint.

The other roughly 2,000 records on those same 24 rolls are untouched. Only the
two members in each swapped pair are wrong; everyone else on the roll is
recorded correctly.

## Scope of the check

All **137 approved Alabama rolls** were compared, covering all 12 LegiScan
sessions from 2019 to 2026. 134 paired with an Alabama vote; 3 could not be
paired confidently and were reported rather than guessed — several near-identical
votes on one bill on one day cannot be told apart.

Session mapping, each confirmed by query: 1621=2019RS, 1706=2020RS, 1756=2021RS,
1836=2022RS, 1854=2021FS, 1857=2021SS, 2014=2023RS, 2060=2023SS2, 2103=2024RS,
2148=2025RS, 2218=2026RS, 2262=2026SS1.

**LegiScan's `roll_number` for Alabama is its own internal id and does not map
to Alabama's `rollCallNbr`.** But Alabama's number is recoverable: LegiScan
writes it into the description, as in "Motion to Read a Third Time and Pass Roll
Call 243". 91 of the 137 carry it that way; the rest were paired by member
agreement.

Tooling: `/Users/shu/legiscan-data/al_verify.py`, with findings in
`al-verify-findings.md` and machine output in `al-verify-report.json`.

## Two cautions for anyone repeating this

**Alabama's own data has its own problems.** Six tallies disagree with Alabama's
member list, and only two of those are LegiScan's fault — the other four are
Alabama publishing a header count and a member list that contradict each other.
Neither Alabama number is trustworthy on its own.

**Alabama's API substitutes current district holders into historical rosters.**
Two names never matched: Mac McCutcheon (43 rolls) and Becky Nordgren (24). In
McCutcheon's place Alabama's 2019–2022 lists print **Phillip Rigsby**, the
current holder of House District 25. So Alabama's old-session names cannot be
relied on. Neither unmatched name was counted as agreement — an unmatched name
must always be reported, never treated as a match.

## What this means for the other states

Montana's defect never touched an imported record. Alabama's touched 46. The
difference is luck, not method, and the lesson is that the check has to be run
per state before that state's records are promoted.

The specific failure mode — two members sharing a surname, one of them printed
in an abbreviated form — is narrow and easy to test for. **Any chamber with two
same-surname members should be treated as suspect until checked.** The states
that can be checked, and how, are in
`cross-state-vote-audit-feasibility.md`.
