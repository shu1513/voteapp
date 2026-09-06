# How this batch was judged

A bill that never became law has no enrolled act, so **the print the chamber
actually voted is the ground truth**. For each selected roll I fetched the last
dated print on or before the vote date and read that.

## The bill summaries are not safe to use, and this batch proves it

Every Colorado print carries a summary at the front. It is written for the bill
**as introduced** and is not rewritten as the bill is amended. Reading the
operative text instead of the summary changed three descriptions here:

| measure | the summary says | the voted text says |
|---|---|---|
| HB 1235 | service attempted on **3 separate days** | **two** separate days |
| HB 1158 | vendor must remove ads within **3 days** | within **30 days** |
| HB 1011 | caps the wait list fee at **$25** | no fee cap at all — it was amended out |
| HB 1291 | fingerprint check, 10-hour driving cap, identity check before each ride, ride data given to complainants | **privately administered** check every six months; no hour cap; no per-ride check; regulators set complainant data-access rules |
| SB 86 | suspend or close an account after any reported rule or law breach | only for four **subject uses**: illicit-substance sales or ads, unlawful gun sales, sex trafficking of a minor, sexually exploitative material |
| HB 1122 | licensed driver must **sit in the driver's seat** | must be aboard, monitoring and able to intervene; the driver's seat only **while hazardous materials are carried** |
| HB 1235 | jury-trial right for every tenant; remote jury appearance by **phone or video** | **public-housing tenants excluded** (court trial only); remote jury appearance by **video** only |
| HB 1004 | the state must run a **public education campaign** | no campaign at all — it was in the introduced bill only and was amended out before the first vote |
| HB 1011 | 60 days' notice before layoffs or enrollment changes | notice only for layoffs or enrollment changes that **follow the takeover** |
| HB 1079 | (drafted as) the **top hired official** | two different tests: a school district's **superintendent** (or designated head administrator), but at a special district **any staff member** the board hires and directs |
| HB 1169 | (drafted as) all local governments; rules for similar housing **nearby** | only places over **2,000 people**; similar housing **anywhere in the same place** |
| SB 124 | (drafted as) **bigger** nonprofit hospitals, with no hint of who is out | the definition itself **excludes** sole community and critical-access hospitals, independents under 50 beds, Denver Health, children's hospitals and several clinic types; "larger" is fair only if the exclusions are named |
| SB 185 | builders owe a new duty to build homes properly | first buyers are owed the **same** duty courts already give later buyers |

Had the summaries been trusted, three of the eighteen descriptions would have
carried a false detail. Two later review passes caught ten more. Four were the
same defect — a detail the summary carries that the voted print amended away.
The other six were scope claims the operative text does not support, three of
them written into the first draft rather than copied from a summary. Every one
of the eighteen measures has now been read line by line against the print its
chamber voted, and the corrections were re-imported (the `rewrite` actions in
`import-rerun-report.json`). **Rule for this scope: quote nothing from the summary
block that is not confirmed in the enacting text, and check every scope word —
who is covered, what is excluded — against the operative section.**

A third pass, on review of the second, tightened two of those six: the SB 124
rewrite had dropped the size gate and named none of the exclusions, and the
HB 1079 rewrite had applied the special-district staff test to school districts
too. Both now follow the definitions section word for word.

## Conditional wording, enforced by the builder

These bills changed nothing, so no sentence may say they did. Every body is
written with "would have", and the builder refuses to write a file unless each
description contains "would have" or "could not have" and contains none of
"the act", "signed it into law" or "became law".

The Colorado session adjourned on 7 May 2025 and none of these measures can
move again, so each tail states a completed fact rather than the time-stamped
hedge Pennsylvania needed for a live session.

## LegiScan calls a failed vote passed

**HB 1187 is recorded with `passed=1` at 32-28.** Colorado's constitution
requires a majority of all members elected — 33 in the House, 18 in the Senate
— not a majority of those voting, and the bill's own history says "House Third
Reading Lost". This is the same defect the Arizona work found, in a new state.

I checked the other direction too: **all 200 previously approved Colorado rolls
clear the constitutional majority**, so nothing already imported is affected.
HB 1187 was dropped on its merits, not because of this.

Three rolls in this batch are genuine losing votes and say so: HB 1011 in the
Senate 16-18, HB 1158 in the House 31-33, and SB 157 in the Senate 16-19.

## One chamber, two records of the same third reading

HB 1147 has two Senate third-reading rolls on 2025-04-04: 24-10 and 24-11. The
bill's history records a single third reading, so one is an incomplete copy.
The judge refused the batch until this was resolved, which is the check working.

**The 24-11 roll accounts for all 35 senators; the 24-10 roll accounts for 34.**
The fuller record is the one judged, and the other is acknowledged as a
same-day peer.

## The version rule cost one roll

SB 124 is the only measure here whose two chambers voted materially different
bills. The Senate's version required nonprofit hospitals to spend 340B gains on
patients and made a breach a deceptive trade practice; the House's version
dropped that enforcement section, dropped a cap tied to total annual expenses,
renamed "profits" to "net revenue", and folded a standalone report into an
existing annual one. Their disagreement is precisely why the bill died.

Rather than describe two different bills with one text, the **Senate roll is
dropped and the House roll imported**, with the description written from the
House-voted print.

## Two direction calls to defend

**SB 77 is scored `against` on anti-corruption.** It would have stretched the
presumed answer time for a records request from three working days to five for
everyone except reporters, and the extension in hard cases from seven days to
ten. It also would have required agencies to post their records policies, which
points the other way, but that is housekeeping beside a delay that applies to
every member of the public who is not a journalist. The governor's veto rested
on the same reading.

**SB 141 is scored `against` on environment and public health.** Excusing towns
under 2,500 people from adopting a home energy code weakens the code, so a yes
vote is a vote against it.

## Two duplicate hand-written records retired

The dry run flagged three existing records as related. Two are true duplicates
of votes this batch imports — a hand-written record of Michael Carter's vote on
HB 1011 and of Naquetta Ricks's vote on SB 86 — and both were retired with the
reason recorded in `retirements.json`. The third flag is the familiar false
positive: a record for the same candidate on the same date about a different
bill.

## A checker that was quietly wrong

The British-spelling check refused this batch over the word "enroll". Its
pattern listed the British "enrol" as a stem, which also matches the American
"enroll". The pattern now requires that a second `l` not follow, and I verified
the fix against known-good and known-bad words before trusting it — "enroll",
"enrollment" and "fulfill" pass, while "enrol", "fulfil", "behaviour",
"licence", "organisation", "practising", "favours" and "offence" are all still
caught.

## Wording

First draft measured a Flesch-Kincaid median of 7.5 with a worst case of 9.2.
Five measures were rewritten, giving a median of 7.0 and a worst of 8.0, and
each rewrite was read back against the voted text. The plain-language lint
reports 0 warnings across all 44 descriptions.
