# Delaware batch-01 — how each measure was judged

Every description was written from the **enacted text**, read top to bottom: the
engrossed print where the bill was amended, and the last draft where it was not. No
description rests on a title, a synopsis, or a summary. No AI provider was called at
any point.

## The version check, and why it is easy in Delaware

Delaware gives three things no other state in this campaign gives together:

1. **The engrossed print names its own amendments in its header** — for example
   `HOUSE BILL NO. 50 AS AMENDED BY HOUSE AMENDMENT NO. 1 AND SENATE AMENDMENT NO. 1
   AND HOUSE AMENDMENT NO. 2`. That is the version check, stated by the document.
2. **Deleted and added words are marked in CSS**, `line-through` and `underline`, so a
   parser can tell struck text from live text. Georgia, Maine, Montana and Kentucky all
   needed a page render for this.
3. **Delaware re-votes in the originating chamber after the other chamber amends**, so
   taking the last kept floor vote per chamber lands both chambers on the enacted text.

All 25 rolls were checked against that last rule and all 25 are their chamber's last
kept floor vote. Four are the second vote in the originating chamber (HB 70 House,
HB 116 House, HB 50 House and Senate); their tail sentences say "passed the final
version" rather than "passed it".

## Checks run before importing

- **Every roll matched to its bill-history line on all four counts**, not just yeas and
  nays: yes, no, not voting and absent. All 25 matched exactly, and every member list
  length equals the sum of its own counts. North Carolina's H244 taught that a full
  member list is no proof of a correct tally, so the check is against Delaware's own
  record rather than against the list length.
- **Every roll confirmed as its chamber's last kept floor vote** by a pass over all
  stored rolls, before judging rather than by waiting for the gate to complain. The
  judge's superseded-stage gate then fired on nothing, and no judgment needed
  `acknowledge_later_rolls`.
- **`candidateRecordPlainLanguageLint` run over all 50 descriptions: 0 warnings.**
- **Roll dates audited against Delaware's own history, with the session-end days
  checked on purpose: 0 skews in 25.** Nine of the batch rolls fall on 2025-06-30,
  the busiest day in the whole feed, and two fall on 2026-07-01, an overnight sitting
  past the constitutional adjournment date. Delaware's history stamps those the same
  way LegiScan does, so no judgment needed an `official_vote_date` override. Illinois
  is the state where this check found real skew; Delaware has none.
- **Comma splice**: the builder joins the body and the tally sentence with a period and
  asserts that `", The "` appears in no description.
- **British spellings**: an explicit word list, not a suffix pattern, because `-ise`
  and `-ised` also end "raise" and "supervised" and a false alarm trains you to ignore
  the check. 0 hits over the descriptions and over these documents.
- **Reading level measured separately**, because the lint only counts words per
  sentence: **Flesch-Kincaid median 8.7, worst 9.7 (HB 344), mean 16.1 words per
  sentence, longest sentence 39 words.**

### The sentence-count trade, stated as a decision

The standard asks for 2 to 4 sentences at a 7th-grade level. **A first draft held to 4
sentences measured a Flesch-Kincaid median of 11.2, with sentences up to 42 words.**
Rewriting into short sentences brought the median to 8.7 but pushed each description to
5 to 8 sentences. Reading level was treated as binding and the sentence count gave way,
which is what Indiana, Montana and North Carolina each concluded. Grade 9 is the honest
floor for text built on defined terms like "lethal violence protective order",
"comprehensive plan" and "chronic violator status"; getting to 7 means dropping the
statutory limits, and dropping exactly those limits is what has caused most of this
campaign's correction rounds.

### The scope-recovery pass

Shortening the sentences dropped qualifiers, exactly as it has in Pennsylvania,
Connecticut, Maryland and Montana. Each measure was re-read against the enacted text
after the rewrite and eleven claims were repaired before importing, among them:

- HB 105 gained the commission and tipped-job carve-outs and the Delaware-jobs scope.
  Saying every ad must state pay was wrong for commission jobs.
- HB 119 gained the word "solely", which appears in the school-library principle and
  not in the public-library one.
- HB 154 says "law enforcement agency", not "police agency"; the statutory term reaches
  sheriffs and the State Police, and Pennsylvania's HB 1866 made this exact mistake.
- HB 205 gained "subject to limits such as cases where none of the acts happened here",
  because §3929 has three exclusions and naming one of three reads as if it were the
  only one.
- HB 210 gained "of the same offense within 10 years" on the repeat-conviction fine.
- HB 344 gained "starting two years after the rest of the rules take effect" on the
  five-year record rule, which is phased, and "unless it is appealing" on the activity
  ban, which is stayed pending appeal.
- HB 70 gained "or certified contractor" among the grounds for more time, and the
  three-year rent freeze now says it runs from the inspection that showed the work was
  needed.
- HB 50 gained "though they may apply again" and the RGGI five-percent change.
- HB 444 gained the actual language thresholds (more than 2% of voting-age citizens,
  but never fewer than 100, or more than 1,000).

## Per measure

**HB 37 — civil_rights / for.** Rewrites the definition of "place of public
accommodation" in the Equal Accommodations Law. The old text already listed state
agencies, local agencies, public libraries, and state-funded agencies performing public
functions, but only inside the "establishment which caters to ... the general public"
clause. The act lists them as covered on their own, widens "state-funded agencies" to
any person receiving state funding to perform a public function, and adds a person
performing a public function under state or local control and supervision. Also adds a
definition of "person". The description says the old law already listed the agencies,
so it does not present existing coverage as new (review fix, PR #1074). The exclusions
for home sales and rentals and for tourist homes with fewer than 10 rental units
survive, and the description says so.

**HB 105 — reduce_wealth_gap / for.** Maine's LD 54 set the precedent that pay-range
transparency sits in this area. New §709C: pay or pay range plus a general description
of benefits in every posting; the same information to an applicant before any offer or
compensation discussion where there is no posting; a written warning for a first
offense and $500 to $10,000 for later ones; records for three years; no application to
employers with 25 or fewer employees; effective two years after enactment.

**HB 119 — civil_rights / for.** The Freedom to Read Act. The mirror of Texas SB 13,
which this campaign scored `civil_rights` / against. Public and school libraries may
not exclude material for the creator's origin, background or views, or for partisan,
ideological or religious disapproval; written objection policies; material stays
available during review; anti-retaliation for staff; the obscenity law in Title 11
still applies.

**HB 205 — womens_reproductive_rights / for.** Widens Delaware's existing
reproductive-health shield to all "healthcare services" as newly defined in §1702(9).
Licensing boards may not discipline for lawful care; §3928's public-policy bar and its
block on issuing legal process are extended; §3929 gives a clawback action with three
exclusions; §2535 bars insurer retaliation; a new 29 Del. C. §611 bars state and local
staff, including law enforcement, from assisting an out-of-state or federal inquiry into
the lawfulness of such care except as federal law requires.

**HB 210 — environment_and_public_health / for.** The Pollution Accountability Act.
Every figure in the description was read out of the enacted sections. See CODE-FINDINGS
§2: the act's own title and synopsis still promise a Title 3 nutrient-management
section that House Amendment 1 struck before the House voted.

**HB 344 — anti_corruption / for.** Record retention, written loan documentation, no
candidate interest on a loan to their own committee, biennial training, an audit power,
a ban on negative ending balances, a 48-hour extension in place of 24, and an activity
ban 30 days after an unanswered citation. Enforcement moves from the Attorney General
generally to the Division of Civil Rights and Public Trust, which must investigate and
prosecute. Effective July 1, 2027.

**HB 427 — gun_control / against, and the one measure with a stated nay.** The act
removes the direct-supervision requirement for 16- and 17-year-olds in both §1445 and
§1448, conditioned on a parent's or guardian's permission and a completed hunter
education course, plus a hunting license for hunting. The description says the old
rule applied "generally" and names the shotgun and muzzle-loader exceptions, because
§1448(a)(5)a-d already exempted those, non-firearm weapons for adults, military and
police, and transport to a lawful activity (review fix, PR #1074). The nay is stated as `for`
because the act is single-subject, its whole operative content is the area's own
mechanism — who may hold a firearm without supervision — and a no vote is a vote to
keep the existing restriction. Every other stance label in the batch carries
`nay: null`, because the realistic objection runs on a different axis from the scored
area: administrative burden, landlord cost, local control, litigation exposure.

**SB 23 — housing_affordability / for.** The Housing for Every Delawarean Act. Note it
required a two-thirds vote of each house under Article IX §1 because it indirectly
amends municipal charters; both chambers cleared that on their own numbers, but see
CODE-FINDINGS §5 on why LegiScan's `passed` flag cannot be trusted on such a bill.

**HB 70 — environment_and_public_health / for.** The description states the March 1,
2028 backstop start date, because the substantive chapter does not begin until the
earlier of that date or twelve months after the Housing Authority publishes a
readiness notice. Saying the certification rules apply now would be wrong.

**HB 116 — social_programs_and_welfare / for.** Deliberately not
`cost_of_living_reduction`, whose definition is about price stability, competition,
tariffs and trade. This is a means-tested discount for low-income households, which is
a safety-net program. The description keeps the permissive "may now approve", because
§303(e)(1) is an authority granted to the Commission, not a mandate.

**HB 50 — social_programs_and_welfare / for.** Same area as HB 116, same reason. The
description states the three-year sunset.

**SB 82 — gun_control / for.** Both operative changes are stated: the maximum term of a
lethal violence protective order goes from one year to five, and the respondent's right
to request a termination hearing goes from "at any time" to once a year. `nay: null`,
because the mainstream objection is due process, which is a different axis from
regulating firearm access.

**HB 154 — gun_control / for.** Safe storage is named in the area's own definition. The
description carries both conditions on the immunity and the fact that the maker,
distributor and seller remain liable.

**HB 444 — civil_rights / for.** The Delaware John Lewis Voting Rights Act. Effective
July 1, 2027, and the tail says so, because the act is law but not yet in force.

## Dropped under filter 5, after a full read

- **HB 140, the End of Life Options Act.** The session's most newsworthy
  divided-and-enacted measure. No research area carries an honest direction:
  `healthcare_affordability` scores it `for` only by assuming assisted death is care
  whose access should improve, which is the contested question itself, and
  `civil_rights` is worse because disability-rights organizations and autonomy
  advocates both claim the area. Maine imported the same shape under `general` with no
  stance, but `general` is a judicial, non-selectable area that must never be tagged on
  a roll-call record, so importing without a stance is no longer available. **Expect to
  be asked.**
- **HB 142** — repeals a private person's power to make a warrantless arrest on an
  out-of-state felony charge. Narrow and procedural.
- **HB 448** — makes candidate security costs an allowed campaign expense; neither
  direction is honest inside `anti_corruption`.
- **HB 175** — a schedule of DNREC permit and license fees. Money only, the same reason
  appropriations are excluded campaign-wide.
- **SB 179** — recodifies the Sentencing Accountability Commission. Housekeeping.
- **SB 116** — lets a tenant stop an eviction for unpaid rent by paying what is owed.
  This is eviction procedure, and `housing_affordability` is defined as housing supply
  and cost burdens. Maryland's HB 767 retraction settled that this does not fit.
- **HB 59** — limits police release of an adult suspect's name and photo. Transparency
  and privacy pull opposite ways inside `public_safety_and_crime_control`.
- **HB 255** — decouples Delaware from federal expensing and depreciation changes.
  Almost all of it is corporate timing, so `personal_income_tax_reduction` would
  misdescribe why members voted.

## The run

- Judge: 25 judgments, **25 updated, 0 errors**. Both approval gates passed with no
  edits — the batch template closes every description with the roll's own tally, and
  every selected roll is already its chamber's final action, so no judgment needed
  `acknowledge_later_rolls`.
- Import dry run (`import-dry-run-report.json`), stamp `2026-09-03T02:03:06.738Z`:
  25 files, **177 planned inserts**, 0 errors, 0 notified, 0 `related` flags.
- Real import (`import-report.json`), stamp `2026-09-03T02:03:28.441Z`: 25 files, **177
  inserts**, 0 errors, 0 notified, 0 `related`, 0 `ambiguous`, **14 distinct
  candidates**.
- Convergence run (`import-rerun-report.json`), stamp `2026-09-03T02:04:47.645Z`: 25
  files, **all 177 unchanged**, 0 errors. The first attempt at this run failed one roll
  (HB 70, roll 1596724) with `citation URL fetch timed out` on the legiscan.com
  citation, which is the known Cloudflare flake; the immediate re-run was clean.

### Reconciled three ways

| check | result |
| --- | --- |
| dry-run plan vs real run | 177 planned inserts, 177 inserts |
| `origin_run_id LIKE 'rollcall:DE:%'`, live rows | **177 records / 14 candidates** |
| `candidate_records` table total | 161,151 → 161,328, exactly +177 |

**Tags: 143, predicted independently from the report before the database was read, and
the database agreed exactly.** The arithmetic is every matched yea voter on the eleven
stance measures, plus HB 427's nay voters, which is the only stated nay in the batch.
By area: civil_rights 26 for, environment_and_public_health 22 for,
social_programs_and_welfare 22 for, gun_control 18 for and 10 against,
housing_affordability 12 for, anti_corruption 11 for, reduce_wealth_gap 11 for,
womens_reproductive_rights 11 for.

**Production holds no Delaware roll-call records.** Everything here is on local
`voteapp`.
