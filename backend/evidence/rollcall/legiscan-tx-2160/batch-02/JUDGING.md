# Batch 02 — judging notes

27 judgments, one per selected roll call, in `judgments.json`. Applied to the
local review queue, then fanned out (`import-report.json`; the pre-import plan
is `import-dry-run-report.json`).

## Grounding

Nine of the fourteen measures are Senate bills, and each was judged from the
Senate Research Center analysis of the **enrolled** version
(`capitol.texas.gov/tlodocs/89R/analysis/html/SB…F.htm`), reached from the
`state_link` in the LegiScan bill record.

The five House bills have **no enrolled analysis** — the Texas Legislature
Online publishes analyses for those only through the engrossed version
(`…E.htm`). So each of those five was judged from the engrossed analysis
**plus the enrolled bill text** (`…/billtext/html/HB…F.htm`), and the two were
compared before a sentence was written. That comparison earned its keep twice:

| measure | in the enrolled text, not in the engrossed analysis |
|---|---|
| HB 521 | new Sec. 64.009(a-6) extends curbside ballot delivery to a voter **escorting** an eligible voter; amended (g) makes the secretary of state retain the forms and open them to attorney general inspection |
| HB 3441 | the definition of "advertise" **excludes** provider-to-patient discussions and materials displayed in a clinical setting — the engrossed analysis says only "Defines 'advertise.'" |

Both descriptions are written from the enrolled text. HB 493, HB 3053, and
HB 5115 came through unchanged in substance.

## The Texas hazard, again

Batch 01's finding holds and was re-checked here: a Texas analysis opens with
the **AUTHOR'S / SPONSOR'S STATEMENT OF INTENT**, which is advocacy, and the
neutral content is the SECTION BY SECTION ANALYSIS below it. Every figure in
this batch's descriptions was taken from the section-by-section text or the
enrolled bill, never from the statement of intent:

- SB 21's `$500 billion` market-capitalization floor and 24-month averaging
  window are Sec. 403.704, not the sponsor's framing.
- SB 2284's `seven acres` / `10 acres` bow-hunting change is SECTION 4
  (Sec. 235.042(a), Local Government Code).
- HB 521's `20 feet` curbside buffer and `seven or more voters` threshold are
  Sec. 61.003(a) and Sec. 64.009(f) of the enrolled text.
- HB 5115's felony grades are Sec. 276.013(b) of the enrolled text.

One stale-caption check: SB 1362's caption says only "prohibiting the
recognition, service, and enforcement of extreme risk protective orders."
The enrolled bill also bars accepting federal grants for that purpose and
carries an express carve-out for Family Code and Code of Criminal Procedure
protective orders. Both are in the description; a reader who stopped at the
caption would have written a broader ban than the statute enacts.

## Labels

Only `general` and `integrity_and_ethics` may carry no stance; every other
research area requires `for` or `against`. Stance direction follows the AREA
DESCRIPTION in `research_areas`, not the bill's framing — the rule that bit
batch 01 on `immigration`.

**Nine measures carry a stance; five stay `general`.**

| measure | label | why |
|---|---|---|
| HB 493 | `election_integrity` / for | adds a disqualification for people convicted of first- or second-degree felonies to the poll-watcher eligibility rules |
| HB 5115 | `election_integrity` / for | raises the grade of election fraud and adds knowing miscounts to the offense; the repeal of Sec. 276.014 folds those offenses into 276.013 rather than removing them |
| SB 510 | `election_integrity` / for | extends the secretary of state's funding-withholding remedy to registrar duties across the Election Code, after notice and a chance to cure |
| HB 3053 | `gun_control` / against | removes a firearm-reduction tool from cities and counties. `gun_control` reads "Regulate firearm access… to reduce gun violence", so a preemption is against it |
| SB 1362 | `gun_control` / against | bars any Texas entity from recognizing or enforcing an extreme risk protective order and criminalizes doing so |
| SB 2284 | `gun_control` / against | broadens firearm preemption and strips the public-place carry exception as applied to license holders |
| SB 965 | `civil_rights` / for | the enacted section does one thing: it shields a public employee's religious speech from government infringement absent strict scrutiny |
| SB 1257 | `healthcare_affordability` / for | a coverage mandate. `healthcare_affordability` is "Reduce out-of-pocket costs and improve access", and the statute requires plans to pay for care they could otherwise deny |
| SB 379 | `social_programs_and_welfare` / against | `social_programs_and_welfare` is "Support vulnerable populations through effective safety-net… programs"; the statute narrows what a SNAP recipient may buy with the benefit |

### The five that stay `general`

- **SB 11** (school prayer period) — the operative text runs in **both**
  directions at once. It creates a daily period for prayer and religious
  reading, and it conditions participation on a consent form containing "an
  express waiver of the person's right to bring a claim… including a claim
  under the Establishment Clause," and SECTION 2 deletes the existing
  prohibition on encouraging a student to engage in or refrain from prayer.
  An expansion paid for with a waiver of a constitutional claim has no single
  honest direction.

  This is why SB 11 and SB 965 are labeled differently even though both are
  religion-in-schools bills from the same session. SB 965 has no offsetting
  restriction anywhere in its text; SB 11 does, in writing.

- **HB 3441** (vaccine manufacturer liability) — `corporate_accountability`
  / for fits the mechanism exactly (a new cause of action against
  manufacturers), and `environment_and_public_health` / against fits the
  subject just as well. Two areas, opposite directions, both defensible from
  the same text — the SB 17 shape from batch 01.

- **SB 2337** (proxy advisory services) — same problem. It imposes
  disclosure duties on firms, which reads as corporate accountability, and
  its stated purpose is to discourage advice based on environmental, social,
  and governance factors, which reads as the reverse. It is a disclosure
  regime, not a ban, so the Ohio S.B. 1 DEI-ban precedent does not reach it.

- **SB 21** (Strategic Bitcoin Reserve) — follows Ohio H.B. 116: a divided
  vote worth recording with no honest direction. It is an investment-authority
  bill, not a tax, spending, or consumer measure.

- **HB 521** (accommodating voters with a disability) — expands curbside
  voting (escort provision, two-officer delivery, a signposted phone number,
  a 20-foot electioneering buffer) and adds burdens in the same bill (a sworn
  statement under penalty of perjury, transporter reporting, a Class A
  misdemeanor for intentionally failing to file the form). Contested by
  construction, not by opinion.

## Votes taken on a pre-conference text

HB 493's house roll (2025-05-13) and SB 379's two third-reading rolls are
votes on texts that later went to a conference committee, so the text those
members voted on is not byte-identical to the enrolled law.

For HB 493 the conference change is not substantive — the enrolled text
restores the pre-existing recording-device clause in Sec. 33.006(b)(6)
alongside the new felony affidavit — so its description states the enacted
law, batch 01's convention (its SB 2 house third-reading description
likewise carries the enacted 85 percent formula).

**SB 379 is the opposite case, and it was caught on review.** The three
versions differ substantively:

| version | scope of the SNAP prohibition |
|---|---|
| Senate engrossed (voted 3/31, 22-8) | energy drinks, sweetened beverages, carbonated beverages, candy, chips, cookies |
| House substitute + floor amendment (voted 5/23, 90-37) | sweetened **carbonated soft** drinks only — no candy |
| Enrolled law (conference) | sweetened drinks (any water-based) and candy |

So each SB 379 description states the version that chamber voted on, named
as such ("the Senate version" / "the House version"), with the enacted scope
appended. The house-voted text is the committee substitute plus the one
adopted floor amendment (Gerdes, 5/22), which only inserts "any amount of"
before "artificial sweeteners" — verified from the amendments page, so the
substitute text is what the chamber passed. The senate's own vote on the
enacted text (conference report, 5/31, 22-9) is not in the queue: LegiScan
carries it without a member list, so the fetch skipped it as unrecorded.

Two rolls in the batch **are** conference committee reports — HB 493 senate
(23-8) and SB 21 senate (23-8) — and their descriptions say so rather than
saying the chamber passed the bill.

## Wording

Descriptions end "and became law" rather than batch 01's "was signed into
law." All 14 measures are LegiScan status 4 (Passed), which records enactment
without recording whether the governor signed or let it become law unsigned.
The weaker claim is the one the evidence supports.

## Result

The real run reconciled exactly to the dry run — same file count, same insert
count, no errors either time. The dry run's 1,741 is a *plan*
(`"dryRun": true`, nothing written); only the real run's 1,741 is rows in the
database:

```text
dry run      files 27 | outcomes {"dry_run": 27}  | errors 0 | planned inserts 1,741 | notified 0
real run     files 27 | outcomes {"imported": 27} | errors 0 | inserts         1,741 | notified 0
rewrite run  files 27 | outcomes {"imported": 27} | errors 0 | unchanged 1,380 + rewrite 361 | notified 0
```

(`import-report.json` keeps a fixed name, so the committed copy is the
rewrite run; the insert run's numbers are recorded here.)

In the local database that is `candidate_records` 61,772 → 63,513, with 1,741
rows across 135 distinct Texas candidates and 27 distinct roll calls.

**Batch 02 carries two `startedAt` stamps, not one.** The initial import
stamped every row `2026-08-25T18:06:23.818Z`; the review-fix run (below)
rewrote 361 rows in place, and a rewrite re-stamps `origin_run_id` with the
rewriting run's `startedAt`:

```sql
select right(origin_run_id, 24) as stamp, count(*)
  from candidate_records
 where origin_run_id like 'rollcall:TX:%:2160:%'
 group by 1 order by 1;
-- 2026-08-25T05:30:09.633Z | 1620   (batch 01, untouched)
-- 2026-08-25T18:06:23.818Z | 1380   (batch 02, initial import)
-- 2026-08-25T18:38:27.616Z |  361   (batch 02, review rewrites)
```

1,380 + 361 = 1,741. The stamp is `startedAt`, stamped once per run and
shared by every roll in that run (`importLegiscanRollCallVotes.ts`,
`originRunId`). **Do not shorten a batch predicate to `'rollcall:TX:%'` or to
the session** — batch 01 is session 2160 as well. Its predicate still returns
exactly 1,620 after both batch-02 runs, which is the proof the batches
separate cleanly.

The first dry run's own stamp, `2026-08-25T18:06:00.572Z`, matches **zero**
rows — positive proof `--dry-run` wrote nothing.

**135 candidates, not 136.** The one member who appears in batch 01 but not
here is Dustin Burrows, the Speaker of the Texas House, who cast no recorded
vote on any of this batch's 14 house rolls. That is the Speaker's normal
practice, not a fan-out gap.

The review queue reads 52 `approved` / 6,132 `pending` — batch 01's 25 plus
this batch's 27. The importer reads the queue, it does not consume it.

Prod is untouched. Promotion is a separate `research:promote` run.

Per measure, records written: SB 1257 132, SB 21 130, HB 5115 129, SB 2337 129,
SB 510 129, HB 3053 127, SB 11 126, SB 1362 126, HB 521 124, HB 493 124,
SB 965 123, SB 379 118, SB 2284 114, HB 3441 110. SB 2284 is house-only, so
its 114 comes from a single roll — the same reach as a two-chamber measure
here, because a senate roll only reaches the 13 sitting senators the
crosswalk maps to Nov-2026 candidates. (13 is a crosswalk-match count, not
a count of senate seats on the ballot.)

`notified` is 0 because every vote is from 2025, well outside the 30-day
notification window.

## Review fixes

Four findings from an external review of the PR, all verified against the
enrolled texts before acting; 361 records rewritten in place
(SB 379 118, SB 2284 114, SB 2337 129), everything else `unchanged`.

1. **SB 379 — both descriptions described the wrong version** (the reviewer
   caught the senate; the house turned out to be wrong too). Fixed as
   described under "Votes taken on a pre-conference text" above.
2. **SB 2284 — "ended Type A general-law municipalities' authority to
   regulate firearms" was overbroad.** The enrolled SECTION 9 strikes only
   `[and firearms]` from Sec. 342.003(a)(8) — the fireworks-and-firearms
   *use* provision — while Sec. 229.001(b) still preserves municipal
   authority over discharge, zoning, and certain carrying. Now: "struck
   firearms from the provision letting a Type A general-law municipality
   prohibit or regulate the use of fireworks and firearms."
3. **SB 2337 — the description merged two disclosure rules into one
   recipient list.** Sec. 6A.101(b) (nonfinancial advice) requires
   disclosure to the client, a copy to the company, and a public website
   statement — no attorney general. Sec. 6A.102(b) (materially different
   advice, and only to clients who had not requested nonfinancial services)
   requires written notice to the clients, the company, and the attorney
   general — no public disclosure. The description now states the two rules
   separately with their own recipients.
4. **The senate-reach explanation conflated two numbers.** 13 is the count
   of sitting senators the crosswalk maps to Nov-2026 candidates — a
   resolution count, not the number of senate seats on the ballot. Both
   `PLAN.md` and the per-measure note above now say so.

## Next

Batch 03, from the 716 divided actions still untouched. The discipline is
unchanged: each sentence replicates ~114 times on the house side, so it gets
read against the statute before it is written, never after.

## Plain-language rewrite (2026-08-30)

All 27 yea and nay descriptions were rewritten from this committed evidence.
The judge dry run and real run both passed. The importer then rewrote 1,741
local candidate records with stamp `2026-08-31T06:34:30.300Z`; a final dry run
reported all 1,741 unchanged. The original `import-report.json` remains
unchanged. Prod remains untouched.

Later-roll acknowledgments required by the current judge are:
1570293→1582993, 1570131→1581601, 1570671→1584058, 1529212→1583939,
1592100→1592101, 1579859→1582596, 1568579→1585434, and 1575103→1581603.
