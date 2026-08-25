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

## Two votes taken on a pre-conference text

HB 493's house roll (2025-05-13) and SB 379's house roll (2025-05-23) are
third-reading votes on measures that later went to a conference committee, so
the text those members voted on is not byte-identical to the enrolled law.
Both descriptions state the enacted law, which is batch 01's convention (its
SB 2 house third-reading description likewise carries the enacted 85 percent
formula). The conference changes here are not substantive: HB 493's enrolled
text restores the pre-existing recording-device clause in Sec. 33.006(b)(6)
alongside the new felony affidavit, and SB 379 is a single prohibition that
survived intact.

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
dry run   files 27 | outcomes {"dry_run": 27}  | errors 0 | planned inserts 1,741 | notified 0
real run  files 27 | outcomes {"imported": 27} | errors 0 | inserts         1,741 | notified 0
```

In the local database that is `candidate_records` 61,772 → 63,513, with 1,741
rows across 135 distinct Texas candidates and 27 distinct roll calls:

```sql
select count(*), count(distinct candidate_id), count(distinct origin_run_id)
  from candidate_records
 where origin_run_id like 'rollcall:TX:%:2160:%:2026-08-25T18:06:23.818Z';
-- 1741 | 135 | 27
```

The trailing timestamp is the run's `startedAt`, stamped once and shared by
every roll in the run (`importLegiscanRollCallVotes.ts`, `originRunId`). It is
the only thing that pins this query to batch 02. **Do not shorten it to
`'rollcall:TX:%'` or to the session** — batch 01 is session 2160 as well, and
either shortcut silently swallows its 1,620 rows. The batch-01 predicate still
returns exactly 1,620 after this import, which is the proof that the two
batches separate cleanly.

The dry run's own stamp, `2026-08-25T18:06:00.572Z`, matches **zero** rows —
positive proof `--dry-run` wrote nothing.

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
here, because only 13 of the 31 senate seats are on the Nov-2026 ballot.

`notified` is 0 because every vote is from 2025, well outside the 30-day
notification window.

## Next

Batch 03, from the 716 divided actions still untouched. The discipline is
unchanged: each sentence replicates ~114 times on the house side, so it gets
read against the statute before it is written, never after.
