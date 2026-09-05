# Delaware batch-02 — how each measure was judged

Every description was written from the **enacted text** — the engrossed print where the
bill was amended, the last draft where it was not — read top to bottom. No description
rests on a title, a synopsis, or a summary. No AI provider was called at any point.

## How the reading was done, and how it was checked

The 20 measures that survived the synopsis screen were read in full by five subagents,
four or five acts each, working from the marked-up enacted text. Each was required to
quote the operative words with a section number, to name what was reprinted existing law
rather than a change, to list counter-provisions, and to answer a mandatory DIRECTION
field that allowed `no_honest_direction` as an answer.

**A subagent report is evidence, not testimony** — the rule Kentucky's fabricated report
established. Every claim a description rests on was re-checked against the text here
before judging. Two of those checks changed the batch:

- **SB 60's synopsis promises a $125 million cap on utility capital spending. The
  enacted act does not contain it.** The engrossed print jumps from Section 1 to
  Section 3; Section 2, which added that cap to § 1008 of Title 26, was struck by
  amendment before passage. A description written from the synopsis would have credited
  29 House members with a cap the act never imposed. This is the second instance in
  Delaware of the stale-synopsis hazard recorded as CODE-FINDINGS §2, after HB 210 in
  batch-01, and it is now clear the hazard is systemic rather than a one-off.
- **SB 250's text carries no amendment markings and bracketed effective-date
  placeholders**, which the reader flagged as possibly an unenacted draft. Checked
  against the bill history: SB 250 has exactly one text and no amendments at all — the
  Senate passed it 13-0 and the House 26-10 with nothing adopted in between. The cached
  text is the enacted text, and the brackets are Delaware's drafting convention, which
  also appear in HB 48 and HB 70.

Two more reader flags were confirmed and then judged as immaterial: HB 36 does narrow a
tenant damages clause and close one open-ended list, and HB 48 removes jail for a repeat
parking offense while raising the fines. Both are small against the acts' main thrust,
and both are stated in the descriptions rather than hidden.

## Checks run before importing

- **All 18 rolls verified as their chamber's last kept floor vote**, and each matched to
  its bill-history line on all four counts — yes, no, not voting, absent. All 18 exact;
  every member list equals its own counts. No judgment needed `acknowledge_later_rolls`
  and the superseded-stage gate fired on nothing.
- **`candidateRecordPlainLanguageLint`: 0 warnings** over all 36 descriptions.
- **Comma splice**: body and tally sentence joined with a period, `", The "` asserted
  absent everywhere.
- **British spellings**: explicit word list. It caught `licence` in SB 201 before import,
  the third time this check has paid for itself in Delaware.
- **Reading level measured separately.** A first pass came in at Flesch-Kincaid median
  9.9, worst 12.5, with a 45-word sentence. Twelve descriptions were rewritten into
  shorter sentences before judging: **median 8.4, worst 10.0 (SB 60), longest sentence
  37 words.** Descriptions run 5 to 12 sentences rather than the 2 to 4 the standard
  asks, the same trade batch-01 recorded and stated: reading level and statutory limits
  are binding, sentence count gives way.

## The run

- Judge: 18 judgments, **18 updated, 0 errors**.
- Dry run (`import-dry-run-report.json`): 18 files, **299 planned inserts**, 0 errors,
  0 `related` flags.
- Real import (`import-report.json`), stamp `2026-09-04T20:23:11.538Z`: 18 files,
  **299 inserts**, 0 errors, 0 notified, 0 `related`, 0 `ambiguous`, 29 candidates.
- Convergence (`import-dry-run-rerun-report.json`): all **299 unchanged**, 0 errors.

### Reconciled three ways

| check | result |
| --- | --- |
| dry-run plan vs real run | 299 planned, 299 inserts |
| this batch's own 18 roll numbers, live rows | **299 records** |
| all Delaware roll-call records | 346 → 645, exactly +299 |

**Tags: 226 added, predicted from the import report before the database was read, and
the database agreed exactly.** Delaware now holds **645 records / 29 candidates / 492
tags across 12 research areas**: civil_rights 103, environment_and_public_health 69,
corporate_accountability 68, social_programs_and_welfare 64, reduce_wealth_gap 39,
healthcare_affordability 30, gun_control 23 for and 17 against, housing_affordability 23,
womens_reproductive_rights 22, anti_corruption 20, immigration 14.

## HB 140, the End of Life Options Act — the drop stands, and it is now forced

Batch-01 dropped HB 140 because no research area carries an honest direction on medical
aid in dying. Asked to settle it, the answer is that the drop is no longer a judgment
call at all:

- `parseRollCallLabels` rejects an empty label list outright, so **every judgment must
  name at least one research area**.
- The only honest label is `general`, a non-stance area — and the label rule adopted
  2026-09-02 bars `general` from any roll-call record, because the tag is hidden from
  every legislative view and user ranking.

So a divided, enacted, highly salient measure that no area can honestly score **cannot be
recorded at all**, with or without a stance. HB 140 drew the closest votes of the session,
House 21-17 and Senate 11-8, and is lost to that gap rather than to any view about the
measure. Written up as CODE-FINDINGS §6, because it is a policy question above this
state: either a roll-call record should be allowed to carry no area, or there should be a
visible non-stance area for votes worth recording that no area can score. Ohio's HB 116,
Maine's LD 613, Missouri's SB 4 and several Alabama measures were all recorded under the
old route and could not be today.

**Production holds no Delaware roll-call records.** Everything here is on local `voteapp`.

## Review corrections, 2026-09-04 — three descriptions re-checked against enacted text

PR review flagged three descriptions as overstating the acts. Each was read again against
the official text and all three held up, so the sentences were tightened:

- **HB 62.** The 8 a.m. to 4 p.m., Monday through Thursday shut-off window is not an
  absolute ban with one safety exception. The adopted House Amendment 1 also lets a
  utility cut service outside those hours "unless such utility provides facilities for
  payment and restoration of services at all times during such period." The December 21
  to January 1 ban keeps only the safety exception, so the two limits are now stated
  separately.
- **SB 13** (Senate Substitute 1). Full assistance is for "medically necessary hospital
  services," not all hospital care; "hospital" excludes exclusively psychiatric,
  rehabilitative, and long-term acute care hospitals; and the hardship policy may carry an
  income ceiling, "but the ceiling may not be lower than 500% of the federal poverty
  level." All three qualifications added; "every hospital" dropped.
- **SB 326**, § 207(b). The Commission "shall provide for management audits ... at least
  once every five years unless the Commission finds that a specific management audit is
  unnecessary," and only "may require" an independent firm. "Must happen" became "must
  order ... unless they find a particular review unnecessary," with the outside firm as an
  option.

Re-run: judge **3 updated**, import (`import-rerun-report.json`) **59 rewrite, 240
unchanged, 0 notified** (HB 62 20, SB 13 19, SB 326 20 — records rewritten in place, ids
and tags kept), convergence dry run (`import-dry-run-rerun-report.json`) **299 unchanged**.
Still 645 records / 29 candidates. Plain-language lint clean on all six sentences.
