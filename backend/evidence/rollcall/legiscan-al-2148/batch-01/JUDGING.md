# Alabama batch-01 — judging notes

## Sources

Every measure was judged from its **enrolled Act**, read top to bottom, at
`https://alison.legislature.state.al.us/files/pdf/SearchableInstruments/2025RS/<BILL>-enr.pdf`
(a browser User-Agent is required; `pdftotext -layout` reads the files cleanly). The introduced
version's SYNOPSIS — written by the Legislative Services Agency, neutral, with no sponsor statement
of intent — was used only for triage, never as grounding. Where a selected roll predates the final
text, the engrossed version was fetched and diffed line by line against the enrolled Act
(SB 53, HB 357).

One neutrality caution found while probing: LegiScan's `amendments[].adopted` flag is unreliable
for Alabama (it reads 0 even where the bill history says an amendment was adopted), so version
pinning used the `history[]` action lines, the enrolled certification block, and full-text diffs.

## Date audit

All 10 selected rolls were checked against the bill history line that names the same roll call
number: 10 of 10 match exactly. No `official_vote_date` override is needed anywhere in this batch.

## Version checks

- **SB 53**: the divided Senate vote (February 13) was on the engrossed Senate text. A full diff
  of engrossed against enrolled shows exactly one change: the House widened the human-smuggling
  exemptions from {attorneys, school excursions} to also cover health care providers,
  noncommercial religious or charitable transport, and transport for government purposes. The
  Senate agreed 30-0 (roll 1573051, acknowledged in the judgment). Both descriptions state the
  Senate version's narrower exemptions and the later change. Note: the introduced bill would have
  created a new "concealing an illegal alien" crime; the Senate removed it before passage, so the
  engrossed and enrolled texts both create only the human-smuggling crime and repeal the old
  harboring crime a federal court had blocked.
- **HB 357**: the divided House vote (April 10) predates the Senate amendment. The full diff shows
  the Senate only split the definitions (giving "cigarettes intended to be heated" their own term)
  and moved the start date from October 1 to November 1, 2025. The tax structure — 1.7 cents per
  unit, heated cigarettes carved out of the 33.75-mill cigarette rate — is identical in both
  versions. The House accepted the changes 89-8 (roll 1568024, acknowledged).
- **SB 63**: the dataset holds only an Introduced and an Enrolled text, no amendments anywhere in
  the history; both chambers passed the same text, so the Senate roll is on the enacted text.
- **SB 116**: the House amended and passed (the selected 77-23 roll); the Senate concurred 24-2
  without further change, so the House roll is on the enacted text.
- **HB 165**: the Senate passed the House text unchanged (history and certification show no Senate
  amendment), so the Senate roll is on the enacted text. The introduced bill's option to observe
  either Jefferson Davis' birthday or Juneteenth is NOT in the enrolled Act — the law simply adds
  Juneteenth as a full holiday and keeps the existing list, so the descriptions say that.
- **HB 202, HB 8, HB 445 (House rolls)**: each selected House roll is the concurrence in the
  Senate's version — the vote on the enacted text. HB 445's Senate roll is on the Senate's own
  amended text, which is the text the House concurred to the same day.

## Label reasoning

Every stance label states its `nay` side explicitly, and every one is `null`: in each measure the
realistic reason for a no vote runs on a different axis than the scored area (cost, federal
overlap, business impact), so a no vote is not evidence of the opposite stance on the area's goal.

- **SB 116 — gun_control, yes = for.** A new felony for possessing or selling parts that convert a
  pistol to a machine gun is squarely a firearm restriction. The exemptions (police, federally
  registered items, devices that stay at two shots per trigger pull) are stated in the
  description.
- **SB 53 and SB 63 — immigration, yes = against.** The area description reads "Welcome
  immigration through a lawful, orderly, and humane system"; enforcement-and-detention measures
  score against it (the Texas SB 8 / Maryland HB 1222 direction rule). SB 63's description also
  carries the statute's own limit that only federal verification, never a state officer's own
  judgment, can establish unlawful presence.
- **HB 165 — civil_rights, yes = for.** Adding Juneteenth as a state holiday is recognition of
  emancipation. The description states that the law kept the Confederate holidays on the books, so
  a reader is not misled about the law's reach.
- **HB 202 — civil_rights, yes = against.** The Act forecloses most civil claims against officers,
  adds a heightened pleading rule, pauses discovery, and creates pretrial criminal immunity
  hearings — a barrier to vindicating rights in court, the Maryland HB 1378 direction (limits on
  suing = against). The counter-strand — mandatory yearly reporting of use-of-force complaint
  data — is small next to the immunity overhaul and is stated in the description, along with the
  two exceptions that preserve suits (reckless risk of death or serious injury without policing
  justification; violation of a clearly established right).
- **HB 8 — environment_and_public_health, yes = for.** A youth-vaping crackdown: unauthorized
  vapes confined to 21-plus specialty stores with ID scans at the door, child-themed advertising
  banned, school prevention programs required. The Texas SB 2024 precedent (e-cigarette
  restrictions score for).
- **HB 357 — general, NO stance.** The Act gives heated tobacco products a 1.7-cent-per-unit rate
  while regular cigarettes pay about 3.4 cents, and carves "cigarettes intended to be heated" out
  of the cigarette rate. Public-health advocates read a below-cigarette rate as undermining
  tobacco-tax deterrence; harm-reduction advocates read cheaper non-combusted products as steering
  smokers away from cigarettes. The direction inside environment_and_public_health is contested
  between evidence bases, so no stance is honest (the fluoride rule). The descriptions state both
  rates and let the reader judge.
- **HB 445 — general, NO stance.** A licensing-and-restriction regime for hemp-derived THC
  products (10 mg per serving cap, 21-plus stores, smokable hemp banned, online sales banned, 10
  percent tax, felonies for repeat violations). Cannabis-adjacent regulation with no honest
  direction, the Ohio H.B. 116 / Pennsylvania HB 1200 precedent: imported without a stance because
  its two rolls were among the most divided of the session.

## Duplicates

The dry run flagged 20 existing hand-written records as related; a wider description sweep over
all Alabama candidates found 2 more. Disposition:

- **7 retired** as duplicates of a batch roll (same vote, same member):
  Chris Pringle, Marilyn Lands, Mark Shirey, and Philip Ensler on the SB 116 House passage;
  Mark Shirey on the HB 357 House passage; Kerry Underwood and Laura Hall on the HB 202 House
  concurrence. Retirement file: `/Users/shu/legiscan-data/al-2148-batch01-retirements.json`
  (re-run it on production at promotion time if those rows exist there).
- **1 rewritten in place by the importer**: Artis J. McCampbell's HB 202 concurrence record
  already cited the canonical roll-call URL, so the importer adopted it as the canonical row
  (the one `rewrite` in the report).
- **The rest kept**: they describe different votes (the HB 202 March passage roll, the SB 53
  House passage this batch does not import) or different bills entirely (SB 5, flagged only
  because it shares a vote date), plus Rick Rehm's HB 165 sponsorship record, which is a distinct
  claim from a vote.

## Import and reconciliation

- Dry run: 10 files, 0 errors, 534 planned actions.
- Real run (stamp `2026-09-02T00:54:17.408Z`): **533 inserts + 1 rewrite = 534 records, 0 errors,
  0 notified.**
- Reconciled three ways: the report's action totals (534); the run-stamp predicate
  `origin_run_id LIKE 'rollcall:AL:%:2026-09-02T00:54:17.408Z'` (534 rows, 118 distinct
  candidates — every candidate the crosswalk maps); and the Alabama roll-call total (0 before,
  534 after).
- Tags: 431 in the database, exactly matching the prediction (both sides tagged on the four
  no-stance `general` rolls = 209; yea side only on the six stance rolls = 222).
- Convergence: a follow-up dry run reports all 534 `unchanged`.

## Writing checks run before import

- `candidateRecordPlainLanguageLint` over all 20 descriptions: 0 warnings (45-word sentence cap).
- Sentence joins built with periods; asserted that `", The "` appears in no description.
- British spellings scanned for and absent.
- Measured reading grade (Flesch-Kincaid, the Pennsylvania score-don't-eyeball rule): median 10.1
  across the yes-side descriptions, worst 13.6 (HB 202). The two heaviest measures (HB 202,
  SB 53) sit above the 7th-grade target because the four-sentence cap packs the statute's
  load-bearing qualifications into long sentences; dropping those qualifications would trade
  accuracy for readability, which the California batch-01 review round established is the wrong
  trade ("grade ~9 is the honest floor" for immunity and immigration statutes).
