# Wisconsin batch-01 — how each measure was judged

Judged 2026-09-09 and 2026-09-10. Local database only; production holds no
Wisconsin roll-call records.

Every description was written from the enrolled act, read with the markup
resolver described below. No description was written from a title or a summary.

## Result

**15 measures, 16 roll calls, 706 candidate records, 99 candidates, 468 area
tags.** Nine measures were dropped with reasons, all recorded here.

The batch covers the whole enacted pool: 24 measures had a closely divided roll
call and became law, and all 24 now carry a disposition.

## ⭐ Wisconsin's enrolled act carries markup, and a plain text extract inverts it

Wisconsin amends a statute in place. Deleted words are struck through and new
words are underlined, and `pdftotext` renders both as ordinary text, run
together. On 2025 Act 42 a plain extract reads "a copy of the rules policy under
par. (a)", where "rules" is being deleted and "policy" added.

The marks are thin **filled rectangles**, not line objects, so pdfminer's
rectangle walk does not see them at all — the first calibration run reported
nine marks on a page and every one was a page border. PyMuPDF's `get_drawings`
finds them. Measured against a passage whose answer was known independently,
and relative to a character's box measured from its top and divided by its
height:

- a strikethrough sits at about **0.50** — the word is being deleted
- an underline sits at about **0.85** — the word is being added

Anything unmarked is existing law being reprinted, not a change. The tool is
`tools/wi_text.py`; it also separates the act's two columns, which
`pdftotext -layout` interleaves.

## ⚠ The version rule cost four measures, and Wisconsin's shape is specific

**Wisconsin rarely takes a recorded vote when the first chamber accepts the
second chamber's amendment.** The bill history prints "Senate Amendment 1
concurred in" with no tally. So when the second chamber amends a bill, the first
chamber's only recorded vote is on the text as it stood before that amendment.

Five slots were in that position. Each amendment was fetched through the
LegiScan `getAmendment` API and read. The line drawn, which is worth reusing:

**Drop when the later amendment removed or replaced an operative provision the
chamber voted on. Keep when it only bounded a provision without changing what
that provision does.**

Dropped on the version rule:

- **AB 320** — Senate Amendment 1 deleted ten lines of the substitute. The
  enrolled act's own "relating to" clause no longer indexes fees to inflation
  and now requires a civil-proceeding interpreter to act remotely, so the
  Assembly voted a materially different bill.
- **AB 453** — the Senate replaced the whole bill with a substitute after the
  Assembly's 55-39 vote, and the Assembly concurred in that substitute by voice.
  A whole-bill substitute is material by definition.
- **AB 75** — Senate Amendment 1 deleted a block running from page 2 to page 3,
  changed "charging recommendation from the referring agency" to "arrest charge
  from the arresting agency", changed "a database" to "an interactive dashboard
  that does not contain any personally identifying information for a criminal
  defendant", and cut the annual report from all the information to a summary.
  The Assembly voted a version with a wider data set and no privacy safeguard.

Kept despite a later amendment, with the reason stated:

- **AB 89** — Senate Amendment 1 added "during a course of conduct" and Senate
  Amendment 2 deleted "in a 6-month period", so the window for adding thefts
  together moved. Both versions aggregate, and the repeat-offense escalation
  that is the act's headline is identical in both. The description is written
  from the enacted act.
- **SB 279** — Assembly Amendment 1 added a sunset: no new grants after 30 June
  2027. The program and its direction are unchanged; the description states the
  sunset as part of the enacted act.

**AB 320, AB 453 and AB 75 have no other closely divided roll**, so all three
leave the batch entirely. AB 453 is the loss that stings: it is the session's
substantial housing supply measure.

## ⚠ AB 1034 and the partial veto

AB 1034 became 2025 Wisconsin Act 203 with a partial veto, and the legislature's
attempt to override that veto failed. Comparing the enrolled print to the
published act shows what the governor struck: the **appropriations** — $14.6
million a year for University of Wisconsin–Madison athletic facility
maintenance, $200,000 a year each for the Milwaukee Klotsche Center and the
Green Bay Phoenix Sports Center — and part of the paragraph barring the use of
general purpose revenue for name, image and likeness deals. The name, image and
likeness framework itself survived.

The measure is **dropped**, for three reasons together: what became law is not
what the Senate voted 17-16; a large part of it is an appropriation, which the
campaign excludes by standing rule; and the name, image and likeness strand has
no research area that fits, since athlete compensation falls in the same gap
that has cost measures in seven other states.

## The other drops

- **SB 45** — the biennial budget act, 2025 Wisconsin Act 15. Appropriations are
  excluded by standing rule. Worth recording separately: **LegiScan marks SB 45
  status 6, failed**, although it became Act 15 with a partial veto. Reading the
  outcome from the bill history rather than the status field is what caught it,
  and it is why the worklist tests for an "approved by the Governor" line.
- **AB 601** — excludes some mobile sports wagers from the criminal definition
  of a bet where the server sits on tribal land under a pre-1993 compact. No
  research area describes gambling.
- **AB 737** — lets a neighborhood improvement district impose a special
  assessment to finance infrastructure for residential development. Inside
  `housing_affordability` it reads both ways: it adds a tool for building
  housing, and it puts a charge on the parcels that runs on to buyers — the act
  adds a line to the seller's condition report precisely because of that.
  `public_infrastructure` was considered and rejected, because it does not
  describe why members divided 64-35.
- **SB 11** — lets a federally chartered youth organization ask a school for
  time to talk to pupils, and the school "may schedule" it. A purely permissive
  provision with no duty on anyone, on the standing rule from California SB 57
  and Alaska SB 50.
- **SB 283** — supplies in-ear hearing protection to police and fire agencies
  through a single contracted manufacturer. `public_safety_and_crime_control` is
  reachable but misdescribes an 18-14 party-line vote that was about a
  single-vendor purchase, not about policing. Dropping a clean single-purpose
  bill beats filing it under the nearest slug.

## The labels, and the reasoning worth reusing

All are `nay: null` except SB 825.

| measure | area | yes means | why |
| --- | --- | --- | --- |
| AB 180 (both chambers) | social_programs_and_welfare | against | Narrows what food assistance may buy. Follows the Alabama SB 57 precedent exactly. |
| AB 2 | public_education_quality | for | Phones out of class time; the area's own words are student outcomes and standards. |
| AB 223 | election_integrity | for | Circulator residency. Follows the Arkansas SB 207 line, where the petition-burden objection maps to no area. |
| AB 35 | election_integrity | for | Creates an orderly withdrawal path and makes a false withdrawal a felony. |
| AB 446 | civil_rights | for | See the note below. |
| AB 592 | public_education_quality | for | Science teacher training. |
| AB 89 | public_safety_and_crime_control | for | Repeat-theft escalation; "justice system performance" is in the area's definition. |
| SB 106 | healthcare_affordability | for | Creates and funds inpatient psychiatric care for under-21s that Wisconsin lacked. |
| SB 108 | social_programs_and_welfare | for | A consent-based crisis plan service for minors. |
| SB 182 | healthcare_affordability | for | Expands the emergency medical workforce and repays training costs. |
| SB 279 | public_safety_and_crime_control | for | Police data-sharing grants. |
| SB 485 | social_programs_and_welfare | for | Safety monitoring in children's residential facilities. |
| SB 56 | environment_and_public_health | for | Lets federal money forgive loans for replacing lead pipes. |
| SB 785 | public_education_quality | for | Teacher license investigations in one public portal; "accountability" is in the area's definition. |
| SB 825 | environment_and_public_health | against, **nay for** | See the note below. |

**AB 446 is the hardest call in the batch and the reasoning is stated plainly.**
The act directs agencies and local governments to consider the International
Holocaust Remembrance Alliance definition of antisemitism when weighing evidence
of discriminatory intent under laws that already ban religious discrimination.
The well-known objection is that the definition's examples chill protected
speech about Israel. That objection is real, but it is foreclosed by the act's
own enacted text: it creates no new penalty and says it may not be read to
diminish any First Amendment right. What the act does on its own terms is
strengthen the evidentiary basis for finding religious discrimination, so
`civil_rights` yes = for. `nay: null`, because a no vote could mean either
"do not adopt this definition" or "this chills speech", and those are not the
same position.

**SB 825 is the only stated nay in the batch.** The act lets a major highway
project clear its environmental review step on a draft statement rather than a
final one, adds a categorical exclusion, which excuses a project from the full
environmental study, and moves the approval from the Federal Highway Administration to the state
transportation department. It is single-subject and its whole content is the
environmental review requirement itself, so a no vote evidences a position for
keeping that requirement. That is the same test used for North Dakota HB 1318
and SB 2339.

## Quality, measured before importing

- Plain-language lint over all 32 descriptions with the repository's own
  `listPlainLanguageWarnings`: **0 warnings**.
- Longest sentence 44 words, under the 45-word limit. One AB 89 sentence
  measured exactly 45 and was split before importing.
- British-spelling scan over the descriptions and these documents: clean.
  It caught six British spellings in the first draft, across colour, programme,
  centre, analyse, licence and misdemeanour words, which is the sixth or seventh
  time that check has earned its keep in this campaign. Note that the scan then
  flagged this very sentence, the North Dakota lesson repeating: re-read any
  sentence that talks about spelling after a scan.
- Each description cites its own roll's tally, and the yes and no versions are
  generated from a single body so they cannot drift apart.

## Reconciliation

- Plan: 706 inserts over 16 rolls.
- Real run: `outcomes {imported: 16}`, `actions {insert: 706}`, 0 errors,
  0 notified, 0 related flags. Run stamp `2026-09-10T04:47:58.229Z`.
- Run-stamp predicate: 706. All Wisconsin roll-call records: 706. Wisconsin held
  zero before this run.
- Tags predicted independently and confirmed: 465 records on the yes side each
  carry one tag, plus the 3 records on SB 825's no side, which is 468.
- 99 distinct candidates. Assembly rolls reach 83 to 88 candidates, Senate rolls
  11.

**⚠ The whole-table row delta is not a valid third check right now.** It read
1,055 against 706 inserted, because five other states were importing at the same
time — Iowa, Michigan, South Dakota, Utah and Oklahoma all wrote candidate
records during this run. The jurisdiction-scoped count is the check that means
something while the campaign runs states in parallel.

**Duplicate sweep: nothing to retire.** Filtering on `origin_run_id NOT LIKE
'rollcall:%'`, one record on a batch measure turned up: a hand-written note that
Representative David Steffen was a prime sponsor of AB 35. It states no vote and
no tally, so it is a distinct claim, matching the Arkansas finding.

## Review fixes, after the first import

Two findings from external review, both checked against the enacted acts and
both real.

- **AB 2 (Act 42).** The description said the health exception was for a need
  "written into a pupil's care plan". The act, s. 120.12 (29) (b) 2. b., says
  only "to manage the pupil's health care"; no written plan is required, and the
  IEP/504 exception is a separate item. The emergency exception also covers "a
  perceived threat", which the description had dropped. Both fixed.
- **SB 825 (Act 110).** The description said a categorical exclusion "means no
  environmental study is required at all". The act defines it by reference to
  23 CFR 771.117: an exclusion from preparing an environmental assessment or
  impact statement. A project still has to be shown to qualify, and unusual
  circumstances can call for more study. Now described as a federal label for
  low-impact projects that excuses them from the full study. The sentence was
  split to stay under the 45-word limit.

Fix path: `build-judgments.py` -> `judgments.json` -> `rollcall:judge` ->
`rollcall:legiscan:import`. Dry run predicted `rewrite 98, unchanged 608`; the
real run at `2026-09-10T05:35:09.281Z` did exactly that (87 records on AB 2, 11
on SB 825), 0 errors. Convergence dry run afterwards: 706 unchanged
(`import-dry-run-rerun-report.json`). Wisconsin still holds 706 roll-call
records. Lint over all 32 descriptions: 0 warnings; longest sentence still 44
words.
