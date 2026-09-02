# Indiana batch-01 — judging

Every judgment was written from the **enrolled act**, read in full, with the Legislative
Services Agency fiscal impact statement used as an index rather than as the text. No AI
provider was called at any point.

## What each measure does, and the direction taken

### SB 289, unlawful discrimination — `civil_rights`, yes = against

The act adds a new article to the Indiana Code making it unlawful discrimination for a
public educational institution, a public employer or a licensing body to base a decision on
a person's race, religion, colour, sex, national origin or ancestry. An employment action is
excepted where it rests on a bona fide occupational qualification; that limit is stated in
the description because dropping it would overstate the ban. It also bars requiring a
licence applicant to affirm, or a public employee to sit through training asserting, that
one group is inherently superior or should be blamed for the past. Separately it rewrites the
eligibility of three named teacher scholarships: the requirement to be a minority is
struck, and in its place a new applicant must come from, live in or agree to teach in one
of five named counties (Allen, Marion, Lake, St. Joseph and Vanderburgh, which the act
calls underserved counties). Renewals for people awarded a scholarship before 1 July
2025 are preserved by the new article's own exception. The act also creates a private
lawsuit for compensatory damages, an injunction, costs and attorney's fees.

The direction follows the settled precedent for this class of act: Ohio SB 1, Texas SB 12
and Tennessee SB 1084 were all recorded as `civil_rights` with a yes vote scoring
`against`. The act's own framing is anti-discrimination, but what it operates on is the
programmes that consider a protected characteristic.

### HB 1393, immigration notice — `immigration`, yes = against

One provision. When someone is arrested for a felony or a misdemeanour and there is
probable cause to believe they are not lawfully present in the United States, the jail must
notify the county sheriff during intake, and the sheriff must report that to the proper
authority. The description says plainly that the act does not change who may be arrested,
because that is the most likely misreading.

The direction follows the area description, not the bill's framing, which is the rule this
campaign has had to restate repeatedly. The `immigration` area is worded as welcoming
immigration, so an enforcement-cooperation mandate scores `against` — the same reading that
made Texas SB 8 and Tennessee HB 749 `against` and Illinois SB 2339 and Connecticut
HB 7066 `for`.

### SB 475, physician non-compete agreements — `corporate_accountability`, yes = for

The act bars a physician and a hospital, a hospital's parent company, an affiliated hospital
manager or a hospital system from entering a non-compete agreement on or after 1 July 2025,
and makes any such agreement void. Three limits are stated in the description because each
one changes the meaning: agreements first signed before that date are untouched; renewing or
amending an old agreement does not count as signing a new one; and the act still permits
confidentiality agreements, one-year bans on recruiting a former employer's staff that may
not restrict patient contact or referrals, and terms attached to the sale of a practice the
physician majority-owned.

The area fits because the act constrains what hospital systems may write into their
contracts, which is the Maryland HB 1020 and Texas SB 1036 line.

### HB 1041, student eligibility in interscholastic sports — `civil_rights`, yes = against

The act requires Indiana public colleges, and private colleges that choose to compete
against them, to designate each team as men's, women's or mixed, and bars a person who is
male based on their biological sex at birth from a women's team. It requires a grievance
procedure, bars retaliation against a student who reports a violation, and gives a student
who loses an athletic opportunity a lawsuit for an injunction, the greater of actual losses
or liquidated damages of up to $1,000, costs and attorney's fees.

The direction follows the Pennsylvania SB 9 and SB 1293 precedent.

## Why every nay side is null

Each label carries `"nay": null`, so a no vote leaves the member untagged rather than
tagged with the opposite stance. The test applied is Connecticut's: a nay stance is only
authored where the act's core mechanism is the area's own mechanism and there is no other
plausible strand to object to.

- **SB 289** — a no vote could object specifically to closing the three teacher
  scholarships rather than to the whole article.
- **HB 1393** — a no vote could object to putting an unfunded duty on county jails.
- **SB 475** — hospitals argue they need to recover recruitment and training costs,
  particularly in rural areas, which is an objection outside the area.
- **HB 1041** — a no vote could rest on leaving the question to the athletic associations
  rather than on the substance.

Because of this, the tag count is 254, exactly the number of yea-side records, not 339.

## Checks run before importing

- Every description body was joined with a period, and the builder asserted that the string
  `", The "` appears nowhere, so the comma splice that hit Illinois twice cannot recur.
- `listPlainLanguageWarnings`, the real 45-word sentence lint, was run over all 12
  descriptions: **0 warnings**.
- Reading level was measured separately, because that lint only counts sentence length:
  **mean sentence 19.8 words, longest 42, Flesch-Kincaid grade 8.8.** Grade 9 or so is the
  honest floor here. Getting to grade 7 would mean dropping the statutory limits — the
  bona fide occupational qualification, the pre-July-2025 carve-out, the "greater of"
  damages rule — and dropping exactly those limits is what has caused most of this
  campaign's correction rounds.
- Each roll's own tally appears in both its yes and its no sentence, which the judge's
  approval gate requires.
- The member list of all six rolls was verified against the official Indiana roll-call PDF.

## Review response, 2026-08-31

Two findings on the first review of this batch, both checked against the enrolled acts and
both real. 180 records were rewritten in place (SB 289's 93 and HB 1041's 87); the other
159 were untouched, and the convergence dry run afterwards reported all 339 `unchanged`.
The rewrite ledger is `import-rewrite-report.json`; the original insert ledger
`import-report.json` is unchanged, since a repeat real run now writes its report to
`import-rerun-report.json`.

**SB 289 scholarships (P1, accepted).** The first description said the act "closes three
state scholarships for minority teachers to new applicants". That is wrong. The enrolled
act's amendment marks — which `pdftotext` renders as plain text, so the pages had to be
rendered and read — show the words "meet the definition of a minority" struck and
underserved-county requirements added in their place, in all three programs. The act
converts the eligibility from minority status to a tie with five named counties; it does
not close anything. The mistaken sentence came from over-reading the new article's
renewal exception, which protects renewals for pre-July-2025 recipients and says nothing
about new applicants. This is the struck-text hazard Georgia and Maine recorded, and it
bit here on the one measure where the stance itself did not hinge on struck text, so the
page-render step was skipped. The lesson: **render the pages for every claim that rests on
what an amendment removed, not only for claims the stance depends on.**

**HB 1041 damages (P2, accepted).** The description said a student may recover "the
greater of their real losses or $1,000", dropping the act's cap wording: liquidated
damages are "not more than one thousand dollars", and the court *may* award them. The
first draft had "up to $1,000" correctly, and the wording pass that shortened the
sentences dropped it — the same simplification regression Pennsylvania and Connecticut
documented, caught here by review rather than by the pre-import diff. The text now reads
"up to $1,000, if a court grants them". JUDGING.md itself already had the correct wording,
which is how the reviewer knew the JSON had drifted from its own analysis.

## Import ledger

Dry run and real run agree exactly.

| | |
| --- | --- |
| Files | 6, all `imported`, 0 errors |
| Planned inserts (dry run, stamp `2026-08-31T06:30:58.785Z`) | 339 |
| Actual inserts (stamp `2026-08-31T06:31:24.854Z`) | 339 |
| Candidates | 101 |
| Area tags | 254 |
| Notifications | 0 |

Reconciled three ways: the run-stamp predicate
`origin_run_id LIKE 'rollcall:IN:%:2026-08-31T06:31:24.854Z'` returns 339 records over 101
candidates; the whole-jurisdiction predicate `'rollcall:IN:%'` returns the same 339, batch-01
being Indiana's only import; and the dry run's own stamp matches **zero** rows, which is
positive proof that `--dry-run` wrote nothing. A convergence dry run afterwards reported all
339 `unchanged`.

## Related-record flags

Two, both inspected and both false positives: hand-written records dated the same day as the
HB 1041 roll but about different bills (HB 1292 and HB 1500). No records were retired.

## Production

Untouched. Indiana has no records in production.
