# Reading brief: Arkansas enrolled Acts

You are reading enrolled Arkansas Acts from the 2025 Regular Session and recording exactly
what each one does. This is research only. Do not edit anything in the repository, do not
touch any database, and do not call any AI provider API.

## Writing style (mandatory for every word you write)

Plain, concise English. Short sentences. Explain any legal or technical term the first time
you use it. No invented abbreviations. No throat-clearing and no account of your process.
Write complete sentences a non-specialist can follow. Do not imitate a compressed or
telegraphic style. Use American spellings.

## The tool you must use

Arkansas prints statutory amendments in place: deleted words are struck through and new words
are underlined, and a plain text extraction renders BOTH as ordinary text. Reading the plain
extract can invert an Act's meaning. Use the reader that resolves this:

    python3 /Users/shu/legiscan-data/ar-work/ar_text.py <doc_id> <first_page> <last_page>

It prints deletions wrapped in [[double square brackets]] and additions in <<double angle
brackets>>. **Text with no markup is existing law being reprinted, not something the Act
changed.** For a fast unmarked pass to see the section headings and page count:

    python3 /Users/shu/legiscan-data/ar-work/ar_text.py --raw <doc_id>

Always run `--raw` first, then run the marked reader over EVERY page that carries an operative
section. **Read the whole Act, top to bottom.** Excerpts are not the text. Never start a read
at an arbitrary line offset — anchor on a section heading, because a page break can fall in
the middle of a sentence and hide the verb or the date.

## Rules that have cost this project real errors

1. **The title is not the text.** Read what the sections do, not what the caption promises.
2. **Say who is bound by each rule** — the specific people, bodies or businesses the statute
   names, not a loose paraphrase. If a statute defines a class of business, read the
   definition before paraphrasing it.
3. **Carry every limit**: exceptions, effective dates, intent or knowledge requirements,
   numeric thresholds, dollar figures, penalty grades, and every "may" versus "must".
4. **Check for a contingent effective date.** Some Arkansas acts take effect only if some
   other condition occurs; such an act changed nothing when it passed.
5. **Read an eligibility clause to its end.** An "and" between conditions is load-bearing.
6. **For a criminal penalty**, state the offense class and whether a first offense is criminal
   or civil.
7. **Report only what you read.** If you could not read something, say so plainly rather than
   filling the gap. Do not delegate any part of the reading to another agent.

## What to write

For EACH Act, write one JSON file to `/Users/shu/legiscan-data/ar-work/reads/<BILL>.json`
(for example `HB1204.json`), with exactly these keys:

```json
{
  "bill": "HB 1204",
  "act": "Act 28 of 2025",
  "doc_id": 3114475,
  "pages_read": "1-6 of 6",
  "what_it_does": ["one operative change per entry, each naming the Arkansas Code section it changes or saying 'new section' or 'uncodified', who is bound, and every limit"],
  "title_mismatch": "what the title or subtitle does not tell a reader, or 'the title is accurate'",
  "runs_both_ways": "name any provisions pushing in opposite directions on the same subject, or 'no'",
  "contingent_or_delayed": "any contingent or future effective date, or 'no'",
  "direction": "one sentence: which way does this Act push, and on what subject",
  "suggested_area": "one slug from the list below, or 'none'",
  "suggested_yea": "for | against | null",
  "confidence": "high | medium | low",
  "quotes": [{"section": "§ ...", "text": "short direct quotation"}],
  "notes": "anything odd worth flagging"
}
```

Give three to six quotations per Act, covering the claims that matter most.

## Research areas you may suggest

anti_corruption, civil_rights, corporate_accountability, cost_of_living_reduction,
data_privacy, election_integrity, environment_and_public_health, government_efficiency,
government_spending_reduction, gun_control, healthcare_affordability, housing_affordability,
immigration, national_defense, peaceful_foreign_policy, personal_income_tax_reduction,
public_education_quality, public_infrastructure, public_safety_and_crime_control,
reduce_wealth_gap, social_programs_and_welfare, womens_reproductive_rights.

Their exact definitions decide direction, so read them:

- election_integrity = elections are secure, accurate, auditable and trusted by the public.
  It is NOT about voter access; measures expanding or restricting access go to civil_rights.
- immigration = welcome immigration through a lawful, orderly and humane system. So an
  enforcement measure scores AGAINST this area, whatever its own framing.
- civil_rights = protect equal rights, anti-discrimination enforcement, fair treatment.
- housing_affordability = increase housing supply and reduce cost burdens.
- corporate_accountability = hold companies accountable for legal compliance, consumer
  protection and public impact. It needs a company-specific target.
- healthcare_affordability = reduce out-of-pocket costs and improve access to affordable,
  quality care.
- government_efficiency = improve service delivery, reduce waste, modernize administration.
  Creating an office counts only when its subject is the machinery of regulation itself.
- public_safety_and_crime_control = safety through effective policing, prevention,
  accountability and justice system performance.
- environment_and_public_health = protect air, water, climate and community health.

Say `"suggested_area": "none"` when no area honestly fits, and say so in `direction`. Say
`"suggested_yea": null` when the Act genuinely pushes both ways inside one area. **A contested
question is fine; a contested DIRECTION is not.** If "more access" and "more caution" are both
defensible readings of the same act on the same axis, say so — that is a real finding, not a
failure.
