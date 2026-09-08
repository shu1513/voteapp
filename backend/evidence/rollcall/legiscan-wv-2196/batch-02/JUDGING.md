# Judging notes, West Virginia 2025 regular session, batch 02

Every bill text here was read on the state's own site, wvlegislature.gov. Where a
chamber amended a bill on the floor and the amended text was never published as
its own version, the adopted amendment itself was read. West Virginia publishes
those, which is what made three of these records possible.

## The version trap, and what it changed

West Virginia dates every text version 0000-00-00, so the usual dated check on
which text a chamber voted cannot be run. The action history and the version
names carry the sequence instead. Three measures turned on this.

**HB 3412 is two different bills under one number.** The House voted a short text
that removes the Legislature from the state open records law and lets it write
its own rules instead. The Senate then struck everything after the enacting
clause and replaced it with a broad narrowing of the open records law that
applies to every public body. The Senate version puts the Legislature back in the
definition of a public body. By the Senate's own adopted title, its rewrite
widens what is exempt, removes the presumption that records are public, extends
the response deadline from five days to 14, allows search and retrieval fees,
and drops the duty on a public body to justify withholding a record. The two
roll calls therefore carry two different descriptions. Treating them as one vote
on one bill would have been wrong.

**SB 460 was not the same text in each chamber either.** The Senate passed a
committee substitute allowing a parent to opt a child out of school vaccine
requirements on religious or philosophical grounds. The House adopted its own
committee amendment, which replaced the whole bill with a medical exemption
written by any physician, physician assistant or nurse practitioner, and then
adopted a floor amendment adding a religious exemption only. The House rejected
that version 42 to 56. The House record says religious objection and does not
claim the philosophical objection the Senate allowed.

**HB 3446 no longer matches its own title.** The bill was introduced to make
filing the federal student aid form a condition of getting a high school
diploma. A floor amendment removed that entirely before the vote, leaving a duty
on schools to help students file, or to record that a family understood the form
and chose not to. The description says what was voted, and says the graduation
condition was removed first.

**HB 3017 grew a second subject in the Senate.** The House passed a bill on
inspecting and auditing ballot counting machines. The Senate adopted a floor
amendment adding a complete complaint and investigation procedure for election
law violations, then rejected the whole thing 16 to 18. The description names
both parts. Both point the same way on election integrity, so the label holds.

## Why each imported measure carries the label it does

- **HB 2710, Truth in Giving** — `corporate_accountability`, yes for. It forces a
  for-profit reseller of donated goods to post that it is not a charity.
- **HB 2719, campaign finance** — `anti_corruption`, yes against. The operative
  change is ending the ban on companies giving directly to candidates. The bill
  does add disclosure for those donors, but that disclosure exists only because
  the ban is lifted, so it does not make the bill an anti-corruption measure.
- **HB 3017, ballot machine audits** — `election_integrity`, yes for.
- **HB 3276, water and sewer rates** — `cost_of_living_reduction`, yes against.
  It lets a public utility raise household bills once a year with no hearing and
  no vote by any elected body, up to four years in a row.
- **HB 3412, both chambers** — `anti_corruption`, yes against. Both texts reduce
  public access to government records.
- **HB 3446, student aid form** — `public_education_quality`, yes for.
- **SB 460, both chambers** — `environment_and_public_health`, yes against. The
  operative effect is to end enforceable school vaccine requirements.
- **SB 548, school violence training** — `public_safety_and_crime_control`, yes
  for. Note that the committee cut the suicide prevention mandate and the
  anonymous tip line that were in the introduced bill, so the description
  describes only the two training hours that were actually voted.
- **SB 579, Home Rule** — `civil_rights`, yes against. The title says home rule
  reform and 18,000 characters of unchanged statute are reprinted, but the one
  operative sentence bars home rule cities from protecting any group state law
  does not already protect, and voids the ordinances they already passed. There
  is no honest second reading.
- **SB 592, storage tanks** — `environment_and_public_health`, yes against. It
  narrows the law West Virginia passed after the 2014 Elk River spill. The
  committee version quietly added an exemption for tanks on coal mining permit
  sites, a subject absent from the introduced bill.
- **SB 718, hospital transparency** — `healthcare_affordability`, yes for.
- **SB 730, forest carbon registry** — `corporate_accountability`, yes for. It
  puts disclosure duties on the companies that arrange the deals.

## The measures dropped for having no honest direction

Six were dropped because a reasonable person could call a yes vote good or bad
on the same topic from the same text.

- **SB 505** cuts the utility rate of return and pushes the cost of failed assets
  onto shareholders, which helps ratepayers, while a capacity-value multiplier
  added in committee reduces the return on wind and solar. Both effects are real
  and they point opposite ways.
- **SB 663** on bank access reads as consumer protection or as forcing banks to
  serve customers they turned away.
- **SB 748** funds policing through a new county sales tax.
- **SB 488** narrows the definition of electioneering, which both protects speech
  near a polling place and loosens a polling place rule.
- **HB 2777** governs home instruction rather than public school quality, and
  pairs looser proof of academic progress with a new child abuse screen and a
  school choice portal.
- **SB 521** puts party labels on judicial and school board ballots, which is not
  a question of election security, accuracy or auditability.

**HB 3422** was dropped under the standing rule that school choice has no
defensible direction under public education quality.

**HB 3111**, a pay raise for judges, lands on the no-stance general label, the
same call made on South Carolina S 933.

## Review round

Four descriptions were corrected after pull request review, each checked against
the bill text before changing a word. The re-import rewrote 55 records and
inserted none; the report is `import-review-fixes-report.json`.

- **HB 3412 (Senate)** said the response deadline "doubled" from five days to
  14. That is not double, and both figures exclude weekends and holidays. It now
  says extended from five business days to 14.
- **SB 548** said "every public school" and "one hour." The act allows one class
  period instead, and section 6 makes it voluntary for charter and private
  schools. Both qualifications are now in the description.
- **SB 592** said the exempted tanks "dropped out of regulation." Every exemption
  depends on the tank sitting outside a drinking water protection zone, and the
  same kinds of tanks inside a zone stay regulated with owner self-certification.
  The description now says so.
- **SB 718** started the $1,000 daily penalty ten days after the filing deadline.
  Section 7(b) starts it ten days after the hospital receives notice.
