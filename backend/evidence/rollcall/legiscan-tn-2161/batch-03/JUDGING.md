# Judging notes, Tennessee batch-03

Thirteen roll calls on eleven measures, every one enacted with a public chapter
number. All eleven chaptered acts were downloaded through the LegiScan API with
byte length and MD5 verified against the dataset manifest, extracted to text and
read. Scope for this batch is `--scope-from 2026-08-01`, Tennessee's primary
date, per the session README.

## The version check found one real mismatch

**HB 618.** The House passed it 61-26 on 27 February 2025. A year later the
Senate substituted, adopted Amendment 2 and passed it 21-10 on 26 February 2026;
the House then concurred 76-14 on 5 March 2026, which is not divided. So the
House's only divided roll is on the pre-amendment text.

Comparing the House-passed draft (795 characters) with the chaptered act (1,458
characters), the rule the House voted is the rule that became law: a local
elected official may not hold a second elected office, with a grandfather clause
and an exception for party executive committee seats. What the Senate amendment
changed is *when* the ban starts. The draft grandfathered officials "until the
terms expire" and took effect on becoming law; the act sets 1 January 2027, and
lets those already holding two offices on that date keep them and stand for
re-election.

That is a real difference, so the two chambers get **two different
descriptions**. The House record describes the text the House voted and says the
Senate later changed when the ban starts; the Senate record describes the act.
This follows the SB 336 precedent from batch-01. The House roll carries
`acknowledge_later_rolls: [1655770]` for the undivided concurrence.

Every other measure in the batch had its roll on the text that became law.

No vehicle-bill trap in these eleven. Byte size was not used as a proxy for
content: HB 618's draft is 8,041 bytes against a 54,815-byte chaptered act, and
the two texts are 795 and 1,458 characters.

## Supersession

Only HB 618 tripped the gate, resolved above.

## Label reasoning

Every label uses `nay: null`.

- **HB 910**, `civil_rights`, against. SECTION 20 terminates the Tennessee Human
  Rights Commission at the end of the fiscal year ending 30 June 2025; SECTION
  19 creates a civil rights enforcement division inside the attorney general's
  office to take the work. The commission had handled discrimination complaints
  in employment, housing and public accommodations.
- **HB 754**, `civil_rights`, against. Gender clinics must report every gender
  transition procedure to the health department, including the patient's age,
  sex and state of residence, drug names, doses, frequency and route, surgical
  CPT codes, and the prescribing provider's name, contact information and
  specialty. It also bars counties and municipalities from prohibiting therapy
  directed at a minor's gender identity, and conditions state funds on a clinic
  agreeing to provide or pay for detransition procedures.
- **SB 376**, `civil_rights`, against. Four-year institutions may not treat a
  person differently by race, color, ethnicity or national origin, "including
  those resulting from affirmative action practices" — the act's own words,
  which is what makes the direction plain rather than inferred.
- **SB 1004**, `womens_reproductive_rights`, **for**. It defines "serious risk
  of substantial and irreversible impairment of a major bodily function" in the
  criminal abortion statute and names qualifying conditions: previable preterm
  premature rupture of membranes, inevitable abortion, severe preeclampsia,
  mirror syndrome with fetal hydrops, and infection that can cause uterine
  rupture or loss of fertility. Naming them widens a standard that had been
  vague enough for physicians to fear prosecution, which is why the direction is
  `for`. The act also says no mental health condition counts, and the
  description says so — that exclusion is a genuine limit and readers should see
  it rather than have it left out to make the label tidier.
- **SB 674**, `reduce_wealth_gap`, against. The general assembly preempts and
  occupies the entire field of employment terms and conditions; no local
  government may adopt or enforce any requirement exceeding state or federal law
  unless state law expressly allows it. That removes local authority over pay,
  hours and working conditions.
- **SB 880**, `environment_and_public_health`, against. An agency may not adopt
  a numeric limit for a contaminant or pollutant in drinking water, water
  pollution control, air quality, hazardous substances, site remediation or
  waste handling unless the best available science establishes a direct link to
  "manifest bodily harm in humans."
- **HB 2569**, `environment_and_public_health`, **for**. Hospitals must offer
  inpatients aged 50 and over, rather than 65 and over, an influenza
  immunization each year from 1 October to 1 March, and a pneumococcal
  immunization, before discharge.
- **HB 22**, `anti_corruption`, **for**, and **HB 2616**, `anti_corruption`,
  against. Both amend Tennessee's open meetings law and pull in opposite
  directions. HB 22 requires every local governing body to reserve a public
  comment period at each meeting, covering both agenda items and anything else
  germane to the body's jurisdiction. HB 2616 lets a governing body interview
  director-level job applicants in executive session with no public notice. The
  description for HB 2616 states the act's own safeguards — no hiring decision
  or deliberation in private, minutes must name everyone present except the
  applicants — because they bound how far the carve-out goes, and a reader is
  entitled to weigh them.
- **HB 618**, `anti_corruption`, for. It bars simultaneous local elected
  offices.
- **HB 1704**, `immigration`, against. It makes it a Class A misdemeanor for a
  person 18 or older to intentionally fail to leave Tennessee within 90 days of a
  final federal removal order, with a mandatory stay while federal challenges
  are unexhausted.

## Descriptions

Each cites its own roll call's tally. Plain-language lint: 26 descriptions, 0
warnings, median Flesch-Kincaid grade 7.6, worst 10.0.

## Duplicates

Swept the 99 candidates who received records for any non-roll-call record on the
same measure and date. 0 found.
