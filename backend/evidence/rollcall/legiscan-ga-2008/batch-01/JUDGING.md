# Judging notes, Georgia 2023-2024, batch 01

Sixteen roll calls on nine measures. Every one became law; the Act number and
signing date are in each bill's own history.

## What each chamber actually voted

Checked for all nine before any description was written. Every text was
downloaded through the LegiScan API with byte length and MD5 verified against
the dataset manifest.

Seven of the nine are straightforward: one chamber passed a substitute and the
other agreed to it unamended, so the Enrolled print is what both chambers voted.
Those are SB 140, HB 1105, SB 44, SB 222, SB 420, HB 1018 and HB 1015.

Two needed a closer look, because the text grew sharply late:

- **SB 63** sat in a conference committee from March 2023 until February 2024.
  Both imported rolls are votes to adopt the conference committee report, taken
  on 1 February 2024 in the Senate and 6 February in the House, and the bill was
  enrolled on 7 February. The feed labels the conference print as `Introduced`
  and dates it 31 January 2024; its `amendments[]` array holds only two 2023
  House floor amendments, not the report. The Enrolled print is therefore the
  document both rolls voted, and it is 128 KB against the 46 KB the bill was
  introduced at in 2023. Read it rather than assuming.
- **SB 420** was reported out of House committee at 84 KB on 20 March 2024 and
  passed the House by a floor substitute the next day at 110 KB. Reading the
  enrolled text explains the 26 KB: the House substitute added an unrelated
  Title 44 subject, transfer-on-death deeds, to a farmland-ownership bill. The
  Senate then agreed to that combined text, so both rolls voted both subjects,
  and the description says so.

No vehicle-bill trap: in every one of the nine, the enacted text is on the
subject the caption and short title name.

## Supersession

One roll tripped the gate. The House vote on SB 222 (roll 1286179, House Vote
#334, passage) shares its day with roll 1286178, House Vote #333, `Agree To
Committee Report`, 100-70. Vote #333 precedes vote #334, so passage is the final
action, and 1286178 is listed under `acknowledge_later_rolls` with that reason.

This is a recurring Georgia pattern worth recording: the House often votes to
agree to its own committee's report immediately before the passage vote on the
same bill on the same day. The gate counts same-day peers, so it flags the
earlier procedural vote every time. The check is still worth running — the
printed vote number settles the order in seconds.

## Label reasoning

Every label uses `nay: null`. In each of these nine, the members who voted no
did so for reasons that do not amount to the opposite stance, so tagging them
would misstate the record. Under the explicit-nay contract, no voters get no tag.

- **SB 140**, `civil_rights`, yea against. It bans hormone replacement therapy
  and sex reassignment surgery for minors treated for gender dysphoria. Follows
  the 2025-2026 precedent that put SB 1, the athletics bill, under
  `civil_rights` with yea against.
- **HB 1105**, `immigration`, yea against. The area's direction is
  pro-immigration, as the existing federal records show: voting against the
  Secure the Border Act is tagged `for`. This Act adds enforcement duties on
  jails and local police and penalties for sanctuary policies, so a yes vote is
  `against`.
- **SB 63** and **SB 44**, `public_safety_and_crime_control`, yea for. Both
  tighten criminal penalties and pre-trial release. Follows the 2025-2026
  precedent for SB 443.
- **SB 222**, `election_integrity`, yea for. Requiring election administration
  to be paid from public money only is an integrity measure on the face of the
  record. The provision moving the State Election Board out of the Secretary of
  State's office is a dispute about control, which is why nay is null.
- **SB 420**, `national_defense`, yea for. The restriction is narrow: it reaches
  agricultural land, and only people acting for governments the United States
  Secretary of Commerce designates as foreign adversaries under 15 C.F.R. 7.4,
  or entities domiciled in those countries or at least 25 percent owned from
  them. That narrowness is what makes `national_defense` the right area rather
  than `civil_rights`; a blanket ban on a nationality would be the other case.
- **HB 1018**, `gun_control`, yea against. It bars the firearms merchant
  category code, bars sharing gun-related purchase records, and bars firearm
  registries.
- **SB 351**, `data_privacy`, yea for. It requires a parent's express consent
  before a social media platform may let a child under 16 hold an account
  (the Act's own definition of a minor; the under-18 definition elsewhere in
  the bill belongs to the obscene-material section).
- **HB 1015**, `personal_income_tax_reduction`, yea for. It cuts the individual
  rate. Follows the 2025-2026 precedent for HB 111 exactly.

## Descriptions

Each cites its own roll call's tally and names the measure's operative
provisions from the enacted text. Plain-language lint: 32 descriptions, 0
warnings, median Flesch-Kincaid grade 8.4, worst 11. The worst is SB 222, where
"State Election Board", "Secretary of State" and "election administration" are
the actual names of the things the Act moves and cannot be shortened without
losing what a voter needs to know.

## Duplicates

Swept the 181 candidates who received records for any non-roll-call record on
the same measure and date. 0 found.
