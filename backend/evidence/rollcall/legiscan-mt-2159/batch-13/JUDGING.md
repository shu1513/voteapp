# Montana batch-13 — how each measure was judged

Every judgment rests on the enrolled bill, plus the session law chapter number
read from `api.legmt.gov`. Every roll was compared member by member against
Montana's own vote record; all nine agree exactly. Bill and title lists were
generated from the worklist file, not retyped.

## Why 58 rolls were disposed without a reading

The joint resolutions were checked at the source rather than assumed. In the
LegiScan dataset every one carries `bill_type` `JR`, status 4, and a history
whose final action is "Filed with Secretary of State". There is no governor's
signature and no session law chapter number, and none amends a statute.

Filter 2 asks whether the measure became law. A joint resolution does not. So all
58 remaining rolls are `dropped:filter-2-not-law`, and the campaign's remaining
queue is 207 bill rolls across 149 bills.

## SB 95 — veteran suicide prevention

Chapter 552. Allocates $300,000 from the state special revenue account in
17-6-603 to the health department for the biennium beginning 1 July 2025, for
grants under 53-21-1101 and 53-21-1111.

The permitted uses are listed in the act and the description follows them:
strategies developed with the federal veterans department and SAMHSA, screening
standards and provider education, more trained peers and better care transitions,
and lethal-means safety including a statewide safe storage campaign.

**The act terminates on 30 June 2027**, and the record says so. Direction **for**
`social_programs_and_welfare`.

## SB 110 — amusement rides

Chapter 156. Amends 27-1-741 through 27-1-745. Every amusement ride must be
inspected by a "qualified inspector" at least once every 12 months, the term is
defined, and a ride "may not be operated for the public" without a certificate
from that inspector. The operator must carry liability insurance of at least
$500,000 for bodily injury or death of one person per occurrence and at least
$1 million per occurrence. Passenger responsibilities are expanded.

Scope: 27-1-742(1)(b) is reversed. The old text read "This definition applies
to amusement rides that are set up and operate in any location in Montana for
not more than 30 days" (the introduced bill left it untouched); the enrolled
act strikes "applies" and inserts "does not apply", so rides in one place for
30 days or less — a traveling carnival — now sit outside part 7 altogether.
The description names the 30-day line and no longer uses fairground rides as
its example.

Direction **for** `corporate_accountability`: mandatory independent inspection
and a floor on the operator's insurance for every ride the act covers. The
carve-out narrows who is covered, but it does not loosen any duty on those who
are.

## SB 181 — Indian Education for All

Chapter 557. The accountability provisions are the substance, and they carry
money. The Office of Public Instruction must report annually to the education
interim committee and the state-tribal relations committee. A district that fails
to file its annual report has its BASE and maximum budget limits and BASE aid
reduced "by the full amount of the Indian education for all payment for
subsequent school fiscal years until the report is filed". A district whose
report fails to show the funds went to the stated purposes has its funding
reduced for the following year.

The act also requires tribal consultation and sets out the role of Indian
language and cultural specialists. Direction **for**
`public_education_quality`.

## SB 182 — Indian language preservation

Chapter 558. Gives tribes more flexibility in the educational partnerships the
programme uses, and directs that the programme "be integrated into Indian
language immersion programs of" school districts. Collaborative professional
development is encouraged. Both this act and SB 181 recite Article X, section
1(2) of the Montana constitution, the state's commitment to preserving American
Indian cultural integrity. Direction **for** `public_education_quality`.

## SB 276 — voter identification

Chapter 381. Amends 13-13-114, 13-13-602 and 13-15-107. The marks show four
changes, and they do not all run the same way.

Tightening: identification must be "current, valid, and readable" (inserted),
and the declaration of reasonable impediment route into a provisional ballot is
removed, as the enacted title states.

Neutral: the words "including but not limited to a school district or
postsecondary education photo identification" are struck from the second-tier
photo option, but the option itself — "photo identification that shows the
elector's name" paired with a name-and-address document — survives unchanged in
13-13-114(1)(a)(ii)(B). Striking an example does not exclude it, so a school
district card still qualifies there. The description says so.

Loosening: a "student photo identification card issued by the Montana university
system or a school that is a member of the national association of intercollegiate
athletics" is inserted into the **first** tier, where a student card previously
sat lower and was described more broadly as from "a Montana college or
university".

Judged **for** `election_integrity` on the net: the readability standard and
the loss of the impediment declaration both tighten proof of identity, and the
student card change narrows which schools qualify even as it promotes the card.
The description states the promotion, the survival of the back-up option, and
the removal of the impediment route.

## SB 457 — legislative subpoenas

Chapter 405. New law plus amendments to 5-5-109 and 5-11-107. A chamber, or a
committee with subpoena power, may commit for criminal contempt a witness who
fails to appear or a person who fails to produce records. In session this is done
by simple resolution adopted by a majority of members present; out of session, by
a proclamation sent to members through a legislative poll.

The penalty is quoted rather than characterised: "a misdemeanor and on conviction
must be fined not to exceed $1,000, or be imprisoned in the county jail for a
term not to exceed 12 months, or both."

Direction **for** `anti_corruption`. See PLAN.md for why this sits consistently
alongside HB 531 being judged "against" the same area.

## SB 492 — business disclosure

Chapter 614. Amends 2-2-106. Two marked changes carry the act. In (2)(c) the
words "an interest" are struck and "more than a 10% interest, or if the company
is publicly traded, more than a 1% interest" inserted. In (2)(e) the same
substitution is made for real property other than a personal residence.

So an official who previously had to list every holding now lists only those
above the threshold. Three things the description keeps straight: the filing
cadence in (1)(a) is "prior to December 15 of each even-numbered year" for
sitting officials (candidates within 5 days of filing, appointees on
confirmation or assumption), not yearly; (2)(b) employing entities and (2)(d)
officer or director posts are untouched and carry no ownership threshold; and a
new (4) bars an individual from assuming or continuing in office until the
statement is filed (old (4)-(5) renumbered (5)-(6)).

Direction **against** `anti_corruption`: the thresholds are the operative
change and they shrink what is disclosed. The new filing gate enforces a
narrower statement; it does not widen it.

## SB 560 — nonprofit hospital community benefit

Chapter 627. A nonprofit hospital pays no property tax. The act requires each one
to report charity care and community benefit spending to the health department,
calculated from schedule H of IRS form 990 — net community benefit expenses, net
community building expenses, and total bad debt and Medicare shortfall.

Section 2 sets the test: the annual community benefit must exceed "the prior
year's potential property tax liability the nonprofit hospital would have
incurred". The revenue department supplies that figure. A hospital falling short
pays a fee, and the proceeds fund a new critical access health care account for
small rural hospitals.

Timing: section 8 makes the act effective January 1, 2027, and section 9 applies
the reporting duty (section 1) to critical access hospitals from that date but
the benefit test, fee, account and related amendments (sections 2 through 6) to
them only from January 1, 2031. The description carries both dates rather than
presenting the scheme as already running.

Direction **for** `corporate_accountability`.

## What was not judged, and why

**SB 147** and **SB 430** are left `unbatched`. SB 147's whole operative content
is "Section 55, Chapter 716, Laws of 2023, is repealed" and the enrolled text
never says what that section did; the archive would not serve the 2023 chapter,
so the effect is unconfirmed. SB 430 revises civil commitment and emergency
detention across nine statutes and supersedes the unfunded mandate laws, which is
too much to judge from a title. Both wait for a proper reading rather than a
guess.

**SB 498** was read in full and dropped under filter 5; the reasoning is in
`../survey/filter-5-drops.md`.
