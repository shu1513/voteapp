# Montana batch-18 — how each measure was judged

Every judgment rests on the enrolled bill. Chapter numbers were fetched from
`api.legmt.gov` in this session. All twenty-nine rolls agree with Montana's own
vote record member for member. Bills were read in parallel by agents; every fact
used here was checked against the enrolled text or the API before it changed a
judgment.

## SB 535 — the roll that had to be argued for

Chapter 621. The importer's superseded-stage gate refused the House roll, and
working out why took the official record rather than the LegiScan feed.

The House voted twice on 25 April 2025. Roll 1558163 is "(H) 3rd Reading Failed"
at 50-50 — a tie, which fails. Roll 1558165 is "(H) 3rd Reading Concurred" at
49-48. Montana's action trail gives the sequence unambiguously:

> 2025-04-25 (H) 3rd Reading Failed
> 2025-04-25 (H) Scheduled for 3rd Reading
> 2025-04-25 (H) Scheduled for 3rd Reading
> 2025-04-25 (H) 3rd Reading Concurred
> 2025-04-25 (H) Returned to Senate with Amendments

and separately "(H) Reconsidered Previous Action; Remains in 3rd Reading
Process". The Senate concurred in the House amendments on 28 April and passed the
bill 29 April.

So the concurrence is the decisive House vote and the failed roll is superseded
by the reconsideration. Roll 1558163 is named in `acknowledge_later_rolls`. It is
the only use of that key in the campaign, and it is documented here rather than
applied quietly, because the whole value of the gate is that overriding it costs
an explanation.

## SB 535 — what the act does

The record carries four things, because any one of them alone would mislead. A
centre must now hold a state licence, at $10,000 to apply and $5,000 a year. The
definition of an experimental treatment widens: it used to require a treatment
that had passed phase 1 **and** remained in an FDA-approved trial; it may now
instead rest on "a demonstrated safety record through documented clinical
evidence from a qualified medical institution". Consent may be a recording rather
than a signed form, and two safeguards are struck, including the duty to warn
that "new, unanticipated, different, or worse symptoms might result". And direct
agreements between a centre and a patient are placed outside the insurance code.

Direction **against** `environment_and_public_health`.

## SB 218 — precedent, not a new call

Chapter 560. This measure had been held as a question for the user. It is decided
on precedent already set four times: HB 690 (batch-09), HB 655 and HB 694
(batch-16) and HB 446 (batch-17) are all `civil_rights` "against" for an act that
narrows what one group may do or receive inside a health or family system.

The conditions are the act and are all in the record: the patient must have been
a minor; the treatment must be on the listed surgical, hormone or puberty blocker
lists; and liability requires "a deviation from the applicable medical standard
of care ... as established by qualified expert testimony". The limitation period
is two years from discovery and does not begin until the person turns 23, with
further pausing where the clinician concealed or applied pressure. Treatment of a
child born with a medically verifiable disorder of sex development is excluded.

## SB 74 — one word with a fiscal effect

Chapter 550. The taxable retail price of marijuana changes from the price
"before" any discount to the price "after" it, so tax follows what the customer
actually pays. That is the whole of the tax change, and it is backdated to tax
quarters beginning after 30 June 2025.

The record also carries the licence fee restructure — the maker's fee moves from
each facility to the licensee's total output, so a multi-site company pays once —
and the hotline change, where complaints may now be anonymous but become
confidential. Direction **against** `government_spending_reduction`, following
the four tax-break measures in batches 10 and 15.

## SB 113 — towed vehicles

Chapter 178. The act's preamble says it clarifies. It narrows, three ways, and
the record says all three. A gate is inserted so the protected list applies only
to vehicles of "an uninsured or underinsured person, or for which an insurance
claim was not filed or insurance coverage for towing and storage cannot be
verified". Each protected item shrinks, so only **original** licences, identity
papers and records are covered. And containers lose protection: "wallets, or
purses, bags, or other containers that contain the items listed" becomes "wallet
or purse". Direction **against** `corporate_accountability`.

## SB 143 — deadlines to sue

Chapter 174. Written contract claims fall from 8 years to 6. Construction and
land surveying claims fall from 10 years to 6. The late-injury window that ran
into the tenth year now runs into the sixth. Direction **against**
`corporate_accountability`.

## SB 149 — emotional support animals

Chapter 360. Two statutes are amended and **they are not amended alike**, which
the record reflects. Both change "Supporting information may include" to "must
include", change the list connector from "and" to "or", require the practitioner
to be "licensed to practice in this state", and require that the practitioner has
determined the animal provides support. Only 70-33-110, covering mobile home
lots, gains the further provision that supplying documentation "may not be
construed to mandate or compel the landlord or a business to permit the entry or
presence of the emotional support animal". Direction **against** `civil_rights`.

## SB 194 — public assistance appeals

Chapter 366. The Board of Public Assistance is abolished and appeals go to the
department instead. The record notes the wind-down, because it is what a person
with a pending appeal needs: the board "shall remain operational until all
appeals that were filed with the board on or before" the effective date are
resolved. Direction **against** `social_programs_and_welfare`: the body deciding
an appeal is now the body that made the decision.

## SB 249 — carers giving evidence

Chapter 146. The statute exists in a temporary and a July 2025 version and **both
are amended identically**, which was checked. The change is the opening clause:
the duty to hear evidence from carers applied only "In a case in which
abandonment has been alleged"; that is struck, so it applies in every
adjudication. Direction **for** `social_programs_and_welfare`.

## SB 253 — scholarship organisations

Chapter 592. The gatekeeping flips from notice to permission: an organisation
must now apply for certification before accepting donations that earn the tax
credit, the department decides within 60 days, certification lasts at most two
tax years, and a final refusal "may not be appealed". Each organisation must
publish its award processes, the providers it funded, and a stated sentence
telling parents the scholarship may be used at any qualified provider of their
choice. The steering ban tightens from "a particular education provider" to
"limit, restrict or reserve" scholarships for "a single education provider or any
particular type". Direction **for** `public_education_quality`.

## SB 278 — and a void section

Chapter 594. Sections 1 and 2 create the advanced opportunity facilitator role
and let a district pay for one from leftover advanced opportunity aid.

**Section 3 is void.** SB 278's own coordination instruction provides that if
HB 252 also passed and amends 20-9-327, then section 3 is void and HB 252's
section must instead carry the facilitator language. HB 252's enrolled text does
amend 20-9-327. So the quality educator payment reaches the statute book through
HB 252, and the record describes the outcome rather than SB 278's own void
section.

## SB 390 — vaping

Chapter 574. One word does the work. "'Smoking' or 'to smoke' includes" becomes
"means", which turns an open list into a closed one, and "inhaling, exhaling" and
"an electronic smoking device" are inserted. Because the Clean Indoor Air Act
already bars smoking in enclosed public places, vaping is now barred everywhere
that Act reaches, without the list of places changing at all. Direction **for**
`environment_and_public_health`.

## SB 471 — pedestrian crossings

Chapter 580. The record carries the escape clause as well as the duty, because
without it the rule would read as absolute: a vehicle that "reaches the
intersection before the pedestrian-actuated device is engaged", or that cannot
safely stop, "may continue through the intersection at a safe speed". The two
fine scales are stated separately, since the higher one applies only where the
flashing device was activated. Direction **for**
`public_safety_and_crime_control` as a road safety duty, not a sentencing
provision.

## SB 508 — THC and drivers under 21

Chapter 412. The act inserts a new paragraph making any amount of THC an offence
for a driver under 21, against the adult threshold of 5 nanograms per millilitre
which is left in place. The record states that inactive metabolites are excluded,
because those can persist long after any effect and their exclusion is what stops
the rule reaching stale traces. Direction **for**
`public_safety_and_crime_control` as a detection standard, on the same footing as
HB 344 and HB 467 in batch-11.

## SB 553 — airline credits

Chapter 625. Most of this act is a committee rename repeated fifteen times. What
survives filter 5 is the travel credit part: a credit "is valid until redemption
and does not terminate", its value "belongs to the possessor and not to the
issuer", it becomes trust property if the airline goes bankrupt, no fee may
reduce it, and a holder may demand cash where the original value was over $5 and
under $5 remains. It applies retroactively to credits issued from 1 January 2025.
Direction **for** `corporate_accountability`.

## SB 495 — tobacco prevention board

Chapter 198. The board is repealed and its funding line struck from 17-6-606. The
record says the underlying programmes and their shares of settlement money are
unchanged, so the act reads as the removal of oversight rather than of the
programmes. Direction **against** `environment_and_public_health`.

## HB 853 — a licensing change hidden in the definitions

Chapter 737. The fee rises are the visible part and are in the record. The quieter
change is in the list of things that are **not** retail food establishments,
where "a hotel, a motel," is struck. Hotels and motels serving food therefore
become licensable and inspectable as retail food establishments. Direction
**for** `environment_and_public_health`.

## HB 932 — conservation financing

Chapter 758. The 20 percent marijuana share that went to the wildlife department
is redirected to a new habitat legacy account, split 75/20/5 between stewardship,
habitat projects and wildlife crossings. The HEART account's share changes from a
$6 million ceiling to 11 percent. The advisory council's membership is rebuilt.
The record does not claim the old matching and weed-control requirements survive,
because the act strikes them and says preference factors "may not be considered
mandatory". Direction **for** `environment_and_public_health`.
