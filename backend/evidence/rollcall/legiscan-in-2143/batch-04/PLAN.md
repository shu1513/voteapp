# Indiana batch-04 — twelve measures read, none kept

**0 records.** Nothing was imported and no judgment was written. This batch is a set of
dispositions and one tool correction. The local database is unchanged at 820 records.

A batch that keeps nothing is a real result, not a failure to try. Twelve measures were
shortlisted on the same test as batch-03 — a House roll where possible, and a title
suggesting a single subject — and each enacted text was read in full. Every one of the
twelve failed a filter, and the reasons are the value of the batch. All seventeen rolls were
member-list checked first; sixteen matched the journal exactly.

## Why nothing survived

The measures that map cleanly onto one research area with an honest direction have largely
been taken. Batches 01 to 03 kept nine such measures. What remains in a session with a large
one-party majority is mostly of three kinds, and this batch hit all three.

**Acts whose subject has no research area.** HB 1064 removes the two provisions barring a
student from transferring between public school districts primarily for athletic reasons. It
is the cleanest act in the batch — single subject, no counter-strand, both rolls on the
enacted text — and there is no area for it. `public_education_quality` is about student
outcomes through teaching, standards, funding and accountability, and an athletic-transfer
rule is none of those. HB 1348, which gives diplomas from nonaccredited private and home
schools legal effect, fails the same way. So does HB 1601, an uncapped sales tax exemption
for quantum and defence computing networks.

**Acts that run both ways inside one area.** HB 1122 creates a 25 foot buffer around a police
officer who orders someone to stop approaching. Inside
`public_safety_and_crime_control` — "effective policing, prevention, accountability, and
justice system performance" — protecting officers and protecting accountability point
opposite ways, and the act has no exception for press, bystanders or people who cannot
retreat. HB 1125 licenses earned wage access providers, requires a free option, and bans
interest, late fees and collection suits — and then declares the advances are not loans and
exempts the charges from Indiana's loansharking rate cap. HB 1412 widens who must report
child abuse and requires police to investigate institutions, and carries a rider stating that
raising or referring to a child consistent with the child's biological sex is neither abuse
nor grounds for a child in need of services finding. HB 1498 orders a new A-to-F school
rating system built by the end of 2025 and, in the same act, gives every school a "null"
grade for 2024-2025. SB 143 gives parents a strict-scrutiny right against government bodies
and bars a government body from telling a child to withhold information from a parent; the
same provision removes a child's confidentiality, inside the same area. SB 318 requires
Indiana media outlets to disclose foreign ownership and funding, and conditions their access
to state government media events on accreditation the state may revoke. HB 1052 orders the
overdue statewide septic rules written and voids local septic ordinances approved after
1 January 2025 that do not conform.

**Acts whose only divided roll predates the act.** HB 1114 raises the penalty for driving
having never held a licence and creates three licence and title fraud crimes. Its only
divided roll is the House third reading of 13 February, and the Senate committee later
removed two whole regimes: mandatory impoundment of the vehicle, and a rule making an
unlicensed at-fault driver or the vehicle's owner pay the insurance deductible of everyone
else in the crash. Describing that vote from the enacted text would credit 93
representatives with a much narrower bill than they voted for. HB 1605, on child welfare
proceedings, fails the same way alongside a subject problem.

## The tool correction

`CODE-FINDINGS.md` section 4, written during batch-03, said the strike-through rule Indiana
draws over deleted words could not be recovered from the file, and that only additions could
be marked. That was wrong. The strike is vector geometry rather than text, so a text-only
extractor misses it, but a library that also reports page shapes finds it directly: it is a
horizontal rule about 0.7 points tall laid across the words it deletes.

`tools/annot.py` now marks additions as `<<...>>` **and** deletions as `[[...]]`. Batch-04
read every act this way. The practical effect is that the residue batch-03 described — claims
about what an amendment removed still needing a rendered page — is gone. Batch-01's SB 289
error, where struck words read as live law, is now visible directly in the text.

## What is left

**34 measures and 68 divided-and-enacted rolls**, each dispositioned in
`../survey/divided-enacted-worklist.tsv`; two still carry the `needs member-list check` flag.
Most of what remains is the session's omnibus work — the budget, and a dozen bills titled
"various education matters", "health matters" and the like — which is the category filter 5
is designed to catch. A later batch should expect a low keep rate and should budget for
reading long acts to reach a drop.

The 2026 Regular Session, LegiScan session 2234, is complete and has never been surveyed.
