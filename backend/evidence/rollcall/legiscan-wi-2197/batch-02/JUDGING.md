# Wisconsin batch-02 — how each measure was judged

Judged 2026-09-10. Local database only; production holds no Wisconsin roll-call
records.

Every description was written from the enrolled print, read with the markup
resolver described in the batch-01 document. No description was written from a
title or a summary.

## Result

**9 measures, 15 roll calls, 839 candidate records, 99 candidates, 484 area
tags.** Four measures were read and dropped, with reasons below.

This batch is the education strand of the vetoed pool. Thirteen measures were
read; the pool holds 74 in total.

## ⭐ Every measure here died the same way, and that made the tails uniform

All nine passed both chambers, were vetoed, and then failed to be passed
again over the governor's objection. The override attempts all happened on the
same day, **13 May 2026**, in the chamber where the bill started, and all of
them failed.

So every description ends: *"the governor vetoed the bill and the Legislature
did not override the veto, so it never became law."* That is a completed fact.
The session has adjourned; none of these can still become law. No description
in this batch needed a time-stamped hedge.

The override votes themselves are separate roll calls with their own captions,
which the state config does not treat as passage votes, so none of them entered
the pool. That is the right result: an override vote is a second vote on the
same position, and the campaign takes one roll per measure per chamber.

## ⚠ The version check, and the one slot it touched

Twelve of the fifteen slots voted text that nothing changed afterwards.

**SB 532** was the exception, and it is the first case in Wisconsin where the
version rule kept a slot rather than dropping one. The Senate passed the bill
18-15 on 18 November 2025. The Assembly then adopted Assembly Amendment 1 and
passed the amended bill 53-45 on 12 February 2026.

Amendment 1 was fetched through the LegiScan `getAmendment` API and read. It
**only adds** a new section requiring institutions to publish each course's
format. It changes nothing in the online course fee limit that the Senate voted
on, and it removes nothing.

Under the Wisconsin version rule — drop when a later amendment removed or
replaced an operative provision, keep when it did not — the Senate slot stays.
It is described honestly: **the two chambers get different bodies.** The Senate
body describes the fee limit alone, because that is what the Senate voted. The
Assembly body describes the fee limit and then names what the Assembly added.

Writing one shared body would have credited senators with a vote on a section
that did not exist when they voted.

## The four drops

- **AB 602** would have put Wisconsin into a new federal tax credit for
  donations to scholarship granting organizations, and required the state to
  publish and certify a list of qualifying organizations. Dropped on filter 5.
  The measure is about private school scholarships, and inside
  `public_education_quality` it has no defensible direction: the area's own
  words are teaching, standards, funding and accountability, and opting into a
  federal donation credit is none of those.
- **AB 757** would have barred the University of Wisconsin from limiting dean
  and department chair jobs to tenured faculty or holders of the highest degree,
  redefined who counts as faculty for governance, required science and
  engineering representation in faculty governance, and required the Board of
  Regents itself to make senior appointments and publish each one's title and
  salary. Dropped on filter 5. There is a real transparency strand, but the
  center of the measure is a contest over who governs the university, and no
  research area gives that a direction a voter could act on.
- **SB 498** would have set campus free speech rules, banned free speech zones,
  limited permit and security fee requirements, narrowed what a campus may
  punish as discriminatory harassment, created a right to sue with damages, and
  added lawyer and evidence rights to campus discipline. Dropped on filter 5.
  Inside `civil_rights` it points both ways at once: it expands expressive
  rights and due process, and in the same act it narrows anti-discrimination
  enforcement, which the area names explicitly. That is the AB 737 reasoning
  from batch-01, applied to a bigger bill.
- **SB 699** would have raised, from 9 passengers to 14, the size of vehicle a
  school may use to carry pupils without meeting school bus requirements.
  Dropped on filter 5. It is a cost-and-flexibility change against a safety
  margin, and no area describes that trade-off.

## The labels

All are `nay: null` except SB 389. Every one is `public_education_quality`.

| measure | yes means | why |
| --- | --- | --- |
| AB 1 | for | Restores the 2019-20 score cutoffs on school report cards and ties grade 3-8 test cutoffs to the national test. Standards are the first word in the area's definition. |
| AB 5 | for | A records-access duty: post the textbook list, show any textbook, curriculum or teaching material to a resident within 14 days. Accountability. |
| AB 166 | for | Requires colleges to report graduate pay, debt, graduation rate and cost, and puts the comparison in front of pupils in grades 10 to 12. Outcomes and accountability. |
| AB 457 | for | A district may not ask voters for money until the state confirms it filed the financial reports it owes. Accountability. |
| AB 582 | for | Makes dual enrollment credits transfer and satisfy general education requirements, and gives a pupil an appeal when a school says no. Student outcomes. |
| AB 614 | for | Teacher authority over a disruptive class, parental notice, and a district code of conduct. Effective teaching. |
| AB 1005 | for | See the note below. |
| SB 389 | **against**, nay **for** | See the note below. |
| SB 532 | for | Bars an extra fee for an online-only course unless it covers a real added cost, and requires course format to be published. Cost and accountability. |

**AB 1005 is the closest call among the keeps.** It would have required
University of Wisconsin-Madison admissions to rest predominantly on ACT or SAT
scores. The well-known objection is that a test-first rule narrows access for
applicants whose scores lag their record. That objection is real, but it does
not have a home in this area: what the measure does on its own terms is impose
a single objective academic standard, and standards are what
`public_education_quality` names. So yes = for, and `nay: null`, because a no
vote could mean "tests are the wrong measure" or "the Legislature should not
set admissions policy", and those are not the same position.

**SB 389 is the only stated nay in the batch.** Wisconsin school districts'
revenue limits currently rise by $325 per pupil every year with no end date.
The measure would have kept that increase through 2026-27 and then stopped it.
It is single-subject, and its whole content is the school funding mechanism
itself, so a no vote evidences a position for keeping that funding. That is the
same test used for SB 825 in batch-01 and for North Dakota HB 1318.

It is also the only measure in this batch where yes = against, which is worth
saying plainly: eight of the nine are accountability or transparency measures
that the area reads as improvements, and the ninth removes money.

## Quality, measured before importing

- Plain-language lint over all 30 descriptions with the repository's own
  `listPlainLanguageWarnings`: **0 warnings**.
- Longest sentence 43 words, under the 45-word limit. Three sentences were split
  after the first lint run: one in AB 457 at 50 words, one in AB 582 at 54, and
  one more in AB 582 that measured exactly 45.
- British-spelling scan over the descriptions and this document: clean.
- Every roll number, chamber, date and tally in the judgments file was checked
  against `legislative_votes` before judging. Batch-01 lost time to seven roll
  numbers written from memory, so this check now runs every time.
- Each description cites its own roll's tally, and the yes and no versions are
  generated from a single body so they cannot drift apart.

## Reconciliation

- Plan: 839 inserts over 15 rolls.
- Real run: `outcomes {imported: 15}`, `actions {insert: 839}`, 0 errors,
  0 notified. Run stamp `2026-09-10T06:18:16.775Z`.
- Run-stamp predicate: 839. All Wisconsin roll-call records: 1,545, which is
  batch-01's 706 plus this batch's 839.
- Tags predicted independently and confirmed: 441 records on the yes side each
  carry one tag, plus the 43 records on SB 389's no side, which is 484. A check
  confirmed that SB 389's are the only tagged no-side records in the batch.
- 99 distinct candidates, the same 99 batch-01 reached.

**The whole-table row delta is still not a valid check.** Other states are
importing in parallel. The jurisdiction-scoped count is the one that means
something.

**Duplicate sweep: nothing to retire.** Two Wisconsin rows turned up outside the
roll-call runs and both are different bills: a co-sponsorship of the **2023**
Assembly Bill 166, and a vote on the 2025 **Senate** Bill 5, which is not the
Assembly Bill 5 in this batch and was not closely divided at 28-4.
