# Montana batch-03 — how each measure was judged

Source is the enrolled text, as always in Montana. Both measures were read top
to bottom, and both needed pages of the enrolled PDF rendered and looked at.

## ⚠⚠ SB 221: the extracted text was self-contradictory, and the render flipped the stance

Montana prints statutory amendments in context, and `pdftotext` renders struck
text as ordinary text. On SB 221 that produced a passage that could not be true:

> An environmental review conducted pursuant to subsection (1) may not include
> ... a greenhouse gas assessment subject to [section 1].

while section 1 of the same act says an agency **shall** conduct a greenhouse
gas assessment for a fossil fuel activity. Both cannot hold.

Rendering page 5 of the enrolled PDF resolved it: **the word "not" is struck.**
The operative sentence reads "An environmental review ... **may include** a
greenhouse gas assessment subject to [section 1]", and what is struck alongside
it is the old flat prohibition on evaluating "greenhouse gas emissions and
corresponding impacts to the climate in the state or beyond the state's
borders."

Two further renders completed the picture:

- Page 6: in subsection (2)(b) the two federal preconditions are struck — a
  review may now evaluate reasonably foreseeable environmental impacts without
  waiting for a joint federal review or for Congress to regulate carbon dioxide.
- Page 7: subsection (6)(a)(ii) is struck in full. That was the rule barring a
  greenhouse-gas-based challenge from vacating, voiding, or delaying a permit.
  The `(iii)(ii)` renumbering was the tell.

**Had the stance been written from the extract, or from the title and preamble,
it would have been inverted across 98 records.** The rule this confirms: in
Montana, a renumbering artefact like `(3)(2)` or `(iii)(ii)` is proof that
intervening text was struck, and any stance that leans on what was removed must
be checked against the rendered page.

## HB 664

The operative directions sit in new sections, so there is no strike-through
ambiguity about them: the department must repeal circular DEQ-12A, the 2014 base
numeric nutrient standards, and strip every reference to it from fourteen named
administrative rules and from guidance, assessment methods and total maximum
daily load calculations. Section 4 repeals 75-5-321, the transition statute, and
section 5 repeals two more rules.

One claim did need a render: page 5 confirms definition (21), the nutrient work
group, is struck with everything after it renumbered. The description says the
act ends that group on that basis, not on the title's say-so.

The description does **not** say what standards remain. The act's own preamble
says narrative standards were never adopted, so any claim that narrative
standards now govern would go beyond the text.

## Direction

`environment_and_public_health` is "protect air, water, climate, and community
health through standards, enforcement, and prevention."

- **SB 221** removes a ban on climate analysis, requires assessment for fossil
  fuel activity, and restores a remedy in court. *For*.
- **HB 664** removes numeric limits on nitrogen and phosphorus in water and ends
  the advisory group that maintained them. *Against*.

Both `nay: null`.

## Date audit and version check

All four roll dates match the third-reading dates in Montana's official action
trail exactly. Neither measure was returned to the first chamber with
amendments, so both chambers voted the enacted text in each case. No
`official_vote_date` override, no `acknowledge_later_rolls`.

## Writing and import

Lint 0 warnings over 8 descriptions, longest sentence 24 words. Flesch-Kincaid
9.1 and 9.2 — higher than batch-02's 7.9 median, and honestly so: "greenhouse
gas assessment", "environmental review" and "nutrient" are the terms these laws
are built on, and replacing them is what caused Connecticut's correction rounds.

Dry run planned 172 inserts across 4 files; the real run inserted 172 across 87
candidates; convergence reports all 172 unchanged; the dry run's stamp matches
zero rows. Stamp `2026-09-02T01:01:13.251Z`.

Montana now holds **1,534 records across 87 candidates, 913 tags, 36 approved
rolls**. Production has zero Montana records.
