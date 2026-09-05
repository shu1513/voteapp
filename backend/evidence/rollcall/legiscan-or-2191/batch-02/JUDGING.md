# Oregon batch-02 — judging

Every description was written from the **enrolled Act**, with `or_bold.py`
run on each one to separate the language the Act adds from the statute it
merely reprints. The staff measure summary was used only as an index.

## What the enacted text changed about each description

- **HB 2138** is 2,355 lines, most of it existing land-use statute reprinted
  around the changes. The font-weight extract cuts that to about 300 lines of
  genuinely new language, which is what the description is built from. Two
  limits that a summary-level read would have flattened are stated: the
  traffic-study exemption **stops** at townhouse and cottage cluster projects
  over twelve units, and the covenant provisions are not immediate — they
  become operative on January 1, 2027.
- **HB 3054.** The six percent cap applies only to facilities with **more
  than 30 spaces**; smaller ones keep the existing statewide formula, so the
  description names the threshold rather than implying a general cap. The
  twelve percent route is not a simple alternative: it carries six separate
  conditions, and the description keeps all of them, including the refund
  duty if the work is not substantially complete within a year of the
  promised date.
- **SB 551.** The mechanism is a **deletion**, which is exactly the case the
  font check exists for. The Act repeals the definitions of "reusable fabric
  checkout bag" and "reusable plastic checkout bag" and strikes them from the
  definition of a single-use bag, so a sturdy plastic bag that used to satisfy
  the law now violates it. Reading the new text alone would have missed the
  point of the bill. The description also keeps the exclusions that survive
  (bags given away from the register, for produce, meat, bakery goods or
  prescriptions) and the five-cent charge with its WIC and benefits-card
  exemption.
- **HB 2586** is short and the summary matched the Act. The description
  carries the statutory definition of asylum seeker — someone with a **pending**
  asylum application under 8 U.S.C. 1158 — rather than the looser everyday
  sense, and notes that refugees and special immigrant visa holders already
  qualified, so the Act adds a group rather than creating the benefit.

## Labels and the nay side

The Connecticut test again: author a nay stance only where the Act is
single-subject, its whole operative content is the area's own mechanism, and
the mainstream objection runs on that same axis.

- **`nay: "against"` on HB 2586 only.** The objection to extending resident
  tuition to asylum seekers is about whether people without settled status
  should receive that benefit, which is the `immigration` axis itself.
- **`nay: null` on HB 2138, HB 3054 and SB 551.** HB 2138 and HB 3054 both
  draw a counter-argument that sits **inside** `housing_affordability` rather
  than against it — that overriding local zoning, or capping rents, reduces
  the housing investment the area is trying to encourage. That is the
  Connecticut HB 7042 shape, where a counter-directional argument within the
  same area beats a de-minimis framing. SB 551's objection is cost and
  convenience, a different axis from environmental protection.

Tag counts reconcile exactly per measure: 171 tags over 221 records, the
untagged records being the nay sides of the three null measures (11 + 23 + 16
= 50), and HB 2586's 15 nay records all tagged.

## Writing

The builder refused to write until every rule passed. It also caught a defect
in its **own** check this batch: the British-spelling rule was a suffix
pattern that flagged "promised". A suffix rule cannot tell `promised` from
`organised`, so it was replaced with an explicit list of words whose British
spelling differs from the American one. Batch-01 was rebuilt against the new
rule as a regression check and still passes.

One wording fix before import: a draft glossed Oregon's EBT card by its brand
name, which the statute does not use. It now says "a state benefits card",
matching the statute's "electronic benefits transfer card issued by the
Department of Human Services".

Reading level measured, not eyeballed. First drafts scored Flesch-Kincaid
grade 12.1 to 13.6 and were rewritten before importing: **median 9.5, worst
10.4, longest sentence 37 words, lint 0 warnings.**

⚠ **HB 2138 sits at 10.2 and that is close to its floor.** Getting lower means
dropping either the housing types the Act names, the twelve-unit limit on the
traffic exemption, or the 2027 date on the covenant rules. Those are the
limits that make the description true, so the grade was traded for them.
Descriptions again run 5 to 7 sentences rather than the house style's 2 to 4,
for the same reason batch-01 did.

## Checks run

- Version check on all 8 rolls: all on the enacted text (see PLAN.md).
- Superseded check run up front over every selected roll; two measures needed
  the later concurrence roll, and no `acknowledge_later_rolls` was needed.
- `related` flags **0**, `ambiguous` 0, errors 0, notifications 0.
- Dry run matched the real run at 221 inserts, and the convergence run
  afterwards reported all 221 `unchanged`.

## Ledger

`import-report.json` — the insert run, 221 records.

Oregon after this batch: **543 live records, 61 candidates, 433 tags, 20
approved rolls.** Production has zero Oregon roll-call records.
