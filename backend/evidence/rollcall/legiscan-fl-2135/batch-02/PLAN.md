# FL 2135 — batch-02 selection

**4 votes / 2 measures / 120 records.** Local `voteapp` only; production untouched.

Batch-01 deferred both of these measures with documented reasons. The user then
directed a fresh investigation grounded in the actual enacted text — "what
legislators actually voted on is the important part, the actual bill, not the
title" — and delegated the call. This batch is the result. The other ten
batch-01 deferrals stay deferred; their reasons in `../batch-01/PLAN.md` held
up (budget vehicles, no nameable subject, or thin grab-bags).

## HB 1205 — Amendments to the State Constitution (ch. 2025-21)

**Why it was deferred:** the direction looked two-sided (verification vs.
restricting a lawful petition process), and the chambers appeared to have voted
materially different texts.

**What the investigation found:**

1. **The version problem dissolves once the full history is read.** The Senate
   rewrote the House bill with delete-all amendment 476344; the House concurred
   in that rewrite **as amended** by its own amendment 258567 (which raised the
   unregistered possession cap from 5 forms to 25) and passed the result 81-30;
   the Senate then concurred in 258567, 28-9. So rolls **1563815** (House) and
   **1564585** (Senate) are both votes on the **exact enacted text** — no
   per-chamber version split at all. Batch-01 had been looking at the earlier
   passage votes (76-31, 28-10), which were on different texts.
2. **The direction resolves under the standing rule** (direction follows the
   area description, never the bill's framing). `election_integrity` reads
   "Ensure elections are secure, accurate, auditable, and trusted by the
   public." Every enacted provision is verification, audit, enforcement, or
   process-tightening: circulator registration and eligibility, voter-ID fields
   on forms, verification notices with a revocation path, mandatory fraud
   investigations above a 25% invalidation rate, RICO applicability, retention
   and digital-transmission rules, and a ban on state government spending
   public funds on amendment advertisements. **No provision runs against**
   secure/accurate/auditable/trusted. The real objection to the bill — that it
   burdens the citizen-initiative process — maps to no research area, and
   "politically contested" is not the two-directions test (every divided vote
   is contested by construction). Texas batch-02 precedent: three
   `election_integrity`/for stances on tightening bills; TX HB 521 went to
   `general` only because it ALSO expanded access — HB 1205 has no
   access-expanding side.

**Judgment:** `election_integrity`, yea = **for**, both chambers, one shared
description that carries the restrictive provisions in full (felony 25-form
cap, 10-day deadline, eligibility bars, sponsor caps) so a reader sees exactly
what was voted, whatever they think of it.

## SB 700 — Department of Agriculture and Consumer Services (ch. 2025-22)

**Why it was deferred:** a 30+-subject omnibus whose famous provision (the
fluoride ban) is two sections.

**What the investigation found:** the enrolled law is **92 sections over 111
pages**, and it is even broader than the committee analysis showed (the
last Senate staff analysis predates adopted floor amendment 340838; the
enrolled text is the ground truth, and it is what both chambers voted — the
Senate passed the engrossed text 27-9, the House passed the identical text
88-27 after both of its floor amendments failed). Its strands pull in
different directions across different areas: the water-additive limitation
ends community fluoridation (`environment_and_public_health`/against on its
own), while the farmworker-housing preemption, the Honest Services Registry
(charities attest to no foreign-source-of-concern funding), EV-charger
consumer regulation, and mosquito-control funding read as neutral-to-for in
other areas. No SINGLE stance can honestly cover the whole act — but on the user's
direction the strands were tagged individually instead of collapsing to
`general` (the pilot precedent: 1,545 tags on 1,163 federal records proves
multi-label fan-out). Each candidate area was tested on its own:

- `environment_and_public_health` → **against**: the fluoride limitation is
  the act's most consequential health provision and the floor fight (the
  failed 13-21 amendment was the fluoride-preservation attempt). The one
  counter-strand in the area — raising the mosquito-control matching-fund
  cap — is de minimis next to ending fluoridation in ~29 counties (the
  HB 351 principle: a minor softening inside a bill does not flip it).
- `housing_affordability` → **for**: s. 163.3162(5) is a real, operative
  preemption — "a governmental entity may not adopt or enforce any
  legislation … to inhibit the construction" of farmworker housing meeting
  H-2A, health and building standards. Supply-expanding on its face.
- `corporate_accountability` → **considered and SKIPPED**: the EV-charger
  consumer regulation resembles TX SB 1036 (for), but the same act's
  plant-based "mislabeling" ban reads as consumer protection OR incumbent
  protection, and the Honest Services Registry is a voluntary charity
  attestation, not corporate accountability — ambiguity within the area
  means no stance for that area.

**Judgment:** `environment_and_public_health` yea = **against** +
`housing_affordability` yea = **for**, both chambers, description leading
with the fluoride provision and honestly framing the omnibus.

## Selected rolls

| Measure | Chamber | Roll | Tally | Official question (FL vote record) |
| --- | --- | --- | --- | --- |
| HB 1205 | house | 1563815 | 81-30 | CS/HB 1205, 1st Eng. — Passage |
| HB 1205 | senate | 1564585 | 28-9 | CS/HB 1205 — Returning Messages (concurrence in 258567) |
| SB 700 | senate | 1549853 | 27-9 | CS/CS/CS/SB 700 — Third Reading |
| SB 700 | house | 1559666 | 88-27 | CS/CS/CS/SB 700, 1st Eng. — Passage, Third Reading |

All four checked against Florida's vote-record PDFs (LegiScan's desc is not
trusted on FL rows — see `../CODE-FINDINGS.md`). All four are divided under
the standing gate and all are votes on text identical to the enrolled law.
