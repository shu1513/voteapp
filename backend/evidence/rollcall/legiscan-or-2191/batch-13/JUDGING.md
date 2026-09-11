# Oregon batch-13: judging

Each measure read from its enrolled text (`ls_text.py`).

## HB 2688
ORS 279C.800(6)(a)(G) adds to "public works" any bespoke fabrication, assembly
or preconstruction done off-site for a particular public works project, on
mechanical, plumbing, electrical, boiler, iron, masonry, roofing and panel, or
insulation systems. So prevailing wage reaches that work. It applies to
procurements from July 1, 2026, and adds $268,664 to the labor bureau for
prevailing wage enforcement.

## HB 2944
ORS 243.676(4)(c): if a public employer violates the dues deduction rules again
(ORS 243.804(4) or 243.806(7)), the Employment Relations Board must impose a
$1,000 to $5,000 penalty, then $5,000 to $10,000 for later violations, with an
excuse for natural disasters, computer crimes and catastrophic fires. ORS
243.806(7) now requires remittance within 30 days of the deduction.

## HB 3194
New section 2 of ORS 658.705 to 658.850 makes a landowner who knew or should
have known of a farmworker camp on the land jointly liable with the operator,
with a rebuttable presumption for a written lease that bars unregistered camps.
ORS 658.805(3) keeps its defendant limit: the suit runs against a person
violating ORS 658.715(1) (not eligible to operate a camp) or 658.755(2)(a)
(operating an unregistered camp), over any violation of the camp laws, and now
recovers damages as well as an injunction. Minimum damages rise from $500 to
$2,000. The landowner's joint liability inherits that scope. Farmworker camps are
employer-linked worker housing under the labor bureau, so the change widens
worker protections.

## Import

Judge: 6 updated. Dry run: 167 planned inserts, 0 errors, stamp
`2026-09-11T07:39:15.650Z`, which matches zero rows. Real run: 6 files imported,
**167 inserts**, 0 notified, stamp `2026-09-11T07:39:17.646Z`, 60 candidates,
117 area tags.

### Review re-run

PR review found two overstatements, both checked against the enrolled text and
fixed in `judgments.json`:

- HB 3194: "any harmed person may sue over any camp violation" dropped the
  defendant limit in ORS 658.805(3). The sentence now names the target: an
  unlicensed or unregistered camp operator.
- HB 2688: "starts with contracts from July 1, 2026" used the wrong trigger.
  Section 2 keys on the solicitation date, with contract date only where
  nothing was solicited. The sentence now says so.

Judge: 4 updated. Real re-import (`import-rerun-report.json`, stamp
`2026-09-11T19:58:02.333Z`): 110 rewritten (54 HB 3194 + 56 HB 2688), 57
unchanged (HB 2944), 0 errors, 0 notified. Local database only; prod has none
of this batch.

## Duplicates

None retired. The sweep found two hand-written records naming these bills, and
neither is a vote: one is public testimony on HB 2688 (February 24, 2025), and
the other is a sponsorship record for HB 3194. Sponsorship records are never
retired.
