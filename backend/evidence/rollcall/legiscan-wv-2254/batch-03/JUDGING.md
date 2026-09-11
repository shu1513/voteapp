# West Virginia 2254 batch-03: judging

## HB 4025
Read from the enrolled committee substitute (`ls_text.py`). Its printed title
still describes Child Protective Services staffing, but the enacted section is
new § 5F-2-10: from July 1, 2026, employees of the Departments of Health
Facilities, Human Services and Health are exempt from the classified civil
service system and the state grievance procedure. Current workers keep both only
while they stay in their current position; one who moves within the listed
departments becomes exempt. The description follows the enacted section, not the
title. § 5F-2-10(b)(4) lets each department secretary keep civil service and
grievance coverage for positions where federal law, regulation or funding
requires it, so the description qualifies the loss with that carve-out. § 5F-2-10(c)
excludes positions appointed by the Governor; those are not classified civil
service anyway, so the description does not mention it. Taking away grievance and civil service protection narrows worker
protections, so a yes vote is `against`.

## Import

Judge: 2 updated. Dry run: 86 planned inserts, 0 errors, stamp
`2026-09-11T07:44:30.645Z`, which matches zero rows. Real run: 2 files imported,
**86 inserts**, 0 notified, stamp `2026-09-11T07:44:31.646Z`, 86 candidates,
60 area tags.

## Duplicates

None. The sweep found no hand-written record on either vote.

## Review fix

Code review noted the descriptions omitted the § 5F-2-10(b)(4) federal-law
carve-out. All four sentences now carry it. Re-applied with `rollcall:judge`
(2 updated), then re-ran the import: dry run planned 86 rewrites, 0 errors;
real run rewrote **86** records, 0 notified, stamp `2026-09-11T20:16:44.612Z`.
Reports: `import-dry-run-rerun-report.json`, `import-rerun-report.json`.
