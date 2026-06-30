# Vermont August 2026 Thin Candidate Record Rerun

Scope: record-only gap-repair passes for Vermont August 11, 2026 office candidates whose prior records were pure candidacy/ballot-listing evidence or otherwise too thin.

Search rule: import only actual records/actions: votes, public office actions, advocacy work, organizational leadership, bills, decisions, litigation, enforcement, public service, prior government service, or source-backed conduct. Do not import candidate listings, filing-to-run rows, campaign launches, or platform promises.

## Outcomes To Verify

- H. Brooke Paige: one actual litigation/action record found; import as neutral `general` because it is not safe to apply one office-specific stance/tag to a repeat candidate across four offices in the current non-election-scoped tag schema.
- Ryan McLaren: one neutral public-service/work record found; import with `candidate_records.only_general_labels` confirmed after focused search found no stronger issue/action record.
- Esther Charlestin: one neutral professional/public-service record found; import with `candidate_records.only_general_labels` confirmed after focused search found no stronger issue/action record.
- Dan Towle: no reliable actual record/action found in focused search; run confirmed `candidate_records.no_records_found`.
- Nicholas Graeter: no reliable actual record/action found in focused search; campaign site was reviewed, but it contained candidacy/platform evidence rather than actual record/action evidence; run confirmed `candidate_records.no_records_found`.
- Rachel Shaw: no reliable actual record/action found in focused search; run confirmed `candidate_records.no_records_found`.
- Zachary Hampl: no reliable actual record/action found in focused search; run confirmed `candidate_records.no_records_found`.

`manual:candidate-records:write` does not delete stale records by itself, so after the verified rerun a narrow cleanup removed the six exact old candidacy-only rows and their tags:

- Dan Towle listed-as-candidate row
- H. Brooke Paige listed-as-candidate row
- Nicholas Graeter listed-as-candidate row
- Rachel Shaw listed-as-candidate row
- Ryan McLaren listed-as-candidate row
- Zachary Hampl listed-as-candidate row
- Esther Charlestin nominee-status row
