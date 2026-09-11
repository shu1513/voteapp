# Colorado batch-12: judging

## SB 5
Read from the enrolled text (`ls_text.py`). Section 8-3-108(1)(c)(I) of the
Labor Peace Act lets an employer sign an all-union agreement only if the workers
approve it in a second, separate election, by a majority of all eligible workers
or three-quarters of those voting, whichever is greater. The bill struck that
condition, repealed the ratification process for pre-1977 agreements, and
repealed the 20 percent petition to revoke an agreement. The director could
still end an agreement when a union unreasonably refuses to admit a worker.
Section 2 made the same change for building and construction agreements.
Removing a hurdle to union-security contracts widens union rights, so a yes vote
is `for` under the scope rules.

## Import

Judge: 2 updated. Dry run: 51 planned inserts, 0 errors, stamp
`2026-09-11T07:33:11.102Z`, which matches zero rows. Real run: 2 files imported,
**51 inserts**, 0 notified, stamp `2026-09-11T07:33:36.582Z`, 51 candidates,
35 area tags.

## Rerun: threshold wording

Review caught the descriptions saying the second vote needs a majority of all
eligible workers *or* three-quarters of those voting. The statute takes
whichever is greater, so both thresholds must be met. All four descriptions now
say "both a majority of all eligible workers and three-quarters of those
voting", which is the same rule in plain words. Judge: 2 updated. Dry run
(`import-dry-run-rerun-report.json`): 51 planned rewrites, 0 errors. Real run
(`import-rerun-report.json`): **51 rewrites** in place, 0 inserts, 0 notified,
stamp `2026-09-11T19:25:52.879Z`. The database holds 51 rows on the new stamp
and none with the old wording.

## Duplicates

None. The sweep's one candidate hit was on a different vote, not this bill.
