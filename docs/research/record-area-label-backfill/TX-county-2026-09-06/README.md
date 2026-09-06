# Follow-up sweep, 2026-09-06

The main backfill left the live untagged set at 1,990 manual/null-origin records.
1,891 of those are already accounted for in the per-state files here (skips, retire
candidates, and the deferred office-catalog gap). The remaining 99 had never been
triaged: 45 were written to the shared local database after the state queues were
built, and 54 belong to candidates with no non-withdrawn office election, so the
queue builder never picked them up.

This directory covers the 45. All are Texas county offices and judgeships.

- 6 labeled (all `for`): a budget adopted at a lower tax rate, two COVID public-health
  orders, county election-worker pay, a fairgrounds alcohol and fireworks ban, and an
  extension of COVID paid leave.
- 39 skipped, with the reason recorded per record in `batch1-output.json`.

The skips fall into four groups:

1. **Judicial and lawyer-of-record acts** — sentencings, contempt orders, appointed
   counsel work. These are not stances.
2. **Routine procurement and paperwork** — architect hires, equipment leases, roof
   bids, settlement signature motions.
3. **Tax abatements and reinvestment zones for solar and wind projects** (8 records) —
   the allowed areas for a county commissioner have no home for economic development
   or energy policy.
4. **Election administration by county commissioners** (3 records) — `election_integrity`
   is not in the county commissioner allowlist. This is the same
   `office_research_areas` catalog gap already flagged in the main backfill.

No AI provider was called: every write went through
`ai:candidate-records:relabel --labels-file` (`provider: labels-file`, `model: null`).

## Left alone

The 54 records on candidates with no office election are untouched. They are not
reachable from a ballot, and labeling them would mean inventing an office allowlist
for a candidate who has no office.
