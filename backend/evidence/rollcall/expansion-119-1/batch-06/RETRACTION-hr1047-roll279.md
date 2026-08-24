# Retraction: House 119-1 roll 279 (H.R. 1047, GRID Power Act)

Retracted 2026-08-23, after review of the batch-6 judgment.

The judgment described dispatchable power as "mainly fossil-fuel
generation" and labeled every yea voter `environment_and_public_health =
against`. The CRS summary offers fossil fuels only as an example ("e.g.,
certain fossil fuel projects"), and the bill's definition of dispatchable
power is fuel-neutral on its face — it covers nuclear, hydro, and
geothermal as much as gas and coal. The characterization overstated the
source, and the stance stood on that overstatement.

The consistent treatment was already in this run: the other
grid-reliability bills (H.R. 3628, H.R. 3632, H.R. 3616) were left
pending because no research area maps onto them without inventing a
direction. H.R. 1047 belongs with them.

Actions taken on the local database:

- All 88 fanned-out records (`origin_run_id` prefix
  `rollcall:US:house:119-1:279:`) retired, with a reason recording this
  retraction.
- The `legislative_votes` row returned to `pending` with its judgment
  fields cleared.

The roll-279 entry was removed from this directory's `judgments.json` so
re-applying the file cannot re-approve the judgment; the original entry
remains in git history. The XML stays as fetch evidence — the importer
reports a pending roll as `not_approved` and writes nothing.

The import reports in this directory predate the retraction and still
show the 88 inserts.
