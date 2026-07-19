# Ballot-measure plain-language rewrites — 2026-07-18 (Group E)

Replay payloads for the 15 `ballot_measures` rows rewritten during the
Group E content audit (PR #355). Each file is a contract-shaped
ballot-measure payload; the filename is the **election id**.

Written to the local DB on 2026-07-18 via the manual writer. To replay
against any environment (the writer only accepts local DB targets, so
replay on the target host itself):

```bash
cd backend
for f in ../scratch/ballot-measure-rewrites-2026-07-18/*.json; do
  npm run manual:ballot-measure:write -- \
    --election-id "$(basename "$f" .json)" --file "$f" --dry-run
done
# then re-run without --dry-run
```

The writer revalidates every URL and research-area tag on replay — do
not bypass it with SQL. Wording is sourced from the official texts
cited in each payload's `sources`; research-area tags and sources are
carried over unchanged from the original rows.

Filenames carry the election ids from the DB where the rewrite ran. If
the target environment's elections have different ids, resolve each
measure by `elections.official_ballot_title` + district and pass that
environment's id to `--election-id`; the payload content is
environment-independent.
