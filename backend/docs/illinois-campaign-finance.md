# Illinois campaign finance

VoteApp treats the Illinois State Board of Elections (SBE) campaign-disclosure system as the source of authority. The Illinois Sunshine/DataMade dataset can be used to produce the normalized input artifact because it is derived from SBE records, but it is not treated as a separate authority.

## Covered offices

The loader accepts the existing statewide and General Assembly offices plus these place-level offices when the election record has an exact Illinois municipality:

- mayor; village or town president
- municipal clerk, treasurer, and assessor
- at-large alderman or alderperson
- at-large city council member
- at-large village or town trustee

Ward, district, county, township, school, park, library, fire, sanitary, and other special-district offices remain excluded. Local legislative and trustee offices fail closed unless the source explicitly says they are at large. A city, village, or town type mismatch also fails closed.

Coverage still depends on a canonical local election, candidate, and place district already existing in VoteApp. Campaign-finance ingestion does not invent elections or candidates.

## Normalized SBE artifact

Local auto-linking requires a complete, versioned JSON artifact passed with `--normalized-artifact` or `ILLINOIS_SBE_NORMALIZED_ARTIFACT_PATH`. The artifact contains explicit SBE candidate-to-committee relations and D-2 report summaries. The parser rejects incomplete artifacts, malformed dates or URLs, negative amounts, duplicate relation identities, and duplicate report IDs.

The current schema is `illinois_sbe_normalized/v1`. See `tests/fixtures/illinoisFinance/normalized-artifact.json` for a minimal example. Each relation carries the SBE candidate and committee IDs, election year, office, district type, municipality, at-large evidence, committee status, and source URL. D-2 rows carry a stable report ID, reporting period, filing timestamp, receipts, disbursements, ending cash, debts, and source URL.

Run a dry pass before enabling writes:

```sh
npm run illinois-candidates:finance:sync-due -- --normalized-artifact /absolute/path/illinois-sbe-normalized.json
```

Then run with the script's existing live/force flags only after reviewing the dry-run output. The campaign-finance and sync feature flags remain authoritative.

## Bulk itemized receipts

The bulk download page also publishes `Receipts.txt`, the itemized receipts for every committee. Passing it with `--receipts-tsv` (or `ILLINOIS_SBE_RECEIPTS_TSV_PATH`) supplies the occupation and contribution-size breakdowns for links keyed by an SBE committee ID; without it (or itemized contribution CSVs, which also feed the breakdowns) those sections stay empty on the artifact path. It requires the normalized artifact: the artifact's candidate-committee relations are the committee allow-list that keeps the multi-decade file from being held in memory, and rows older than two years before the run are dropped for the same reason (the due window only looks ahead). Rows marked `Archived` are superseded by amendments and are skipped. The `D2Part` leading digit classifies each receipt; only part 1 (individual contributions) feeds the breakdowns, matching the search-export semantics.

When receipts are loaded, an ID-keyed link whose committee is in the artifact's allow-list gets exactly its scanned rows — an empty result is an honest zero that clears stored breakdowns. A committee absent from the allow-list was never scanned, so its link is treated as having no receipts source: it falls back to name-matched contribution CSVs when those are loaded and otherwise preserves its stored breakdowns. Name-keyed links keep the CSV path. A committee funded entirely by transfers from other committees therefore shows totals with no occupation or size sections, which is the correct reading of its filings rather than missing data.

A refresh that loads no itemized source at all — the D-2 summaries-only invocation above — leaves stored breakdowns untouched rather than replacing them with nothing, so running without `--receipts-tsv` does not erase them. An itemized source that reports nothing for a committee still clears that committee's rows, because that is a real zero.

A few published receipt rows contain an unescaped tab or newline, which splits or widens the row so its field positions cannot be trusted. Those rows are skipped and counted, and the loader logs the count; the run as of 2026-07-26 skipped 31 of 6,515,432 scanned rows. The header check stays strict, so genuine schema drift still fails the run. The small bulk files keep the strict row-width check as well, because there a wrong width can only mean drift.

## Summary semantics

D-2 reports are authoritative for total receipts, total disbursements, cash, and debts. For an amended reporting period, the latest filed report wins. Itemized contribution CSVs and bulk receipts supply occupations and contribution-size breakdowns; they are not substituted for missing D-2 totals. Missing data is represented as unavailable, never as zero.

When a candidate has multiple official committees, VoteApp keeps one link and summary per committee. The ballot response does not combine receipts or disbursements because transfers between those committees could be counted twice. Cash and debts may be added across the distinct committee balances.
