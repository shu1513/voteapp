# Propagation runs for the 2026-08-29 judgment re-authoring

After the rejudge waves corrected 229 `legislative_votes` judgments, these
import runs propagated them onto the fanned-out `candidate_records` rows.
Original per-batch `import-report.json` ledgers are preserved as committed —
this file records the propagation reruns instead, so the history of the FIRST
import of each batch is not overwritten.

Net effect across all runs: **13,272 records rewritten in place**, tags
re-synced from `labels_json` on every touched record (legacy auto-inverted
nay chips fell from 13,089 to 72), and record/judgment drift reached zero
after a final `content:backfill-plain-language` pass fixed 203 records whose
members had left the Nov-2026 scope (importer gap since fixed: out-of-scope
maintenance voters).

## Run 1 — all committed evidence dirs (2026-08-30 ~03:50-04:00 UTC)

Per directory (`outcomes` / `actions` as reported by the importer):

```
=== FED backfill-118-117/batch-01
  files 25 outcomes {'imported': 25} actions {'unchanged': 1188, 'rewrite': 32}
=== FED backfill-118-117/batch-02
  files 15 outcomes {'imported': 15} actions {'unchanged': 761, 'rewrite': 10}
=== FED backfill-118-117/batch-03
  files 13 outcomes {'imported': 13} actions {'unchanged': 667}
=== FED backfill-118-117/batch-04
  files 20 outcomes {'imported': 20} actions {'unchanged': 982, 'rewrite': 21}
=== FED backfill-118-117/batch-05
  files 13 outcomes {'imported': 13} actions {'unchanged': 384, 'rewrite': 355}
=== FED backfill-118-117/batch-06
  files 20 outcomes {'imported': 20} actions {'rewrite': 910}
=== FED backfill-118-117/batch-07
  files 15 outcomes {'imported': 15} actions {'rewrite': 763, 'unchanged': 85}
=== FED backfill-118-117/batch-08
  files 28 outcomes {'imported': 28} actions {'rewrite': 1532}
=== FED backfill-118-117/batch-09
  files 12 outcomes {'imported': 12} actions {'rewrite': 758}
=== FED expansion-119-1/batch-01
  files 9 outcomes {'imported': 9} actions {'rewrite': 613, 'unchanged': 13}
=== FED expansion-119-1/batch-02
  files 14 outcomes {'imported': 14} actions {'rewrite': 770}
=== FED expansion-119-1/batch-03
  files 16 outcomes {'imported': 16} actions {'rewrite': 712, 'unchanged': 13}
=== FED expansion-119-1/batch-04
  files 11 outcomes {'imported': 11} actions {'rewrite': 726, 'unchanged': 84}
=== FED expansion-119-1/batch-05
  files 8 outcomes {'imported': 7, 'error': 1} actions {'rewrite': 534, 'unchanged': 87}
=== FED expansion-119-1/batch-06
  files 11 outcomes {'imported': 10, 'not_approved': 1} actions {'rewrite': 203}
=== FED expansion-119-1/batch-07
  files 7 outcomes {'imported': 7} actions {'rewrite': 461, 'unchanged': 13}
=== FED expansion-119-1/batch-08
  files 9 outcomes {'imported': 9} actions {'rewrite': 796}
=== FED expansion-119-1/batch-09
  files 1 outcomes {'imported': 1} actions {'rewrite': 13}
=== FED rollcall/pilot-119-1
  files 20 outcomes {'imported': 20} actions {'rewrite': 1166}
=== LS CA batch-01
  files 20 outcomes {'imported': 20} actions {'unchanged': 729}
=== LS CA batch-02
  files 24 outcomes {'imported': 24} actions {'unchanged': 859}
=== LS CA batch-03
  files 18 outcomes {'imported': 18} actions {'unchanged': 645}
=== LS CT batch-01
  files 17 outcomes {'imported': 17} actions {'unchanged': 1457}
=== LS FL batch-01
  files 11 outcomes {'imported': 11} actions {'unchanged': 385}
=== LS FL batch-02
  files 4 outcomes {'imported': 4} actions {'unchanged': 60, 'rewrite': 60}
=== LS GA batch-01
  files 18 outcomes {'imported': 18} actions {'unchanged': 1132, 'rewrite': 593}
=== LS GA batch-02
  files 9 outcomes {'imported': 9} actions {'rewrite': 727, 'unchanged': 308}
=== LS IL batch-01
  files 22 outcomes {'imported': 22} actions {'unchanged': 1364}
=== LS IL batch-02
  files 54 outcomes {'imported': 54} actions {'unchanged': 3289, 'rewrite': 30}
=== LS IL batch-03
  files 3 outcomes {'imported': 3} actions {'rewrite': 92, 'unchanged': 65}
=== LS ME batch-01
  files 24 outcomes {'imported': 24} actions {'unchanged': 1189, 'rewrite': 327}
=== LS PA batch-01
  files 5 outcomes {'imported': 5} actions {'unchanged': 882}
=== LS PA batch-02
  files 32 outcomes {'imported': 32} actions {'unchanged': 5642}
=== LS TN batch-01
  files 14 outcomes {'imported': 14} actions {'unchanged': 691}
=== LS TX batch-01
  files 25 outcomes {'imported': 25} actions {'unchanged': 1287, 'rewrite': 333}
=== LS TX batch-02
  files 27 outcomes {'imported': 27} actions {'rewrite': 132, 'unchanged': 1609}
=== LS TX batch-03
  files 12 outcomes {'imported': 12} actions {'unchanged': 623, 'rewrite': 344}
=== OH batch-01
  files 2 outcomes {'imported': 2} actions {'unchanged': 79, 'rewrite': 10}
=== OH batch-02
  files 11 outcomes {'imported': 11} actions {'unchanged': 490, 'rewrite': 89}
=== OH batch-03
  files 6 outcomes {'imported': 6} actions {'unchanged': 323, 'rewrite': 87}
=== OH batch-04
  files 4 outcomes {'imported': 4} actions {'unchanged': 98, 'rewrite': 73}
=== OH batch-05
  files 1 outcomes {'imported': 1} actions {'unchanged': 81}
DONE
```

## Run 2 — re-fetched federal roll calls (`refetch-2026-08-29/`)

189 federal roll calls whose original fetch evidence had been written to
temp directories were re-fetched (all byte-identical to the stored rows)
and re-imported:

```
=== house-118-1
  files 23 outcomes {'imported': 23} actions {'unchanged': 1568}
=== house-118-2
  files 38 outcomes {'imported': 38} actions {'unchanged': 2547}
=== house-119-1
  files 64 outcomes {'imported': 64} actions {'unchanged': 5539, 'rewrite': 90}
=== senate-117-1
  files 4 outcomes {'imported': 4} actions {'unchanged': 42}
=== senate-117-2
  files 4 outcomes {'imported': 4} actions {'unchanged': 42}
=== senate-118-1
  files 11 outcomes {'imported': 11} actions {'unchanged': 132}
=== senate-118-2
  files 7 outcomes {'imported': 7} actions {'unchanged': 84}
=== senate-119-1
  files 38 outcomes {'imported': 38} actions {'unchanged': 481}
DONE
```
