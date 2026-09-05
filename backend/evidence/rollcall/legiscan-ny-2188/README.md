# New York roll-call import — LegiScan session 2188

New York's 2025-2026 General Assembly, both years in one LegiScan dataset (session id
2188, dated 2026-08-30): 25,313 bills, 14,737 roll calls, 221 people. All work here is on
the local `voteapp` database. **Production holds no New York roll-call records.**

## Layout

- `crosswalk.json` — the reviewed map from LegiScan `people_id` to our candidate rows.
- `legiscan-people-ny-2188.json` — the people snapshot the crosswalk was written against.
- `survey/` — the description histogram the config was written from, and the ledger of
  every divided-and-enacted measure with its disposition.
- `batch-01/` — plan, judging notes, judgments, the 22 roll evidence files, and the
  import ledgers.
- `CODE-FINDINGS.md` — feed defects found and deliberately not fixed in code.

## What the feed looks like

New York has the smallest floor vocabulary of any state in this campaign. Exactly two
descriptions are floor votes and both name the question in words: `Senate Floor Vote -
Final Passage` (3,614 rolls, 61-63 of 63 seats) and `Assembly Floor Vote - Final Passage`
(1,856 rolls, 148-150 of 150). All 212 other families name a committee and none reaches
40 votes, so the pipeline's tally check rejects them and the config carries no exclusion
rules at all.

Feed health is the cleanest tier: 0 repeated roll call ids, 0 summary-only rolls, 0 rolls
whose tally disagrees with their own member list, 68 identity-duplicate extras that the
fetcher collapses. One roll can never be stored; see `CODE-FINDINGS.md`.

Fetch stored **5,459** rows and reconciles: 5,459 floor + 9,258 committee + 12 on
concurrent resolutions + 7 collapsed duplicates + 1 parse error = 14,737.

## The pool

1,055 divided floor votes (Assembly 371, Senate 684). **343 are divided and on measures
that became law, across 236 measures**, and every one of those 236 carries a disposition
in `survey/divided-enacted-worklist.tsv`.

New York passes a bill by having the second house substitute its own companion and vote
the first house's bill number, so 234 of the 236 measures carry one floor vote per chamber
under a single bill id. Only 4 measures in the whole session have a companion that also
took its own floor vote, and none of them is in batch-01.

## How a New York measure is judged

Source: the Assembly's bill page, which plain `curl` serves with a browser user agent:

```
https://nyassembly.gov/leg/?default_fld=&bn=<BILL>&term=2025&Summary=Y&Actions=Y&Text=Y
```

One page carries the neutral one-line summary, the full action history with dates, and the
complete text of the print in force — enough for both the judgment and the version check.

**The sponsor's memo is never read.** Every New York bill carries one on both
nysenate.gov and the Assembly site, and it is advocacy, the same class of document as the
Texas author's statement of intent. Adding `Memo=Y` to that URL is what fetches it; the
reader in `/Users/shu/legiscan-data/ny_bill.py` deliberately never asks for it.

**New York prints added language in bold and underline and deletions in [brackets]**, and
stripping the HTML throws that away, exactly as `pdftotext` throws away Kentucky's bold.
The reader marks added text `{NEW ... NEW}` so a description can tell what an act changes
from statute it merely reprints. Read a New York act with that marking on.

`www.nysenate.gov` sits behind a Cloudflare challenge and answers `curl` with 403. The
Assembly site does not.

## Fan-out

The crosswalk maps **55 of 219 serving members**: 54 of 150 Assembly seats and 1 of 63
Senate seats. So an Assembly roll writes about 51 records and a Senate roll writes 1.
That is our roster, not the feed — a roster campaign is filling New York in parallel, and
extending this crosswalk and re-importing adds those members without duplicating anything
(the California precedent).

## State

Batch-01: 11 measures / 22 rolls / **570 records / 53 candidates / 354 tags**. Production
still has none.

Batch-04 (2026-09-05): A 10710 and A 10711, the two vaccine-schedule measures the user directed
to import with neutral wording and no direction tag (`general`, non-stance): 4 rolls / **106
records**. Every row of the worklist now carries a disposition.
