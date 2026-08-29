# Federal roll-call plain-language rewrite

On August 29, 2026, all 532 yea and nay descriptions for the 266 approved federal roll calls were rewritten in the 19 judgment files under `pilot-119-1`, `expansion-119-1`, and `backfill-118-117`.

The rewrite used only the existing evidence and descriptions. It did not call an AI provider or research new facts. Each description is one or two sentences, each sentence is at most 45 words, and no description is longer than its original. An invariant check confirmed that every number and every field other than the two descriptions stayed unchanged. The plain-language lint reported zero warnings.

`rollcall:judge` updated all 266 local `legislative_votes` rows. The federal import dry run and real run agreed: 14,342 existing candidate records were rewritten in place, 400 records were inserted for newly in-scope candidates Ami Bera and Tom McClintock, and there were no errors, ambiguous rows, or notifications. The 400 inserts raised the local live federal total from 14,531 to 14,931 records and added 434 tags.

The current resolver omitted 189 existing records because Chuck Edwards and Lindsey Graham are now marked withdrawn. Those record IDs remain intact but were not rewritten. All 14,531 pre-run record IDs were preserved. The importer also tried to restore 529 tags on existing records; those additions were removed after a guarded count-and-hash check so this wording-only campaign did not change the original records' labels. Their 15,376-tag baseline and hash were restored exactly.

All 19 original `import-report.json` files were restored byte-for-byte after the run. Production was not changed and still has the prior wording; promotion is separate work.

PR review found two reversed Title IX explanations, three NDAA descriptions that blurred authorization and appropriations, and five yea descriptions missing bill titles. All were corrected. Reapplying the nine affected judgment files updated 10 legislative votes and rewrote 579 candidate records in place, with no inserts or errors. The focused imports re-added tags on existing records; those additions were removed after each run, restoring the exact pre-review tag count and hash.
