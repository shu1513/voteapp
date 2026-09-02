# Federal nay-stance repair, 2026-09-02

Closes the last of the repair population created when PR #950 removed `flip()`, the
importer step that mechanically inverted a measure's yea stance onto everyone who voted
no. After #950 a label states both sides, and an absent or null `nay` means nay voters
carry **no** tag rather than the opposite one.

The federal records were the last set still holding pre-#950 labels. Every other
jurisdiction had either been repaired by hand (Connecticut #960, Maryland #966,
Pennsylvania) or was judged after the contract landed (Maine, Missouri, Indiana).

## What was actually wrong, measured first

The federal set was already **72% repaired** before this run — 203 of its 283 approved
labels carried an explicit `nay`, written during the 2026-08-29 rejudge waves. Auditing
every live federal nay-side stance tag against its own roll's approved label gave:

| | tags |
| --- | --- |
| authorised by the approved label | 2,979 |
| contradicting the approved label | 7 |
| on a retired record, hidden from readers | 64 |

So the user-visible defect was **7 tags, not thousands**. What remained was mostly a
*metadata* gap: 80 labels on 78 rolls had no `nay` key at all. Their nay voters correctly
carried no tags, but the rolls could not be re-judged — the same trap that blocked
Missouri batch-01 — and the committed evidence disagreed with the contract.

## What was done

**1. A nay side was authored for all 80 labels.** The rule applied, which is Connecticut's
test made explicit:

> Author a nay stance only where the act is single-subject, its whole operative content is
> the area's own mechanism, and the mainstream objection is to that mechanism rather than
> to cost, constitutional structure, or a rider.

That produced **30 `against`, 2 `for`, and 48 `null`**. The reasoning per measure is in
`JUDGING.md`.

**2. Re-judged and re-imported.** 32 rolls changed, 46 compared unchanged (an explicit
`"nay": null` normalises to the same stored value, so those rows keep their old shape —
the Maryland behaviour; the committed `judgments.json` is now the authority and carries
all 80 keys). The import rewrote 11 records and left 4,231 unchanged.

**3. Re-synced 5 rolls carrying stale tags.** Six of the seven contradicting tags sat on
rolls whose labels had been repaired in August but which were never re-imported afterwards,
so their tags were never re-synced. `stale-tag-resync/` holds that run.

## Result

| | before | after |
| --- | --- | --- |
| nay-side stance tags authorised by their label | 2,979 | 3,694 |
| nay-side stance tags contradicting their label | 7 | **0** |
| labels with an explicit `nay` | 203 | 236 |

Records: 14,950 live federal roll-call records, unchanged in count. Yea-side stance tags
untouched at 2,720. Both convergence runs are clean — the judge re-applies as 78
`unchanged` and the import re-runs as 4,242 `unchanged`, 0 rewrites.

## Two findings worth keeping

**A withdrawn candidate is not the reason tags go stale — a missing re-import is.** All
seven contradicting tags belonged to one person, Lindsey Graham, whose only 2026 candidacy
is marked `withdrawn`. That looked like the cause, because the resolver excludes withdrawn
candidacies. It is not: the importer deliberately treats such a member as a *maintenance
voter* and still maintains their existing records, and one of the seven tags cleared in the
main run, which proves the path works. The other six were simply on rolls nobody
re-imported after changing their labels. **A label change only reaches the tags when the
roll is re-imported.**

**The 11 rewrites were duplicate folding, not drift.** They are hand-written vote claims
that cite the same roll call by URL (three shapes are folded) being replaced by the
canonical record — the behaviour the pilot exercised on 59 rows. They surfaced now only
because these 78 rolls had not been re-imported since those hand-written rows appeared.

**Left alone deliberately:** the 64 tags on retired records, which sit on the two retracted
rolls (H.R. 1047 and the superseded H.R. 239 roll 160). `candidateRecordStore` filters
`retired_at IS NULL`, so they are invisible to readers, and the importer does not touch
retired rows. Deleting them by hand would carry risk with no reader-visible benefit.

## Production

These records **are** in production, so this repair is not complete until it is promoted.
Local only for now.
