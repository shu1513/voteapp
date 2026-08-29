# Pennsylvania code findings (recorded, not fixed)

## 1. The crosswalk proposer cannot see Pennsylvania's working names

`proposeLegiscanCrosswalk` matches on LegiScan's `first_name` + `last_name`.
Pennsylvania's people records put the member's **legal** first name in
`first_name` and the name they actually use in **`nickname`**, and
`last_name` sometimes carries a multi-part surname:

| people_id | `name` | `first_name` | `middle_name` | `last_name` | `nickname` | candidate files as |
| --- | --- | --- | --- | --- | --- | --- |
| 19976 | Liz Hanbidge | Laura | Elizabeth | **Frances Hanbidge** | Liz | Liz Hanbidge |
| 19981 | Mike Jones | **Paul** | Michael | Jones | Mike | Mike Jones |
| 19987 | Natalie Mihalek | Natalie | Nicole | **Mihalek Stuck** | | Natalie Mihalek |
| 22036 | Milou Mackenzie | **V.** | Milou | Mackenzie | Milou | Milou Mackenzie |
| 22046 | Craig Williams | **Wendell** | Craig | Williams | Craig | Craig Williams |

Nine of these are cases where the roster `name` field and the candidate name
are **byte-identical** and the proposer still missed them, because it never
reads `name` or `nickname`.

That is 24 sitting representatives — a tenth of the House — hand-added to
`crosswalk.json` in this campaign, every one confirmed as a sitting
`PA State Representative` off `candidates.current_office`. In Texas the same
class was 7 entries; in Georgia 6; here it is the dominant gap.

**Suggested fix, not made here:** have the proposer also try the `name` and
`nickname` fields (and strip a multi-part `last_name` down to its final
token) before giving up, still reporting each as a proposal for a human to
accept. It is not made in this PR because it changes proposals for every
already-committed state's crosswalk, which is a separate, reviewable change.

## 2. Pennsylvania constitutional amendments are absent from this dataset

Session 2192 carries exactly two bill types: `B` (3,982) and `R` (953). There
is no `JR`, `JRCA` or `CA`, so no proposed constitutional amendment can be
queued from this dataset even in principle. This is **not** the Georgia
finding (where amendments ride resolutions that the kept-types list drops
before the config is read) — here the instrument type simply does not appear.
Recorded so a future PA batch does not go looking for ballot questions in
this feed.

## 3. `resolve-report.json` is 34 MB for Pennsylvania

The full-resolution report carries every matched member of every roll. Same
shape as the Texas 88 MB report; **never commit it.** This directory keeps
`crosswalk.json`, the people snapshot, and the survey instead.
