# batch-10 — what was decided and why

## The area-description trap, hit twice

`election_integrity` reads *"Ensure elections are secure, accurate, auditable, and trusted by the
public."* AB 1116 and AB 1249 are **access** bills — online registration without a driver's
license, and more early voting. Tagging either one `election_integrity: for` would have claimed
the legislator voted for election security, which is not what the vote was about, and is close to
the opposite of how an opponent would describe it.

Both were tagged **`civil_rights`** instead ("equal rights ... and fair treatment under law"),
which is what a voting-access expansion actually is. This follows the standing rule that a
misleading area tag is worse than an uncovered area.

## AB 1078 cuts both ways, and the description says so

The bill is mostly a tightening of concealed-carry licensing, which sits squarely on the
`gun_control` description. But it also **raises the 30-day handgun purchase limit from one to
three**. Judging it `gun_control: for` while hiding that would misdescribe the vote, so the
purchase change is stated plainly in the description, along with the fact that a federal court had
already blocked the one-gun limit. A reader can see both halves and disagree with the tag.

## Completeness audit caught a gap

The first draft of AB 1078 covered four of the digest's six items. The non-resident licensing
rules (residency attestation, live-fire per handgun, remote psychological exam) and the
out-of-state felony relief were missing, and were added before judging. Sentence lint: 16
descriptions, 0 warnings, longest sentence 33 words against the 45-word limit.

## Roll screens

All eight rolls are one-per-measure-per-chamber: a Senate third reading of the Assembly bill,
then an Assembly concurrence in the Senate's amendments. No duplicate-date twins, no rescissions.
Every roll passes the offline version check.

## Reconciliation — three ways, all agreeing

| check | result |
| --- | --- |
| row-count delta | 4,560 -> 4,872 = **312**, the predicted insert |
| per-roll sum | 10+11+11+68+68+65+11+68 = **312** |
| re-run dry-run | `unchanged: 4872`, zero inserts |

## Note on the report filenames

Today's runs wrote `import-dry-run-rerun-report.json` and `import-rerun-report.json`, because
`importReportFileName()` routes to the `-rerun-` name once a ledger exists in the shared evidence
directory. They are copied here under the plain names. They are also the first committed reports
written **after PR #1030**, and they carry relative paths (`~/legiscan-data/...`) rather than
developer-local absolute ones — the fix confirmed in real use.

## Owed later

AB 1116 is `enrolled` and written in the conditional. It joins the six measures already waiting on
the governor; the last day to act is 2026-09-30. **The watch list is now seven.**
