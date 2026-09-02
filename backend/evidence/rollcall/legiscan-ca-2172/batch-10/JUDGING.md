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

## AB 1078 — imported, then retracted on review

The first version of this batch judged AB 1078 `gun_control: for` on the argument that its
dominant thrust tightens concealed-carry licensing, and stated the loosening parts (the 30-day
purchase limit raised from one to three, firearm rights restored for some out-of-state felonies,
the lock-box transit exemption) in the description. Review pointed out two things that decide it:

- **batch-01 had already dropped AB 1078 under filter 5** for exactly this reason. Overriding a
  documented decision without saying so is the wrong way to change a rule.
- **The description is not the label.** Candidate sorting reads the machine label, and a voter
  filtering on gun control would see "for" on a bill that raised the purchase limit.

The 78 records (10 Senate, 68 Assembly) were retired through `manual:records:retire`
(`retirements.json` here, reason on every row), the two rolls were set back to `pending`, and a
following import dry-run reported nothing to insert. `judgments.json` keeps the pending rows so the
worklist continues to treat the measure as worked.

The standing rule holds and is now written down twice: **a mixed-direction bill gets no stance
label, and a measure with no defensible label is dropped, not described around.**

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
| after retraction | 78 retired, **4,794 live**; import dry-run `unchanged: 4794` |

## Note on the report filenames

Today's runs wrote `import-dry-run-rerun-report.json` and `import-rerun-report.json`, because
`importReportFileName()` routes to the `-rerun-` name once a ledger exists in the shared evidence
directory. They are copied here under the plain names. They are also the first committed reports
written **after PR #1030**, and they carry relative paths (`~/legiscan-data/...`) rather than
developer-local absolute ones — the fix confirmed in real use.

## Worklist fixes made on the same review

- **Procedural rolls leave the pool.** A motion to table an amendment or an appeal of the chair
  is a vote on a motion, not on the bill. SB 106 and AB 100 had *only* a divided procedural roll
  and were being offered as measures; four such measures now drop out in `rank.py`.
- **Same-day ties are deterministic and visible.** Where a chamber has two rolls on one date the
  worklist takes the highest roll id and records the rest under `same_day_alternatives`. After the
  procedural filter that field is empty for every entry, but the check stays.
- **`rank.py` owns the `pkg` and `vague` screens.** They had been added by hand after the fact,
  so regenerating destroyed them. `pool.py` now finds the backend directory from its own location
  instead of one worktree's absolute path.
- The chamber counts in `worklist/README.md` are taken from the script's output, not retyped.

## Owed later

AB 1116 is `enrolled` and written in the conditional. It joins the six measures already waiting on
the governor; the last day to act is 2026-09-30. **The watch list is now seven.**
