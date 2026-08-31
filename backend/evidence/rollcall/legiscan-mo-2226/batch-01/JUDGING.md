# Missouri special session batch-01 — judging and import

**Result on local `voteapp` 2026-08-30: 4 files all `imported`, 0 errors, 229 inserts, 0 notified,
115 candidates. PRODUCTION UNTOUCHED.** Missouri now holds **1,174 records** across three batches.

Reconciled three ways: the dry run planned 229 and the real run inserted 229; rows carrying the run
stamp `2026-08-30T06:21:17.833Z` number 229 across 115 candidates; a convergence dry run reports all
229 `unchanged`, and the dry run's own stamp matches zero rows.

## Sources

Both measures were judged from the **enrolled text** — `3344H.01T` (HB 1) and `3353H.03T` (HJR 3) —
because the House publishes no Truly Agreed summary for special-session bills (both `T` summary URLs
answer 200 with a 793-byte error page). The Perfected summaries were used only as an index, and each
claim in the descriptions was read back out of the enrolled text. For HJR 3 the perfected text the
House voted (`3353H.03P`) and the enrolled text differ only in the header stamp, verified by diff.

Roll identity came from the roll-call PDFs under `bills254/rollcalls/`, matched to LegiScan on
`(Total Yes, Total No, Total Present)` and cross-checked against the bill history's `Third Read and
Passed` lines: HJR 3 `98-58`, HB 1 `90-65`.

## The guard fired once, and was right to

The judge refused roll 1601420 as possibly not the chamber's final kept floor vote on HJR 3, naming
roll 1601419. Both rolls carry the identical desc `House: HJRs FOR THIRD READING HCS HJR 3` on the
same day — 1601419 (104-51) is PDF `006.002`, whose header carries the extra line **PREVIOUS
QUESTION**, and 1601420 (98-58) is PDF `006.003`, the passage vote that matches the bill history.
Approved with `acknowledge_later_rolls: [1601419]` and a per-judgment `note` saying so. Read the note
rather than assuming the guard was waved through.

## Labels

Both measures carry `general` with `"yea": null, "nay": null` — recorded, no stance. The reasoning,
including why HJR 3's separable-looking strands still do not earn a label, is in `PLAN.md`.

## Wording checks

Body-tail joins built with a period; the builder asserts `", The "` appears in no description. The
real `candidateRecordPlainLanguageLint` ran over all 8 descriptions before importing: 0 warnings.

## Review response (2026-08-30)

Two findings on HJR 3, both verified against primary sources and both fixed. **All 229 records
rewritten in place** (stamp `2026-08-30T06:45:46.776Z`, ledger `import-rewrite-report.json`);
convergence dry run reports all 229 `unchanged`; `import-report.json` remains the insert ledger.

**1. The amendment had already been voted on, and defeated.** The descriptions said it "takes effect
only if voters approve it at the November 2026 general election, or at an earlier special election
called by the governor". The governor used exactly that power: a proclamation announced 2026-05-22
placed HJR 3 on the **August 4, 2026 primary ballot as Amendment 4**, and the governor's official
statement of 2026-08-05 records "the defeat of Amendment 4". So the sentence was not merely stale —
it told 115 legislators' readers that a rejected amendment might still take effect. The descriptions
now say it went to voters as Amendment 4 on 2026-08-04 and they rejected it, and the body moved from
"would" to "would have" throughout (the LSC convention for provisions that never took effect).

**⚠ A BALLOT-MEASURE DESCRIPTION HAS AN EXPIRY DATE.** Any judgment that says "if voters approve"
is a claim about the future and must be revisited once that election happens. Scope checked across
every roll-call record in the database: the only other forward-looking ballot language is Missouri
**HJR 73** (regular session, 112 records). It is **correct as written** — it names no election date,
and the same proclamation release confirms that "all other ballot measures, if certified, will be on
the November general election ballot", so HJR 73 has not been voted on yet. It will need this same
treatment after November 2026.

**2. The foreign-contribution ban has an intent element on one side only.** Enrolled § 54.3(1) is two
sentences: a committee may not "**knowingly or willfully** receive, solicit, or accept" foreign money,
while "**No** foreign adversary of the United States or a foreign national **shall make any**
contribution or expenditure" — strict, with no mens rea. The old wording described only the committee
side and omitted the intent element, which broadened criminal liability. The fix adds "knowingly or
willfully" to the committee ban **and** states the separate, unqualified ban on the foreign donor,
which the description had left out entirely. This is the Texas SB 2972 rule: when a statute qualifies
a prohibition by intent, the description carries the qualification.

One line of HB 1 was tightened in the same pass, for the same class of risk: "the new boundaries take
effect with the election of the 120th Congress" now reads "**under the act**, the new boundaries first
apply at the election of the 120th Congress", so it describes what the statute provides rather than
predicting an election outcome.

## Roll dates

All four roll dates match the dates printed on the official House roll-call PDFs and the Senate
actions in the bill history (2025-09-09 House, 2025-09-12 Senate). No `official_vote_date` override
was needed.
