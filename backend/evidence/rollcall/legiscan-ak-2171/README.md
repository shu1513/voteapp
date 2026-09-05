# Alaska roll-call votes — 34th Legislature, 2025-2026 Regular Session

LegiScan session **2171**, dataset cut 2026-08-30 (hash `51e5a1d82bb922a845dafa310f16386d`):
848 bills, 1,068 roll calls, 85 people. Alaska's House has 40 seats and its Senate 20, and
both chambers run on bipartisan coalitions under a governor who vetoes often, so divided
votes here do not follow party lines.

## Layout

- `crosswalk.json` — LegiScan `people_id` to VoteApp candidate id, 62 entries, 6 mapped.
- `legiscan-people-ak-2171.json` — the people snapshot the crosswalk is written against.
- `resolve-proposals-report.json` — what the proposer suggested, before review.
- `survey/divided-enacted-worklist.tsv` — every divided roll on a measure that became law,
  each with a disposition and a reason.
- `batch-01/` — measures that became law: plan, judging notes, judgments, roll evidence, ledgers.
- `batch-02/` — measures the governor vetoed. Same layout.
- `batch-03/` — measures that passed one chamber and then died. Same layout.
- `survey/remaining-pool-worklist.tsv` — every divided roll outside the enacted set, with a
  disposition and a reason, covering batch-02 and batch-03.
- `CODE-FINDINGS.md` — data defects found and deliberately not fixed in code.

## What the survey measured

Everything in the feed is a whole-chamber vote. `total` is 40 on every House roll and 20 on
every Senate roll, with three exceptions, all veto overrides. **There are no committee votes
in this dataset at all**, so the tally cut never has to separate them. Feed health is in the
cleanest tier: 0 repeated `roll_call_id`s, 0 summary-only rolls, 0 tally mismatches inside a
roll, 0 parse errors.

Fetch stored **863 rows**: 430 kept floor votes (226 House, 204 Senate), 430 excluded
questions kept for the audit trail, and 3 surfaced for a human. It rejected 111 votes on
excluded measure types and collapsed 94 identity duplicates, which reconciles to 1,068.

**112 divided floor votes; 50 of them on 31 measures that became law** (38 House, 12
Senate). A further 40 divided rolls sit on bills the governor vetoed, 17 on bills that passed
one chamber only, and 5 on bills that passed both chambers without being signed.

## Reading the feed

**A trailing `Effective Date(s)` is an annotation, not the question.** `Senate: Third Reading -
Final Passage Effective Date(s)` is the passage vote. The journal line under its tally reads
`EFFECTIVE DATE(S) SAME AS PASSAGE` and there is no separate roll. A real vote on the
effective date puts that question at the FRONT of the desc (`House: Third Reading Effective
Date`), which is why every exclusion in the config is anchored at the start of the desc.

**The dataset carries its own oracle.** Alaska's bill history prints the tally on its action
lines — `PASSED Y18 N2`, `AM NO 1 ADOPTED Y15 N5`, `CONCUR AM OF (H) Y15 N5` — so every desc
family in the config was matched to the journal action it names rather than guessed.

## Judging sources

The **enrolled Act** is ground truth: `https://www.akleg.gov/PDF/34/Bills/<BILL>Z.PDF`, plain
curl with a browser User-Agent, and `pdftotext -layout` is clean. The dated version stack
(Introduced / Comm Sub / Enrolled) comes from the dataset's own `texts[]`, so the version
check is exact.

Alaska publishes **no neutral legislative-agency analysis** of the kind Ohio, Georgia,
Maryland or Connecticut provide. The bill page carries a **Sponsor Statement**, which is
advocacy and is never a source, and a **Sectional Analysis**, which is also sponsor-prepared
and is at best an index. Montana is the closest comparison: read the Act.

**Alaska prints new statutory language in BOLD and deletions in [BRACKETED CAPITALS], and
`pdftotext` throws the bold away.** A whole reprinted statute then looks like new law.
`ak_bold.py` (see below) prints each line with its share of bold characters: 0% is existing
law, a bold run is what the Act does. One nuance — a section the Act adds whole is printed
plain, so the bold read finds insertions into reprinted statute, not new sections.

**The named roll call lives in the chamber journal**, and the bill page's history table gives
the journal page for every action. The journal search 500s unless every form field is present,
including the empty ones:
`https://www.akleg.gov/basis/Journal/Search/34?Chamb=H&Date1=&Date2=&Page1=951&Page2=951&Root=&Button=Display+Journal+Text`

**⚠ The bill page prints DATE | JOURNAL PAGE | (CHAMBER) ACTION — the date and page come
BEFORE the action.** Pairing an action with the date that follows it shifts every row by one.
That off-by-one made HB 35's concurrence look as though it happened on 7/30/2025 when it was
5/20/2025.

## Helpers

Outside the repo at `/Users/shu/legiscan-data/ak-work/`, because scratch does not survive a
session: `load.py` (dataset reader), `rules.py` (the config patterns in Python), `ak_docs.py`
(version stack download), `ak_bold.py` (bold-run reader), `ak_journal.py` (bill history and
journal pages), `audit.py` (member-list audit against the journal), `score.py` (reading level).
