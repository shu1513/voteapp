# Nebraska roll-call evidence, 109th Legislature (LegiScan session 2185)

Nebraska is the only state with a single house. Its 49 members are called
State Senators, so every roll is stored under `senate`, and the config names
only `senate` in its chamber sizes.

## The dataset

LegiScan session **2185**, "109th Legislature", covering 2025 and 2026 in one
file. Dated 2026-07-12, hash `0deae4fd8cb0a40e36bc796941b20b71`. 1,847 bill
files, 1,774 roll calls, 64 people.

**There is no second session to register.** Every other Nebraska dataset
LegiScan lists belongs to the 108th Legislature or earlier. That was checked
by enumerating the whole list rather than assumed, after the Alabama case
where a stale comment hid a session nobody had worked.

## Layout

- `crosswalk.json` — LegiScan people_id to VoteApp candidate id, 50 entries
- `legiscan-people-ne-2185.json` — the people snapshot the crosswalk is checked against
- `survey/divided-enacted-worklist.tsv` — every roll in the pool, with its disposition
- `batch-01/` — the first batch: plan, judging notes, judgments, roll evidence, import ledgers

The dataset itself and the full evidence set live outside the repository at
`/Users/shu/legiscan-data/ne-2185*`, with the run state at
`/Users/shu/legiscan-data/ne-run-state.md` and the tools in
`/Users/shu/legiscan-data/ne-work/`.

## How Nebraska votes, and what that means for selection

A bill passes through three floor stages: General File, Select File, and Final
Reading. Only Final Reading decides whether the measure passes. The two earlier
stages are amend-and-advance steps, so their rolls are excluded, the same way
second readings are excluded in Texas, California, Missouri and Montana.

Nebraska votes the emergency clause together with the bill, not separately the
way Arkansas and West Virginia do, so `Passed on Final Reading with Emergency
Clause` is kept as a passage vote.

Because a bill that fails earlier never reaches Final Reading, **every divided
Final Reading roll in this session sits on a bill that passed**. Nebraska has
no equivalent of the one-chamber scope other states opened.

## The pool, measured

- 372 floor rolls stored, 1,383 excluded questions, nothing surfaced
- 50 divided
- **49 divided and enacted after taking each measure's last kept roll**, on 49
  measures; 1 superseded
- of those 49, **5 measures were vetoed and never became law** (see below), so
  the real enacted pool is 44

## Three things about this feed to keep

**LegiScan's status field says these five measures were enacted, and they were
not.** LB 287, LB 319, LB 839, LB 929 and LB 1029 were all returned by the
Governor without approval. Four of them had an override vote, and all four
failed. Read a measure's fate off its own history, never off `status`.

**Nebraska never uses the word veto.** Its history reads "Returned by Governor
without approval", and an override is a motion "that the bill becomes law
notwithstanding the objections of the Governor". A search for the word finds
nothing and makes the session look veto-free.

**A veto override is filed as a numbered motion.** LegiScan captions those four
override votes `Rountree MO259 failed` and the like, exactly as it captions any
other motion, so the config's rule against amendment and motion votes excludes
them. Only the bill history names the question. Reaching them would need a
per-roll disposition file, the design already parked for Maine.

## Two rolls are held

Every one of the 373 kept rolls was checked against the caption Nebraska's own
bill history prints for that vote. Two disagree, and in both the feed claims a
bill passed where the state says it failed, because LegiScan's `passed` flag is
a bare-majority check that does not know Nebraska's 33-vote thresholds:

- **LB 258, 2025-05-14, 31-17.** The bill amends a law Nebraska's voters adopted
  by initiative, which takes 33 votes. It passed 33-16 the following year, and
  that later roll is the one this batch imports.
- **LB 48A, 2025-05-30, 27-21.** An emergency clause also takes 33 votes.

Both are stored and visible but can never be queued.

## Reading Nebraska's acts

**The words an act removes are struck through and the words it adds are
underlined, and `pdftotext` throws both marks away.** LB 258's escalator clause
flattens to "by one and three-quarters percent the increase in the cost of
living", which is the new rule and the old rule printed one after the other.
`ne_text.py` recovers the marks from the PDF's drawn lines and prints deletions
as `[[...]]` and additions as `<<...>>`; `ne_text.py --check` proves it against
that clause before anything rests on it. Text with no mark is existing law
being reprinted, not a change.

Sources, all from `nebraskalegislature.gov/FloorDocs/109/PDF/`:

- `Slip/<BILL>.pdf` — the enacted act, and the ground truth
- `CS/<BILL>.pdf` — the Committee Statement: official, neutral, section by
  section. **It also prints a labeled list of the people who testified for and
  against. Read only the summary sections.** And it describes the bill as the
  committee sent it out, which is not always what passed.
- `Final/`, `Engrossed/`, `Intro/`, `AM/<AM####>.pdf`
- **Never `SI/<BILL>.pdf`.** That is the introducer's own statement of intent,
  which is advocacy. `ne_docs.py` will not fetch it.
