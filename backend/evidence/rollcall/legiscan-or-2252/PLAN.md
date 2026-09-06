# Oregon 2026 short session — plan

## What this is

Oregon's 2026 regular session ran from 2026-02-02 to sine die on 2026-03-06.
It is a separate LegiScan dataset from the 2025 long session, so it gets its
own config entry, `OR-2252`, and its own evidence directory.

## The gate

A roll call is eligible only if it is **divided** — the smaller side is at
least a quarter of the larger — and the measure **became law**. That is 85
roll calls over 51 measures.

## The five selection filters

1. **Divided**, as above.
2. **Enacted.** Signed into law. Budget bills and fee ratification bills are
   excluded, because a vote on them is a vote on arithmetic rather than on a
   policy anyone can name.
3. **A nameable subject.** A reader must be able to say what the measure did
   in a sentence.
4. **One roll per measure per chamber: the chamber's final action on the
   text that actually became law.** This filter did real work here. Two
   measures were dropped by it. HB 4145 divided the House 33-19 on the
   A-Engrossed text, but the Senate then rewrote it, passed the new text 30-0,
   and the House concurred 50-3. SB 1517 has the same shape in reverse. In
   both cases the divided vote was cast on a text that never became law, so
   recording it would tell a reader something false.
5. **A defensible stance.** A measure is only judged when a yes and a no are
   each a position a reasonable person could hold within one research area.
   Where the counter-argument sits inside the same area, the nay side gets a
   record and no stance tag.

## Writing rules

Descriptions say what the measure did and what a yes or a no meant. Plain
words, short sentences, no sentence over 45 words, and the reading level is
measured before anything is imported — the target is roughly seventh to ninth
grade. Sentences are joined with periods.

## Order of work

Survey, then config, then fetch, then crosswalk, then batches. Each batch is
judged from the enrolled Act, with the staff measure summary used only as an
index, then measured for reading level, then dry-run, then imported, then run
again to confirm every record comes back `unchanged`.
