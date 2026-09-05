# Nevada 2025 (LegiScan session 2144) — code and data findings

These are recorded, not fixed. Each one either costs nothing today or has a
naive fix that would be wrong.

## 1. Nevada bill numbers can end in a letter, and the dataset parser rejects them

Forty-six Nevada bills in this session are numbered with a trailing letter:
`SB88A`, `AJR6A`, `AB160A` and so on. `readLegiscanDataset` requires a bill
number of the shape `<letters><digits>`, so all forty-six come back as file
errors.

Nothing reachable is lost. All forty-six are dead bills — LegiScan status 6 —
and every one of them has **zero roll calls**, checked across all 1,333 votes
in the session. They are Nevada's early-session casualties: bills returned from
the Secretary of State and given no further consideration.

Two consequences worth knowing:

- **A Nevada fetch or survey run reports 46 file errors. That is expected, not
  a failure.** The run still stores every valid row. Montana's 42 parse errors
  set the same precedent.
- **`rollcall:legiscan:resolve` HARD-FAILS on these, where the fetcher only
  reports them.** The resolver throws on any unreadable file before it reaches
  the people list. The workaround, and what this campaign used, is to build the
  people snapshot from the dataset's `people/` directory by hand and pass
  `--people-file` instead of `--dataset-dir`. The committed
  `legiscan-people-nv-2144.json` is that snapshot, and it is byte-for-byte what
  the resolver would have written from the same files.

The naive fix — widening the bill-number pattern to allow a trailing letter —
is not obviously right. `measure_id` is the key a judgment names and a record
cites, and `SB 88A` and `SB 88` are two different bills that would then differ
only by one character. Since no `A`-suffixed bill can ever carry a vote, the
cost of the current behaviour is one confusing error list.

## 2. A same-day reconsider-and-revote REVERSES LegiScan's roll id order

Nevada lets a chamber reconsider its own passage vote and take it again the
same day. When it does, **LegiScan gives the vote that stands the LOWER roll
call id.** Verified against the Legislature's own action history:

| Bill | First vote | Reconsidered | Vote that stands |
|---|---|---|---|
| AB 123 (Senate, 2025-05-31) | 13-8, roll 1582878 | yes | **14-7, roll 1582877** |
| AB 451 (Senate, 2025-05-22) | 16-5, roll 1576767 | yes | **15-6, roll 1576766** |

So ordering Nevada's same-day rolls by id picks the wrong vote, every time.
The bill history is the only authority: look for `Action of passage
reconsidered` followed by a second `Read third time. Passed.` line.

The superseded-stage gate in `rollcall:judge` scans by date and cannot see
within-day order, so it flags the roll that stands as if it were the earlier
one. Answer it with `acknowledge_later_rolls` naming the superseded roll and a
note saying which way round the two go. Connecticut's Senate has the same
reversal for a different reason, so this is not unique to Nevada.

Across days the ids do ascend normally. AB 249's Senate passed 13-8 on
2025-05-23, reconsidered, and passed 20-0 on 2025-05-26 with the higher id.
That second vote is not divided, so AB 249's only divided Senate roll is
superseded by it and cannot be imported — the Maryland SB 255 shape.

## 3. LegiScan carries no concurrence roll for Nevada, so a chamber's only
   recorded vote is often on text that did not become law

The whole Nevada feed is two descriptions, `Assembly Final Passage` and
`Senate Final Passage`. There is no concurrence roll, no conference-report roll
and no veto-override roll anywhere in the session. When the second chamber
amends a bill, the first chamber concurs without a recorded vote, so its only
roll sits on a superseded reprint.

Measured over the whole divided-and-enacted pool: **25 of 104 rolls were cast
on text that was amended afterwards.** This is not a defect in the pipeline —
it is what Nevada records — but it means the version of every selected roll has
to be checked against the bill history before the roll is judged, and a roll
whose text changed materially needs either its own version-specific wording or
to be dropped.

## 4. One Senate roll lists 2 of 21 senators

SB 26's Senate roll (1550268, 2025-04-16) is recorded 2-0 with a two-member
list. Every other Senate roll in the session lists all 21. The small-tally
guard classifies it `null` and surfaces it rather than queueing it, which is
the wanted outcome, so nothing here needs a rule. It is the session's only
surfaced roll.

## 5. LegiScan's people file holds 75 members for 63 seats

Nevada has 42 Assembly seats and 21 Senate seats. The people file carries 75
non-committee members because several seats changed hands during or just before
the session and both holders appear at the same district. One of them, Cameron
Miller at Assembly District 7, casts no vote at all across the whole session;
Tanya Flanagan, listed at the same district, casts the votes. The crosswalk
records this so the inert mapping is not mistaken for a gap.

## 6. Nevada's change marking survives plain text in ONE direction only

Every Nevada bill print carries this line before the operative text:

> EXPLANATION – Matter in bolded italics is new; matter between brackets
> [omitted material] is material to be omitted.

`pdftotext` keeps the square brackets, so **deleted language is visible in the
extracted text**. It throws away the bold italics, so **added language is not
marked at all** — new text reads exactly like existing law being reprinted.

This is the inverse of the hazard in Georgia, Maine, Montana and Kentucky,
where the extract hides deletions and makes repealed law look live. In Nevada
the risk runs the other way: you can see what was removed, but you cannot tell
an act's new sentence from the statute it was dropped into. Nevada reprints a
whole statute section whenever it amends any part of one, so most of what is on
the page is untouched existing law.

The fix is cheap and does not need page rendering: the Legislative Counsel's
Digest at the top of every print enumerates exactly what changed, section by
section. Use the Digest to find the change, then read the operative section to
get its exact terms. Neither document alone is enough — the same
digest-plus-text method the California campaign settled on.
