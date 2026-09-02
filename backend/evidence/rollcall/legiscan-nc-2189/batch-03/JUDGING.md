# How batch-03 was judged

## What is different about this batch

Every measure here was passed by the legislature and vetoed. None became law.
Two were overridden by one chamber only, which is not enough in North Carolina.
So every description says plainly that the bill is not law, and says why.

One roll is itself a veto override: the Senate's 30-19 vote on Senate Bill 50.
Its description says a yes vote was to pass the bill over the governor's veto,
rather than to move it ahead, because that is what the vote was.

Because these bills are still eligible for an override attempt, the descriptions
say the House or the Senate "has not" voted rather than "will not". Three of the
six vetoed measures sat on a chamber calendar as recently as 2026-08-31.

## Sources

Each bill was read from its ratified text on ncleg.gov, which is the text as
finally passed. The General Assembly's own staff analyses were used as an index
and every claim checked against that text. No sponsor, advocacy group or news
outlet was used for what a bill does.

## Version check

All three measures had both chambers' final votes on the same text.

The trap here is the earlier vote. Every one of these bills was voted on much
earlier by the House on a very different text, and none of those earlier votes
is in the batch:

- House Bill 171 passed the House 69-45 in April 2025 on a version that also
  covered public schools, the university system and admissions. The Senate
  stripped all of that out. The imported roll is the Senate's vote on the final,
  narrower text.
- Senate Bill 50 is the exception. The House passed the same text the Senate
  had passed, after rejecting its one floor amendment.
- House Bill 437 passed the House 76-36 in April 2025 as a drug penalty bill
  only. The public camping ban did not exist yet; it was added by a Senate
  committee 15 months later. The imported roll is the Senate's vote on the text
  that contains both parts.

## Tally check

All four rolls were compared against the official ncleg.gov transcripts and all
four match. Every member list is complete.

The Senate Bill 50 override was confirmed to be an override: transcript S-432
prints the question as "Motion 11 Veto Override". The Senate's passage vote on
that bill was a separate record months earlier.

## Direction calls

- **House Bill 171** is a ban on diversity, equity and inclusion programs in
  state agencies and local government. Civil rights, a yes vote against. The
  whole act sits in that area, so a no vote is scored for.
- **Senate Bill 50** is permitless concealed carry. Gun control, a yes vote
  against. A no vote gets no stance, because the same bill raised death benefits
  for police and firefighters and created new felonies for armed felons, so a no
  vote cannot be read as being about the carry provision alone. This follows the
  treatment of House Bill 193 in batch-01, which had the same shape.
- **House Bill 437** carries two labels. Raising drug penalties near shelters
  and creating a felony for a shelter worker is public safety, a yes vote for.
  Barring local governments from allowing anyone to camp on public land is
  social programs and welfare, a yes vote against. Neither strand covers the
  whole act, so a no vote gets no stance on either.

## Dropped on the fifth filter

- **House Bill 958, Election Law Changes.** It shortens early voting and raises
  the campaign money reporting thresholds, and it adds a citizenship answer to
  registration, State Auditor audits of county boards and list maintenance. A
  fair reader could call a yes vote either a restriction or a safeguard. It also
  bundles campaign finance, which is not about voting. Same reason House Bill
  834 was dropped from batch-02.
- **House Bill 377, 2026 Court Changes.** A vehicle bill. The House passed it
  113-0 in April 2025 as an estates and trusts package, and the Senate replaced
  the contents a year later. Most of the final bill is routine court procedure,
  and it was a conference report, which cannot be amended.
- **House Bill 96, Expedited Removal of Unauthorized Persons.** Despite the
  title it is about squatters, not immigration. It bundles a fast removal
  process with an unrelated ban on local pet shop rules, and no research area
  fits a squatter removal procedure.

## Descriptions

Flesch-Kincaid grade runs 6.7 to 7.6, mean 7.2. No sentence runs over 45 words.
Descriptions are seven sentences, longer than the two to four the house style
asks for and longer than batch-02. These bills need a sentence saying the bill
did not become law and a sentence saying why, which passage records do not. The
reading level was again treated as the more important constraint.

## Results

- Judge: 4 approved, 0 errors. No superseded-stage failures and no
  `acknowledge_later_rolls` needed; each roll is its chamber's last kept vote.
- Import: 4 files, 216 records, 0 errors, 0 notified, 0 related, 0 ambiguous.
- The database held 216 rows under this run's stamp, matching the report.
- A dry run after the import reports 216 unchanged.
- North Carolina now holds 2,004 live records across 147 candidates.
- Production was not touched. No AI provider was called.

## Correction after review

The first House Bill 437 descriptions overstated the bill in two places. They
said it would stop "any" city or county from allowing public camping, when the
bill lets a local government pick one site, get it state certified, and use it
for up to a year. And they said a "shelter worker" who "knowingly" allowed drug
activity would commit a felony, when the bill reaches a facility operator who
intentionally allows it. Both descriptions were rewritten, the roll re-judged,
and the import re-run: 39 records rewritten in place, 177 unchanged, and a dry
run after that reports 216 unchanged. The re-run ledger is
`import-rerun-report.json`; the original `import-report.json` is untouched.
