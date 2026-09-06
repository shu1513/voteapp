# New Mexico feed defects

Defects in LegiScan's New Mexico data, recorded and not fixed. The two modern-session findings are
in `legiscan-nm-2251/CODE-FINDINGS.md`; this file covers what the historical backfill turned up.

## The stored roll-call tally is wrong often enough to need a check every time

LegiScan stores each roll call twice over: once as a member-by-member list with its own totals, and
once as a line in the bill's history (`Passed in the House of Representatives - Y:44 N:23`). The two
are parsed separately from the state's website and they do not always agree.

Across the nine historical sessions, **28 of the 123 divided-and-enacted House rolls disagree**, a
little under a quarter of the pool. Almost all are off by one or two votes in a single column, which
is the shape of a member-by-member list that dropped a member and recorded them absent.

Where the two were checked against New Mexico's own roll-call sheet, **the history line was right
and the member list was wrong**: 2187 Senate Bill 3 reads 44-23 in the history and on the official
sheet, and 42-23 in the stored roll call. The same holds for 2251 Senate Bill 151.

The consequence is not just a wrong printed number. A member the list records as absent, who in fact
voted, gets no record at all. Those rolls are held rather than imported.

## Two House final passage rolls on the same bill, same day

2020 first special session, Senate Bill 4: the House rejected it 32-38, adopted a motion to
reconsider, and passed it 44-26 the same afternoon. LegiScan stores both as `House Final Passage`
with no marker distinguishing them. The judge script's `acknowledge_later_rolls` guard caught this
and refused the import until the failed vote was named explicitly.

Anything that treats "the House final passage roll" as unique for a New Mexico bill is wrong.

## No enrolled text for special sessions

LegiScan carries an `Enrolled` document for every bill in the New Mexico regular sessions, but for
the three special sessions (1750, 1830, 1967) it carries the introduced bill only. There is no
version of the law as passed in the feed for those sessions.
