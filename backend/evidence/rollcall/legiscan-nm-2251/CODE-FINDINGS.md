# New Mexico 2026 Regular Session — feed defects

Recorded, not fixed. Neither is fixable in our code.

## 1. LegiScan's stored tally for Senate Bill 151 is wrong, and nothing in the parse can see it

The House vote on Senate Bill 151 (roll 1638031, 2026-02-18) is stored as **42 yea, 19 nay, 9
absent**. New Mexico's official roll call sheet, RCS# 171, reads **43 yea, 19 nay, 6 excused, 2
absent**. Both add to 70, both are internally consistent, and the member list matches the stored
header, so no parser check can catch it. **LegiScan's own bill history agrees with the official
sheet**, recording "Passed in the House of Representatives - Y:43 N:19", so the defect is in the
roll call record alone.

This is the same shape as roll 1496261 in the 2025 regular session (the House vote on Senate Bill
3), where LegiScan stored 42-23 against an official 44-23. **Two instances in two sessions means a
clean parse proves nothing about a New Mexico tally.** Every roll picked for a batch is checked
against its nmlegis.gov official sheet before judging, and a mismatch holds the roll — the approval
gate writes the stored tally into the record text, so importing one would publish the wrong number.

Senate Bill 151 is held and not imported. Fixing it would need an official-tally override, the shape
of the existing `official_vote_date` override.

## 2. House Bill 332's roll disagrees with its own history by a day and a vote

Roll 1634863 is stamped 2026-02-16 with 42 yea and 20 nay. The bill history records the House
passing it on 2026-02-17 with 43 yea and 20 nay. House Bill 332 is a capital outlay reauthorization
bill and is excluded from the batch on its subject, so this was not chased to an official sheet. It
is noted because it is consistent with finding 1: New Mexico roll records and New Mexico histories
disagree more often than in other states, and the history is not always the wrong one.

## 3. The amendments-in-context prints for this session are scanned images

`nmlegis.gov/Sessions/26%20Regular/Amendments_In_Context/<PADDED>.pdf` returns a large scan with
almost no extractable text, so the guillemet-marker trick that isolates adopted amendments in the
2025 session does not work here. The workaround is to read the committee amendment PDFs directly
from `bills/house/<PADDED>FC1.pdf` and the like, which are text and state each change line by line.
This is a source-format change, not a bug, and it is recorded so nobody rebuilds the 2025 tool and
wonders why it returns nothing.
