# Can we check LegiScan against each state's own vote record?

Written after the Montana audit found that LegiScan's Montana feed can put a
member on the wrong side of a vote. See
`legiscan-mt-2159/survey/legiscan-vote-audit.md` for that finding.

The question here is narrow: **for each state in the registry, does the state
itself publish member-by-member roll calls, free and without a key, in a form we
can compare against?** Nothing below has been used to check any records yet.
This is a survey of what is possible.

Every URL below was fetched successfully during the survey unless the entry says
otherwise.

## Summary

| State | Verdict | Form | Legislator identity |
| --- | --- | --- | --- |
| Alabama | **easy** | GraphQL JSON API, no key | full name string |
| California | **easy** | bulk download, 596,917 vote rows | last name + roster file |
| Nevada | **easy** | three plain HTML fetches per bill | "Last, First" string |
| North Carolina | **easy** | HTML index + HTML per vote | name string, party by grouping |
| Pennsylvania | **easy** | HTML per roll call | **numeric member id** |
| Delaware | **easy** | undocumented JSON, sequential ids | surname only |
| Indiana | **easy** | open JSON + roll-call PDFs | surname, roster has ids |
| Georgia | awkward | JSON API behind a minted token | **numeric member id** |
| Illinois | awkward | PDF per roll call | surname only |
| Kentucky | awkward | one PDF per bill | surname, initial for clashes |
| Maryland | awkward | PDF, enumerable, self-describing | surname only |
| Maine | awkward | House HTML, Senate PDF | "SURNAME of Town" + party |
| New Mexico | awkward | PDF in an open directory | surname only |
| New York | awkward | Assembly HTML; Senate 403s scripts | surname only |
| South Carolina | awkward | HTML index + PDFs, some scanned | "Last, First M." |
| Arkansas | awkward | HTML per vote, no bill index | surname |
| Tennessee | awkward | HTML on the bill page | surname + initial |
| Texas | **not feasible** | journal PDFs only, paths disallowed | name in PDF text |

## The recurring problem: no legislator ids

Only **Pennsylvania** and **Georgia** put a stable numeric member id in the vote
record itself. Everywhere else a vote is a name string, usually a bare surname,
and matching it to LegiScan's `people_id` means a roster join plus an alias
table. Montana needed three hand aliases across 151 members, and every state
will need its own.

This matters for correctness, not just effort. In the Montana audit an unmatched
name had to be reported explicitly, never silently counted as agreement —
otherwise a name-matching failure hides a real disagreement.

## Entry points

**Alabama** — `https://gql.api.alison.legislature.state.al.us/graphql`, POST
JSON. Introspection is off, so the two working query shapes are:
`rollCallVotes(sessionAbbreviation:"2025RS", body:House, rollCallNbr:1) { yeas nays abstains passes votes { memberName vote } }`
and `instrumentHistories(where:{sessionAbbreviation:{eq:"2025RS"}, instrumentNbr:{eq:"HB1"}}) { data { calendarDate body voteType voteTitle rollCallNbr yeas nays } }`.
`body` is the enum `House` or `Senate`.

**California** — `https://downloads.leginfo.legislature.ca.gov/`, one ZIP per
two-year session plus daily files. `BILL_DETAIL_VOTE_TBL.dat` inside is
tab-separated with backtick-quoted text: bill id, location, member last name,
date, motion id, AYE/NOES/ABS. `LEGISLATOR_TBL.dat` is the roster. The ZIP is
1.27 GB but the votes table sits near the front, so it can be streamed.

**Nevada** — three fetches, all plain HTML:
`/App/NELIS/REL/83rd2025/HomeBill/BillsTab?...` for bill keys, then
`/Bill/GetBillVotes?billKey=…&voteTypeId=3` for vote keys, then
`/Bill/GetBillVoteMembers?voteKey=…&voteResultPanel=All`. The vote is in both a
CSS class and the text. Verified back to the 2023 session.

**North Carolina** — index at
`/Legislation/Votes/RollCallVoteHistory/2025/H`, detail at
`/Legislation/Votes/RollCallVoteTranscript/2025/H/576`. `robots.txt` asks for a
2-second crawl delay.

**Pennsylvania** — `/house/roll-calls/summary?sessYr=2025&sessInd=0&rcNum=1341`
and the same under `/senate/`. Roll-call numbers run 1..N per session. Each
member row carries `/house/members/bio/1161/…`, so the id is free. Pages are
~290 KB each.

**Delaware** — POST, form-encoded:
`/json/BillDetail/GetVotingReportsByLegislationId` then
`/json/RollCall/GetRollCallVoteByRollCallId`. Roll-call ids are walkable
integers; 54700 was June 2023 and 58000 April 2026. Some amendment roll calls
return an empty member list.

**Indiana** — skip the documented `api.iga.in.gov`, which returns
`403 x-api-key not found`. The site's own unauthenticated backend works:
`https://iga.in.gov/api/getBillDetails?session_lpid=session_2025&bill_basename=HB1001`
gives a `roll_calls` list with `vote_rc_number`; the names are only in the PDF at
`/pdf-documents/{ga}/{year}/{bill_chamber}/bills/{BILL}/rollcalls/{BILL}.{n}_{H|S}.pdf`.
Trap: the chamber in the path is the chamber the bill started in, and the letter
suffix is the chamber that voted. A wrong path returns the site's HTML shell with
a 200, so check the content type.

**Georgia** — real JSON with member ids, but every `/api/` call needs a bearer
token minted from `/api/authentication/token?key=<128 hex>&ms=<epoch ms>`, where
the key is derived from `ms` inside the site's JavaScript. A captured key/ms pair
replays from plain curl and the token lasts about five minutes. So: run a
headless browser once, then use plain HTTP.

**Texas** is the one clear no. Per-member votes exist only inside House and
Senate journal PDFs, `capitol.texas.gov/robots.txt` disallows `/BillLookup/`,
`/Reports/` and `/Search/`, the bulk download page is a 404, and the one JSON
endpoint returns `"VoteExist":false` once the journal is published.

## What this is worth doing in

If the Montana defect rate holds elsewhere, about 1% of rolls would put a member
on the wrong side — 20 of the 1,780 rolls that paired there — and just under half
of those would be on a third reading, the stage this campaign actually uses.
Counting the 23 rolls that show an excused member as voting, the rate of rolls
with a wrong member vote of any kind is 43 of 1,780, or 2.4%.

Montana came through clean because the eight affected final rolls happened not
to be ones already imported. That was luck, not design.

The sensible order is by imported record count among the easy states, since
those need no PDF or browser work: Alabama, California, Nevada, North Carolina,
Pennsylvania, Delaware, Indiana. **No state's records should be promoted to
production before its rolls are checked.**
