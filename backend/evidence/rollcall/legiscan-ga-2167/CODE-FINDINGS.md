# Georgia code findings (recorded, NOT fixed)

## 1. Georgia's constitutional amendments are invisible to the pipeline

Georgia proposes constitutional amendments as **resolutions** (`HR 251`,
`HR 1243`, `SR 838`), which LegiScan types `R`. `LEGISCAN_KEPT_BILL_TYPES`
(`legiscanRollCall.ts`) keeps `B / JR / JRCA / CA` and drops `R` *before* the
state config is consulted, so no Georgia amendment can ever be queued — the
same shape as the Ohio `SJR 10` finding, which LegiScan itself solved for Ohio.

What is lost in this session: two amendments that passed both chambers and will
be **on the same Nov-2026 ballot as the candidates this campaign writes records
for** — `HR 251` (probate judges elected in nonpartisan elections, Senate
adoption 31-18 divided) and `HR 1243` (Georgia Next Generation 9-1-1 Fund,
Senate 31-14 divided). Two more were divided but died in one chamber (`SR 838`
"State Assurance of Voter Eligibility (SAVE) Amendment" 32-23, `SR 875`
state-wide grand juries 28-21) and are out of scope anyway.

**The naive fix is wrong.** 3,239 of the session's 5,480 measures are
resolutions, overwhelmingly commendations and study committees, and several of
those *are* divided (a Trump commendation, `SR 246`, split 31-18 on adoption) —
exactly the trivia the campaign filters exist to keep out. Georgia also words
the final amendment question differently per chamber: the Senate prints
`Adoption Of Constitutional Amendment` (8 rolls, unambiguous), the House prints
a bare `Adopt` shared with every ceremonial resolution.

A defensible fix is a per-state `keptBillTypes` opt-in plus a Georgia-only kept
pattern anchored on `^adoption of constitutional amendment$`, accepting that the
House side of an amendment stays unreachable until a House-side signal is found
(the bill title's trailing ` - CA` marker is the candidate, but it is title text,
not a typed field). Deferred deliberately: it is a code change with real blast
radius and the two reachable rolls are Senate-side only.
