# FL 2135 — batch-02 judging notes

4 votes judged and imported on local `voteapp`. **Production untouched.**

## Sources

- **HB 1205:** the House **Final Bill Analysis** (`h1205z.GOS.PDF`, describes
  the enacted ch. 2025-2) plus the Senate's **House Message summary**
  (`2025h01205.hms.ee.PDF`), which pins what House amendment 258567 changed
  (possession cap 5 → 25 forms). The full floor-amendment history on the bill
  page established that rolls 1563815 and 1564585 are votes on the exact
  enacted text (Senate delete-all 476344 + House 258567).
- **SB 700:** the **enrolled text** (`2025700er`, 92 sections / 111 pages) —
  the last Senate staff analysis (`.fp`, dated 4/09) predates adopted floor
  amendment 340838, so the enrolled act is the only document that describes
  what both chambers voted. The fp analysis grounded the fluoride mechanism
  (additives limited to those meeting/surpassing drinking-water standards,
  removing contaminants, or improving water quality; fluoride for dental
  health falls outside; ~29 counties then fluoridated). Failed Senate floor
  amendment 765612 — the divided 13-21 vote LegiScan mislabels as a third
  reading — was verified to be the fluoride-preservation attempt, which is
  why the description leads with that provision.

## Version check

- HB 1205: both selected rolls are on the enacted text by construction (the
  House passage came after concurring in the Senate rewrite as amended; the
  Senate roll IS the concurrence in the last change). The earlier passage
  rolls (1536108, 1563439) were on superseded texts and were not selected.
- SB 700: the Senate's 27-9 third reading was on the engrossed text
  (post-340838); the House passed the identical text 88-27 after both of its
  floor amendments (649901, 910335) failed; enrolled = that text. Same text
  both chambers → one shared description.

## Label calls

- **HB 1205 → `election_integrity`, yea = for.** Direction follows the area
  description ("secure, accurate, auditable, and trusted"): every enacted
  provision adds verification, audit, enforcement, or process control, and
  none runs against the area. The burden-on-initiatives objection maps to no
  research area; per the standing rule, political contestedness alone is not
  the two-directions test. The description names the restrictive provisions
  in full — third-degree felony for unregistered possession of >25 forms,
  30→10-day deadline, resident/citizen/felon eligibility bars, one-amendment
  and three-cycle sponsor caps — so a nay-leaning reader is not misled about
  what yes meant.
- **SB 700 → `general`, no stance** (Ohio H.B. 116 / GA blockchain
  precedent). Strands run different directions in different areas: the
  fluoride limitation is `environment_and_public_health`/against on its own,
  but the same yes vote enacted farmworker-housing protections, charity
  foreign-influence attestations, EV-charger consumer regulation, and
  mosquito-control funding. Tagging the whole 92-section act with the
  direction of two sections would ascribe the omnibus to its most famous
  strand — precisely the title-over-text mistake this campaign guards
  against.

## Import ledger

Real run on local `voteapp`, `startedAt` **2026-08-27T19:51:43.245Z**:

- 4 files, all `imported`, **0 errors, 120 inserts** (51+51 house, 9+9
  senate), 0 rewrites, 0 notified.
- Reconciled three ways: report 120; `candidate_records` 68,514 → 68,634
  (+120); the stamp predicate returns 120. Convergence dry run: all 120
  `unchanged`. The dry-run stamp (`…T19:51:15.436Z`) matches zero rows.
- FL batch stamps now: `01:26:41.127Z` = 333, `17:24:02.253Z` = 52 (batch-01
  + its review fix), `19:51:43.245Z` = 120 (batch-02). FL total **505
  records / 62 candidates**.

## Duplicate retired

The dry run's one `related` flag was real: **Colleen Burton** carried a
hand-researched VoteSmart vote claim describing this same SB 700 senate vote
(record `085bd48c-6c3f-4c5a-b74e-1272c7f61006`, source
justfacts.votesmart.org). Retired by hand after the import, reason naming the
replacing canonical record (`c0f175ff-7edb-4d31-ae6f-c99d4b667101`) — the
Ohio Bird / federal Husted pattern: the record citing the roll call itself
supersedes a second-hand vote claim.
