# Arizona batch-01 — selection

11 measures, 15 rolls, 408 records across 54 candidates. Local database only; production has
no Arizona roll-call records.

## How the pool narrowed

| step | rolls | measures |
| --- | --- | --- |
| divided floor votes on bills the Governor signed | 219 | 129 |
| after filter 4, one roll per measure per chamber | 184 | 129 |
| after the version rule: the chamber voted the enacted text | 135 | 110 |
| after dropping two superseded failed votes | 133 | 108 |
| read and judged for batch-01 | 15 | 11 |

**The version rule is the Arizona-specific step.** Arizona publishes no member list for a
concurrence vote, so when the second chamber amends a bill the originating chamber's vote on
the text that became law does not exist in the feed and its earlier third reading is a vote on
a superseded draft. 63 of the 129 measures passed the second chamber unamended, so both
chambers' rolls are on the enacted text; for the other 66 only the second chamber's is. That
removed 49 rows.

The two superseded failed votes are HB 2518 (House failed 21-35, then passed 51-2 a week later)
and SB 1661 (failed 19-39, then passed 57-0). Both were dropped rather than imported, following
the federal 117-1 roll 160 retraction: a record citing only the failed vote misrepresents the
members who changed side.

## The five filters

1. **Divided** — both sides non-zero and the losing side at least a quarter of the winning side.
2. **Consequential** — the Governor signed the bill. Verified per measure from the bill
   history's `Governor Signed` line, not from LegiScan's status code. Every measure in the pool
   was signed; none became law without a signature.
3. **A nameable subject that maps to a research area.**
4. **One roll per measure per chamber**, taking the decisive third reading and checking the
   `passed` flag, because Arizona files a failed third reading under the same caption as a
   successful one.
5. **A defensible for-or-against direction.** Arizona takes no no-stance imports: `general` is
   not a user-selectable research area, so its tag would be hidden from every legislative view.
   A measure with no honest direction is dropped, not recorded without one.

## What is in

| measure | chapter | area | yes means | rolls |
| --- | --- | --- | --- | --- |
| HB 2110 adaptive reuse | 41 | housing_affordability | for | House 42-16 |
| HB 2928 accessory dwelling units | 217 | housing_affordability | for | Senate 21-6 |
| SB 1353 municipal permit review | 187 | housing_affordability | for | House 44-14 |
| SB 1529 preapproved house plans | 259 | housing_affordability | for | House 44-12 |
| SB 1590 autism therapy coverage | 142 | healthcare_affordability | for | House 43-12 |
| SB 1247 tobacco and vaping age 21 | 228 | environment_and_public_health | for | House 39-13 |
| SB 1159 unpaid wage claim limit | 38 | corporate_accountability | for | House 42-15 |
| HB 2291 opioid red cap repeal | 45 | environment_and_public_health | **against** | House 36-22, Senate 16-13 |
| HB 2607 fentanyl sentencing | 85 | public_safety_and_crime_control | for | House 46-12, Senate 17-13 |
| HB 2114 sexual conduct sentencing | 49 | public_safety_and_crime_control | for | House 39-20, Senate 17-13 |
| SB 1351 gift card theft | 186 | public_safety_and_crime_control | for | House 33-25, Senate 16-10 |

Six areas, and both directions appear inside `environment_and_public_health` on purpose: SB 1247
raises the tobacco age and HB 2291 repeals an opioid packaging safeguard.

Every label states the nay side explicitly, and every one is `null`. On each measure the
realistic objection runs on a different axis from the area being scored — construction cost and
local control on the housing bills, insurance premiums on SB 1590, sentencing policy on the
criminal bills — so a no vote is not evidence of a position on the area's own goal.

**SB 1247 is a strike-everything amendment**: it reached the House as a bill about the Arizona
teachers academy and the House replaced its whole text with the tobacco rules. The description
says so, because a reader who looks the bill up by number will find the old subject.

**HB 2928 carries two counter-strands** — local governments may require an owner to live on site
where a short-term rental holds one of these units, and historic districts come out of a
fast-track review. The description states both rather than hiding them. The stance holds
because the act's operative weight is a mandate on every county to permit accessory dwelling
units, and the carve-outs are narrow.

## What was dropped, and why

- **SB 1395** international medical graduates — eases licensure for doctors trained at
  unapproved schools by requiring enrollment in, rather than completion of, 24 further months of
  training. The objection is patient safety, and `healthcare_affordability` is defined as
  access to "affordable, quality care", so the counter-reading sits inside the same area.
  Follows Montana HB 218.
- **SB 1319** election officer certificates — sets issue and expiry dates. Administrative
  housekeeping with no direction.
- **HB 2727** county water authority — forming one authority for one basin, technical and local,
  and it points no single way.
- **HB 2201** utility wildfire mitigation plans — **deferred, not rejected.** Requiring
  mitigation plans reads as prevention, but wildfire bills commonly pair a planning duty with a
  liability safe harbor, and that has to be read in full before a stance is defensible.
  Batch-02.
- **HB 2518** and **SB 1661** — superseded failed votes, above.

## What remains

113 rows on 93 measures are marked `candidate:batch-02` in
`../survey/divided-signed-worklist.tsv`, each already confirmed to be on the enacted text.
Sixteen budget bills (SB 1735 to SB 1750) are in that set and are excluded by the standing
appropriations rule when batch-02 is selected. The 36 concurrent resolutions that went to the
ballot cannot be reached at all — see `../CODE-FINDINGS.md` finding 1.
