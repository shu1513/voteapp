# State ballot contest-order rules — reference

Companion to `docs/plans/state-ballot-order-research.md`. One section per
jurisdiction, fixed template, graded A/B/C (see plan). Findings land here as
research completes; PENDING means unresearched, not "no rule".

Baseline (what "no deviation" means): the majority pattern encoded in
`backend/src/pipeline/address/ballotContestRank.ts` — President → US Senate →
US House → statewide executives → state senate → state house → county →
municipal → school → judicial late block (supreme → appeals → trial) →
measures last.

## Entry template

```text
### XX — State (FIPS nn) — GRADE [A|B|C] [partial]
- Authority: <cite + URL> (accessed YYYY-MM-DD)
- Office order: <sequence as prescribed>
- Judicial: <within-level | late block | separate section; retention placement>
- Measures: <last | first | interleaved; amendments vs local props>
- County discretion: <none | partial | full — cite>
- School/special: <placement>
- Corroboration: <county, cycle, URL — what it confirmed>
- Baseline delta: <none | describe override>
- Notes: <wrinkles, primary-order differences, conflicts>
```

---

## Batch 1

### CA — California (FIPS 06) — GRADE A
- Authority: Cal. Elections Code §13109 (Stats. 2002 ch. 784),
  https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=13109.&lawCode=ELEC
  (accessed 2026-08-16; verification pass got full text from plain curl w/
  browser UA — no browser needed despite the JSF frontend). Related:
  §§13109.5, 13109.7–13109.10 (LA County alternate order), §13111 (rotation
  by Assembly district), §13112 (randomized-alphabet drawing), same URL
  pattern.
- Office order: §13109 "Beginning in the column to the left": (a) President/VP
  → (b) President (primary rows) → (c) STATE: Governor → Lt. Gov → SOS →
  Controller → Treasurer → AG → Insurance Commissioner → Board of
  Equalization member → (d) US SENATOR → (e) US REPRESENTATIVE → (f) State
  Senator → (g) Assembly Member → (h) county committee → (i) JUDICIAL →
  (j) SCHOOL → (k) COUNTY → (l) CITY → (m) DISTRICT → (n) MEASURES. Statewide
  executives ABOVE US Senate and US House. Superintendent of Public
  Instruction is NOT in the state block — it heads (j) SCHOOL.
- Judicial: separate MID-BALLOT block, §13109(i), after Assembly/county
  committee and BEFORE school/county/city/district: Chief Justice → Associate
  Justice Supreme Court → Presiding Justice Court of Appeal → Associate
  Justice Court of Appeal → Superior Court judge → Marshal. Appellate
  retention YES/NO questions print inside this block, ahead of Superior Court
  races.
- Measures: last. §13109(n): "ballot measures in the order, state through
  district shown above, and within each jurisdiction, in the order prescribed
  by the official certifying them"; §13109(o) mandatory: "state measures
  shall always precede local measures."
- County discretion: partial, explicit — §13109(o): official "may vary the
  order of subdivisions (j), (k), (l), (m), and (n) as well as the order of
  offices within these subdivisions", but Superintendent of Public
  Instruction always precedes school/county/city offices and state measures
  always precede local. Subdivisions (a)–(i) (president → judicial) NOT
  reorderable. §13109.5: county-committee block movable. §§13109.7–13109.10:
  Los Angeles County alone gets a wholly different order (Notes).
- School/special: SCHOOL = (j), right after judicial, BEFORE county and city —
  Supt. of Public Instruction → county supt. → county board → college →
  unified → high school → elementary districts. Special districts = (m),
  alphabetical by district name.
- Corroboration: (1) San Bernardino County 2022 general (cycle 2022 —
  statewide executives only run in gubernatorial years, 2024 can't test
  Governor placement), official VIG ballot facsimile,
  https://uploads.rov.sbcounty.gov/rov/Elections/2022/1108/VIG-CMBEN.pdf —
  exact §13109 match (STATE OFFICES → US Senator full then short term → US
  Rep → State Senator → Assembly → Judicial YES/NO → School w/ Supt. first →
  County → City → District → Measures, state first). (2) Los Angeles County
  2024 general facsimile (style 29H34-NP-EN,
  https://mrca.ca.gov/wp-content/uploads/2024/09/Ballot-Measure-H.pdf — ONE
  page, 3 of 6: corroborates local-first + Superior Court inside COUNTY, but
  NOT the federal-last tail, which rests on §13109.8 text) + LA 2022 sample
  booklet
  (https://cdnsm5-hosted.civiclive.com/UserFiles/Servers/Server_212309/File/2022%20General%20Election%20Sample%20Ballot.pdf,
  facsimile pp. 17–21 — opens with NATIONAL ELECTION/US Senate because of
  the §13109.10 consolidated-special override, then CITY/LOCAL → COUNTY →
  STATE → STATE JUDICIAL) — both confirm LA on the ALTERNATE order. No
  statute-vs-sample conflict; each ballot matches the statute governing it.
- Baseline delta: REAL, substantial — (1) statewide executives BEFORE US
  Senate/House; (2) executive internal order has Controller before Treasurer,
  both before AG; (3) Supt. of Public Instruction heads SCHOOL, not a
  statewide-executive slot; (4) judicial mid-ballot (after legislature,
  before school/county/city), not late; (5) SCHOOL precedes county and city
  (baseline: county → municipal → school); (6) Board of Equalization has no
  baseline slot. Measures-last + state-before-local match.
- Notes: LA COUNTY CARVE-OUT (FIPS 06037) — §13109.8 alternate order is
  essentially the baseline inverted: CITY/LOCAL (incl. State Senate/Assembly/
  US House + city measures) → DISTRICT → COUNTY (incl. Superior Court judges
  + county measures) → STATE (executives + Supt. + state measures) → STATE
  JUDICIAL (appellate retention) → NATIONAL (President, US Senate) — local
  first, federal LAST, measures interleaved per jurisdiction. §13109.10 puts
  consolidated special+regular contests for the same office first (LA Nov
  2022 did exactly this with US Senate full+short). Pilot §13109.7
  self-repeals, but §13109.9 is permanent and DISCRETIONARY ("may use the
  alternate ballot order") — LA still used it Nov 2024; treat as standing
  county-option, revertible without statutory change. IMPLEMENTATION SCOPE:
  the planned override is keyed on state_fips only, so it cannot distinguish
  LA from the rest of "06" — the CA override therefore encodes the §13109
  statewide order ONLY, and LA is EXCLUDED from the first implementation
  (accepted limitation: LA voters' printed ballots differ; being a
  discretionary county option it could never be grade-A statewide anyway;
  county-scoped placement is ballot-facsimile Phase 3's territory).
  Primaries: same list +
  (b)/(h) blocks; §13109.5 committee move. Rotation (informational): §13112
  = the SOS randomized-alphabet drawing; the Assembly-district rotation
  itself is §13111 ("the name appearing first in the last preceding Assembly
  district shall be placed last") — within-contest name order only.

### TX — Texas (FIPS 48) — GRADE A
- Authority: Tex. Elec. Code §52.092 (office order), §52.093 (non-county
  subdivisions), §52.072(c)–(d) (propositions), §52.094–.095 (name/proposition
  order), §274.004 (constitutional amendments), §2.053(c) (unopposed block) —
  https://statutes.capitol.texas.gov/?tab=1&code=EL&chapter=EL.52&artSec= and
  https://statutes.capitol.texas.gov/?tab=1&code=EL&chapter=EL.274&artSec=
  (accessed 2026-08-16). Site is an Angular SPA — every path incl.
  /Docs/EL/htm/EL.52.htm and the PDF route serves only the shell to a plain
  fetch; text read from the rendered DOM in a browser. SOS corroboration
  (static): https://www.sos.texas.gov/elections/laws/advisory2024-25.shtml —
  "The order of statewide, district, county, and precinct offices on the
  ballot and the order of parties on the ballot are governed by Sections
  52.091 and 52.092 of the Election Code."
- Office order: §52.092(a): "(1) offices of the federal government; (2)
  offices of the state government: (A) statewide offices; (B) district
  offices; (3) offices of the county government: (A) county offices; (B)
  precinct offices." Federal (b): president/VP → US senator → US rep.
  Statewide (c): governor → lt. governor → AG → comptroller → land
  commissioner → agriculture → railroad commissioner → chief justice supreme
  court → justice supreme court → presiding judge CCA → judge CCA. District
  (d): SBOE → state senator → state rep → chief justice court of appeals →
  justice court of appeals → district judge → criminal district judge →
  family district judge → district attorney → criminal district attorney.
  County (e): county judge → county court at law → county criminal court →
  county probate court → county attorney → district clerk → district+county
  clerk → county clerk → sheriff → sheriff/tax assessor-collector → tax
  assessor-collector → treasurer → county school trustee (county ≥3.3M pop) →
  surveyor. Precinct (f): county commissioner → JP → constable. Then §52.093
  subdivision offices, then measures, then — CONDITIONALLY, only when a
  county uses the §2.053 declaration — the unopposed-declared-elected block
  dead last (informational, no votes cast). Effective sequence: contested
  offices → measures → unopposed block.
- Judicial: within-level, confirmed — NOT late block. Supreme Court + CCA =
  tail of statewide block; appeals + district judges inside district block;
  county courts LEAD the county block; JP/constable = precinct (last partisan
  contests). No retention mechanism — all partisan (party label under every
  judicial candidate on Harris 2024). §52.092(j): multicounty statutory county
  court judge listed as county office.
- Measures: last among VOTED items, after offices — §52.072(c): "each
  proposition stating a measure shall appear on the ballot after the listing
  of offices"; the conditional §2.053(c) unopposed block can still print
  after the measures (see Office order). Carve-outs:
  §52.072(d) contingent-office proposition prints BEFORE offices; §274.004:
  "A proposed constitutional amendment must be placed on the ballot before all
  other propositions" (internal order = SOS public drawing, §274.002(d)–(e)).
  Otherwise §52.095(a): ordering authority decides. Joint-election practice:
  "after the listing of offices" applies PER SUBDIVISION — each subdivision's
  section carries its own offices + its own props (Harris 2024).
- County discretion: partial — zero over the §52.092 partisan sequence
  (§52.092(i): SOS designates new offices' position); §52.093 delegates
  non-county-subdivision office order to "the authority ordering the
  election"; §52.095(a) same for propositions. §52.002: ballot prep split
  county clerk / city secretary / subdivision secretary — no single owner of a
  joint ballot.
- School/special: three-way split — SBOE FIRST in district block
  (§52.092(d)(1)); county school trustee a county office in ≥3.3M counties
  (§52.092(e)(13)); all other ISD/special-district offices = §52.093 tail
  after the partisan block, self-ordered. Harris 2024 practice: countywide
  special district → cities alphabetical → ISDs alphabetical → MUDs.
- Corroboration: Harris County 2024 general composite "Yellow" sample (19pp),
  https://files.harrisvotes.com/harrisvotes/prd/SampleBallot/1124_Yellow%20Sample%20Ballot_EN_SP.pdf?sv=2017-04-17&sr=b&si=DNNFileManagerPolicy&sig=6Bq4UIVFC6I7HCE5GZmLGvvHVYyRKrKrZH/uKs72o2o%3D
  — exact §52.092 match top to bottom (President → … → Constable Pcts 1–8 →
  Flood Control Prop A → cities alphabetical → Lone Star College District →
  ISDs alphabetical → MUD bond). Plus Sabine County 2022
  (https://www.co.sabine.tx.us/upload/page/3469/SB%20PCT%204.pdf, cycle
  2022) exercising the executive block: Governor → Lt. Gov → AG → Comptroller
  → Land Comm → Agriculture → Railroad Comm → Supreme Court → CCA → State
  Senator, exact §52.092(c)–(d) match. No statute-vs-sample conflict.
- Baseline delta: REAL, many — (1) judicial within-level (statewide courts
  after railroad commissioner, appeals/district judges after state house);
  (2) county courts lead the county block; (3) JP/constable last partisan;
  (4) SBOE before state senate/house; (5) DAs at end of district block;
  (6) county school trustee inside county block (big counties); (7)
  municipal/school §52.093-delegated, local props interleave per subdivision
  rather than pooling at the end; (8) amendments ahead of other props,
  contingent props ahead of offices.
- Notes: §52.092(a) opens "Except as provided by Section 2.053(c)" —
  unopposed-declared-elected offices list separately after measures, "grouped
  in the same relative order prescribed for the ballot generally", no votes
  cast. §52.092(g): place numbers part of title, numerical order. Primary
  uses the same §52.092 sequence (keys on "offices regularly filled at the
  general election"); name order drawn by county party chair (§52.002(2)).
  Name order (informational): party columns per §52.091; drawings per
  §52.094(a); no precinct rotation.

### FL — Florida (FIPS 12) — GRADE A
- Authority: Fla. Admin. Code R. 1S-2.032(7) "Uniform Design for Election
  Ballots" (eff. 4-23-2020), index https://flrules.org/gateway/ruleNo.asp?id=1S-2.032
  (operative text only in the linked .doc:
  https://flrules.org/gateway/readFile.asp?sid=0&tid=23116863&type=1&file=1S-2.032.doc
  — the rule page itself is an index shell); statutory base Fla. Stat.
  §101.151(2)(a), https://www.flsenate.gov/Laws/Statutes/2024/101.151, and
  §105.041(1), https://www.flsenate.gov/Laws/Statutes/2024/105.041 (all accessed
  2026-08-16)
- Office order: Rule 1S-2.032(7): "the ballot shall list the contests in the
  order specified in Sections 101.151 and 105.041, F.S." — (7)(a) partisan:
  Federal → State → County → Municipal → District/special district → Party
  offices. §101.151(2)(a) internal sequence: 1. President/VP; 2. US Senator and
  US Representative; 3. Governor+Lt. Gov (joint), AG, CFO, Commissioner of
  Agriculture, State Attorney, Public Defender; 4. State Senator and State
  Representative; 5. Clerk of Circuit Court, Sheriff, Property Appraiser, Tax
  Collector, District Superintendent of Schools, Supervisor of Elections;
  6. County Commission + other county/district offices "in the order fixed by
  the Department of State". (Item 5 also includes "Clerk of the County
  Court, when authorized by law".) §101.151(2)(d): offices not involved are
  omitted, the rest keep the named order.
- Judicial: separate NONPARTISAN section that judicial LEADS — not a
  school-after block. §105.041(1): nonpartisan candidates and "justices and
  judges seeking retention to office shall be grouped together on a separate
  portion of the general election ballot". Rule 1S-2.032(7)(b) orders it:
  Supreme Court (retention) → DCA (retention) → Circuit Judge → County Judge →
  nonpartisan county office → School Board → nonpartisan municipal →
  nonpartisan district/special district. Retention prints at the head of the
  same block, high→low court.
- Measures: last. Rule 1S-2.032(7)(d): statewide amendment/measure → county →
  municipal → special district.
- County discretion: partial, narrow — residual county/district offices ordered
  by the Department of State, not the supervisor; 1S-2.032(14) allows
  deviation only for six enumerated formatting/space reasons plus a catch-all
  for "extraordinary circumstances which cannot reasonably be accommodated
  except by deviation". Local variation is packaging (e.g. Miami-Dade
  separate municipal ballot), not sequence.
- School/special: School Board = item 6 of the nonpartisan block — AFTER
  judicial and nonpartisan county offices. Partisan district/special offices
  tail the partisan block after municipal.
- Corroboration: Miami-Dade County, 2024 general,
  https://www.miamidade.gov/elections/library/sample-ballots/2024-11-05-general-election-sample-ballot.pdf
  — observed: President → US Senator → US House → State Senator → State Rep →
  Clerk → Sheriff → Property Appraiser → Tax Collector → Supervisor of
  Elections, then nonpartisan: Supreme Court retention → 3rd DCA retention →
  County Judge → County Commissioner (nonpartisan by charter) → School Board,
  then Amendments 1–6 last. Matches (7)(a)→(b)→(d) exactly, incl.
  school-below-judicial. No statute-vs-sample conflict.
- Baseline delta: REAL — (1) whole nonpartisan judicial section (retention +
  trial) prints BEFORE school board and nonpartisan municipal (baseline has
  school 80 before judicial 82–90); (2) School Board itself precedes
  nonpartisan MUNICIPAL offices — rule (7)(b) item 6 vs items 7–8 — inverting
  the baseline's municipal-before-school within the nonpartisan section;
  (3) partisan municipal precedes district/special-district offices.
  Federal→state→county spine, Governor after US House,
  supreme→appeals→trial, measures-last all match baseline.
- Notes: 2024 = no governor race, so statewide-executive internal order rests
  on §101.151(2)(a)3. text alone. Primaries differ: §105.041(1) separate
  portion/ballot; §101.151(4)(a) alphabetical surnames. Name order within a
  general contest (informational): NO rotation — §101.151(3)(a) seats the
  last-gubernatorial-winner's party first. Design forms DS-DE 200–208
  incorporated by reference, not fetched.

### NY — New York (FIPS 36) — GRADE C (conflict)
- Authority: NY Election Law §7-104(11)(a)–(b) (L.2019 c.411, "Voter Friendly
  Ballot Act"), https://www.nysenate.gov/legislation/laws/ELN/7-104; proposals
  §7-110, https://www.nysenate.gov/legislation/laws/ELN/7-110; party rows
  §7-116, https://www.nysenate.gov/legislation/laws/ELN/7-116; NY Const. art.
  VI §2(e) (Court of Appeals appointed) + §6(c) (Supreme Court elected);
  Education Law §2002 (May school elections) (all accessed 2026-08-16)
- Office order: §7-104(11)(a) fixed list: President/VP electors → Governor+Lt.
  Gov (joint, Gov first) → Comptroller → AG → US Senator → US Rep → State
  Senator → Assembly Member; closing sentence: "Any office which is not listed
  in this paragraph shall not appear on the ballot in a position before or
  ahead of an office which is listed." (11)(b): everything else "in the
  customary order", partisan before nonpartisan, judicial after all other
  partisan offices. Statewide executives ABOVE both congressional offices.
- Judicial: statute = late-but-not-last (after all partisan, before
  nonpartisan). REAL BALLOTS DISAGREE: Supreme Court Justice prints THIRD,
  right after US Senator, in both counties checked (Suffolk also County +
  Family Court there), ahead of Congress/legislature. Court of Appeals
  appointed — never a contest. No retention.
- Measures: last, separate section, normally ballot BACK — §7-110 "Ballot
  proposals shall appear on the ballot in a separate section or on a separate
  sheet or card"; §7-104(16) front must say turn over. Practice numbers the
  statewide amendment first (Suffolk 2024 Proposal One = state ERA, Two =
  county charter).
- County discretion: partial — the eight (11)(a) offices fixed; the rest is
  "customary order" chosen by county boards subject to
  partisan-before-nonpartisan + judicial-after-partisan. 9 NYCRR 6210.7 =
  print specs only, no sequence.
- School/special: almost never on November ballot (Education Law §2002 = May).
  Exception: Big-5 charters — Buffalo Board of Education at-large printed on
  the Erie 2024 general, nonpartisan, after every partisan contest; in the
  ballot's bottom band it sits RIGHT of the state amendment and the county
  proposal (verifier-checked left-to-right: Proposal One → County Proposal
  No. One → school board). NYC: no elected board.
- Corroboration: Suffolk County 2024 general official sample booklet (80pp),
  https://apps2.suffolkcountyny.gov/boe/documents/2024General-SampleBallotBooklet.pdf
  — verified on all 39 ballot faces: President/VP → US Senator → Supreme
  Court Justice 10th JD (vote for up to 8) → County Court → Family Court →
  US Rep → State Senator → Assembly; some styles then append District Court
  Judge / Town Justice / Council member AFTER Assembly (trailing local tail,
  not all-judicial-up-top); proposals on reverse (One = ERA, Two = Suffolk
  charter; two East Hampton backs carry a third). Erie County 2024 — single
  Buffalo style BFLO ELL 002, vendor-mirrored,
  https://townsquare.media/site/11/files/2024/11/attachment-Erie-County-NY-Sample-Ballot.pdf:
  President → US Senator → Supreme Court Justice (no district printed on the
  header) → US Rep → State Senator → Assembly → Family Court → DA → Buffalo
  City Court; bottom band left-to-right = Proposal One (state amendment) →
  County Proposal No. One (Erie charter) → Buffalo Board of Education
  Member-at-Large.
- Baseline delta: statute would give (1) statewide executives above Congress
  and (2) judicial late-but-before-nonpartisan; PRACTICE puts judicial-district
  courts 3rd. Measures-last matches baseline WITH a Big-5 qualifier: when a
  Buffalo school-board contest is present (Erie style above), the nonpartisan
  school contest prints to the RIGHT of the proposals, so the proposal
  section is not final on that ballot (single-style observation; Suffolk had
  nothing after its reverse-side proposals).
- Grade rationale: C — explicit statewide statute vs two large-county 2024
  ballots in direct conflict on judicial placement (printing Supreme Court
  third violates BOTH (11)(b) and (11)(a)'s closing sentence; looks like a
  size-of-constituency "customary order" habit surviving the 2019 act);
  recorded both ways, no override until resolved. The federal/state spine,
  partisan-before-nonpartisan, and measures-last legs are corroborated; the
  Governor-before-US-Senate leg is statute-only (2024 not a gubernatorial
  year; Suffolk 2022 booklet 404s). Verification pass 2026-08-16 re-fetched
  every URL and re-read both ballots (39 Suffolk faces).
- Notes: grid ballots — party rows lettered by last gubernatorial vote
  (§7-116(1)); office sequence is the perpendicular axis. Fusion voting:
  same candidate on multiple party rows, informational only. Name order: no
  rotation; §7-116(2) lot on demand within party group. Primaries: §7-104(11)
  precedence still governs; within-party placement by lot (9 NYCRR 6204.3).

### PA — Pennsylvania (FIPS 42) — GRADE B
- Authority: PA Election Code (Act of June 3, 1937, P.L. 1333) §1003 "Form of
  Official Election Ballot" (25 P.S. §2963) (a)/(b)/(g) — official text only
  via the iframe route
  https://www.palegis.us/statutes/unconsolidated/law-information/view-statute?51&iFrame=true&txtType=HTM&SessYr=1937&SessInd=0&ActNum=0320.&chpt=10&subchpt=000.&sctn=3&subsctn=000.
  (accessed 2026-08-16; the human-facing wrapper is a script-rendered empty
  shell, and legis.state.pa.us ↔ palegis.us redirect-loop on per-section
  URLs). Word-identical mirror:
  https://codes.findlaw.com/pa/title-25-ps-elections-electoral-districts/pa-st-sect-25-2963/.
  §1109-A (25 P.S. §3031.9) covers electronic layout and imports the paper
  order: ballot labels "shall, as far as practicable, be in the same order or
  arrangement as provided for paper ballots." No Dept. of State ordering
  directive located.
- Office order: prescribed only by §1003(a)'s statutory SPECIMEN ("shall be in
  substantially the following form"): Presidential Electors → United States
  Senator → Governor → Representatives in Congress → Senator in the General
  Assembly. So Governor/statewide executives BEFORE US House. No enumerated
  internal order for AG/Auditor General/Treasurer; practice (2024 sample) =
  AG → Auditor General → Treasurer between US Senate and US House. Gov+Lt.Gov
  joint ticket. Nothing below state senate is statute-ordered.
- Judicial: normally ABSENT from the even-year general entirely. Pa. Const.
  art. V §13(a): judges "shall be elected at the municipal election" =
  odd-year November. Retention constitutionally segregated, art. V §15(b):
  "on a separate judicial ballot or in a separate column on voting machines".
  Odd-year internal placement not statute-fixed; observed practice appellate
  contests top, retention near the end — NOT sample-verified here (county
  archives only served GE24).
- Measures: last. §1003(g): "each amendment or other question so submitted
  may be printed upon the ballot below the groups of candidates … and, when
  required by law, shall be so printed." No placement distinction amendments
  vs local questions.
- County discretion: partial — counties print ballots (25 P.S. §2642(c)) and
  everything below the specimen's five office types (county row, municipal,
  school, judicial blocks) is de facto county/vendor practice, not statute.
- School/special: school directors = odd-year municipal (24 P.S. §3-303,
  verified verbatim: "At each municipal election, three school directors,
  except as otherwise provided in this act, shall be elected at large for
  terms of six (6) years"); never on the even-year general.
- Corroboration: Montgomery County 2024 general, Abington W1 P1 official
  sample,
  https://webapp07.montcopa.org/voterservices/sampleballots/Sample%20Ballots/Sample%20Ballots%20GE24/Abington/Abington%201-1%20-%20Official.pdf
  (archive index is POST-only; text extracts cleanly). Observed: Presidential
  Electors → US Senator → AG → Auditor General → State Treasurer → US Rep
  4th → State Rep 153rd. Matches the specimen; no conflict. Layout caveat:
  AG sits below US Senator in column 2 while Auditor General tops column 3 —
  the AG → Auditor order depends on column-major reading.
- Baseline delta: REAL — (1) statewide executives BEFORE US House; (2) NO
  judicial on even-year generals (odd-year municipal only), so the baseline
  judicial block is simply empty; (3) no county/municipal/school contests
  either (odd-year); (4) measures last matches.
- Notes: why B not A — no statute says "offices shall be arranged in the
  following order"; the ordering evidence is an illustrative specimen
  ("substantially the following form") stopping at state senate. Top-of-ballot
  leg alone would be A; full-ballot rule set alone would be C; B is the
  honest composite. Act 77 (2019) killed straight-party in practice; §1003(f)
  party-column text unrepealed. Name order (informational): no rotation —
  §1003(b) party order by last gubernatorial vote, primary order by lot
  (§§915–916). Odd-year municipal ballots differ substantially (judicial +
  retention + county row + school).

### IL — Illinois (FIPS 17) — GRADE A
- Authority: 10 ILCS 5/16-6 (amendments placement),
  https://www.ilga.gov/Documents/legislation/ilcs/documents/001000050K16-6.htm;
  10 ILCS 5/16-6.1 (separate green retention ballot),
  https://www.ilga.gov/Documents/legislation/ilcs/documents/001000050K16-6.1.htm;
  10 ILCS 5/16-3 (ballot form/column order),
  https://www.ilga.gov/Documents/legislation/ilcs/documents/001000050K16-3.htm;
  10 ILCS 5/16-7 (local questions, separate colored ballots),
  https://www.ilga.gov/Documents/legislation/ilcs/documents/001000050K16-7.htm;
  IL State Board of Elections 2026 Ballot Preparation Guide p.13 "Gubernatorial
  Order of Offices",
  https://www.elections.il.gov/agencyforms/1%20BALLOT%20PREPARATION%20GUIDE%20TESTING%20AND%20SECURITY%20FOR%20VOTING%20SYSTEMS/2026%20Ballot%20Preparation%20Guide.pdf
  (accessed 2026-08-16; verification pass fetched both PDFs plain, HTTP 200 —
  no User-Agent trick needed). Authority caveat: the guide's Preface
  self-describes as "information and suggested guidelines" — the office-
  category sequence is official SBE guidance (SOS-manual-class authority per
  this plan), not statute; the measures-first, retention-ballot, and
  SBE-certification legs ARE statutory (16-6, 16-6.1, 16-3).
- Office order: SBE guide p.13 verbatim list: Constitutional Questions →
  Statewide Advisory Questions → US Senator → Governor and Lt. Governor
  (bracketed joint) → State Officer(s) ("As certified by the State Board of
  Elections") → US Representative → State Senator → State Representative →
  Sanitary District Trustees → County Offices (incl. Educational Service
  Region superintendent) → Judicial Officers → Judicial Retention → Local
  Public Questions. NOTE this is a COMPOSITE logical sequence across
  physically separate ballots: the CANDIDATE ballot ends at Judicial
  Officers; Judicial Retention is its own green ballot (16-6.1) and each
  local public question its own distinctly-colored ballot (16-7) — neither
  holds a position ON the candidate ballot; the guide's tail order is how
  the set is conventionally presented (and how our single-list model should
  order them). Governor BEFORE US House; US Senate above Governor.
  Statewide-executive internal order not enumerated for the general — deferred
  to SBE certification (guide's nomination list runs Gov/Lt.Gov, AG, SOS,
  Comptroller, Treasurer, matching Ill. Const. art. V). President/VP heads
  candidate section in presidential years.
- Judicial: late block — after county offices, grouped Supreme → Appellate →
  Circuit → Subcircuit. Retention = wholly SEPARATE ballot, 16-6.1: "shall be
  separate from all other ballots voted on at the general election", green
  paper ("No other ballot at the same election shall be green in color");
  Supreme retention first group, Appellate second, circuit last; seniority
  order within group.
- Measures: SPLIT — statewide FIRST, local LAST. 16-6: amendments "shall be
  printed at the top of the 'Official Ballot' preceding the names of
  candidates"; convention propositions outrank amendments; statewide advisory
  questions follow amendments, still ahead of candidates. Local questions =
  separate distinctly-colored ballots (16-7), conventionally presented dead
  last after retention "by the order of initiation" — that phrase and the
  last-position are the SBE guide's (the guide only "recommends" public
  questions follow retention); 16-7 itself assigns no position, only the
  separate ballot.
- County discretion: partial — 16-3 lets election authorities set party-COLUMN
  order, but candidates print "in the order certified by the State Board of
  Elections"; party placement via post-primary lottery (10 ILCS 5/7-60).
  Office-category order not county-discretionary.
- School/special: sanitary/water-reclamation trustees BEFORE county offices
  (own slot). Regular school boards elect at April consolidated election, not
  November; exception Chicago Board of Education (School Code art. 33) printed
  LAST on the candidate ballot, after judicial.
- Corroboration: Chicago (Cook County) 2024 general specimen, Style 230 Ward
  32 Pct 03, https://ballots.chicagoelections.gov/cboeballots61/1_3203_230_SP.pdf
  — 3 statewide advisory questions → President/VP → US Rep 5th → State Rep
  11th → Water Reclamation Commissioners → Clerk of Circuit Court → State's
  Attorney → County Clerk → Supreme → Appellate ×4 → Circuit ×11 → Subcircuit
  → Chicago Board of Education 4th; separate retention card labeled "Ballot
  Style: 230 B". Confirms measures-first, sanitary-before-county, judicial
  late block, retention separate. No conflict. "President/VP heads the
  candidate section in presidential years" is sample-only — the guide's p.13
  list never mentions Presidential (it's the gubernatorial-year list).
- Baseline delta: REAL, four — (1) statewide measures FIRST not last (local
  referenda still last); (2) Governor + statewide executives BEFORE US House;
  (3) sanitary district trustees before county offices; (4) retention =
  separate ballot, and Chicago school board prints AFTER judicial (inverts
  baseline school-then-judicial).
- Notes: "blue ballot" folklore stale — 16-6 now requires amendments on
  "plain white paper"; 5 ILCS 20/2 still says separate ballot = stale
  cross-reference, 16-6 controls and the Chicago sample follows it. Primary
  order differs (10 ILCS 5/7-19, per-party ballots). Name order
  (informational): no rotation; post-primary party lottery fixes columns.

### OH — Ohio (FIPS 39) — GRADE A
- Authority: RC 3505.03(C) (office order),
  https://codes.ohio.gov/ohio-revised-code/section-3505.03; RC 3505.04
  (nonpartisan ballot), https://codes.ohio.gov/ohio-revised-code/section-3505.04;
  RC 3505.06(B)(1),(F)(1) (questions/issues),
  https://codes.ohio.gov/ohio-revised-code/section-3505.06; Ohio Const. art.
  IV §6 (judges elected); SOS Election Official Manual Ch. 5 (Directive
  2025-37),
  https://www.ohiosos.gov/globalassets/elections/directives/2025/eom/dir2025-37-ch05.pdf;
  SOS Directive 2024-25 prescribed sample ballot,
  https://www.ohiosos.gov/globalassets/elections/directives/2024/dir2024-25_sampleballot.pdf
  (all accessed 2026-08-16; ohiosos.gov behind a maintenance page — both PDFs
  read from Wayback captures of those exact URLs, 20250822140504 +
  20250305034715; codes.ohio.gov live).
- Office order: RC 3505.03(C) verbatim: "governor and lieutenant governor,
  attorney general, auditor of state, secretary of state, treasurer of state,
  chief justice of the supreme court, justice of the supreme court, United
  States senator, representative to congress, state senator, state
  representative, judge of a court of appeals, county commissioner, county
  auditor, prosecuting attorney, clerk of the court of common pleas, sheriff,
  county recorder, county treasurer, county engineer, and coroner."
  President/VP heads the ballot above that enumeration. FEDERAL SPLIT: US
  Senator + US Rep print BELOW the statewide executives and BELOW the Supreme
  Court block. Gov+Lt.Gov joint vote.
- Judicial: split across THREE places, not one late block — (a) Supreme Court
  (chief + justices) on the PARTISAN ballot mid-sequence, after treasurer of
  state, BEFORE US Senator, with party labels (SB 80, eff. 2021-09-30);
  (b) Court of Appeals partisan, between state representative and county
  commissioner; (c) only common pleas / county court / municipal court are
  nonpartisan and form the late block after every partisan office, §3505.04:
  "county judicial offices shall be listed first on the ballot, followed by
  municipal and township offices, and by offices of member of a board of
  education, in the order stated." Seats of one court sort by term start,
  full before unexpired (EOM §5.03, RC 3505.03(D)). No retention — art. IV §6
  elects every judge.
- Measures: last — EOM §5.04: "Put candidate contests on the ballot first,
  followed by questions and issues." §3505.06(B)(1): "State questions and
  issues shall always appear as the top group"; the FOUR LOCAL GROUPS ROTATE
  one place per calendar year (statute quoted in entry source; EOM publishes
  the cycle — 2026/2030 = state, school+other, county, municipal, township).
  County-vs-municipal-vs-school measure order is year-dependent, not fixed.
- County discretion: partial, narrow — office order prescribed + certified by
  SOS (identical clause in 3505.03(C) and 3505.04); boards choose only "the
  sequence for specific questions within each group" (EOM Ch. 5).
- School/special: LOCAL board of education = last in the nonpartisan tail,
  after municipal and township (§3505.04, odd-year in practice). STATE Board
  of Education differs: not in the 3505.03(C) enumeration; SOS prescribed
  form places it after Coroner, BEFORE Judge of Common Pleas — head of the
  nonpartisan block, ahead of trial judicial. Special districts: no general-
  election candidate contests; as issues they ride the "school or other
  district" group.
- Corroboration: Hamilton County 2024 general ballot proof,
  https://votehamiltoncountyohio.gov/wp-content/uploads/2024/09/BALLOTPROOF-0_Style25_English_8.5x14_Paper.pdf
  — President/VP → Supreme Court ×3 (party labels) → US Senator → US Rep →
  State Senator → State Rep → Court of Appeals ×4 (party labels) → county
  offices → Common Pleas ×2 (no labels) → Issue 1 (state) → city → village →
  township → school → special → county issues LAST → END OF BALLOT; matches
  the offices Directive 2024-25's form covers (the form also shows State BOE
  and County Court, absent from Hamilton's 2024 contests) and independently
  confirms the §3505.06 rotation (2024 slot = county last).
  Judicial-before-school untestable on even-year ballots
  (municipal/township/school are odd-year) — tested on Hamilton County Nov 4
  2025 proof,
  https://votehamiltoncountyohio.gov/wp-content/uploads/2025/09/G25-BALLOT-PROOF.pdf:
  PARTISAN municipal offices (Cheviot, Norwood, Reading… w/ party labels)
  print FIRST per 3505.03, then the nonpartisan ballot opens with Municipal
  Court Dists 1–7 → nonpartisan mayors/councils (Cincinnati, Blue Ash…) →
  township trustees → Boards of Education (city/exempted-village districts →
  Hamilton County ESC mid-block → local districts) → issues. So judicial
  leads the NONPARTISAN tail only; partisan municipal precedes it, and the
  ESC board is mid-school-block, not last. No statute/sample conflict.
- Baseline delta: REAL, large — (1) Supreme Court mid-partisan-ballot between
  executives and US Senate (NOT late block); (2) Court of Appeals between
  state rep and county offices (NOT late block); (3) US Senate/House pushed
  below state executives + Supreme Court (top of baseline inverted); (4) only
  trial judicial forms the late block, ahead of NONPARTISAN
  municipal/township/school (partisan municipal offices still print before
  it, on the partisan ballot);
  (5) local measure groups rotate annually (no fixed state→local nesting
  below the state group); (6) State BOE ahead of trial judiciary, not at the
  school tail. Holds: measures last, county above municipal, judicial-then-
  school within the nonpartisan tail.
- Notes: SUPERSEDES the earlier seed reading — the seed generalized §3505.04
  to all judicial; supreme/appeals actually sit in the partisan sequence per
  §3505.03(C). Even/odd-year split is the biggest practical wrinkle: even
  years = federal/state/county + supreme/appeals + common pleas + state BOE +
  issues; odd years = municipal/township/municipal-court/school. No printed
  section headers in practice — one continuous ballot, partisan/nonpartisan
  boundary visible only via party labels (statute-vs-practice divergence on
  labeling only). SOS 2024 form footer: "This SAMPLE ballot provides the
  CORRECT TITLES and ORDER OF OFFICES for ballot layout." Rotation
  (informational): §3505.03(E)(2) rotates candidate names by precinct, never
  contest order. Primaries: per-party ballots + separate issues ballot.
  Failed sources for the record: Franklin lookup-only, Cuyahoga
  script-rendered, lookup.boe.ohio.gov Cloudflare-gated.

### GA — Georgia (FIPS 13) — GRADE B
- Authority: NO statute or SEB rule prescribes a federal→state→local sequence.
  Chain: Ga. Comp. R. & Regs. 183-1-12-.07 ("in the order specified in
  O.C.G.A. §§ 21-2-379.4 and 21-2-379.5"), https://rules.sos.ga.gov/gac/183-1-12
  (official SOS rules site) → §21-2-379.4(b) ("conform as nearly as
  practicable") → §21-2-285(c) — chain terminates without fixing office
  sequence. BMD arrangement delegated: §21-2-379.23(b) "The form and
  arrangement of ballots marked and printed by an electronic ballot marker
  shall be prescribed by the Secretary of State." Placement rules that DO
  exist: §21-2-285.1 (nonpartisan last), §21-2-138 (judicial on primary
  date), §21-2-285(f)/§21-2-379.5(f) (measures after candidates). (accessed
  2026-08-16.) Official O.C.G.A. = LexisNexis portal, fully script-rendered
  (non-JS fetch → tracking pixel); Justia 403; text read from FindLaw/onecle
  mirrors (unofficial, verbatim), e.g.
  https://law.onecle.com/georgia/title-21/21-2-138.html,
  https://codes.findlaw.com/ga/title-21-elections/ga-code-sect-21-2-285-1.html
- Office order (printed practice, no statute): US Senate (or President/VP in
  presidential years) → Governor → Lt. Gov → SOS → AG → Agriculture →
  Insurance → School Superintendent → Labor → [PSC] → US HOUSE → State Senate
  → State House → county offices (DA/Solicitor, Clerk of Superior Court,
  Sheriff, Tax Commissioner, Surveyor, Commission) → Soil & Water supervisor →
  measures. US House AFTER all eight statewide executives — but only the two
  2022 midterm ballots (plus the May 2022 primary) can test that: Fulton 2024
  is a presidential year with no executives and no US Senate contest, so US
  House prints straight after President/VP there. PSC slot (after Labor,
  before US House) from Richmond May-2022 primary (two PSC contests, D2+D3);
  PSC absent from 2022/2024 generals (Rose v. Raffensperger litigation).
- Judicial: STATE-COURT judicial (Supreme/Appeals/Superior/State Court) not
  on the November ballot — §21-2-138 puts those elections "in a nonpartisan
  election to be held and conducted jointly with the general primary" (May;
  probate/magistrate are NOT in this section). Locally-partisan judicial
  offices CAN still appear in the November county block (Richmond exception
  below). There, §21-2-285.1 lists them last "insofar as
  practicable": "separated from the names of candidates for party nomination
  to other offices by being listed last on each ballot" — statutory caption
  "OFFICIAL NONPARTISAN ELECTION BALLOT"; the printed Richmond ballot's
  caption reads "NONPARTISAN GENERAL ELECTION". Observed May-2022 internal
  order: Supreme ×3 → Court of Appeals ×3 → Superior ×3 → State Court ×2,
  then the section continues with NON-judicial nonpartisan offices (Mayor of
  Augusta, commission seats) — not purely judicial. No retention — all
  contested nonpartisan. Exception: a locally-partisan judicial office sits
  in the November county block (Richmond 2022 "For Chief Judge, Civil and
  Magistrate Court" — ONE contest — between Tax Commissioner and School
  Board, incumbent listed "Democrat").
- Measures: last among the REGULAR election's items — amendments → statewide
  referenda → local questions. §21-2-285(f) "may be printed upon the ballot
  following the groups of candidates" (permissive "may", universally
  followed); §21-2-379.5(f) "below the groups of candidates". Consolidated
  SPECIAL-election contest sets print after the measures (Fulton 2024: two
  municipal specials after Referendum A; Gwinnett 2022: SPLOST special after
  the statewide questions) — measures-last holds within the regular set
  only.
- County discretion: partial on paper (§21-2-284 — note: that section is
  "Form of Official Primary Ballot" and its county/SOS split concerns
  certifying party-question wording, not general-ballot prep), uniform de
  facto — SOS prescribes all BMD ballots statewide (§21-2-379.23(b));
  sequence lives in the statewide ballot-build config, no published document
  located.
- School/special: nonpartisan boards (§21-2-139; the "109 of ~180 districts"
  count is unsourced — treat as approximate) print on the MAY nonpartisan
  ballot AFTER judicial (confirmed Gwinnett + Rockdale May 2024); partisan
  boards ride the November county block (Richmond 2022, after the Chief
  Judge contest, before Soil & Water). Soil & Water supervisor =
  consistently last office before measures.
- Corroboration: four ballots, two cycles, all consistent — Gwinnett Nov 2022
  (https://www.gwinnettcounty.com/static/departments/elections/2022-Election/gwinnett-county-elections-general-and-special-sample-ballot-nov-8-2022-zh-yue.pdf,
  vector outlines, read via page render); Richmond Nov 2022
  (https://www.augustaga.gov/DocumentCenter/View/16238/Composite-Sample-Ballot-2022);
  Richmond May 2022 primary
  (https://www.augustaga.gov/DocumentCenter/View/15805/Sample-Ballot-85x14-REP-2022);
  Fulton Nov 2024
  (https://www.fultoncountyga.gov/-/media/Departments/Registration-and-Elections/Sample-Ballot/2024-Sample-Ballots/November-5-General-Election/Composite-Sample-Ballot--November-5-2024-General-Electionrevised-9232024.pdf
  — zero judicial/school/nonpartisan CONTESTS; incidental text hits like
  "Atlanta Judicial Circuit" in office titles exist). No statute-vs-sample
  conflict; the gap is authority, not agreement.
- Baseline delta: REAL, four — (1) US House after statewide executives
  (federal block split); (2) no STATE-COURT judicial on the November ballot
  (May last block instead; locally-partisan judicial can ride the county
  block); (3) school after judicial on the May ballot / mostly absent in
  November; (4) no municipal tier on even-year November (GA municipal =
  odd-year; consolidated special-election sets print after measures).
  Soil & Water = last office slot. Matches: President/Senate top, state
  senate before house, county after legislature, measures last within the
  regular set.
- Notes: grade B — judicial/nonpartisan/measures legs are
  MIRROR-text+sample-confirmed, but the office sequence itself has no
  authority text; rests on 4 consistent samples + §21-2-379.23(b)
  delegation. SOURCING CAVEAT (plan rule: primary sources only): every
  O.C.G.A. quote in this entry comes from unofficial mirrors
  (FindLaw/onecle) because the official code is behind LexisNexis's script
  portal — mitigated by two independent mirrors agreeing verbatim, by the
  verification pass re-reading both, and by the OFFICIAL SOS rule
  183-1-12-.07 citing the same section numbers; still, treat the statutory
  quotes as pending primary re-verification if O.C.G.A. access opens, and
  do not cite this entry's statute text as primary-verified. Primary
  (one line): single ballot = party primary → party questions → captioned
  nonpartisan section last (§21-2-285.1). Name order (informational): party
  columns by descending last-gubernatorial vote (§21-2-285(c)); alphabetical
  in primaries/specials; no rotation. Open gap: Cobb County (partisan school
  board in November) unverified — sample PDF 403s, site script-rendered.
  Start-hint correction: 183-1-15 = returns/recounts, nothing to do with
  ballot order; operative rule is 183-1-12-.07.

### NC — North Carolina (FIPS 37) — GRADE A
- Authority: NC GS §163-165.6 "Arrangement of official ballots" (a), (b)(1)–(6),
  (h),
  https://www.ncleg.gov/EnactedLegislation/Statutes/HTML/BySection/Chapter_163/GS_163-165.6.html
  — a delegation with guardrails ("The State Board of Elections shall
  promulgate rules prescribing the order of offices"); the operative sequence
  is 08 NCAC 06B .0103 "Arrangement of Official Ballots" (readopted eff.
  2019-06-01, amended eff. 2022-01-01),
  http://reports.oah.state.nc.us/ncac/title%2008%20-%20elections/chapter%2006%20-%20partisan%20elections/subchapter%20b/08%20ncac%2006b%20.0103.html
  (both accessed 2026-08-16, plain HTML; oah.state.nc.us is HTTP-ONLY —
  https on :443 refuses, so a fetcher that force-upgrades will fail)
- Office order: 08 NCAC 06B .0103(b): (1) Federal — President/VP, US Senate,
  US House; (2) State — Governor, Lt. Gov, AG, Auditor, Agriculture,
  Insurance, Labor, SOS, Superintendent of Public Instruction, Treasurer,
  Supreme Court Chief Justice, Supreme Court Associate (by seat), Court of
  Appeals (by seat); (3) District — NC Senate, NC House, Superior Court,
  District Court, District Attorney; (4) partisan county; (5) partisan
  municipal; (6) nonpartisan county; (7) nonpartisan municipal; (8) referenda.
  Within a class: alphabetical by office / numeric by district; full terms
  before partial.
- Judicial: WITHIN-LEVEL, seed confirmed — never a late block. Appellate
  (Supreme → Appeals) tails the STATEWIDE class, after Treasurer and BEFORE
  the General Assembly; trial (Superior → District) tails the DISTRICT class
  after NC House, before county; DA last in class. §163-165.6(b)(4):
  "Judicial offices and district attorney shall be listed, in that order,
  after other offices in the same class." No retention: (b)(6)'s retention
  placement rule is dormant — the 2015 scheme (S.L. 2015-66) was held
  unconstitutional (Faires v. State Bd., 2016) and never used.
- Measures: last. §163-165.6(a): "Candidate ballot items shall be arranged on
  the official ballot before referenda." (h): amendments (chronological,
  labeled only "Constitutional Amendment") → other statewide referenda →
  local referenda. Carve-out in .0103(b)(8): last "unless the voting system
  design requires referenda to be before candidate ballot items".
- County discretion: none over contest order — .0103(a): State Board
  "shall certify to the county boards of elections the order of the offices";
  counties implement only.
- School/special: placement follows PARTISANSHIP not office type — nonpartisan
  school boards (majority) land in class (6) nonpartisan county, AFTER
  partisan municipal; partisan-by-local-act school boards land in class (4).
  Soil & Water supervisor also class (6).
- Corroboration: Wake County 2024 general, style B0140,
  https://www.sunfox.com/wp-content/uploads/2024/11/1295_92_B0140.pdf
  (third-party-hosted copy of the official county style — NCSBE serves sample
  ballots only via per-voter lookup, no static PDF). Printed order matches the
  rule exactly: President/VP → US House 2 → Governor → …Treasurer → Supreme
  Court Seat 6 → Appeals Seats 12/14/15 → NC Senate 14 → NC House 38 →
  District Court 10B → Wake Commissioners → Register of Deeds → [NONPARTISAN]
  Board of Education D4 → Soil & Water → Raleigh Mayor/Council → [REFERENDA]
  Constitutional Amendment → Wake library bonds. Zero conflict.
- Baseline delta: REAL, two axes — (1) judicial within-level: appellate
  between Council of State and legislature, trial between state house and
  county; (2) partisan/nonpartisan split outranks level below district class:
  partisan county → partisan MUNICIPAL → nonpartisan county (incl. school) →
  nonpartisan municipal — baseline's county → municipal → school chain
  interleaves. Matches baseline: federal first, Senate before House, state
  senate before house, measures last.
- Notes: statutory guardrails verbatim — (b)(2) "State and local offices
  shall be listed according to the size of the electorate"; (b)(3) "Partisan
  offices, regardless of the size of the constituency, shall be listed before
  nonpartisan offices". HISTORICAL BREAK: pre-2018 NC judicial races were
  nonpartisan and printed in a genuinely late block — S.L. 2017-3/2018-13
  made them partisan; old cycles do NOT match the current order. (b)(1) pins
  "Member of the United States House of Representatives shall be listed
  immediately after United States Senator" (no US Senate seat in 2024 →
  absent from sample). (f) bans straight-party. Name order (informational):
  (c) single statewide drawing, alphabetical or reverse-alphabetical.
  Primaries: contest order unchanged; per-precinct NAME rotation added by 08
  NCAC ch. 10.

### MI — Michigan (FIPS 26) — GRADE A
- Authority: MCL 168.697 (partisan office order),
  https://www.legislature.mi.gov/Laws/MCL?objectName=mcl-168-697; MCL 168.699
  (nonpartisan section + internal order),
  https://www.legislature.mi.gov/Laws/MCL?objectName=mcl-168-699; MCL 168.703
  (party-COLUMN order only); MCL 168.706 (arrangement delegated to SOS);
  Const 1963 art II §4(1)(c) (straight-party); Michigan Ballot Production
  Standards, Sept 2024 edition (compulsory per Ch. 1 p.4),
  https://www.michigan.gov/-/media/Project/Websites/sos/01mcalpine/BallotStandards.pdf
  (all accessed 2026-08-16). Fetch gotchas: legislature.mi.gov fails
  WebFetch/curl with a TLS chain error ("unable to verify the first
  certificate") — browser only; michigan.gov PDFs 403 to plain fetch (curl +
  browser UA works); mvic.sos.state.mi.us script-rendered + Cloudflare-gated.
- Office order: Partisan Section, MCL 168.697 verbatim: "Electors of President
  and Vice President of the United States; governor and lieutenant governor;
  secretary of state; attorney general; United States Senator; Representative
  in Congress; senator and representative in the state legislature; members
  of the state board of education; regents of the University of Michigan;
  trustees of Michigan State University; governors of Wayne State University;
  county executive; prosecuting attorney; sheriff; clerk; treasurer; register
  of deeds; …" then township officers "in substantially the following order:
  supervisor, clerk, treasurer, trustees, and constables." Standards Ch. 13
  adds County Commissioner at county-block end, partisan City between county
  and township, Precinct Delegate last. Straight Party Ticket position FIRST
  (Const art II §4(1)(c)). Office-block form; party labels under names
  (168.703 orders candidates within office, not offices).
- Judicial: separate nonpartisan section after the whole partisan section,
  judicial FIRST inside it. MCL 168.699 verbatim: "…in the following order:
  justices of the supreme court, judges of the court of appeals, judges of
  the circuit court, judges of the probate court, judges of the district
  court, community college board of trustees member, intermediate school
  district board member, city officers, …local school district board member,
  metropolitan district officer, and district library board member."
  Standards add Probate District + Municipal Court slots. No retention —
  incumbency designation instead (MCL 168.409b, Const art VI §24; within-
  office order Incumbent → Non-Incumbent → New Judgeship).
- Measures: last — Proposal Section after the nonpartisan section (Standards
  Ch. 11/20/21; Ch. 17: "Previous legislation passed affected only candidate
  race order and did not influence the order of proposals"). Internal order
  (Ch. 17 p.33): State → County → City → Township → Village → Local School →
  ISD → Community College → Metropolitan → District Library — but labeled
  "strongly encouraged", i.e. guidance, unlike the compulsory Ch. 11/13
  rules.
- County discretion: effectively none over sequence (SOS arrangement per MCL
  168.706, Standards compulsory). Pockets: city office order governed by city
  charter (both partisan + nonpartisan city blocks); authority items follow
  their parent unit; vendor layout latitude (banner scaffolding varies —
  Farmington Hills prints no section banners; ordering identical).
- School/special: all in the nonpartisan tail AFTER every judicial race
  (order above). BUT State Board of Education + U-M regents + MSU trustees +
  WSU governors are PARTISAN, printing between the state legislature and
  county offices ("State Boards" header) — no baseline slot.
- Corroboration: Mason County 2022 general (cycle 2022; SMALL county — the
  researcher retracted two earlier populous-county 2024 citations as
  unverified, see Notes), official sample ballot for a real precinct,
  published by the SOS (Mock-Election-Portal folder):
  https://www.michigan.gov/sos/-/media/Project/Websites/sos/Elections/Mock-Election-Portal/Mason-Sample-Ballot.pdf
  (City of Ludington Pct 1). Confirms in printed order: Partisan Section →
  Straight Party → State (Governor+Lt.Gov joint → SOS → AG) → Congressional
  (US Rep 2nd) → Legislative → State Boards (State BOE → U-M Regent) →
  County → Nonpartisan Section → Judicial (Supreme Court → Court of Appeals
  w/ Incumbent/Non-Incumbent positions) → Community College → City → Local
  School District → Proposal Section → State (22-1/22-2/22-3) last.
  Judicial-before-school, executives-before-Congress, proposals-last all
  confirmed on a real ballot; reinforced by the SOS's own full-ballot
  specimen in Standards Ch. 21. No statute/sample conflict on any point.
- Baseline delta: REAL, three — (1) statewide executives (Gov+Lt.Gov → SOS →
  AG) BEFORE US Senator/US Rep (statutory, MCL 168.697); (2) judicial leads
  the nonpartisan section, ahead of nonpartisan county/municipal/school —
  reverse of baseline school-before-judicial; (3) partisan education/
  university boards between state legislature and county. NOTE the
  nonpartisan tail does NOT reduce to a county→municipal→school progression:
  education contests sit on BOTH sides of the municipal offices — Community
  College and Intermediate School District print BEFORE city, while Local
  School District prints AFTER village (MCL 168.699 order above) — so an
  implementation needs distinct ranks for CC/ISD vs local school board.
  Matches: President first, legislature after Congress, measures last.
- Notes: RESEARCH RETRACTION recorded — the researcher's first report cited
  Wayne (Brownstown Twp) and Oakland (Farmington Hills) Nov-2024 sample
  ballots; it then retracted both as never actually fetched/verified. Do NOT
  cite those URLs; Mason 2022 + the Standards specimen are the only verified
  samples. Grade A stands on the rubric (authority + matching real ballot)
  but with caveats: sample is a small county, cycle 2022, and 2022 had
  neither presidential electors nor a US Senate race — so Electors-of-
  Pres/VP placement and US Senator's position relative to the executives
  rest on MCL 168.697 text alone. A populous-county 2024 ballot remains an
  open follow-up (mvic.sos.state.mi.us has it but the
  /PublicBallot/GetMvicBallot endpoint is script-rendered +
  Cloudflare-gated). 168.697 says "substantially in the following order"
  (softener); 168.699 says "in the following order" (strict); Standards
  remove the slack. Mason PDF two-column extraction scrambles the intra-
  state proposal sequence — only section placement confirmed there.
  Precinct delegates elected at August primary, never on the general.
  Primaries (one line): per-party sections, nonpartisan primary = judicial +
  some city only, undersubscribed offices omitted. Name order
  (informational): MCL 168.569a + R168.774(9) precinct rotation on
  nonpartisan ballots when contested; alphabetical otherwise; partisan
  general follows 168.703 party order. Standards republish per cycle — carry
  the edition forward.

## Batch 2

### NJ — New Jersey (FIPS 34) — PENDING

### VA — Virginia (FIPS 51) — PENDING

### WA — Washington (FIPS 53) — GRADE B (partial)
- Authority: RCW 29A.36.161(3),
  https://app.leg.wa.gov/RCW/default.aspx?cite=29A.36.161 (accessed
  2026-08-16): state measures "must appear after the instructions and before
  any offices".
- Measures: state measures print FIRST, before offices — inverse of the
  baseline's measures-last.
- Baseline delta: REAL, user-visible — measures rank must move ahead of offices
  for WA once grade A. Highest-priority override candidate.

### AZ — Arizona (FIPS 04) — PENDING

### TN — Tennessee (FIPS 47) — PENDING

### MA — Massachusetts (FIPS 25) — PENDING

### IN — Indiana (FIPS 18) — PENDING

### MO — Missouri (FIPS 29) — PENDING

### MD — Maryland (FIPS 24) — PENDING

### WI — Wisconsin (FIPS 55) — PENDING

## Batch 3

### CO — Colorado (FIPS 08) — PENDING

### MN — Minnesota (FIPS 27) — GRADE B (partial)
- Authority: Minn. Rule 8250.1810 subp 5,
  https://www.revisor.mn.gov/rules/8250.1810/ (accessed 2026-08-16); statute
  backdrop MN 204D.13, https://www.revisor.mn.gov/statutes/cite/204D.13
- Judicial: Judicial Offices dead last among offices — matches baseline.
- Measures: questions interleave per jurisdiction (each jurisdiction's
  questions with that jurisdiction's offices), NOT a single trailing block.
- Baseline delta: measures interleaving diverges; decide during full pass
  whether our tier granularity can express it or the entry documents it as an
  accepted deviation.

### SC — South Carolina (FIPS 45) — PENDING

### AL — Alabama (FIPS 01) — PENDING

### LA — Louisiana (FIPS 22) — PENDING
- Notes: November all-comers primary structure — verify which November ballot
  the order statute describes.

### KY — Kentucky (FIPS 21) — PENDING

### OR — Oregon (FIPS 41) — PENDING

### OK — Oklahoma (FIPS 40) — PENDING

### CT — Connecticut (FIPS 09) — PENDING

### UT — Utah (FIPS 49) — PENDING

## Batch 4

### IA — Iowa (FIPS 19) — PENDING

### NV — Nevada (FIPS 32) — PENDING

### AR — Arkansas (FIPS 05) — PENDING

### KS — Kansas (FIPS 20) — PENDING

### MS — Mississippi (FIPS 28) — PENDING

### NM — New Mexico (FIPS 35) — PENDING

### NE — Nebraska (FIPS 31) — PENDING
- Notes: unicameral nonpartisan Legislature — decide `state_upper` vs
  `state_lower` scope mapping explicitly.

### ID — Idaho (FIPS 16) — PENDING

### WV — West Virginia (FIPS 54) — PENDING

### HI — Hawaii (FIPS 15) — PENDING

## Batch 5

### NH — New Hampshire (FIPS 33) — PENDING

### ME — Maine (FIPS 23) — GRADE B (partial)
- Authority: 21-A §601(3) ("Order of offices"),
  https://legislature.maine.gov/statutes/21-A/title21-Asec601.html (accessed
  2026-08-16): "President, United States Senator, Governor, Representative to
  Congress, State Senator and Representative to the Legislature…"
- Office order: Governor prints before US House (3rd vs 4th) — deviates from
  the baseline's federal-first tiers.
- Baseline delta: REAL — Governor/US House swap for ME once grade A.

### MT — Montana (FIPS 30) — PENDING

### RI — Rhode Island (FIPS 44) — PENDING

### DE — Delaware (FIPS 10) — PENDING

### SD — South Dakota (FIPS 46) — PENDING

### ND — North Dakota (FIPS 38) — PENDING

### AK — Alaska (FIPS 02) — PENDING

### VT — Vermont (FIPS 50) — PENDING

### WY — Wyoming (FIPS 56) — PENDING

### DC — District of Columbia (FIPS 11) — PENDING
- Notes: no state tier — Delegate, Mayor, Council, ANC need their own
  mini-order.
