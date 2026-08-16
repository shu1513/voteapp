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

### NJ — New Jersey (FIPS 34) — GRADE A
- Authority: R.S. 19:14-8 "Arrangement of ballots" (as amended through
  L.2017, c.206, s.4), official NJ Division of Elections posting,
  https://nj.gov/state/dos-statutes-elections-19-10-19.shtml (accessed
  2026-08-16). Supporting: R.S. 19:14-2 + 19:14-13 + 19:14-14 (public
  questions), 19:14-12 (ballot draw), R.S. 19:49-2 as amended by P.L.2025,
  c.32, s.7 (county-clerk final arrangement),
  https://nj.gov/state/dos-statutes-elections-19-40-49.shtml; R.S. 19:60-9
  (school elections),
  https://nj.gov/state/dos-statutes-elections-19-60-63.shtml; P.L.2025,
  c.32 session law, https://pub.njleg.gov/Bills/2024/PL25/32_.HTM; NJ
  Const. via https://www.njleg.state.nj.us/constitution.
- Office order: statutorily fixed top-to-bottom within each party column
  (party-column "blanket" ballot at generals): presidential electors → US
  Senator → GOVERNOR → US House → State Senate → General Assembly → county
  executive (charter counties) → sheriff → county clerk → surrogate →
  register of deeds and mortgages → county supervisor → county
  commissioners (statute: "chosen freeholders") → coroners → mayor +
  municipal governing body → "any other titles of office". Governor sits
  BEFORE US House in statute but the pairing never occurs — Governor is
  odd-year (Const. art. XI (Schedule) §III ¶1, "at the general election to
  be held in the year one thousand nine hundred and forty-nine and every
  fourth year thereafter"; NOT art. V §III, which is the militia clause —
  citation fixed in verification), so no shared ballot. Lt. Governor runs
  conjointly (single vote); no other statewide executive elected (AG/SOS
  appointed, art. V §IV ¶3).
- Judicial: NONE — NJ elects no judges at any level (Const. art. VI §VI ¶1,
  gubernatorial appointment w/ Senate consent); no retention elections.
  Judicial block should be omitted entirely for FIPS 34.
- Measures: after all offices, "in a separate space at the foot of the
  ballot" under "Public Questions to be voted upon" (19:14-2, 19:14-14).
  Internal order (19:14-13): STATEWIDE first (SOS-certified order) →
  MUNICIPAL second → COUNTY last (municipal/county by clerk lot-draw).
  Exception: school-district money questions print NEAR the school-board
  candidates, not in the foot block (19:60-9, "below or to the right of"
  at clerk's option) — AUTHORITY-ONLY, no sampled ballot exercised it
  (Teaneck's foot-block question was municipal open-space).
- County discretion: real but bounded — 19:49-2 gives each county clerk
  "the specifications for, and the final arrangement of" general ballots
  "to the extent not inconsistent with the provisions of this Title";
  19:14-8 sequence stays mandatory. Named carve-outs: school/fire section
  layout at clerk discretion (19:14-8); school-question below-vs-right
  (19:60-9). Party COLUMN order + candidate order = clerk lot-draw
  (19:14-12), not statute.
- School/special: school board + fire commissioners pulled OUT of the
  sequence into a separate section — "shall be listed in a section of the
  ballot that is separate from the section featuring other candidates
  whenever possible in a layout at the discretion of the county clerk"
  (19:14-8). WHERE that section sits is clerk layout, not statute: Bergen
  prints a "NON-PARTISAN SCHOOL ELECTION" block below the partisan block;
  Hudson (Jersey City 2024) prints Board of Education as a separate
  right-hand "COLUMN J". Regional board prints ABOVE local board
  (19:60-9). Nonpartisan municipal contests likewise get their own
  "NON-PARTISAN MUNICIPAL ELECTION" block (Bergen: between partisan and
  school).
- Corroboration: Bergen County 2024 general, Hackensack sample,
  https://www.bergencountyclerk.gov/_Content/pdf/voting/sample-ballots/2024-general/Hackensack.pdf
  — presidential electors → US Senator → US House → Sheriff → County
  Commissioners, then separate "NON-PARTISAN SCHOOL ELECTION" / Local Board
  of Education. Teaneck same cycle,
  https://www.bergencountyclerk.gov/_Content/pdf/voting/sample-ballots/2024-general/Teaneck.pdf
  — adds NON-PARTISAN MUNICIPAL (Township Council) after county offices +
  "PUBLIC QUESTION TO BE VOTED UPON" in the bottom band. 2025 odd-year
  Hackensack,
  https://www.bergencountyclerk.gov/_Content/pdf/voting/sample-ballots/2025/Hackensack-Gen25.pdf
  — Governor & Lt. Governor → General Assembly → County Commissioners →
  separate school block. Second county (verification): Hudson, Jersey City
  Form 6, 2024,
  https://www.hudsoncountyclerk.org/wp-content/uploads/2024/09/2024-General-Election-F06-JC.pdf
  — presidential electors → US Senator → US House → Surrogate, Board of
  Education in separate COLUMN J. No statute-vs-sample conflict.
- Baseline delta: SEVERAL. (1) Governor between US Senate and US House in
  statute — moot in practice (odd-year), record only. (2) County tier has a
  mandated internal order (executive → sheriff → clerk → surrogate →
  register → supervisor → commissioners → coroners), not one bucket.
  (3) Judicial block does NOT EXIST — delete for NJ, don't sort late.
  (4) School/fire = physically separate nonpartisan section, not a late
  tier; nonpartisan municipal separate too. (5) Measures-last holds but
  internal order statewide → MUNICIPAL → COUNTY (baseline has no
  municipal-before-county rule). (6) School money questions print beside
  school-board contest — exception to measures-last.
- Notes: even-year NJ generals carry NO state contests at all (Governor +
  Assembly odd-year, Senate 2023/2027 cycle) — federal + county + local
  only. P.L.2025, c.32 (A5116, the county-line/office-block redesign after
  Kim v. Hanlon) amends PRIMARY ballot statutes + 19:49-2 only — it does
  NOT touch 19:14-8, so general-election contest order unchanged (decisive
  negative finding from the session-law text). Candidate order within
  contest (informational): clerk capsule draw 3pm on the 85th day before
  the general (19:14-12); never implemented. Statute's "board of chosen
  freeholders" prints as "Board of County Commissioners"; "coroners" is
  dead in practice. Dangling cross-reference: 19:14-2 cites a nonexistent
  19:14-15 (likely repealed into 19:14-13 by L.1979, c.191 — unconfirmed,
  practical effect nil). IMPLEMENTATION CAUTION: "NJ elects no judges"
  holds for every court, but the elected SURROGATE is a probate officer
  who sits as judge of the Surrogate's Court — if the office classifier
  tags "Surrogate" as judicial, NJ's deleted judicial block would misfile
  a mandatory county-tier office (N.J.S.A. 2B:14-1 unverified — flag, not
  assertion).

### VA — Virginia (FIPS 51) — GRADE A
- Authority: Va. Code § 24.2-613 "Form of ballot" (A), (C), (D),
  https://law.lis.virginia.gov/vacode/title24.2/chapter6/section24.2-613/ —
  delegates to the State Board; operative order = Virginia SBE "Ballot
  Standards", August 2022 edition, pp. 8, 19-22, 40 ("Ballot Placement"
  numbered list 1-33),
  https://www.elections.virginia.gov/media/formswarehouse/election-management/ballots/2022-08-SBE-Ballot-Standards-and-Verification-Procedures.pdf
  (confirmed current edition on the ELECT Forms Warehouse,
  https://www.elections.virginia.gov/formswarehouse/election-management/).
  (all accessed 2026-08-16)
- Office order: single numbered list 1-33 used for EVERY November general
  (absent offices skipped, filter not re-order): 1 President → 2 US Senate →
  3 US House → 4 GOVERNOR → 5 Lt Governor → 6 AG → 7 Senate of Virginia →
  8 House of Delegates → locality-wide: 9 Clerk of Court → 10
  Commonwealth's Attorney → 11 Sheriff → 12 Commissioner of Revenue → 13
  Treasurer → 14 Chairman Board of Supervisors / Mayor → 15 Supervisors/
  Council at-large → 16 Chairman School Board → 17 School Board at-large →
  district/ward: 18 Supervisors/Council district → 19-20 School Board
  district → 21 Soil & Water Conservation Director → town: 22 Mayor → 23-24
  Town Council → 25 Recorder → 26 Treasurer → measures 27-33. Governor
  AFTER US House (matches baseline — a first among researched deviating
  states); executives Gov → LtGov → AG. Tie-break: general before special
  for the same office.
- Judicial: NONE — Va. Const. art. VI §7, justices and "judges of all other
  courts of record" chosen by majority vote of each house of the General
  Assembly. Negative corroboration: placement list 1-33 has no judicial
  office; zero judicial contests across 538pp (2024, 269 styles) + 530pp
  (2025, 265 styles) of Fairfax sample books.
- Measures: all after all offices — § 24.2-613(D) "all offices to be
  elected shall appear before any questions presented to the voters".
  Internal order (Standards p.22): 27 statewide constitutional amendment →
  28 statewide bond → 29 regional referendum → 30 locality bond → 31
  locality referendum → 32 district/ward → 33 town. § 24.2-615: separate
  question per amendment.
- County discretion: NONE by default — order list is a mandatory Board
  requirement, § 24.2-613(A) makes SBE standards binding; 1VAC20 carries NO
  ballot-order rule (ten chapters, none ballots). Only escape = per-locality
  waiver approved by the Commissioner of Elections (Standards p.40).
- School/special: school board NOT a late block — INTERLEAVED into locality
  blocks: at-large seats 16-17 right after at-large governing body, before
  district-level offices; district seats 19-20 inside the district block.
  Soil & Water Conservation Director 21 = only special-district slot, after
  school, before town.
- Corroboration: Fairfax County, both cycles, no conflict. (a) Nov-2024
  all-styles book (538pp),
  https://www.fairfaxcounty.gov/elections/sites/elections/files/Assets/sample%20ballots/2024/2024-11-5/2024_Nov_FullBallots_Web.pdf
  — 104-CHAPEL: President → US Senate → US House 11th; reverse:
  Constitutional Amendment Q1 → county Transportation Bonds; 319-HERNDON#1
  adds Mayor Town of Herndon → Town Council in the town slot. (b) Nov-2025
  book (530pp),
  https://www.fairfaxcounty.gov/elections/sites/elections/files/Assets/Documents/PDF/2025_November_General_Web.pdf
  — Governor → Lt Gov → AG → House of Delegates 14th; reverse: Public
  School Bonds; Vienna style: Mayor → Town Council → bond. Confirms slots
  1-6, 8, 22-23, 27, 30 + offices-before-questions. SCHOOL INTERLEAVE
  ballot-confirmed by verification: Stafford County Nov-2025
  (https://cdn.staffordcountyva.gov/Voter%20Registration/Website%20Sample%20Ballots.pdf)
  — Governor → LtGov → AG → House of Delegates 64th → Supervisors Hartwood
  District → SCHOOL BOARD Hartwood District → End of Ballot (slot 18 → 20
  adjacency, every district style); City of Alexandria Nov-2024
  (https://www.alexandriava.gov/sites/default/files/2024-09/2024_nov_sample_ballot_district_a_colored_background.pdf)
  — President → US Senate → US House 8th → Mayor → City Council (vote for
  six) → School Board District A → Proposed Constitutional Amendment (slots
  14 → 15 → 20 → 27).
- Baseline delta: SEVERAL. (1) Judicial block does not exist — drop for VA
  ("Judge"/"Justice" = zero hits across both Fairfax books). (2) School NOT
  late — interleaved at 16-17/19-20 inside locality blocks (biggest
  structural delta; district seats 18→20 + at-large-city 15→20 now
  BALLOT-CONFIRMED via Stafford 2025 + Alexandria 2024; still
  authority-only: slots 16-17 Chairman/At-Large School Board and 21 Soil &
  Water). (3) County + city merged into shared locality
  blocks; TOWN separate and last (after Soil & Water). (4) Soil & Water
  slot 21 — baseline has none. (5) Measures internally sequenced statewide
  → regional → locality → district → town. (6) Constitutional-officer run
  (Clerk of Court → Commonwealth's Attorney → Sheriff → Comm. of Revenue →
  Treasurer) ahead of governing body. NO delta on federal head or
  executives: Pres → US Sen → US House → Gov → LtGov → AG matches baseline
  exactly.
- Notes: ONE list for odd + even Novembers — a filter, not two orders.
  Even-year November = federal + local/town + questions (slots 4-8 empty);
  odd-year = executives + General Assembly + local/town (slots 1-3 empty);
  2023/2027 cycle carries Senate of Virginia + constitutional officers
  (slots 7, 9-13 — authority-only, neither sampled cycle had them;
  Chesterfield's Nov-2023 book is the right test but 404s live and
  Wayback's sole capture 503s).
  Primary difference (one line): same office order; candidate order flips
  to filing order (Standards p.29, § 24.2-529). Candidate order within
  contest (informational): party order by SBE lot, recognized parties next,
  independents last by filing time (§ 24.2-613(C)); never implemented.
  § 24.2-640 is REPEALED (Acts 2014) — live sample-ballot section is
  § 24.2-641 (posting only, no order). Separate RCV ballot standard (June
  2022) exists, unread — may affect layout where a locality adopts RCV.

### WA — Washington (FIPS 53) — GRADE A
- Authority: RCW 29A.36.121 "Order of positions or offices",
  https://app.leg.wa.gov/RCW/default.aspx?cite=29A.36.121; WAC 434-230-025
  "Order of offices" (explicit 25-item sequence; authority RCW 29A.04.611;
  last amended WSR 24-03-053 eff. 2024-02-10 — DELETED "Advisory votes",
  repealed 2023; pre-2024 mirror copies are stale),
  https://app.leg.wa.gov/wac/default.aspx?cite=434-230-025, list
  independently confirmed via the rulemaking order
  https://lawfilesext.leg.wa.gov/law/wsr/2024/03/24-03-053.htm; seed
  re-confirmed RCW 29A.36.161(3) (state measures "must appear after the
  instructions and before any offices"); WAC 434-230-030 (local measures);
  RCW 29A.72.290 (state-measure internal order). (all accessed 2026-08-16)
- Office order: WAC 434-230-025 items (1)-(25): state measures (1)-(5) →
  (6) countywide measures → (7) President/VP → (8) US Senator → (9) US
  Representative → (10) Governor → (11) Lt Gov → (12) SOS → (13) Treasurer →
  (14) Auditor → (15) AG → (16) Public Lands Comm → (17) Superintendent of
  Public Instruction → (18) Insurance Comm → (19) State Senator → (20) State
  Rep → (21) county officers → (22) Supreme Court → (23) Court of Appeals →
  (24) Superior Court → (25) District Court → all other jurisdictions
  (city/school/special) per county auditor procedures. Federal ABOVE
  Governor — WA matches the baseline's federal-first spine (minority pattern
  among researched states so far).
- Judicial: late block but NOT last — items (22)-(25) sit after county
  officers and BEFORE municipal/school/special-district contests. Internal
  order Supreme → Appeals → Superior → District. SPI, though nonpartisan, is
  NOT in the judicial block — interleaved in the executive run at (17),
  labeled "nonpartisan office" (WAC 434-230-035; labeling ≠ reordering).
- Measures: state measures FIRST, before any office (RCW 29A.36.161(3)),
  internal order initiatives-to-people → referendum measures → referendum
  bills → initiatives-to-legislature (+alternates) → constitutional
  amendments (RCW 29A.72.290, serial-number order within headings).
  COUNTYWIDE measures immediately after state measures, still before offices
  (WAC 434-230-030 "listed immediately following state ballot measures").
  Other local measures print with their jurisdiction in the late tail (or a
  county-option special-measures area).
- County discretion: bounded — items (1)-(25) mandatory statewide; below
  (25), each county MUST adopt written procedures fixing sub-county
  jurisdiction order (WAC 434-230-030), consistent election to election. The
  tail's POSITION (after judges) is statewide; order WITHIN it is county-set.
- School/special: school/fire/port/water districts = "all other
  jurisdictions" after item (25); normally ODD-year contests (RCW 29A.04.330
  "city, town, and district general elections ... in the odd-numbered
  years") so rarely on even-year generals — their district MEASURES do
  appear there, same tail position.
- Corroboration: King County Nov-2024 general, style SEA 36-1386,
  https://cdn.kingcounty.gov/-/media/king-county/depts/elections/current-election/202411/sample-ballots/sample-ballot.pdf
  — Instructions → state measures (I-2066 "Initiative to the People", then
  initiatives to the legislature) → President/VP → US Senator → US Rep →
  Governor → Lt Gov → SOS → Treasurer → Auditor → AG → Public Lands → SPI
  ("nonpartisan office") → Insurance Comm → State Reps → Supreme Court →
  Court of Appeals → Superior Court → City of Seattle Prop 1 → Seattle
  Council Pos 8 → "End of Ballot". Confirms measures-first, executive order
  incl. SPI wedge, judicial above city, city measure printing with its city.
- Baseline delta: LARGE, four departures. (1) Measures FIRST (state, then
  countywide), not last. (2) Judicial block EARLIER than baseline — after
  county, before municipal/school/special. (3) Municipal/school/special dead
  last, internal order county-determined. (4) Executive INTERNAL order is
  FIXED (Gov → LtGov → SOS → Treasurer → Auditor → AG → Public Lands → SPI
  → Insurance) — the baseline ranks all statewide executives at one tier
  with a generic title tie-break, so WA's mandated sequence diverges.
  (SPI sitting inside the executive run is NOT itself a delta — the
  baseline already treats statewide non-judicial offices as one tier.)
  Matching baseline: federal-first spine, senate-before-house, Supreme →
  Appeals → trial.
- Notes: RCW 29A.36.121(1) is written for the PRIMARY ballot; the general
  "shall be substantially the same as on a primary ballot" (RCW
  29A.36.121(2)) + measures-first + President-first additions. Top-two
  general normally = 2 candidates/office ordered by primary vote, highest
  first (RCW 29A.36.170(1)) — EXCEPT judicial + SPI, where a
  majority-in-primary winner prints ALONE in the general (RCW
  29A.36.170(2); King's 2024 ballot shows single-name Supreme Court Pos 8/9
  and both Appeals seats); initial candidate order by lot (RCW 29A.36.131)
  — informational, never implemented. Unobserved on a real ballot: item
  (21) county officers (King = charter county, odd-year offices; Pierce
  County PDF 403-blocked) and item (25) District Court. Possible conflict
  recorded, unresolved: pypdf extracted King's initiatives-to-legislature
  as 2124 → 2117 → 2109 vs RCW 29A.72.290's serial-number order — likely a
  multi-column extraction artifact, needs visual read or second county.
  Corroboration URL sits on a /current-election/202411/ path — expect rot.

### AZ — Arizona (FIPS 04) — GRADE A
- Authority: A.R.S. § 16-502 "Form and contents of ballot",
  https://www.azleg.gov/ars/16/00502.htm; A.R.S. § 19-125 (measure
  numbering), https://www.azleg.gov/ars/19/00125.htm; AZ SOS 2025 Elections
  Procedures Manual (approved 2025-12-22 — operative for Nov 2026), Ch. 8
  § V.A.3 "Order of Candidate Races" (pp. 167-169) + A.5.b "Order of Ballot
  Measures" (pp. 172-173),
  http://apps.azsos.gov/election/files/epm/2025/Election-Procedures-Manual-2025--FINAL-12-22-25.pdf.
  EPM has force of law — A.R.S. § 16-452(C) makes violation a class 2
  misdemeanor. (all accessed 2026-08-16)
- Office order: § 16-502(C) fixes five coarse tiers (presidential electors
  → US senator → US representatives → "the several state offices" → county
  and precinct offices); the EPM enumerates: SECTION ONE PARTISAN — 1
  Presidential electors → 2 US Senator → 3 US Representative → 4 GOVERNOR
  AND LT GOVERNOR (joint ticket, first used 2026; 2023 EPM = Governor
  alone) → 5 STATE SENATOR → 6 STATE REPRESENTATIVE → 7 SOS → 8 AG → 9
  Treasurer → 10 Superintendent of Public Instruction → 11 Mine Inspector
  → 12 Corporation Commissioner → 13 county offices → 14 Justice of the
  Peace → 15 Constable. KEY WRINKLE: the LEGISLATURE splits the executive
  block — § 16-502(D) puts legislative candidates "immediately below the
  candidates for the office of governor", and the EPM keeps legislature
  before the remaining statewide offices even in non-gubernatorial years.
  Governor sits AFTER US House (federal-first partisan section). JP +
  Constable are PARTISAN and close section one, after county offices.
- Judicial: NOT a late block — judicial OPENS the nonpartisan section, but
  as an EPM DEFAULT with a lawful county override, not a hard statutory
  rule: § 16-502(I) prints the heading "Section Two / Nonpartisan Ballot";
  § 16-502(J) is ONE sentence covering judges AND "school district
  officials and other nonpartisan officials ... in an order determined by
  the officer in charge of the election" — only the EPM fixes
  judicial-first, and its "reasonably adjust" escape has three grounds
  (reverse-side avoidance, reverse-side uniformity, blank-space
  elimination). EPM nonpartisan order: 1 Supreme Court
  (retention, all counties) → 2 Court of Appeals Div 1 → 3 Div 2 → 4
  Superior Court RETENTION (Maricopa/Pima/Pinal/Coconino only) → 5 Superior
  Court CONTESTED (other 11 counties; partisan primary, NO party label in
  general — Ariz. Const. art. VI § 12) → then school → community college →
  JTED → special taxing district → city/town mayor → city/town council.
  Vacant unexpired nonpartisan terms print under a separate heading below
  (§ 16-502(K)).
- Measures: LAST — § 16-502(L) "immediately below the names of candidates
  for nonpartisan positions". EPM order: state constitutional amendments →
  statewide statutory initiatives → statewide referenda → county measures
  (incl. school/JTED/college/special-district measures) → city/town
  measures. Numbering (§ 19-125(B)): constitutional = 100-series;
  statutory initiative = 200; referendum = 300; county/local = 400;
  amendments "by themselves at the head of the ballot column".
- County discretion: real but bounded — (a) § 16-502(J) nonpartisan order
  "determined by the officer in charge of the election", constrained by the
  EPM list w/ escape only for reverse-side/blank-space adjustments; (b)
  internal order of the eight county offices (Supervisor, Assessor,
  Attorney, Clerk of Superior Court, Recorder, School Superintendent,
  Sheriff, Treasurer) is officer-determined; (c) local measure placement
  delegated (§ 16-502(L)). Partisan sequence NOT reorderable.
- School/special: school/college/JTED/special-district boards nonpartisan,
  AFTER the judicial block, BEFORE city/town — municipal offices are the
  LAST candidate races. Their bond/override QUESTIONS print separately in
  the county tier of the measures block, not beside board races.
- Corroboration: Pinal County Nov-2024 official ballot style
  PINA_105_001G05,
  https://www.pinal.gov/DocumentCenter/View/21280/PINA_105_001G05_1_1_NON-DEC
  — presidential electors → US Senator → US Rep CD-2 → State Senator LD-7 →
  State Rep LD-7 → Corporation Commission → Supervisors → Assessor →
  Attorney → Recorder → Sheriff → School Superintendent → Treasurer →
  "Section Two - Nonpartisan Ballot" (verbatim printed heading; "Section
  One - Partisan Ballot" heads the partisan run) → Supreme Court retention
  → Superior Court retention → "MEASURES SUBMITTED TO VOTERS" → Props
  137-140 (statewide constitutional) → 311-315 (statewide) → 486 (Pinal
  county road tax) → 489 (Ray USD budget override). Confirms
  legislature-before-Corporation-Commission, the literal Section Two
  heading w/ judges beneath, § 16-502(L) measures-last, AND the EPM
  measure sub-order (state → county → school-district override).
  (Verification pass re-extracted the PDF cleanly with PyMuPDF — the
  original researcher's "headings undecodable" caveat was a tooling
  limitation, since removed.) Secondary: Maricopa Nov-2024 final canvass
  (264 contests) — same federal→legislative→CorpComm→county→JP/Constable
  head; measures tail exactly § 19-125 series (133-140 → 311-315 → 479 →
  486).
- Baseline delta: SUBSTANTIAL, three departures. (1) Statewide executives
  SPLIT by the legislature: Gov(+LtGov) → state senate → state house → SOS
  → AG → Treasurer → Supt → Mine Inspector → Corp Comm — only Governor
  precedes the legislature. (2) Judicial NOT late — opens the nonpartisan
  section ahead of school/college/special/municipal (internal supreme →
  appeals → trial matches). (3) Municipal comes after school and LAST among
  candidate races. Matching: federal-first top, senate-before-house, county
  before local, measures last.
- Notes: CONFLICT RECORDED, NOT RESOLVED — Maricopa 2024 canvass lists
  school/college/JTED/fire/city races BEFORE judicial retention, reverse of
  the EPM; but the same canvass interleaves district QUESTIONS right after
  their board races, which § 16-502(L)/EPM forbid on printed ballots → the
  canvass is grouped by jurisdiction, NOT printed order, so it is not
  reliable evidence below the county tier; the Pinal style read carried no
  school/municipal races and cannot break the tie. Follow the EPM
  (judicial first), flagged unconfirmed against a printed ballot carrying
  both school + retention contests. No 2022 printed ballot examined —
  Governor + down-ballot executives never observed in the same election
  (statute + both EPM editions explicit; re-check w/ Nov-2026 samples,
  which also debut the Gov+LtGov joint ticket, 2022 Prop 131). EPM edition
  delta (UNVERIFIABLE as of the verify pass — the 2023 EPM URL went dead
  between research and verification; claims retained but not
  re-established): 2023 EPM listed Governor alone at slot 4 and had
  special-taxing-district measures as a separate final bullet after city;
  2025 folds them into county measures. Party column
  order (informational): descending gubernatorial vote per county
  (§ 16-502(E)); candidate rotation across districts (§ 16-502(H)); recalls
  of partisan officials print in the partisan section (§ 19-213); never
  implemented. Fetch gotchas: azsos.gov + elections.maricopa.gov Cloudflare
  403 vs curl — needed in-browser same-origin fetch; Pima samples
  address-gated (no static PDFs).

### TN — Tennessee (FIPS 47) — GRADE A
- Authority: T.C.A. § 2-5-208 (Arrangement of material on ballots), subsecs.
  (a), (c)(1)-(3), (d), (f)(1) —
  https://codes.findlaw.com/tn/title-2-elections/tn-code-sect-2-5-208/
  (accessed 2026-08-16). SOURCING CAVEAT (GA-style): the statute text is
  MIRROR-sourced — the official LexisNexis portal 403s — but TWO independent
  mirrors agree letter-for-letter across the whole (A)-(CC) list (FindLaw
  "last updated Jan 2, 2024" + law.justia.com "2024 Tennessee Code",
  justia readable via the r.jina.ai fetch proxy), currency is confirmed by
  the official 2023 Public Chapter 346 PDF
  (https://publications.tnsosfiles.com/acts/113/pub/pc0346.pdf — deletes
  old subsec. (j), explaining the (i)→(k) gap both mirrors show), and all
  sample ballots below match exactly; still pending primary re-verification
  if official T.C.A. access opens. Supporting: T.C.A. § 2-3-202 (August
  offices) + § 2-3-203 (NOVEMBER offices — the positive basis: "(1)
  Representative in the general assembly; (2) Representative in the United
  States congress; (3) Senator in the general assembly; (4) Senator in the
  United States senate; (5) Governor; and (6) Electors for president and
  vice president"),
  https://codes.findlaw.com/tn/title-2-elections/tn-code-sect-2-3-202/;
  T.C.A. § 17-4-105 (retention at the regular August election),
  https://codes.findlaw.com/tn/title-17-judges-and-chancellors/tn-code-sect-17-4-105/;
  Tenn. Const. art. XI, § 3, official SOS-published constitution (PRIMARY,
  https://publications.tnsosfiles.com/pub/2023%20TN%20Constitution.pdf).
- Office order: § 2-5-208(c)(1) — "The order of the titles of the offices to
  be filled ... shall be substantially as follows": (A) Presidential electors
  → (B) GOVERNOR → (C) US Senate → (D) US House → (E) TN Senate → (F) TN
  House → (G) Supreme Court judge → (H) Court of Appeals → (I) Court of
  Criminal Appeals → (J) Circuit court → (K) Chancellor → (L) Criminal court
  → (M) District attorney general → (N) Public defender → (O) County mayors
  → (P) County legislative → (Q) Assessor → (R) Trustee → (S) General
  sessions judge → (T) Juvenile court judge → (U) Sheriff → (V) Clerks of
  courts → (W) County clerk → (X) Register → (Y) elective county department
  offices (incl. SCHOOL BOARDS) → (Z) Municipal executive → (AA) Municipal
  legislative → (BB) Municipal judicial → (CC) unclassified. Governor sits at
  slot 2, ahead of the ENTIRE federal delegation — and Governor is TN's only
  statewide-elected executive (no elected SOS/AG/Treasurer). Ties within a
  class: § 2-5-208(c)(2) "arranged in alphabetical order".
- Judicial: split into TWO EARLY blocks, not one late block. Appellate +
  major trial courts at (G)-(L), immediately after the state house and
  BEFORE county; general sessions (S) and juvenile (T) judges sit inside the
  county block; municipal judicial dead last at (BB). Retention questions,
  when present, go to the END of the ballot per (c)(3) — but § 17-4-105
  holds retention at the regular AUGUST election, and § 2-3-202 sends
  "judges of all courts" + DA + most county offices to August, so November
  generals carry effectively NO judicial contests (Davidson 2022 + 2024
  November ballots: zero).
- Measures: TWO rules. State constitutional amendments print "directly after
  the list of candidates for governor" (§ 2-5-208(f)(1)) — slot 2.5, NOT
  last; amendments only appear in gubernatorial Novembers (art. XI § 3: "at
  the next general election in which a Governor is to be chosen"). ALL other
  questions (local referenda/charter amendments) go "at the end of the
  ballot"; if a retention question is present, other questions print after
  it.
- County discretion: NONE as to order — § 2-5-208(a): "The requirements of
  this section apply to all ballots." "Substantially" in (c)(1) tolerates
  absent classes, not resequencing (all three ballots read match exactly).
  Format (not order) for vendor systems set statewide under § 2-5-206.
- School/special: school boards are NOT a separate tier — inside county
  class (Y) ("school boards" named in the class text), after Register,
  BEFORE municipal. Usually August but NOT statutorily bound to it —
  default school-board general dates are August AND November of even years
  (Knox Aug-2026 ballot confirms bottom-of-county-block placement). No
  special-district class → (CC) last.
- Corroboration: Davidson County Nov 2022 general,
  https://www.nashville.gov/sites/default/files/2022-09/Sample_Ballot_November_8_2022_State_Federal_Municipal_Elections.pdf
  — Governor → Amendments #1-#4 → US House → TN Senate → TN House →
  municipal (proves Governor-first inversion AND amendments-after-Governor).
  Davidson Nov 2024,
  https://www.nashville.gov/sites/default/files/2024-09/Sample_Ballot_November_5_2024_Elections.pdf
  — President → US Senate → US House → TN Senate → TN House → municipal →
  transit referendum LAST. Knox County Aug 2026 county general,
  https://www.knoxcounty.org/election/pdfs/General%20Sample%20Ballot_08.06.26.pdf
  — county half in exact statutory sequence. No conflicts.
- Baseline delta: SUBSTANTIAL, four departures. (1) Governor moves to slot
  2, before US Senate/House. (2) Judicial early (after state house, before
  county) and split — general sessions/juvenile mid-county, municipal
  judicial last; internal supreme → appeals → trial sequence preserved.
  (3) School inside county block → county → school → municipal, inverting
  baseline's municipal-before-school. (4) State constitutional amendments
  print second (after Governor), NOT last — only local questions trail. Plus
  DA/public defender occupy a tier between judicial and county that the
  baseline lacks.
- Notes: AUGUST/NOVEMBER SPLIT IS THE WHOLE STORY for November. Precision
  on the split: § 2-3-202's August list is exactly NINE items (assessor,
  constable, county clerk + court clerks, trustee, DA, judges of all
  courts, county legislative body, register, sheriff) — county MAYOR's
  August date comes from § 5-6-102(2), and SCHOOL BOARDS have NO August
  statute (TN default school-board general dates are August AND November
  of even years), so class (Y) is NOT reliably empty in November; § 2-3-203
  fixes what IS in November (see Authority). Effective November ballot:
  President → Governor → [amendments] → US Senate → US House → TN Senate →
  TN House → [occasional county-tail e.g. school board] → municipal →
  local questions. Governor IMMEDIATELY before US Senate IS
  ballot-confirmed — Nov-2018 carried both, and four county ballots print
  GOVERNOR → UNITED STATES SENATE → US HOUSE → TN Senate → TN House:
  Wayne https://tnsos.org/elections/ballots/418.pdf, Unicoi
  https://tnsos.org/elections/ballots/390.pdf, Jackson
  https://tnsos.org/elections/ballots/405.pdf (all three on the SOS's OWN
  ballot repository tnsos.org, found by verification), Hamblen
  https://www.hamblencountytn.gov/wp-content/uploads/delightful-downloads/2018/10/HamblenTN-G18-Infinity999.pdf;
  Wayne 2018 also ends "CITY OF CLIFTON / Retail Package Store Referendum"
  (local questions last). Davidson-2022 nuance: Governor printed slot 1
  (no US Senate race that year) — statutory (B)=slot-2 unaffected;
  verification confirmed amendments-after-Governor is genuinely VISUAL
  (same column, 23pt below Governor's write-in line), not a
  content-stream artifact. Knox Aug-2026 file is stamped "Preview Ballot
  06/15/2026". Candidate order within contest (informational): party
  columns majority → minority → minor (§ 2-5-208(d)(1)), alphabetical
  within; never implemented.

### MA — Massachusetts (FIPS 25) — GRADE A
- Authority: M.G.L. c.54 § 43A ("manner and order of appearance on
  ballots"),
  https://malegislature.gov/Laws/GeneralLaws/PartI/TitleVIII/Chapter54/Section43A
  (accessed 2026-08-16). Supporting: c.54 § 40 (state secretary prepares
  ALL state ballots), c.54 § 42 (candidate order within contest; questions
  after candidates; regional school committee paragraph), c.54 § 41A
  (Gov/LtGov grouped ticket), c.53 § 19 (district public-policy questions);
  Mass. Const. Pt. 2, c. II § I art. IX + Pt. 2, c. III art. I (judges
  appointed, good-behavior tenure), https://malegislature.gov/Laws/Constitution.
- Office order: statutory, fixed, statewide: Presidential elector → Senator
  in Congress → GOVERNOR AND LT GOVERNOR (one grouped ticket) → Attorney
  General → Secretary of State → Treasurer and Receiver General → Auditor →
  Congressman (US House) → COUNCILLOR (Governor's Council) → Senator in
  General Court → Representative in General Court → "all other offices ...
  in such order as the secretary of state may determine" (practice: county
  block — DA, Sheriff, Clerk of Courts, Register of Deeds, Register of
  Probate, County Commissioner, + regional district school committee) →
  questions last. Statewide executives BEFORE US House; Councillor tier
  between US House and state senate.
- Judicial: NONE — all judges appointed by the Governor w/ Governor's
  Council consent, good-behavior tenure (Const. cites above). Mandatory
  age-70 retirement = Amendment Art. XCVIII ("upon attaining seventy years
  of age said judges shall be retired" — annulled and replaced Ch. III
  art. I; on the same malegislature.gov Constitution page; Art. LVIII is
  only DISCRETIONARY retirement, no age). Nov-2024 specimen: zero judicial
  contests. Clerk of Courts / Register of Probate = elected court
  ADMINISTRATIVE offices → county block, never a judicial block.
- Measures: LAST — load-bearing cite is c.54 § 42 (unconditional): question
  "shall be printed on the ballot after the names of the candidates";
  § 43A's questions clause ("follow all of said offices, in such order as
  the secretary of state may determine") is textually CONDITIONED on
  vertical-row party arrangement, so cite it second. Statewide questions
  numbered 1..n; district public-policy questions (c.53 § 19) placed by the
  state secretary, no statutory sub-placement.
- County discretion: NONE — c.54 § 40 "All ballots for use in elections of
  state officers shall be prepared and furnished by the state secretary";
  no county election administration exists. Only discretion = the STATE
  secretary's, over the trailing block + question order. County govts
  abolished in Middlesex/Franklin/Hampden/Worcester/Hampshire/Essex/Suffolk
  (1997-2000) but every county still elects Register of Deeds, Register of
  Probate, Sheriff, DA (SOC: "All counties, even those with abolished
  governments, elect their own registers of deeds, registers of probate,
  sheriffs, and district attorneys."); intact-government counties add
  Commissioners + Treasurer.
- School/special: regional district school committees elected district-wide
  DO print on the state general ballot — c.54 § 42 dedicated paragraph
  (separate paper ballot allowed if they don't fit). Ordinary municipal
  school committees = separate city/town elections, never on the state
  ballot.
- Corroboration: Watertown (Middlesex Co.) SOC-issued "STATE ELECTION
  OFFICIAL SPECIMEN BALLOT" Nov-5-2024, Pcts. 1-8,
  https://content.civicplus.com/api/assets/ma-watertown/b1d6201b-6e9f-41e3-90b2-8176ed227b38?cache=1800
  (index https://www.watertown-ma.gov/specimen-ballots) — ELECTORS OF
  PRESIDENT AND VICE PRESIDENT → SENATOR IN CONGRESS → REPRESENTATIVE IN
  CONGRESS → COUNCILLOR → SENATOR IN GENERAL COURT → REPRESENTATIVE IN
  GENERAL COURT → CLERK OF COURTS → REGISTER OF DEEDS → QUESTIONS 1-5
  (non-gubernatorial year — executives absent; verification keyword sweep:
  JUDGE 0, SHERIFF 0, DA 0). The EXECUTIVE BLOCK is ballot-observed via
  Gloucester (Essex Co.) Nov-2018 specimen
  (https://www.gloucester-ma.gov/885/Election-Results-and-Specimen-Ballots)
  — SENATOR IN CONGRESS → GOVERNOR AND LIEUTENANT GOVERNOR → ATTORNEY
  GENERAL → SECRETARY OF STATE → TREASURER → AUDITOR → REPRESENTATIVE IN
  CONGRESS (6th) → COUNCILLOR → SENATOR IN GENERAL COURT → REPRESENTATIVE
  IN GENERAL COURT → DISTRICT ATTORNEY → CLERK OF COURTS → REGISTER OF
  DEEDS → QUESTION 1 — § 43A end to end. Gloucester Nov-2022 matches
  (… → DA → SHERIFF, no US Senate race in 2022). Stoughton (Norfolk Co.)
  Nov-2018 shows the regional-school-committee slot: DA → CLERK OF COURTS
  → REGISTER OF DEEDS → COUNTY COMMISSIONER → COUNTY TREASURER (TO FILL
  VACANCY) → 5× SOUTHEASTERN REGIONAL SCHOOL COMMITTEE → QUESTIONS 1-3
  (regional school committee after ALL county offices, before questions).
  DISCARDED: a Sudbury-hosted Nov-2022 specimen PDF initially used as a
  second corroborator — verification found its selectable text layer is a
  NON-PRINTING residue of a Nov-2018 EASTON (Bristol Co.) ballot layered
  under the real Sudbury 2022 print layer, and the researcher-reported
  sequence merged the two (SENATOR IN CONGRESS / CLERK OF COURTS /
  REGISTER OF DEEDS / COUNTY COMMISSIONER / REGIONAL SCHOOL COMMITTEE all
  render at 0.0 dark-pixel fraction = never printed). Nothing rests on it.
- Baseline delta: SIGNIFICANT. (1) Statewide executives BEFORE US House
  (US Sen → Gov/LtGov → AG → SOS → Treasurer → Auditor → US House).
  (2) Gov+LtGov one grouped contest. (3) Councillor tier between US House
  and state senate — baseline has no slot. (4) NO judicial block (judges
  appointed) — leave empty; don't map Clerk of Courts/Register of Probate
  into it. (5) NO municipal offices on state ballots. (6) School not a late
  tier — regional school committee inside the trailing SOC-ordered block.
  Matching: President first, US Senate second, senate-before-house, county
  after legislature, measures last.
- Notes: only the HEAD of the order is statutory — the county/
  regional-school tail is SOC discretion (§ 43A "in such order as the
  secretary of state may determine"), and observed practice varies by what
  is up: do NOT hard-code the county sub-order (the earlier "two ballots
  disagree" conflict was a PHANTOM built on the contaminated Sudbury file
  — deleted; the caution stands on the statute alone). Observed tail
  orders: Gloucester 2018/2022 DA → Clerk → Register [→ Sheriff 2022];
  Stoughton 2018 adds Commissioner → Treasurer(vacancy) → regional school
  committee. § 43A also governs state PRIMARIES with the same sequence
  (Gov/LtGov separate there — "as a group at a state election" limits
  grouping to the general). Candidate order within contest
  (informational): elected incumbents first alphabetically, then party
  candidates alphabetically, then others (c.54 § 42, § 41A same rule for
  Gov/LtGov groups); never implemented. Researcher discarded an unreliable
  WebFetch PDF summary (phantom Sheriff/DA contests) after local
  extraction contradicted it — all ballot claims come from local text
  extraction. Unverified search-artifact claim of a 2026 amendment to § 42
  (St. 2026, c.137) — not shown on malegislature.gov; recheck before the
  2027 cycle. SOC county-officers page URL has gone stale (now a generic
  Government overview) — quote real but re-point to the sibling
  gov-county.htm if cited again.

### IN — Indiana (FIPS 18) — GRADE A
- Authority: Indiana Election Administrator's Manual (2026 ed., rev. Apr
  2026), Indiana SOS / Election Division, "Public Questions & Straight Party
  Devices" + "General Election Ballot Office Order" (pp. 133-138) —
  reproduces and cites IC 3-11-2-12, IC 3-11-2-12.4(c), IC 3-11-2-12.5,
  IC 3-11-2-14, IC 3-11-2-15, IC 3-11-13-11(e)-(l), IC 3-11-14-3.5(e)-(m),
  IC 3-10-1-19.
  https://www.in.gov/sos/elections/files/2026-Election-Administrators-Manual-Revised-Apr-2026.pdf
  (accessed 2026-08-16). The literal statutes were subsequently read in
  full by the verification pass on TWO independent mirrors agreeing
  verbatim (law.justia.com "IN Code § 3-11-2-12 (2025)" + FindLaw "Current
  as of January 01, 2026", both via the r.jina.ai fetch proxy — iga.in.gov
  itself is a JS-gated SPA w/ key-gated API): IC 3-11-2-12(a) "The
  following offices shall be placed on the general election ballot in the
  following order after the public questions described in section 10(a)":
  (1) federal/state — President/VP → US Senator → "Governor and lieutenant
  governor" → SOS → "State comptroller (auditor of state)" (renamed
  P.L.227-2023; manual still says Auditor) → Treasurer → AG → US
  Representative; then (2) legislative, (3) circuit + county judicial,
  (4) county, (5) township, (6) city, (7) town — identical to the manual.
  Plus IC 3-11-2-10(a) (public questions first: "(1) Ratification of a
  state constitutional amendment. (2) Local public questions."; (b)
  straight party after them), IC 3-11-2-12.4(b) (at-large block "after the
  offices described in section 12 ... and before the offices described in
  section 12.9"), IC 3-11-2-12.5 (county-board discretion), IC
  3-11-2-12.9(a) (school board "after the offices described in section
  12.4").
- Office order: cautionary statement + instructions → PUBLIC QUESTIONS
  (statewide first, then local) → STRAIGHT PARTY section → President/VP →
  US Senator → GOVERNOR & LT GOVERNOR (single ticket) → SOS → Auditor →
  Treasurer → AG → US REPRESENTATIVE → State Senator → State Rep → circuit &
  county JUDICIAL block (Circuit Court judge → Superior Court judge →
  Probate judge (St. Joseph Co. only) → Prosecuting Attorney → Circuit Court
  Clerk) → county offices (Auditor → Recorder → Treasurer → Sheriff →
  Coroner → Surveyor → Assessor → Commissioner → Council) → township
  (Assessor → Trustee → Board → Marion small-claims judge/constable) → city
  (Mayor → Clerk → City Court judge → Council) → town → hoisted AT-LARGE
  block (vote-for-N partisan offices) → School Board (at-large then
  district) → JUDICIAL RETENTION QUESTIONS dead last. Statewide executives
  ALL precede US House. Presidential years carry only Gov+Lt Gov and AG
  (SOS/Auditor/Treasurer are midterm-cycle).
- Judicial: SPLIT. Elected trial courts (circuit/superior/St. Joseph
  probate) print EARLY — own block right after State Rep, before county.
  Supreme Court + Court of Appeals are RETENTION questions printing DEAD
  LAST after school board: Supreme Court justices → Court of Appeals → Tax
  Court → authorized local retentions (Lake/St. Joseph/Marion superior) →
  Allen/Vanderburgh nonpartisan judicial. Retention questions are expressly
  NOT public questions — never ride the measures block ("A state or local
  level judicial retention question is not a public question and appears
  later on the ballot", p.133).
- Measures: FIRST, not last — public questions print below the voter
  instructions at the top, statewide (constitutional amendments) before
  local. Order among multiple local questions undirected by statute (manual:
  order certified to the county election board). School referenda
  (IC 20-46-1/20-46-9, IC 6-1.1-20) are local public questions, even-year
  generals only.
- County discretion: narrow and enumerated — IC 3-11-2-12.5 lets the county
  election board move Prosecuting Attorney, Circuit Court Clerk, and County
  Offices ahead of the county judicial offices; local-question order
  undirected; multi-district offices alphabetical/numerical. No general
  delegation.
- School/special: school board nonpartisan, even-year November, after ALL
  partisan offices (incl. the hoisted at-large block), immediately BEFORE
  the retention block. At-large seats before district seats. Straight-party
  marks never reach school board.
- Corroboration: Lake County Nov-2024 general, Precinct 164-H3 16,
  https://www.lakecountyin.org/departments/voters/2024-GENERAL-ELECTION-BALLOT-SAMPLES/Pct%20164-H3%2016%20Act%2001-GENERAL.pdf
  (7pp): public question (constitutional amendment) p1 → straight party +
  President p2 → US Senator → Gov&LtGov → AG → US Rep D1 → State Senator p3
  → State Rep → county p4 → "NON-PARTISAN OFFICES" School City of Hammond
  at-large p5 → "JUDICIAL RETENTION QUESTIONS" Supreme → Appeals → Lake
  Superior local retentions pp6-7. Three further precincts (341-WT 01,
  008-CCT 04, 269-MER 10) identical.
- Baseline delta: SUBSTANTIAL, three structural. (a) Measures FIRST
  (statewide then local), not last. (b) Statewide executives precede US
  House (Senate → Gov/LtGov → SOS → Auditor → Treasurer → AG → US House).
  (c) Judicial SPLIT — elected trial courts + prosecutor + clerk EARLY
  (after state house, before county); only the retention block is late, and
  it is the very LAST thing, after school. Plus: straight-party section
  between measures and first contest; hoisted at-large block; township tier
  between county and city. Matching baseline: senate-before-house, county →
  municipal, school after local partisan offices, supreme → appeals within
  retention block.
- Notes: STRAIGHT PARTY leads the office portion — not a contest, but a
  contest-order model needs a slot for it; since 2016, a straight-party mark
  does NOT cast at-large votes (why IC 3-11-2-12.4 hoists those offices).
  AT-LARGE HOIST CAVEAT: the manual's Exception returns a
  mixed-at-large/district body to the normal IC 3-11-2-12 position
  (at-large listed first, then district); the STATUTE (12.4(c)) only says
  at-large before district and never itself returns the block — manual
  gloss and statute pull apart, so do NOT encode an unconditional hoist.
  IC 3-11-2-14 alphabetical ordering applies ONLY to the Allen/Vanderburgh
  local nonpartisan judicial offices dead last in the retention block, not
  statewide. CONFLICT RECORDED, NOT RESOLVED: all four Lake 2024 precincts
  print County CORONER BEFORE RECORDER — a STATUTORY violation
  (IC 3-11-2-12(a)(4) fixes Recorder (B) ahead of Coroner (E); 12.5
  authorizes moving the county block, never reordering WITHIN it);
  verification proved it visual (same column, Coroner 188pt above
  Recorder), systematic across precincts; a benign alphabetical county
  sort is not excluded by one county — needs a second county (Hamilton's
  2024 PDFs 404'd, Marion = address-lookup only).
  Primary difference (one line): IC 3-10-1-19 May primary drops straight
  party, puts Governor immediately after US Senator (BEFORE US Rep), omits
  convention-nominated executives, appends Political Party Offices last.
  Candidate order within contest (informational): party order by last SOS
  election finish in the county; nonpartisan judicial alphabetical
  (IC 3-11-2-14); never implemented.

### MO — Missouri (FIPS 29) — GRADE C
- Authority: NO statewide authority prescribes the order of OFFICES /
  candidate contests — verification swept ALL 365 sections of RSMo ch. 115,
  all of ch. 116, and every 15 CSR Division 30 chapter: zero office-order
  language (the only regulatory mention is the 15 CSR 30-10.010(12)
  DEFINITION of "ballot style", which acknowledges order exists without
  prescribing one). §115.237 governs ballot CONTENTS + candidate order
  only, https://revisor.mo.gov/main/OneSection.aspx?section=115.237
  (accessed 2026-08-16); §115.237.6 ORDERS the SOS to promulgate uniform
  ballot-layout rules — a duty NEVER DISCHARGED (zero occurrences of
  "115.237"/"layout" in the SOS's own complete rule inventory,
  https://www.sos.mo.gov/CMSImages/SOSMain/SOSRuleReview.pdf) — and yields
  to the vendor system; §115.247 vests ballot production in each election
  authority. TWO narrow statewide placement rules DO exist: (a) statewide
  MEASURES have a prescribed heading sequence — §116.230 sample-ballot
  form ("CONSTITUTIONAL AMENDMENTS" heading first, then "STATUTORY
  MEASURES"; §116.210 numbers amendments in passage order, §116.220
  letters propositions), binding on election authorities via §116.240
  (SOS sends the sample ballot) + §115.127.3 ("The election authority
  shall print the official ballot as the same appears on the sample
  ballot"); (b) retention judges go on a SEPARATE ballot: Mo. Const. art.
  V, §25(c)(1) "on a separate judicial ballot, without party designation",
  https://revisor.mo.gov/main/OneSection.aspx?constit=y&section=V++25(c)(1).
- Office order (OBSERVED, identical 4/4 jurisdictions — customary, not
  statutory): President/VP → US Senator → GOVERNOR → Lt Gov → SOS →
  Treasurer → AG → US Representative → State Senator → State Rep →
  county/city offices → [judicial block] → state measures → local measures.
  All statewide executives ABOVE US House, immediately below US Senate;
  internal order = Mo. Const. art. IV §17 list order (Gov, LtGov, SOS,
  Treasurer, AG). The SOS certification booklet (§115.401,
  https://www.sos.mo.gov/CMSImages/Elections/CertificationOfCandidatesNovember2024.pdf)
  lists offices in this same sequence — but §115.401 imposes no sequence,
  so that is the SOS's drafting convention, the likely SOURCE of the 4/4
  uniformity, not a directive. State Auditor = midterm cycle
  (2018/2022/2026), absent from all four Nov-2024 ballots.
- Judicial: TWO different things. (a) PARTISAN circuit/associate-circuit
  contests (non-Plan circuits) print INSIDE the partisan sequence, after
  state legislative and BEFORE county offices — ballot-confirmed by
  verification on Callaway (13th Circuit: "FOR CIRCUIT JUDGE CIRCUIT 13
  DIVISION 3", the one contested 2024 race, Osete-R vs Morrell-D, between
  State Rep 43 and Eastern District Commissioner) + single-name FOR CIRCUIT
  JUDGE listings in the same slot on Cass/StCharles/Cole/Ralls/Vernon;
  internal order circuit then associate circuit, ascending division.
  (b) Nonpartisan Court Plan RETENTIONS = party-free block, internally
  Supreme → Appeals (voter's district; no circuit-level retention appeared
  on ANY 2024 ballot read — Plan trial retentions exist but none were up
  outside the cities). Banner "OFFICIAL JUDICIAL BALLOT" + "VOTE ON EACH
  JUDGE" in most counties (constitutional basis art. V §25(c)(1) "separate
  judicial ballot"); St. Louis City prints retention items as ORDINARY
  contests with no banner, Vernon repeats per-office headings instead —
  the "separate ballot" is honored as a labeled section at best, sometimes
  not at all. Retention BLOCK placement: after county offices, BEFORE
  state measures in 7 of 9 jurisdictions read (StL City, KC, Jackson,
  Cass, StCharles, Cole, Ralls); dead LAST after Proposition A in Greene +
  Vernon — and the split tracks the ballot-PRINT FORMAT (the two dead-last
  counties share the JUDGE'S-INITIALS/END-OF-BALLOT vendor layout), not a
  county rule.
- Measures: after all offices (observed 4/4; position not statutory) —
  WITHIN the statewide block the order IS statutory: constitutional
  amendments (numbered in passage/submission order, §116.210) before
  statutory measures (lettered Proposition A..., §116.220), per the
  §116.230 sample-ballot form the election authority must reproduce
  (§115.127.3). Then county question → city/charter props last (observed).
  §115.245 fixes wording ("no other wording shall be used"), not order.
- County discretion: HIGH and dispositive — §115.247 "Each election
  authority shall provide all ballots for every election within its
  jurisdiction"; §115.237.6 vendor-system yield above. This is the C.
- School/special: ABSENT from November — §115.121.3 puts political
  subdivision + special-district officers on the APRIL general municipal
  election day; §162.291 elects school directors "at municipal elections".
  Greene Nov-2024: zero school/special OFFICE items across all 30 styles
  (12 Springfield styles do carry City of Springfield Question 1). Local
  QUESTIONS do appear in November.
- Corroboration: NINE jurisdictions read across research + verification,
  every one agreeing on the partisan spine Pres → US Sen → Gov → LtGov →
  SOS → TREASURER → AG (Treasurer before AG universal) → US House → State
  Sen → State House → [partisan circuit judges] → county; amendments 2,3,
  5,6,7 → Proposition A; local questions last. Research set: St. Louis
  City BEC
  https://www.stlouis-mo.gov/government/departments/board-election-commissioners/documents/upload/Nov24-All-Races-Sample-Ballot.pdf;
  Kansas City EB https://www.kceb.org/useruploads/Sample_Ballot-FINAL_11-24.pdf;
  Jackson County EB https://jcebmo.org/wp-content/uploads/Notice-Of-General-Election.pdf
  (NOTE: a §115.127 NOTICE OF GENERAL ELECTION, jurisdiction-grouped
  newspaper layout, not a ballot card — and KC EB + Jackson EB are two
  boards covering ONE county, so the research set = three jurisdictions,
  not four); Greene County
  https://vote.greenecountymo.gov/wp-content/uploads/2024/09/24GMOGRE_6-Samples-for-Website.pdf
  (60pp = 30 two-sided styles — 21 precinct, 8 federal-only, 1 interstate
  former-resident; "12" is just the Springfield styles). Verification
  added: Cass
  https://www.casscounty.com/DocumentCenter/View/4039/November-2024-Sample-Ballotpdf;
  St. Charles https://www.sccmo.org/DocumentCenter/View/25411/2024-11-05_Sample-Ballot;
  Cole (Wayback,
  https://web.archive.org/web/2024id_/https://colecounty.org/DocumentCenter/View/9690/November-5-2024-Combined-Sample-Ballot);
  Ralls https://rallscountymo.gov/wp-content/uploads/2024/09/sampleballots110524.pdf;
  Vernon https://vernoncountymo.org/wp-content/uploads/SAMPLE-BALLOTS-FINAL.pdf;
  Callaway (13th Circuit contested judge)
  https://callawaycountyclerk.com/wp-content/uploads/2024/09/November-5-Sample-Ballot.pdf
  (scanned/rotated — OCR'd). Boone publishes NO sample PDFs (per-voter
  lookup only); its Electionware results reports are election-definition
  order, not proven print order (2020+2022 match the 7-county majority;
  2024 report interleaves retention after circuit races — treated as a
  definition quirk, not evidence).
- Baseline delta: (1) statewide executives above US House (9/9 observed;
  Treasurer before AG); (2) partisan circuit judges between state house
  and county — baseline has no such slot; (3) municipal + school slots
  EMPTY in November (April cycle; "county before municipal" therefore
  unobserved); (4) Auditor never in presidential years (Boone 2022 report
  shows it in the executive run at midterms); (5) retention placement:
  majority = after county, before state measures (Greene/Vernon vendor
  layout puts it dead last). Matches baseline: retention internal order
  (Supreme → Appeals), state measures after offices, local questions last.
  NO override possible — office order is election-authority custom, not
  law (grade C); if a customary default is ever wanted anyway, the safe
  one is judicial-after-county-before-measures (7 of 9).
- Notes: (a) Retention-placement conflict RESOLVED as vendor-format split,
  not county policy: 7 of 9 (StL City, KC, Jackson, Cass, StCharles,
  Cole, Ralls) print retention after county before state measures; Greene
  + Vernon (the two sharing the JUDGE'S-INITIALS/END-OF-BALLOT layout)
  print it dead last. Still not statewide LAW — stays C. (b) Retention
  statewide at Supreme/Appeals; trial-level only in Plan counties (22nd
  StL City, 21st StL Cty, 16th Jackson, 7th Clay, 6th Platte, 31st
  Greene); other circuits elect partisan — now ballot-confirmed
  (Callaway). (c) April municipal day (§115.121.3, paired w/ §162.291
  "at municipal elections") explains the empty November municipal/school
  slots. (d) Primaries: §115.395 one ballot per party, filing order w/
  first-day random draw; §115.237.5 bans straight-ticket. (e) Candidate
  order (informational): party columns by prior gubernatorial vote
  (§115.239) → REP → DEM → LIB → BTR → GRN on all 2024 ballots; never
  implemented. (f) Extraction caveats: research-set sequences were
  content-stream order; verification re-derived Cass/StCharles/Cole/
  Ralls/Vernon from column-major coordinates and OCR'd Callaway — the
  spine held everywhere. Greene carried no State Senator contest in 2024
  (that link of the chain unobserved there).

### MD — Maryland (FIPS 24) — GRADE A
- Authority: Md. Code, Election Law § 9-210 "Arrangement of ballots —
  Candidates and offices" + § 9-211 (questions) + § 7-103 (question
  identifiers) + § 9-207 (State Board certifies "the content and
  arrangement of each ballot"),
  https://mgaleg.maryland.gov/mgawebsite/Laws/StatuteText?article=gel&section=9-210&enactments=false
  (and sibling section URLs); COMAR 33.10.01.17A ("The content and
  arrangement of all ballots shall comply with Election Law Article, Title
  9, Subtitle 2"), https://regs.maryland.gov/us/md/exec/comar/33.10.01.17.
  (all accessed 2026-08-16)
- Office order: § 9-210(a) verbatim sequence — (1) statewide: President/VP
  → GOVERNOR + LT GOV → Comptroller → Attorney General → US SENATOR (fifth!)
  → (2) Representative in Congress → (3) State Senate → House of Delegates
  → (4) county executive → county council/commissioner → (5) Baltimore
  City: Mayor → Council President → Comptroller → Council → (6) JUDICIAL →
  (7) county row offices: treasurer → State's Attorney → clerk of circuit
  court → register of wills → judge of orphans' court → sheriff → other
  partisan → (8) party offices → (9) nonpartisan offices. § 9-210(b)
  unlisted offices follow; § 9-210(c) at-large before district within any
  category.
- Judicial: MID-ballot block at (6) — after the county governing body,
  BEFORE county row offices/school/measures. Internal order INVERTED vs
  baseline: circuit court FIRST (contested, no party label per § 9-210(g)),
  then appellate "continuance in office" retention: Supreme Court of
  Maryland → Appellate Court of Maryland. Orphans' Court judges NOT here —
  county partisan block (7)(v), between register of wills and sheriff.
- Measures: all questions last, after every contest (ballot-confirmed;
  NOTE: no express statute says questions follow offices — § 9-211 only
  orders questions among THEMSELVES: new State Constitution → MD
  constitutional amendments → other General Assembly enactments → county
  charter → other county enactments → other). § 7-103: statewide/referendum
  = numeric ("Question 1"); county/municipal = alphabetic ("Question A"),
  letter series continuous across local types.
- County discretion: effectively none on order — statute fixes sequence;
  § 9-202(b) is the on-point cite: "Each local board shall place
  questions, names of candidates, and other material on the ballot in
  that county in accordance with the content and arrangement prescribed
  by the State Board"; § 9-207 (certification/display), § 9-204 "as
  uniform as possible", COMAR 33.10.01.17A. Local boards vary only
  content/pagination.
- School/special: Board of Education = nonpartisan → slot (9), LAST among
  offices, after judicial AND county row offices, immediately before
  questions. § 9-210(g) bars party labels; at-large BoE seat before
  district seats (§ 9-210(c)).
- Corroboration: four TEXT-LAYER official SBE ballots (archive path).
  Montgomery 2024,
  https://elections.maryland.gov/elections/archive/2024/general_ballots/Montgomery.pdf
  (514pp) — President → US Senator → US Rep D8 → Circuit Court Judge
  (Circuit 6) → Appellate Court At Large ×3 "For Continuance in Office" →
  BoE At Large → D2 → D4 → Question 1 (statewide) → Question A (county
  charter) → End of Ballot. Montgomery 2022 (gubernatorial leg),
  https://elections.maryland.gov/elections/archive/2022/general_ballots/Montgomery.pdf
  — Governor/LtGov → Comptroller → AG → US Senator → US Rep D8 → State
  Senator → House of Delegates → County Executive → Council At Large →
  Council D7 → Circuit Court Judge → Court of Appeals → Court of Special
  Appeals ×3 → State's Attorney → Clerk → Register of Wills → Sheriff →
  BoE. Baltimore City 2024 (same archive path) — Mayor → City Council
  President → Comptroller → Council D1 → Circuit Court C8 → "Justice,
  Supreme Court of Maryland" → Appellate ×4 (Supreme-before-Appellate
  confirmed). Baltimore County 2022 — State's Attorney → Clerk → Register
  of Wills → JUDGE OF THE ORPHANS' COURT → Sheriff → BoE (Orphans'-Court
  slot confirmed). The county-issued Montgomery 2024 voter-guide booklet
  (mcg.montgomerycountymd.gov CD-4 PDF) is RASTER (no text layer) — kept
  only as the researcher's original visual read; every claim above rests
  on the SBE text-layer ballots.
- Baseline delta: SUBSTANTIAL, five departures. (1) US Senate FIFTH —
  after Gov/LtGov, Comptroller, AG (statewide executives outrank the
  entire federal delegation below President). (2) US House after the whole
  statewide block. (3) Judicial MID-ballot — before county row offices,
  school, measures. (4) Judicial internal order INVERTED: trial (circuit)
  first, then Supreme, then Appellate. (5) School AFTER judicial (baseline
  inverts). Matches: senate-before-house, measures last.
- Notes: court renames 2022 (Court of Appeals → Supreme Court of Maryland;
  Court of Special Appeals → Appellate Court) — 2022 ballots old names,
  2024 new. Circuit judges: PARTISAN primaries (cross-filing allowed), no
  party label in the general (§ 9-210(g)) — longstanding, NOT a 2022
  change. Montgomery elects no Orphans' Court (circuit judges sit as it) +
  no county treasurer → those rows verified via Baltimore County/other
  counties or authority-only (treasurer = authority-only, no ballot
  exercised it). Party offices (8) = primary-only in practice, empty slot
  on generals, unconfirmed. Municipal outside Baltimore City = § 9-210(b)
  catch-all after nonpartisan, unconfirmed on a ballot. Prompt-premise
  fix: COMAR 33.06 is "Petitions", NOT ballots — ballots live at COMAR
  33.10.01.17. Wording variance recorded: statute/ballots "continuance in
  office" vs SBE candidacy page "retention in office" — cosmetic.
  Latent edge (§ 8-802(b)): the nonpartisan-BoE rule "does not apply ...
  if Title 3 of the Education Article requires a partisan election" — a
  partisan BoE would land in § 9-210(a)(7)(vii) with the county row
  offices, not last; no MD county currently does this. Candidate order
  within contest (informational): same-party candidates alphabetical by
  surname (§ 9-210(j)(3)); never implemented. Fetch gotcha: SBE live
  ballot paths soft-404 (HTTP 200 HTML error page) — the
  /elections/archive/<year>/general_ballots/<County>.pdf path serves the
  real PDFs.

### WI — Wisconsin (FIPS 55) — GRADE A
- Authority: Wis. Stat. § 5.64(1)(d) (general election ballots),
  https://docs.legis.wisconsin.gov/statutes/statutes/5/ii/64/1/d —
  incorporates the office list of § 5.62(3),
  https://docs.legis.wisconsin.gov/statutes/statutes/5/ii/62/3 (both
  accessed 2026-08-16). Supporting: § 5.02(5) (general-election scope),
  § 5.02(21) (spring election), § 5.64(2)(am)+(c) (referendum ballot),
  § 5.655 (consolidated ballot), § 7.08(1)(a) + § 7.10(1)(a) (WEC
  prescribes form, county clerk conforms "in substantially the same form").
- Office order: President/VP (presidential years) OR Governor/Lt Governor
  (gubernatorial years) FIRST, then § 5.62(3): "governor, lieutenant
  governor, attorney general, secretary of state, state treasurer, U.S.
  senator, U.S. representative in congress, state senator, representative
  to the assembly, district attorney and the county offices". Statewide
  executives ABOVE US Senate + US House. Gov + LtGov = one joint contest at
  the general (§ 5.64(1)(f)). DA = own block between Assembly and county.
- Judicial: NONE in November — § 5.02(5) general election elects "state
  officers other than the state superintendent and judicial officers";
  § 5.02(21) puts judicial at the April spring election (§ 5.60 ballots).
  Zero Supreme/Appeals/Circuit contests across 302pp of Brown County
  Nov-2022 + Nov-2024 samples. Late judicial block = empty for WI.
- Measures: LAST, after every office. § 5.64(2)(am) nominally separate
  referendum ballot; § 5.655 consolidation = universal practice. Observed
  sub-order: State → County → School District. § 5.64(2)(c) separates
  state/county from municipal/special-district referenda, numbers state
  referenda chronologically. (Measures-last is observed practice on the
  consolidated ballot, not statutory position — statute only mandates the
  separate-ballot fiction.)
- County discretion: minimal on order — WEC prescribes forms (§ 7.08(1)(a)),
  clerks conform (§ 7.10(1)(a)). County-block internal order comes from the
  WEC template, not statute (§ 5.62(3) says only "and the county offices";
  EL-203ms prints Sheriff → Coroner → Clerk of Circuit Court for the
  gubernatorial cycle). TRAP: "Clerk of Circuit Court" is an ADMINISTRATIVE
  county office, not a judicial contest — naive "Circuit Court" keyword
  matching false-positives here.
- School/special: school board = April spring nonpartisan (§ 5.02(21)
  "educational officers"), effectively never in November — zero regular
  contests across every ballot read; EXCEPTION: SPECIAL school-board
  elections for unexpired terms can land on a November ballot (Milwaukee
  Public Schools Board District 4 special appeared in a subset of 2024
  Milwaukee city wards, per the county election notice — secondary-sourced,
  .gov hosts WAF-blocked). School-district REFERENDA do appear in November
  (§ 121.91(3) allows "partisan primary or general election"; Denmark
  School District 2-question referendum on the Nov-2022 ballot).
- Corroboration: Brown County (Green Bay) official samples + Milwaukee
  County (Whitefish Bay) samples + the WEC blank template itself. WEC
  EL-203ms general-election optical-scan template (Rev. 2022-07),
  https://elections.wi.gov/sites/default/files/documents/EL-203ms%20-%202022%20-%20Optical%20Scan%20-%20General%20Election%20-%20Oval.pdf
  — the template prints the order: Statewide (Gov/LtGov, AG, SOS,
  Treasurer) → Congressional (US Senator, US Rep) → Legislative (State
  Senator, Assembly) → County (Sheriff, Coroner, Clerk of Circuit Court) →
  Referendum (PDFs under /sites/default/files/ fetch fine; the HTML pages
  403). WEC Election Administration Manual (Aug 2024, p.41,
  https://elections.wi.gov/sites/default/files/documents/EA%20Manual-August%202024.pdf):
  "For optical scan or consolidated ballots: a. There must be a separate
  REFERENDUM section on the ballot. ... c. The title of the ballot must
  include the language 'and Referendum.'" (citing §§ 5.51(5), 5.60(7),
  5.64(2), 5.655) — referendum = bottom section of the SAME ballot, not a
  physically separate one. Whitefish Bay (Milwaukee Co.) Nov-2024,
  https://www.wfbvillage.gov/DocumentCenter/View/2201/November-5-2024-Sample-Ballot
  — Pres/VP → US Sen → US Rep D4 → State Sen D8 → Assembly D23 → DA →
  County Clerk → Treasurer → Register of Deeds → Referendum/State;
  Whitefish Bay Nov-2022,
  https://www.wfbvillage.gov/DocumentCenter/View/1684/Whitefish-Bay---Sample-November-8-2022
  — Gov/LtGov → AG → SOS → Treasurer → US Sen → US Rep D4 → Assembly D23 →
  Sheriff → Clerk of Circuit Court → Referendum/County. Portage County
  Nov-2024 (76pp) identical structure. Brown County Nov-2022 gubernatorial,
  https://www.browncountywi.gov/i/f/files/County-Clerk/Elections/Sample%20Ballots/2022/Nov/20221108%20Sample.pdf
  — headers Statewide (Gov/LtGov, AG, SOS, Treasurer) → Congressional →
  Legislative → County (Sheriff → Coroner → Clerk of Circuit Court,
  matching EL-203ms exactly) → Referendum (County → School District),
  identical across all wards (160pp). Nov-2024 presidential,
  https://www.browncountywi.gov/i/f/files/County-Clerk/Elections/Sample%20Ballots/2024/20241105%20Sample.pdf
  — President/VP → US Senator → US Rep → State Senator → Assembly → DA →
  County Clerk → Treasurer → Register of Deeds → Referendum (State)
  (142pp). Confirms executives-before-federal, DA slot, measures-last,
  zero judicial/school.
- Baseline delta: SUBSTANTIAL. (1) Statewide executives ABOVE US Senate +
  US House. (2) Gov/LtGov joint contest tops the midterm ballot. (3) DA
  block between state house and county — baseline has no DA slot. (4) No
  municipal block in November. (5) No school block in November. (6) Judicial
  block empty. (7) Measures last — matches.
- Notes: hard calendar split — November = partisan federal/state/DA/county;
  April spring = ALL judicial, state superintendent, school board,
  municipal, county supervisors + county executive. County block alternates
  by cycle (Sheriff/Coroner/Clerk of Circuit Court 2022 vs County Clerk/
  Treasurer/Register of Deeds 2024). § 5.62(3) is BY ITS OWN TERMS the
  partisan-PRIMARY list ("whenever these offices appear on the partisan
  primary ballot"); the general adopts it via § 5.64(1)(d)'s
  cross-reference (Gov and LtGov voted separately at the primary, jointly
  at the general). Brown Nov-2024 also shows a "Special Congressional
  Election" section printed ABOVE the regular Congressional section —
  special federal contests outrank their regular peers. Candidate order
  within contest (informational): WEC lot draw (§ 5.60(1)(b)); party
  columns left-to-right by last presidential/gubernatorial vote
  (§ 5.64(1)(b)); never implemented. Prompt-premise fix: § 5.58 = spring
  PRIMARY ballots, not partisan; partisan primary = § 5.62.

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
