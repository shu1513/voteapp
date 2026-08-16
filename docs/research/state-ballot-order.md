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
  recorded, unresolved — and EXCLUDED from the grade-A scope: pypdf
  extracted King's initiatives-to-legislature as 2124 → 2117 → 2109 vs RCW
  29A.72.290's serial-number order — likely a multi-column extraction
  artifact, needs visual read or second county; until then no override may
  encode the WITHIN-group serial order of state measures (immaterial to
  the ranker anyway — measures share one tier, sub-ordering individual
  measures is below its granularity). The measures-BEFORE-offices
  placement itself is unaffected (statute + ballot agree).
  Corroboration URL sits on a /current-election/202411/ path — expect rot.

### AZ — Arizona (FIPS 04) — GRADE A (scoped — see Notes)
- Grade scope: A covers the partisan sequence, the nonpartisan-section
  position, retention placement, and measures. It EXCLUDES the placement
  of contested (elected) Superior Court judges in the 11 non-retention
  counties — the one leg with conflicting evidence (2025 EPM nonpartisan
  slot 5 vs La Paz 2022 printing it in the partisan section) — so no
  override may encode that sub-office until the conflict resolves.
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
  486). Late verification round added: (a) additional Pinal 2024 styles
  (https://www.pinal.gov/DocumentCenter/View/21383 = style 042G02 w/
  school bond, .../21430 = 061G04 w/ JP, .../21505 = 093G07 w/ town
  office+measure; index https://www.pinal.gov/QuickLinks.aspx?CID=634) —
  SCHOOL BOARDS PRINT AFTER THE JUDICIAL RETENTION BLOCK on every style
  carrying both (Apache Junction USD / Casa Grande UHS board members
  after the Supreme + Superior retentions), and Justice of the Peace is
  the LAST partisan contest directly before the Section Two header; local
  props print after ALL statewide props in number order, countywide (486)
  before sub-county (487/492). (b) La Paz County Nov-2022 all-styles book
  (https://www.co.la-paz.az.us/DocumentCenter/View/7966/Nov-8th-Sample-Ballots-PDF)
  — the EXECUTIVE SPLIT IN PRINT on a gubernatorial ballot: US Senator →
  US Rep → GOVERNOR → State Senator → State Rep → SOS → AG → Treasurer →
  Superintendent → Mine Inspector → Corporation Commissioner → county
  offices → JP → Constable; Section Two = Supreme Court retentions →
  Appeals Div I → school/town offices; measures 128-132 → 209/211 →
  308-310.
- Baseline delta: SUBSTANTIAL, three departures. (1) Statewide executives
  SPLIT by the legislature: Gov(+LtGov) → state senate → state house → SOS
  → AG → Treasurer → Supt → Mine Inspector → Corp Comm — only Governor
  precedes the legislature. (2) Judicial NOT late — opens the nonpartisan
  section ahead of school/college/special/municipal (internal supreme →
  appeals → trial matches). (3) Municipal comes after school and LAST among
  candidate races. Matching: federal-first top, senate-before-house, county
  before local, measures last.
- Notes: the Maricopa-canvass question is now SETTLED by printed ballots —
  the canvass (school/city before judicial) is jurisdiction-grouped
  reporting, and Pinal styles carrying BOTH contests print judicial
  retention FIRST, school boards after, matching the EPM; the EPM default
  holds on every printed ballot examined. 2022 executive split now
  ballot-observed (La Paz above); the Gov+LtGov joint ticket still debuts
  Nov-2026 (2022 Prop 131). CONFLICT RECORDED, NOT RESOLVED — and
  EXCLUDED from the grade-A scope (see Grade scope above): La Paz 2022
  prints its ELECTED Superior Court judge INSIDE Section One (partisan),
  between Clerk of the Superior Court and JP — the 2025 EPM puts
  contested Superior Court at NONPARTISAN slot 5, and § 16-502(J) itself
  delegates nonpartisan order to the election officer, so neither reading
  can be assumed. The 2022 ballot predates the 2023/2025 EPM editions
  (possibly superseded practice); resolve via an elected-judge county
  (Apache/Cochise/Yavapai...) Nov-2026 sample. Community-college
  boards never observed on any style (no seats up) — placement inferred
  from the EPM only. EPM edition
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

### TN — Tennessee (FIPS 47) — GRADE B
- Grade rationale: B is a SOURCING cap, not a content doubt. The plan gates
  grade A on PRIMARY authority, and grade A gates a code override — TN's
  controlling statute text is mirror-sourced and by this entry's own
  caveat "pending primary re-verification", so it cannot clear that gate,
  however low the practical risk (two mirrors letter-identical + official
  ballots matching). RESTORE PATH to A: read § 2-5-208 on an official
  source (LexisNexis-hosted official T.C.A. if access opens, or an
  official SOS/Coordinator of Elections manual reproducing the complete
  order). Everything below stands as researched.
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
- County discretion: NONE over state-ballot order — c.54 § 40 "All ballots
  for use in elections of state officers shall be prepared and furnished
  by the state secretary". Counties play no role in the cited statutes;
  elections are administered by city/town clerks, whose ballot authority
  covers only their own MUNICIPAL elections (outside this entry's scope).
  Only order discretion on the state ballot = the STATE secretary's, over
  the trailing block + question order. County govts
  abolished in Middlesex/Franklin/Hampden/Worcester/Hampshire/Essex/Suffolk
  (1997-2000) but every county still elects Register of Deeds, Register of
  Probate, Sheriff, DA (SOC: "All counties, even those with abolished
  governments, elect their own registers of deeds, registers of probate,
  sheriffs, and district attorneys."); intact-government counties add
  Commissioners + Treasurer.
- School/special: regional district school committees elected district-wide
  DO print on the state general ballot — c.54 § 42 dedicated paragraph
  (separate paper ballot allowed if they don't fit). Their PLACEMENT
  within the trailing block is AUTHORITY-ONLY (no verified ballot carries
  one; a Stoughton-2018 observation was retracted as fabricated).
  Ordinary municipal school committees = separate city/town elections,
  never on the state ballot.
- Corroboration: Watertown (Middlesex Co.) SOC-issued "STATE ELECTION
  OFFICIAL SPECIMEN BALLOT" Nov-5-2024, Pcts. 1-8,
  https://content.civicplus.com/api/assets/ma-watertown/b1d6201b-6e9f-41e3-90b2-8176ed227b38?cache=1800
  (index https://www.watertown-ma.gov/specimen-ballots) — ELECTORS OF
  PRESIDENT AND VICE PRESIDENT → SENATOR IN CONGRESS → REPRESENTATIVE IN
  CONGRESS → COUNCILLOR → SENATOR IN GENERAL COURT → REPRESENTATIVE IN
  GENERAL COURT → CLERK OF COURTS → REGISTER OF DEEDS → QUESTIONS 1-5
  (non-gubernatorial year — executives absent; verification keyword sweep:
  JUDGE 0, SHERIFF 0, DA 0). The EXECUTIVE BLOCK is ballot-observed via
  TWO SOC-issued Nov-2022 specimens, coordinate-extracted in the main
  review session: Danvers (Essex Co.),
  https://www.danversma.gov/DocumentCenter/View/1132/State-Election---November-8-2022---Sample-Ballot
  — col 1: GOVERNOR and LIEUTENANT GOVERNOR (y272) → ATTORNEY GENERAL →
  SECRETARY OF STATE → TREASURER → AUDITOR → REPRESENTATIVE IN CONGRESS
  (y992); col 2: COUNCILLOR → SENATOR IN GENERAL COURT → REPRESENTATIVE IN
  GENERAL COURT → DISTRICT ATTORNEY → SHERIFF; QUESTIONS 1-4 on the back —
  § 43A's executive run end to end (no US Senate race existed in 2022);
  Reading (Middlesex Co.),
  https://www.readingma.gov/DocumentCenter/View/7180/2022-11-08-State-Election-Sample-Ballots
  — identical sequence, both precinct sheets. US-Senate-versus-Governor
  relative order remains statute-only (never co-observed: 2022 had no
  Senate race, 2024 no executives — same shape as TN pre-2018).
  RETRACTED BY VERIFIER (recorded): a verification report initially also
  cited Gloucester 2018/2022 and Stoughton 2018 specimens plus a
  dark-pixel test — on interrogation the verifier admitted it never
  fetched those documents (it wrote them up before its own subagent
  reported; the subagent never returned). All three ballots and the pixel
  test are STRUCK; nothing above rests on them. DISCARDED corroborator:
  a Sudbury-hosted Nov-2022 specimen PDF — the verifier's OWN decode
  (which stands) shows a real Sudbury/Middlesex 2022 print layer
  (Healey/Driscoll, Campbell, Galvin, Goldberg, DiZoglio, Middlesex
  Sheriff) with Nov-2018 residue in the raw text stream (Warren/Diehl
  Senate race, Baker/Polito) — contaminated file, nothing rests on it,
  and the county sub-order "conflict" once built on it stays deleted.
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
  orders (verified ballots only): Watertown 2024 Clerk of Courts →
  Register of Deeds; Danvers + Reading 2022 DA → Sheriff. § 43A also
  governs state PRIMARIES with the same sequence
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

### IN — Indiana (FIPS 18) — GRADE A (scoped — see Notes)
- Grade scope: A covers the section structure (measures → straight party →
  partisan sequence → nonpartisan school → retention last) and the
  inter-tier office order. It EXCLUDES the county block's INTERNAL order,
  where the only sampled county violates the statute (Coroner before
  Recorder, unresolved). Immaterial to overrides anyway: our ranker holds
  all county offices at one tier, so within-county order is below its
  granularity and no override would encode it.
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
  (Mayor → Clerk → City Court judge → Council) → town → at-large partisan
  offices, hoisted here CONDITIONALLY (IC 3-11-2-12.4; where a body has
  both at-large and district seats on one ballot, the manual returns them
  to normal § 12 position, at-large first — see Notes caveat) → School
  Board (at-large then district) → JUDICIAL RETENTION QUESTIONS dead
  last. Statewide executives
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
- Office order (OBSERVED, identical in all NINE jurisdictions read —
  customary, not statutory): President/VP → US Senator → GOVERNOR → Lt Gov → SOS →
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
- Office order: § 5.64(1)(d) hoists President/VP (presidential years) or
  the joint Governor/Lt Governor contest (gubernatorial years) to the top,
  "then the REMAINING offices in the order designated under s. 5.62(3)" —
  so the two effective sequences are: presidential years President/VP →
  [AG → SOS → Treasurer, if up] → US Senator → US Rep → State Senator →
  Assembly → DA → county; gubernatorial years Gov/LtGov (once — hoisted,
  not repeated from the list) → AG → SOS → Treasurer → US Senator → US Rep
  → State Senator → Assembly → DA → county. § 5.62(3) list verbatim:
  "governor, lieutenant governor, attorney general, secretary of state,
  state treasurer, U.S. senator, U.S. representative in congress, state
  senator, representative to the assembly, district attorney and the
  county offices". Statewide executives ABOVE US Senate + US House. Gov +
  LtGov = one joint contest at the general (§ 5.64(1)(f)). DA = own block
  between Assembly and county.
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

### CO — Colorado (FIPS 08) — GRADE A (scoped — see Notes)
- Authority: C.R.S. § 1-5-403(5) (full office order + judicial-retention
  placement in one sentence-chain),
  https://content.leg.colorado.gov/sites/default/files/images/olls/crs2024-title-01.pdf
  (CRS 2024 Title 1; accessed 2026-08-16). Supplemented by § 1-5-407(5)(a)
  (measures after candidates + tier order), § 1-5-407(5.5) (separate-ballot
  option), § 1-5-203(1)(a)(I) (SOS certifies "the order of the ballot and the
  ballot content"), § 1-1-104(39)/(41)/(42) (school = odd-year Nov; RTD =
  concurrent with general; special district = May odd-year), § 32-9-119.3(2)
  (RTD sales-tax questions "immediately following any statewide amendments
  and propositions",
  https://content.leg.colorado.gov/sites/default/files/images/olls/crs2024-title-32.pdf),
  and 8 CCR 1505-1 Rule 4.5.2 (SOS Election Rules, Rule 4 as adopted
  9/09/2024,
  https://www.sos.state.co.us/pubs/rule_making/CurrentRules/8CCR1505-1/Rule4.pdf)
  for measure numbering/ordering.
- Office order: President/VP → US Senator → US Representative → joint
  Governor/Lt. Governor (one contest, one oval) → other state candidates →
  legislative (senate/house) → district attorney → RTD board of directors →
  other district offices greater than a county → county commissioner → county
  clerk & recorder → county treasurer → county assessor → county sheriff →
  county surveyor → county coroner ("The positions on the ballot shall be
  arranged as follows: First, candidates for president and vice president…").
  Governor sits AFTER US House — matches the baseline's federal-first spine.
  The statute does NOT order "other state candidates" internally; the SOS
  fixes it per cycle — full chain SOS → Treasurer → AG → State Board of
  Education (At-Large, then CD seat) → CU Regent observed on Douglas 2022
  (the only ballot with the whole chain; Arapahoe 2024 shows only
  SBE-before-Regent, Denver 2024's state head is a single Regent contest),
  and CONFIRMED by the official SOS "2026 General Election Ballot Order"
  page (verify pass;
  https://www.sos.state.co.us/pubs/elections/vote/generalBallotOrder.html,
  lot drawing July 28, 2026): US Senate → US House → Governor/Lt Governor
  → SOS → Treasurer → AG → State Board of Education → CU Regent → State
  Senate → State House — official artifact, though NOT the § 1-5-203
  certificate itself (still unlocated). Colo. Const. art. IV § 1(1) covers
  only SOS → Treasurer → AG; SBE and Regent placement is practice + the
  SOS page. Unlisted offices: clerk uses "substantially the form
  prescribed by this section".
- Judicial: Colorado elects NO judges — retention questions only. Fixed
  internal order supreme court → court of appeals → district court →
  county court, and precedence over all measures, from § 1-5-403(5)'s
  final sentence ("…shall be placed on the ballot in that order and shall
  precede the placement of ballot issues…"); the block's position AFTER
  every candidate contest is observed practice (Denver/Arapahoe/Douglas —
  the statute fixes only order-within-block and before-measures).
- Measures: after all offices and after retention. Tier order
  (§ 1-5-407(5)(a)): referred amendments → initiated amendments → referred
  propositions → initiated propositions → county → municipal → school
  district → other subdivisions in more than one county → other subdivisions
  wholly within one county. RTD sales-tax-rate questions carve out to
  "immediately following any statewide amendments and propositions"
  (§ 32-9-119.3(2)). Within a grouping: referred before initiated, tax →
  excess-revenue retention → debt → other (Rule 4.5.2(c)). CONFLICT
  (practice vs statute, unresolved — recharacterized by the verify pass):
  Rule 4.5.2(e) expressly DEFERS ordering to the statute ("Ballot issues
  from the various political subdivisions must be ordered on the ballot as
  provided in section 1-5-407(5), C.R.S") — its 1A-7Z letters are
  DESIGNATIONS, not an order (note (e)(2)'s initiated-local series numbers
  greater-than-county 500-599 BEFORE wholly-within 600-699, the opposite
  sense of (e)(3)'s 6/7 lettering). But observed practice does not follow
  the statute's last two tiers: Denver 2024 printed DDA 6A (wholly within)
  before RTD 7A (greater than a county); Arapahoe 2024 printed Polo
  Reserve Metro 6A before RTD 7A AND ran its school tier 5A/5B/5C before
  4A/4B. Counties also differ in intra-tail method — Denver + Douglas
  ascending designation series, Arapahoe alphabetical by subdivision
  (Rule 4.5.2(e)(5) grouping). Neither 2024 RTD 7A question was a
  § 32-9-119.3(1) sales-tax-RATE increase (both revenue-retention), so
  the § 32-9-119.3(2) carve-out does not explain it. Treat within-tail
  sequencing below the school tier as county-variable.
- County discretion: limited but real — (1) unlisted offices via the
  "substantially the form" catch-all (municipal offices printed after county,
  before retention, on Arapahoe 2024 + Douglas 2022 — clerk practice, no
  statutory sentence names that position); (2) § 1-5-407(5.5) separate-ballot
  option with a materially different measure order (candidates → tax
  increases → debt increases → citizen petitions → referred measures);
  (3) Rule 4.5.2(e)(5) lets the designated election official assign final
  measure numbers, grouping each subdivision's measures together.
- School/special: school tier structurally EMPTY on an even-year general —
  school director elections are odd-year November (§ 1-1-104(39)); special
  district directors = May odd-year (§ 1-1-104(42)). Only their MEASURES ride
  the general ballot. Exception: RTD directors print in a prescribed slot
  after district attorney (§ 1-1-104(41) + § 1-5-403(5)).
- Corroboration: Denver 2024 general composite
  (https://www.denvergov.org/files/assets/public/v/1/clerk-and-recorder/documents/elections-division/2024/ballot-measures/2024-general-composite-sample-engspa.pdf)
  — federal → state → DA → RTD, retention block supreme→appeals→district→
  county between offices and measures, statewide measure tiers in statutory
  order, DPS 4A → DDA 6A → RTD 7A tail; Arapahoe 2024 general
  (https://files.arapahoeco.gov/Your%20County/Arapahoe%20Votes/Documents/Records%20And%20data/Past%20Elections%20File%20Library/2024/2024%20GENERAL%20ELECTION%20SAMPLE%20BALLOT-ENG-20241003_0.pdf?t=202504090233470)
  — municipal offices (Cherry Hills Village) after county, before retention;
  printed measure headers State → County → Municipal → School District →
  Special District; Douglas 2022 general composite
  (https://www.douglas.co.us/documents/2022-general-election-composite-sample-ballot.pdf/)
  — Governor/Lt Gov pair after US House and before SOS → Treasurer → AG →
  SBE → Regent; county internal order exactly as § 1-5-403(5) (cycle 2022).
  All fetched curl + browser UA → pypdf. No office/judicial/measure-placement
  conflict; only the Rule-vs-statute tier inversion above.
- Baseline delta: (1) judicial = retention questions only, and the block
  lands after municipal/local OFFICES but before measures — baseline's late
  judicial block shape holds, but as Yes/No questions with zero elected
  judgeships; (2) RTD directors = prescribed contest between district
  attorney and county offices — no baseline analogue (multi-county special
  district ABOVE county tier); (3) school tier empty (odd-year); (4) county
  office internal order statutory (commissioner → clerk → treasurer →
  assessor → sheriff → surveyor → coroner); (5) DA prints before county
  offices (judicial-district office above county tier).
- Notes: § 1-5-402(1)(b) orders primaries similarly (no President, no
  Gov/LtGov pair, no RTD line) — general-only above. Candidate order within
  a contest: major parties by lot drawn July 1-15, then minor parties, then
  other organizations, each by lot (§ 1-5-404); President/VP and Gov/LtGov
  pairs alphabetical by top-of-ticket — informational only ("candidates
  are ordered by a lot drawing with the exception of the office of
  Governor and Lt. Governor, which are ordered by the last name of the
  gubernatorial candidate" — SOS generalBallotOrder.html). "Other state
  candidates" internal order is observed practice + the official SOS
  ballot-order page, not statute — flagged for the override decision (the
  § 1-5-203 certificate itself remains unlocated). GRADE SCOPE: A covers
  the office order, the judicial-retention block placement, and measure
  tiers through school district. NOT covered by the A: (a) sequencing
  WITHIN the school tier and of the final two "other political
  subdivisions" tiers — observed county practice contradicts
  § 1-5-407(5)(a) and differs between counties; (b) municipal-office
  placement (after county, before retention) — clerk practice under the
  "substantially the form" catch-all, not statute.

### MN — Minnesota (FIPS 27) — GRADE A
- Authority: Minn. R. 8250.1810 ("Format of ballots for optical scan
  systems"), subps. 5, 6, 10, 11, 18,
  https://www.revisor.mn.gov/rules/8250.1810/ (accessed 2026-08-16) — the
  only live rule FIXING CONTEST ORDER in ch. 8250 (8250.1800 repealed, 34
  SR 1561; 8250.0375 "FORM OF JUDICIAL BALLOT" is live but governs the
  overflow judicial ballot's form only); current text includes the June 1,
  2026 amendments (50 SR 971), in force for Nov 3, 2026. Statutory backbone: Minn. Stat. § 204D.13 subd. 1
  (partisan order + delegation: "The candidates for state offices shall
  follow in the order specified by the secretary of state",
  https://www.revisor.mn.gov/statutes/cite/204D.13); § 204D.11 subds. 1, 6
  (single "state general election ballot"; separate judicial ballot only
  when one card can't fit — card split, not reorder; implementing rule
  8250.0375); § 204D.15 subd. 1 (amendment titles). SOS implementation: 2026
  + 2024 statewide Example Ballots + Explanatory Notes (subp. 18: "The
  official ballots must conform in all respects to the example ballot").
- Office order: President/VP → US Senator → US Representative → State
  Senator → State Representative → Governor and Lieutenant Governor →
  Secretary of State → State Auditor → Attorney General → county block
  (Commissioner → Auditor → Treasurer → Auditor-Treasurer → Recorder →
  Sheriff → County Attorney → Surveyor → Coroner → Park Commissioner → Soil
  & Water Conservation District Supervisor → Conservation District
  Supervisor) → County Questions → city block
  (Mayor → Council → Clerk → Treasurer) → City Questions → town block → Town
  Questions → School Board Member → School District Questions → special
  district offices → Special District Questions → Judicial Offices.
  LEGISLATURE PRINTS BEFORE GOVERNOR — statewide executives are the TAIL of
  the State Offices block, not its head. No elected State Treasurer.
  At-large seats before district seats of same type; numbered seats in
  numerical order. § 204D.13 subd. 1 orders only the partisan backbone
  (US Sen → US Rep → state senator → state representative) and delegates the
  rest to the SOS; Rule 8250.1810 subp. 6 IS that specification — no
  conflict between the instruments. (Earlier seed's "204D.13 subd 2" was off
  by one — subd. 2 is presidential CANDIDATE order, not office order.)
- Judicial: DEAD LAST — after every office AND every question. Subp. 6:
  "Judicial offices must follow special district offices and appear in the
  following order: Chief Justice - Supreme Court / Associate Justice -
  Supreme Court / Judge - Court of Appeals / Judge - District Court."
  Numbered seats numerical, except a court's single-candidate seats print
  after that court's contested seats. § 204D.11 subd. 6 separate judicial
  ballot = overflow card only (8250.0375 subp. 1).
- Measures: two treatments. (1) Statewide constitutional amendments print
  EARLY — subp. 5 puts "Constitutional Amendments" third, immediately after
  State Offices, before County Offices. (2) Everything else interleaves per
  jurisdiction — subp. 10: "Ballot questions must be printed after offices
  of the same jurisdiction." No trailing measures block exists; judicial
  follows all of it. Amendments: SOS titles + AG approval (§ 204D.15
  subd. 1); failure-to-vote-counts-as-no notice beneath the heading
  (subp. 11); multiple amendments numbered.
- County discretion: narrow — auditor prepares ballots "subject to the rules
  of the secretary of state" (§ 204D.11 subd. 1) and must conform to the
  example ballot (subp. 18). Genuine delegation: unlisted county offices
  follow SWCD Supervisor "in the order determined by the county auditor"
  (subp. 6); position-by-lot when candidates ≤ seats (subp. 7); question
  titles written locally (subp. 10).
- School/special: School Board Member = own type after Town Questions;
  school questions immediately behind school offices; special districts
  (rule's example: Hospital District Board Member) between School District
  Questions and Judicial. Split precincts carry school district number on
  the style (subp. 1).
- Corroboration: SOS 2026 example ballot (gubernatorial year — settles
  Governor slot; "November 3, 2026",
  https://www.sos.mn.gov/media/5c1d4i4a/2026-primary-general-and-judicial-example-ballots.pdf)
  + SOS 2024 example ballot
  (https://www.sos.mn.gov/media/ejvpcipi/2024-primary-general-and-judicial-example-ballots.pdf)
  + REAL Hennepin County 2024 general (Golden Valley P-4,
  https://goldenvalleymn.gov/DocumentCenter/View/4326 — amendment before
  local tiers, school questions interleaved, judicial dead last,
  Supreme→Appeals→District) + REAL Rice County 2024 general (Northfield W4
  P2, https://www.northfieldmn.gov/DocumentCenter/View/21355/W4-P2 —
  full-stack: SWCD as county office, county question between county and
  city offices, judicial after all questions; small county, completeness
  corroborator) + Anoka County 2026 PRIMARY (Coon Rapids W-5 P-3,
  https://coonrapidsmn.gov/DocumentCenter/View/3957/Coon-Rapids-Ward-5-Precinct-3-PDF
  — supporting only, labeled primary). All curl+UA → pypdf. Zero conflicts.
  Verify-pass bonus: the single-candidate-seat exception is OBSERVED on
  both real 2024 ballots — Court of Appeals seat 12 (contested) prints
  before seats 2/3/4/6/8, and Northfield prints Associate Justice 6 before
  5, exactly subp. 6's "only one candidate filed must appear after all
  other judicial offices for that same court".
- Baseline delta: SUBSTANTIAL — (1) legislature before statewide executives
  (Gov/SOS/Auditor/AG follow state house); (2) measures NOT last: statewide
  amendments near top (after State Offices, before county), local questions
  interleaved per jurisdiction; (3) judicial last AFTER measures (baseline
  puts judicial before measures) — internal Supreme→Appeals→District order
  matches; (4) SWCD Supervisor = county office (second-to-last in the
  county block, ahead of the Conservation District Supervisor line), not
  special district.
- Notes: SOS 2026 Explanatory Notes misquote subp. 5 (omit the two Special
  District lines present in the Revisor text — SOS's own 2026 example ballot
  renders them; trust the Revisor). Primary difference (one line): partisan
  primary uses party columns ordered by ascending prior-general average
  vote, nonpartisan section behind a demarcation (subp. 2). Candidate
  rotation (informational): names rotate precinct-to-precinct (§ 204D.13
  subd. 2a, § 204D.08 subd. 3) — never model as stable. Open: no real 2026
  GENERAL ballot exists until ~Sept 18, 2026 (46-day print deadline) —
  Governor slot rests on rule text + official example ballots + a real 2026
  primary ballot; re-check on live 2026 generals if desired. Pre-1997 "pink
  ballot" amendment history unverified (Revisor 1996 archive unfetchable) —
  no claim made; § 204D.15 subd. 2 marked repealed 1997.

### SC — South Carolina (FIPS 45) — GRADE A (scoped — see Notes)
- Authority: no fixed statutory sequence for the whole ballot — two statutes
  control jointly. S.C. Code § 7-13-320(E) DELEGATES arrangement (offices
  placed "in an order as arranged by the State Election Commission" for
  SEC-distributed ballots, by the county board of voter registration and
  elections for county-distributed ones, incl. State Senator + State House;
  multi-county uniformity duty with SEC tiebreak if boards deadlock within
  60 days of the general — tiebreak also requires "written certification by
  at least one commissioner, that they have failed to act"); § 7-13-330
  supplies the statutory ballot TEMPLATE ("The arrangement of general
  election ballots containing the names of candidates for office must
  conform as nearly as possible to the following plan") whose blocks
  run STATE (Governor → Lt Gov → SOS) then CONGRESSIONAL (US Senator → US
  Rep), made binding by § 7-13-335 ("shall conform these ballots to the
  requirements of Section 7-13-330"). § 7-13-340 splits who arranges what
  (SEC: presidential electors, state officers, US Senate/House; county
  boards: State Senate/House, county, local, circuit; municipal authorities:
  municipal — expressly including "school districts, public service
  districts and like political subdivisions"). § 7-13-410 amendments.
  https://www.scstatehouse.gov/code/t07c013.php (accessed 2026-08-16). NO
  admin rule on order — S.C. Code of Regs Ch. 45 (SEC) fetched in full,
  covers vote recorders/ballot cards only
  (https://www.scstatehouse.gov/coderegs/Chapter%2045.pdf). § 7-13-440
  (machine-ballot arrangement) repealed by 2022 Act No. 150 § 17.
- Office order: Straight Party selector → President/VP (presidential years)
  → statewide executives (gubernatorial years — the President block and
  the executive block are MUTUALLY EXCLUSIVE by cycle, so their relative
  order is notional): Governor and Lieutenant Governor (single joint
  contest) → Secretary of State → State Treasurer → Attorney General →
  Comptroller General → Superintendent of Education → Commissioner of
  Agriculture → US Senate → US House → State Senate (presidential years;
  4-yr terms) → State House → Circuit Solicitor → county row officers
  (Sheriff, Probate Judge, Clerk of Court, Coroner, Auditor, County
  Treasurer, Register of Deeds — Register of Deeds position relative to
  County Council VARIES by county: Greenville before, Charleston after) →
  County Council → Soil &
  Water District Commission → School Board of Trustees → Public Service
  Districts → Constituent School Boards → Statewide Constitutional
  Amendments → Local Questions. Municipal = physically separate ballot in
  OBSERVED practice (Sumter 2024 city sheet; Charleston 2024 municipal
  special elections print after the questions) — § 7-13-340 assigns
  municipal printing/arrangement to municipal authorities and construes
  "municipal" to include school + public service districts, yet school/PSD
  contests print INLINE on the county ballot; cite § 7-13-340 for who
  arranges, the ballots for the physical separation. STATEWIDE EXECUTIVES
  SIT ABOVE US SENATE AND US HOUSE — statutory (§ 7-13-330 STATE block
  before CONGRESSIONAL block, verified in raw HTML source order) + observed
  (Charleston 2022).
- Judicial: Probate Judge is the ONLY judicial contest SC voters see —
  countywide, partisan, 4-year (§ 14-23-30: "elected by the qualified
  electors of the respective counties"), printed INSIDE the county
  row-officer block, not late. All other courts: Supreme/Appeals/Circuit =
  joint public vote of the General Assembly (S.C. Const. art. V §§ 3, 8,
  13); Family (§ 2-19-80) + Administrative Law (§ 1-23-510(A)) = General
  Assembly; magistrates = Governor + Senate consent (art. V § 26); municipal
  judges = council-appointed (§ 14-25-15); masters-in-equity =
  Governor-appointed (§ 14-11-20). Judicial late block EMPTY.
- Measures: last, two labeled blocks — "Statewide Constitutional
  Amendments" then "Local Questions" (printed headings on 2024 Georgetown +
  Sumter ballots). § 7-13-410: statewide amendments under that heading,
  white paper; local constitutional amendments = separate non-white ballot,
  counties alphabetical. § 7-13-450 permits a separate measure ballot.
- County discretion: EXPLICIT — § 7-13-320(E) hands order to SEC (top
  block, uniform statewide in practice) and county boards (below State
  House), which is where inter-county variation shows: Charleston puts
  Register of Deeds AFTER County Council; Georgetown/Sumter run row
  officers contiguously before Council. County block = soft-ordered.
- School/special: nonpartisan, after County Council, before measures: Soil &
  Water → School Board of Trustees → Public Service District → Constituent
  School Board. Names alphabetical in nonpartisan/at-large multi-seat races
  (§ 7-13-335).
- Corroboration: Charleston 2022 general ACTUAL sample ballots, styles 272 +
  178
  (https://www.charlestoncounty.org/departments/bevr/files/2022-General-Election_Sample-Ballots/Charleston_County_Sample_Ballot_Style272.pdf
  and …Style178.pdf) — executives above US Senate/House, internal executive
  order, probate judge in county block, amendments last; Georgetown 2024
  general (56 styles,
  https://www.gtcountysc.gov/DocumentCenter/View/3223/20241105-General-Election-Sample-Ballots-PDF)
  — President top, row-officer block, two-block measures tail; Sumter 2024
  general
  (https://www.sumtersc.gov/sites/default/files/uploads/Departments/NewsMedia/sample_ballot_public.pdf)
  — measures tail + municipal as physically separate ballot; Charleston
  2024 OFFICIAL BALLOT BOOKLET, all 301 styles (emergency/provisional,
  https://www.charlestoncounty.org/departments/bevr/files/Sample_EMG.pdf?v=536
  — ROLLING FILENAME, overwritten each election; content = Nov 5 2024 as
  of 2026-08-16; %PDF magic verified, 5.73MB, 602 pp): style B001 =
  Straight Party → President → US House D1 → State Senate 32 → State House
  112 → Solicitor Circuit 9 → Sheriff → Clerk of Court → Coroner → Auditor
  → County Treasurer → Soil & Water → School Board of Trustees D2 →
  Constituent School Board → Statewide Constitutional Amendment 1 → Local
  Questions 1-2; across styles: County Council always between County
  Treasurer and Soil & Water (145 styles); Public Service District after
  Soil & Water (98 styles); MUNICIPAL SPECIAL ELECTIONS DEAD LAST after
  both questions (B005 Isle of Palms, B29 Kiawah); Sheriff → Clerk →
  Coroner → Auditor → Treasurer identical in all 301 styles. Also
  Charleston 2024 results report as validated proxy
  (https://www.charlestoncounty.org/departments/bevr/files/10110524-ElectionResults.pdf?v=3)
  and Greenville County official 2024 candidate list (grade-B corroborator,
  https://www.greenvillecounty.org/VoterRegistration/pdf/2024GeneralElectionCandidates.pdf
  — same sequence; county section = Sheriff, Clerk, Coroner, Register of
  Deeds → County Council → nonpartisan Soil & Water → school trustees →
  Watershed → Fire District). All curl+UA → PyMuPDF.
- Baseline delta: (1) statewide executives BEFORE US Senate + US House
  (President stays top); (2) judicial block EMPTY — probate judge mid-ballot
  with county row officers; (3) straight-party selector heads the ballot;
  (4) Circuit Solicitor between legislature and county block; (5) municipal
  = separate ballot, after questions when combined (baseline: municipal
  before school); (6) measures last in two blocks — matches baseline.
- Notes: no candidate rotation (partisan contests ordered per SEC
  arrangement; nonpartisan alphabetical). Primary (one line): § 7-13-610
  splits primaries onto two alphabetical ballots (state+federal vs General
  Assembly/county/circuit). GRADE SCOPE: A applies to the SEC-arranged
  block (presidential electors, statewide executives, US Senate, US
  House) — § 7-13-330's STATE-before-CONGRESSIONAL template made binding
  by § 7-13-335, corroborated by Charleston 2022. Everything from State
  Senate DOWNWARD is delegated to county boards (§ 7-13-320(E) +
  § 7-13-340), fixed by no statute or regulation (Regs Ch. 45 order-free),
  and VARIES between counties (Register of Deeds before Council in
  Greenville, after in Charleston) — treat the county-and-below sequence
  as observed practice, grade B. Open: probate-judge cycle — § 14-23-30
  prescribes a UNIFORM statewide cycle ("each alternate general election,
  reckoning from the year 1890" → 2022, 2026), Charleston 2022 fits, but
  Georgetown ran a probate contest in 2024 while Charleston's 602-style
  2024 booklet (zero "Probate Judge" hits) and Sumter 2024 had none —
  observed variation is a DEPARTURE from the statute, so verify per
  county, per cycle; repealed § 7-13-440 prior text unavailable; no
  written SEC order directive located (ordering = operational practice
  implementing the § 7-13-330 template). President-vs-US-Senate CLOSED
  (late child agent): Greenville County 2020 general sample ballot,
  precinct Saluda, prints President and Vice President FIRST with U.S.
  Senate immediately second (then US House 04 → State Senate 6 → State
  House 19 → Sheriff → Clerk → Coroner → Register of Deeds → Soil & Water
  → School Board D19) —
  https://ballotpedia.s3.amazonaws.com/images/7/78/2020_South_Carolina_sample_ballot_%28Greenville_County%29.pdf
  (PDF creationDate 2020-10-20; SHA-256 stable across two fetches;
  down-ballot races verified real). PROVENANCE CAVEAT: list-format
  voter-generated copy hosted on Ballotpedia's S3, not a county host — SC
  samples live behind the per-voter VREMS login, so this is the artifact
  class that survives; the list-format methodology is licensed by
  Charleston 2022, where the SCVotes list-format sample matches the
  printed ballot contest-for-contest (verified against printed styles 1 +
  50). President-vs-GOVERNOR stays structurally open (never co-ballot).

### AL — Alabama (FIPS 01) — GRADE A (scoped — see Notes)
- Authority: Ala. Code § 17-6-25 ("Order of Listing of Candidates on
  Ballots") — full 24-item office ladder,
  https://alison.legislature.state.al.us/code-of-alabama?section=17-6-25
  (OFFICIAL legislature host, browser-fetched 2026-08-16; law.justia mirror
  via r.jina.ai byte-identical). Companions: § 17-6-24 (party columns
  alphabetical from left + blank column; ballot style/design delegated to
  SOS APA rules), § 17-6-35 (straight-party mark "at the head of the
  ticket"), § 17-6-41 (amendment Yes/No format only — NOT position),
  § 17-6-33/-48/-49 ("Place No." for multi-seat; AOC recommends places to
  SOS by Dec 1 prior year), § 17-6-26; Ala. Const. 2022 art. XVIII
  § 284.01(b) (local amendment "on the ballot only in the county ...
  affected",
  https://alison.legislature.state.al.us/constitution?section=284.01).
- Office order (§ 17-6-25): (1) President (if preference primary) →
  (2) Governor → (3) Lt Governor → (4) US Senator → (5) US Representative →
  (6) Attorney General → (7) State Senator → (8) State Representative →
  (9) Supreme Court Justice → (10) Civil Appeals → (11) Criminal Appeals →
  (12) SOS → (13) Treasurer → (14) Auditor → (15) Commissioner of
  Agriculture & Industries → (16) Public Service Commissioner → (17) State
  Board of Education → (18) Circuit Court Judge → (19) District Attorney →
  (20) District Court Judge → (21) Circuit Clerk → (22) other public
  officers "in the order prescribed by the judge of probate" → (23)
  convention delegates → (24) party officers. GOVERNOR + LT GOV ABOVE US
  SENATE/HOUSE. Executives split into THREE runs: Gov/LtGov (before
  Congress) → AG alone (between US House and State Senate) →
  SOS/Treasurer/Auditor/AgComm/PSC (after the appellate courts).
  Legislature sits between AG and Supreme Court. Candidates within each
  office alphabetical by surname (no rotation). Straight-party prints as
  contest #1 on 3/3 verified ballots — PRACTICE: § 17-6-35 prescribes only
  HOW to mark ("at the head of the ticket"), § 17-6-24(a) orders only the
  party COLUMNS; neither fixes the straight-party block's position. Party
  columns alphabetical left-to-right + rightmost blank column (§ 17-6-24)
  — modern ballots print office-by-office with party labels.
- Judicial: within-level, TWO runs, no late block — appellate (9)-(11)
  between State Rep and SOS; trial (18)-(20) Circuit Judge → DA → District
  Judge → Circuit Clerk after SBOE, before county tier. Chief Justice
  before Associate places = practice (statute says only "Supreme Court
  Justice").
- Measures: NO statutory placement rule — complete Art. 2 (§§ 17-6-20 to
  -49) section list pulled; only § 17-6-41 touches amendments (format).
  Observed uniform on 3/3 verified ballots: dead last after every contest —
  recompilation/constitution question → "PROPOSED AMENDMENTS TO APPEAR ON
  THE BALLOT STATEWIDE" 1..N → "PROPOSED AMENDMENTS OF LOCAL…" 1..N → "END
  OF BALLOT". Local amendments print only in the affected county
  (§ 284.01(b)). Measure tier = observed practice, not statute.
- County discretion: explicit — § 17-6-25(22)/(24) delegate everything
  below Circuit Clerk to each county's probate judge. Sheriff, Probate
  Judge, County Commission, Coroner, county BOE, Constable etc. have no
  state internal order; observed sequences differ (Madison 2022: Sheriff →
  Coroner → BOE Supt → BOE Member; Jefferson 2024: Circuit Clerk (Bessemer)
  → Probate P1/P2 → Treasurer → Deputy Treasurer → Asst Tax Collector →
  County BOE → Constable).
- School/special: State BOE Member = item (17), partisan, mid-ballot before
  trial courts. County boards of education = item (22) county tail. No
  special-district tier in statute or on any ballot read.
- Corroboration: Madison 2022 general
  (https://www.sos.alabama.gov/sites/default/files/sample-ballots/2022/gen/Madison-Sample.pdf)
  — items (2)-(22) in exact statutory sequence, measures last; Jefferson
  2024 general
  (https://www.sos.alabama.gov/sites/default/files/sample-ballots/2024/gen/Jefferson-Sample.pdf)
  — President first, appellate block right after US House, PSC → SBOE →
  trial courts → county tail → measures; Jefferson 2022 general
  (https://www.sos.alabama.gov/sites/default/files/sample-ballots/2022/gen/Jefferson-Sample.pdf)
  — statewide amendments 1-10 then LOCAL AMENDMENT 1, all after contests.
  All curl -k + Chrome UA (sandbox TLS interception forces -k; integrity
  via md5 7e47602f8932a46d002c8ed1f47ee328 on the SOS Jefferson-2024 copy,
  reproduced independently by the verifier; the researcher's second-host
  match used
  https://jeffcoprobatecourt.com/wp-content/uploads/2024/11/2024-Pres-General-Sample-Ballot.pdf,
  which now 404s — index at https://www.jeffcoprobatecourt.com/sample-ballots/)
  → PyMuPDF coordinate extraction. Researcher re-verified all three
  itself; five additional subagent-reported county ballots NOT relied on.
  Verifier re-derived all three ballots independently — exact match.
- Baseline delta: LARGE — (1) Gov/LtGov before US Senate/House; (2) AG
  detached, between US House and State Senate; (3) legislature between AG
  and Supreme Court; (4) appellate judiciary mid-ballot; (5) second
  executive run after appellate courts; (6) SBOE between PSC and trial
  courts; (7) trial judiciary before all county offices; (8) straight-party
  contest #1. Tail only PARTLY matches baseline: measures last, yes — but
  there is NO separate school tier; county boards of education are item
  (22) county-tier entries and other county offices can follow them
  (Jefferson 2024: County BOE → Constable).
- Notes: § 17-6-25 descends from a 1975 primary act (items 1/23/24 are
  primary artifacts; § 17-6-26(b) allows amendments on primary ballots with
  amendment-only fallback) — Act 2006-570 recodified it into the general
  Ballots article and 3/3 general ballots follow items (2)-(22) exactly, so
  general-election application settled empirically; no interpretive
  document found (flag for override decision). SOS ballot-design APA rules
  promised by § 17-6-24(b)/-27/-35 appear NEVER promulgated — Ala. Admin.
  Code Title 820 Ch. 2 swept in full (chapters 820-2-1..12; 13-15 = 404),
  no ballot-design chapter. Municipal CLOSED at the statutory level
  (verify pass): § 17-6-45 — for municipal elections held apart from state
  generals, "the duties herein prescribed for the judge of probate …
  shall be discharged … by the mayor or other chief executive officer" —
  so Chapter 6 (incl. the § 17-6-25 ladder) governs stand-alone municipal
  elections with the mayor in the probate judge's role; municipal ORDER
  still unobserved on a ballot. Heading strings vary by county vendor;
  sequence invariant. Legacy alisondb host retired (DNS dead) — use
  alison.legislature.state.al.us SPA (curl gets the Next.js shell; browser
  or the site's POST /graphql `codeOfAlabamaSection` endpoint works).
  GRADE SCOPE: A covers the statutory ladder items (2)-(21) + the three
  matching official samples. NOT covered: measure placement (no authority
  anywhere — observed practice only) and the item-(22) county tier
  (expressly delegated to each probate judge, observed county-variable).

### LA — Louisiana (FIPS 22) — GRADE A
- Authority: La. R.S. 18:551 ("Ballots") — one statute prescribes both
  ballot order and candidate numbering, https://legis.la.gov/Legis/Law.aspx?d=81637
  (accessed 2026-08-16; current through Acts 2025 No. 386 §2). Ballot
  preparation CENTRALIZED in the SOS (18:551(A): "The secretary of state
  shall prepare and certify the absentee by mail ballots, the early voting
  ballots, and the ballots to be used on the voting machines"). SOS
  corroborating page: https://www.sos.la.gov/elections-voting/ballot-numbers.
  18:551(B)(2) covers "a primary or general election" — one order for BOTH,
  so the all-comers-November wrinkle doesn't change the order.
- Office order (18:551(B)(2)): party primary offices (closed-party races,
  grouped by party alphabetically) ABOVE everything → (a) President/VP →
  (b) presidential preference nominees → (c) STATE offices: Governor → Lt
  Gov → SOS → AG → Treasurer → Commissioner of Agriculture → Commissioner
  of Insurance → US SENATOR → US REPRESENTATIVE → Supreme Court justice →
  Court of Appeal judge → Public Service Commission → other state
  boards/commissions (BESE) → any other state office → (d) LOCAL offices:
  state senator → state representative → district judge → district
  attorney → parish court judge → sheriff → clerk of court → assessor →
  coroner → police juror → city court judge and marshal → SCHOOL BOARD →
  other local boards → justice of the peace → other local (constables) →
  (e) MUNICIPAL: mayor → chief of police/marshal → alderman/city council →
  municipal boards → (f) political party offices (state central committee,
  parish executive committee). Governor prints ABOVE US Senate/House by
  statute — but they never co-print (statewide execs = odd years 2023/2027,
  Congress = even years); statutory only, empirically untestable.
- Judicial: NO late block — split exactly as elected. Supreme Court +
  Court of Appeal INSIDE the state block, after US Representative, before
  PSC. District judge + DA at the TOP of the local block (slots 3-4, ahead
  of sheriff/clerk). Parish court judge next; city court judge/marshal
  later in (d) after police juror; JP second-to-last in (d) with
  constables after. All ballot-confirmed.
- Measures: statute SILENT on placement — 18:551(E) is typography only
  ("uniform size and style"); 18:1299/.1 = drafting, not position. Uniform
  practice on 6/6 ballots across 4 cycles (2020/2022/2023/2024): measures
  LAST — statewide constitutional amendments in CA-number order →
  parishwide propositions → municipal/special-district propositions.
  Strong practice, not codified law.
- County discretion: none — SOS central preparation; parish clerks only
  certify local candidates' NAMES (18:551(C)(1)(b)). Residual discretion
  is the SOS's: unlisted offices "in the order determined by the secretary
  of state" (18:551(B)(4)); special-election offices may move to the end
  (18:551(B)(3)); plus two discretions INSIDE the order list (verify
  pass): (B)(2)(b) lets the SOS lift presidential-preference nominees
  above same-party primary offices, and (B)(2)(f) lets political party
  offices move to just after the party-primary block instead of the end.
- School/special: parish school boards = LOCAL block (d), after police
  juror + city court judge/marshal, BEFORE municipal offices —
  ballot-confirmed (Lafayette 2023: "Member of School Board District 6"
  immediately above "City Council Member District 2, City of Lafayette").
  BESE = STATE office in block (c).
- Corroboration: official SOS precinct sample ballots via the GeauxVote
  portal's parish/ward/precinct-keyed endpoint (NOT address-gated):
  https://voterportal.sos.la.gov/SampleBallot/RacesByRegion?electionDate=…&parishId=…&wardId=…&precinctId=…
  — 6 single-precinct reads across 4 cycles: East Baton Rouge Nov 2024
  (measures tail: CA 1 → parish HRC amendment → Rec & Park props →
  city millage); Orleans Nov 2020 (Supreme Court after US Rep; Civil/
  Criminal District Courts + Magistrate + DA + Juvenile atop local block;
  school board after courts; CA 1-7 last); Orleans Nov 2022 (Court of
  Appeal 4th → PSC 3 → State Senator; CA 1-8 → parish prop); St. Tammany
  Oct 2023 gubernatorial (Gov → LtGov → SOS → AG → Treasurer → BESE →
  State Rep → District Judge → Sheriff → Clerk → Parish President →
  Council → CA 1-4); St. Tammany Nov 2020 (JP Ward 4 → Constable Ward 4
  directly before measures); Lafayette Oct 2023 (school-before-municipal).
  All curl+UA, single-precinct contiguous reads (researcher explicitly
  re-fetched individual precincts; parallel subagent's unreturned work NOT
  relied on). Verify-pass additions (verifier's own reads): Jefferson
  Parish 11/03/2020 (27/172/1750) — PSC 1 → Judge 2nd PARISH COURT Div A →
  JP 2nd Justice Court → Constable 2nd → CA 1-7 → PW Sports Wagering →
  Inspector General Special Services Funding District (parish-court slot +
  sub-parish measure tail CONFIRMED); EBR 10/12/2019 (18/134/1113) — full
  block-(c) executive run Gov → LtGov → SOS → AG → Treasurer → Ag &
  Forestry → Insurance → BESE 8, then (d) … Sheriff → Clerk → Assessor →
  CORONER (coroner slot confirmed).
- Baseline delta: (1) statewide executives ABOVE US Senate/House; (2) no
  late judicial block — appellate in state block after US House, trial
  courts + DA atop local block ahead of sheriff; (3) school board BEFORE
  municipal; (4) JP/constable tail of local block, still before municipal;
  (5) party offices after municipal; (6) closed-party-primary races print
  above President; (7) measures last matches baseline (practice only).
- Notes: NO rotation — names alphabetical by surname, numbered statewide,
  numbers permanent primary→general (18:551(C)(1)(c)(i)). November 2026 =
  hybrid: "U.S. Senate General/Open U.S. Representative Primary/Open
  Primary Election" (SOS election-dates page; closed party primary
  effective May 2026 for US House/Senate/Supreme Court/PSC/BESE, but Act 7
  of 2026 RS moved US House back to the fall open primary after a SCOTUS
  ruling — SOS press release 5/14/26,
  https://www.sos.la.gov/media/dcvl5ojl/051426-fall-house-races.pdf).
  Nov 3 2026 ballot not yet built — portal's election dropdown offers only
  ElectionId 343 (06/27/2026) as of 2026-08-16; re-pull in September to
  see the hybrid sequencing + whether a party-primary block appears.
  Open: measures-last has no authority; "party primary office" definition
  (R.S. 18:2(9)) not fetched first-hand; municipal (mayor/alderman)
  placement rests on statute alone (parish court judge + coroner CLOSED by
  the verify pass's Jefferson 2020 + EBR 2019 reads).

### KY — Kentucky (FIPS 21) — GRADE A (scoped)
- Authority: no single statute prescribes the full sequence. CAUTION
  (verify pass): 31 KAR 2:010 §1(11)'s "in the order under which they have
  been certified pursuant to KRS 118.215(1)" sits inside the DEFINITION of
  "Zero-file" — a TABULATION file, not the ballot; §2(2)(a) has no
  ordering words; §2(4)'s "appear in the correct positions" is circular —
  so the ballot↔certification order link is an INFERENCE from practice,
  not regulatory text
  (https://apps.legislature.ky.gov/law/kar/titles/031/002/010/). KRS
  117.383(2) DIRECTS the SBE to regulate ballot placement ("shall
  promulgate administrative regulations … and shall provide methods to: …
  (2) Place items on any ballot"), and no such regulation exists — 31 KAR
  chapters 1-6 swept 2026-08-16 with no contest-sequence chapter: Kentucky
  has left the sequence unregulated. KRS 118.215(1) (SOS certification +
  EXPRESS statutory order for the county/local-state tier: "Commonwealth's attorney, circuit clerk,
  property valuation administrator, county judge/executive, county
  attorney, county clerk, sheriff, jailer, county commissioner, coroner,
  justice of the peace, and constable",
  https://apps.legislature.ky.gov/law/statutes/statute.aspx?id=56458);
  KRS 118.225 (names within office by lot); KRS 118A.090(4) (judicial =
  separate "Judicial Ballot" column immune to straight-party); KRS
  160.230 ("School Candidates" device); KRS 118.415(3) (amendments
  "indicated on the ballots" — no position); KRS 424.290 (published
  facsimile). All apps.legislature.ky.gov, curl+UA → pypdf, 2026-08-16.
  GRADE SCOPE (widened by the verify pass): statutory (A) = the
  county/local-state tier's internal order (KRS 118.215(1)) and the
  judicial/school ballot DEVICES (KRS 118A.090(4), 160.230). Everything
  else is B: the federal→state descent rests on an SOS certification
  order (the located instance is a 2026 PRIMARY certification), not
  statute; and the POSITIONS of the judicial, school, city and amendment
  blocks rest on county KRS 424.290 publication facsimiles (17 counties,
  2024 cycle), not on any authority text.
- Office order (Nov even-year, as certified and printed): straight-party
  box FIRST → President/VP → US Senator → US Representative → State
  Senator → State Representative → Commonwealth's Attorney → Circuit
  Clerk → other county offices (statutory sequence above) → nonpartisan
  judicial block → nonpartisan tail (soil & water / school board / city
  offices — SUB-ORDER VARIES BY COUNTY, see Notes) → constitutional
  amendments (countywide questions can follow them). STATEWIDE EXECUTIVES NEVER APPEAR IN EVEN
  YEARS — Ky. Const. § 95 fixes Governor/LtGov/Treasurer/Auditor/AG/SOS/
  Ag Commissioner to 1895+4k (2023, 2027); Governor-vs-US-House moot on
  the even-year ballot. Commonwealth's Attorney + Circuit Clerk are
  judicial-CIRCUIT offices but PARTISAN, heading the county block, not in
  the judicial block.
- Judicial: nonpartisan, own block under printed "NONPARTISAN JUDICIAL
  BALLOT" banner; KRS 118A.090(4) requires separate column/line
  "identified by the words 'Judicial Ballot'" so straight-party can't
  reach it; no party designation (KRS 118A.150(1)). Statute does NOT fix
  block position — McCracken 2024 prints it after partisan county
  offices, before the rest of the nonpartisan tail. Internal order
  CLOSED by the multi-county sweep (Kenton 2022 composite): Supreme Court
  → Court of Appeals → Circuit Judge (Family Court divisions interleaved
  by division number) → District Judge — the full chain on one county's
  ballot.
- Measures: constitutional amendments NEAR-last ("ALL PRECINCTS";
  McCracken 2024 prints them after everything on that ballot). CORRECTED
  by the multi-county sweep: CITY questions ride WITH that city's races
  (cannabis questions inline in the city blocks — Boone/Campbell/Hardin);
  COUNTYWIDE questions print AFTER the two constitutional amendments
  (Hardin QUESTION-HARDIN CANNABIS last, Bullitt QUESTION-BULLITT
  CANNABIS last, Fayette AD VALOREM TAX FOR PUBLIC PARKS last). KRS
  118.415(3) has no position rule — all placement = practice, and
  inferred from KRS 424.290 publication-facsimile COMPOSITES (layouts
  span precinct styles), not single-precinct ballot images.
- County discretion: SUBSTANTIAL and the real source of local sequencing
  — county clerk composes the certification listing (31 KAR 2:010
  §2(2)(a)), verifies positions (§2(4)); position unchangeable once
  designated (KRS 118.215(6)); independents + city candidates by clerk
  lot (KRS 118.215(2)-(3)). Bounded by the KRS 118.215(1) list only for
  the offices it NAMES — off-list county offices are slotted by the clerk
  inside that run (McCracken 2024 prints County Surveyor between Circuit
  Clerk and Justice of the Peace; Surveyor is not in the § (1) list).
- School/special: school board nonpartisan under printed NONPARTISAN
  "SCHOOL CANDIDATES" banner (KRS 160.230 verbatim device); order by
  lot; common-school divisions before independent-district seats; placed
  after judicial, before city. Soil & water supervisors nonpartisan,
  county-clerk-filed (KRS 262.210/.220), grouped with local nonpartisan
  offices (Kenton 2024 filings).
- Corroboration: McCracken County (Paducah) 2024 general official KRS
  424.290 facsimile, clerk-certified ("I, Jamie Huskey, County Clerk …
  true copy of the ballots"),
  https://mccrackencountyky.gov/wp-content/uploads/2020/10/2024-General-election-sample-ballot.pdf
  (scan → PyMuPDF render → visual read): STRAIGHT PARTY → President/VP →
  US Rep 1st → State Rep 1/2/3/6 → Commonwealth's Attorney 2nd Circuit →
  Circuit Clerk → County Surveyor (unexp.) → Justice of the Peace 2nd
  (unexp.) → NONPARTISAN JUDICIAL BALLOT (Appeals 1st App. Dist. 2nd
  Div. unexp. → District Judge 2nd 2nd unexp.) → NONPARTISAN "SCHOOL
  CANDIDATES" (Bd of Ed 2nd/4th/5th → Paducah Independent) → NONPARTISAN
  CITY BALLOT (Mayor Paducah → City Commissioners) → Strawberry Hills
  wet/dry QUESTION → (p.2) CONSTITUTIONAL AMENDMENT 1 → 2. County
  relative order matches KRS 118.215(1) (CA 1st, Circuit Clerk 2nd, JP
  11th). Supporting: Fayette 2026 PRIMARY publication ballot
  (https://web.sos.ky.gov/ballots/FAYETTE%202026P.pdf — federal→state→
  county descent inside party columns); SOS 2026 PRIMARY certification
  (May 19, 2026, SOS Michael G. Adams — a PRIMARY document; via Kenton
  clerk,
  https://kentoncountykyclerk.com/wp-content/uploads/2026/02/2026-KentonCertification.pdf)
  — operative order language (verify pass; the earlier quote was a scope
  recital): p.6 "Pursuant to KRS 118.215: The County Clerk of each county
  shall indicate on the ballots the following in order certified (where
  applicable). Republican Party / Democratic Party / Nonpartisan /
  Judicial Ballot"; p.19 (Kenton page) "You will place on the ballot the
  STATEWIDE OFFICES as listed in Section 4, then place the following on
  the ballot:" → US Representative → State Representative 63/64/69/78,
  with Section 4 (p.14) opening at United States Senator; Kenton "How to
  Read a Publication Ballot"
  ("The first item on everyone's ballot will be the Straight Ticket
  option"). MULTI-COUNTY SWEEP (child agent, per-county URLs in evidence
  file): 17 counties total — 2024 composites/publication ballots for
  Fayette, Kenton, Boone, Campbell, Warren, Hardin, Daviess, Bullitt,
  Madison (clerk-certified), Jessamine, Woodford, Franklin, Clark,
  Oldham, Laurel + Jefferson clerk candidate list ("Candidates are listed
  in the order in which they will appear on the ballot" — LOCAL offices
  only) + Kenton 2022 composite (full county tier: PVA → County
  Judge/Executive → County Attorney → County Clerk → Sheriff → Jailer →
  County Commissioner → Coroner → Surveyor → Magistrate → Constable —
  matching KRS 118.215(1) with Commonwealth's Attorney + Circuit Clerk
  heading, then the FULL judicial chain). Dominant 2024 skeleton
  confirmed statewide: Straight Party → President/VP → US Rep → State
  Senator → State Rep → Commonwealth's Attorney → Circuit Clerk →
  leftover partisan county (unexpired terms) → NONPARTISAN JUDICIAL
  BALLOT → soil & water → school → city → amendments. Several 2024 SOS
  copies recovered via Common Crawl (web.sos.ky.gov purges to current
  cycle; Jefferson live host Akamai-403).
- Baseline delta: (1) straight-party box first, ahead of President;
  (2) NO statewide executives in even years (state legislature follows US
  House directly); (3) judicial block NOT last — county → JUDICIAL →
  school → city (baseline: county → municipal → school → judicial);
  (4) partisan Commonwealth's Attorney + Circuit Clerk head the county
  block; (5) amendments-last matches baseline.
- Notes: primary (one line): party columns + nonpartisan column, same
  descent. NO rotation — single public lot drawing fixes name order (KRS
  118.225, 118A.090(1)), then unalterable. KRS 118.235 ("Form of
  ballots") repealed 2003 — researcher-verified via the chapter index;
  the verifier could NOT re-verify (chapter.aspx renders via JS) — treat
  as single-sourced. NONPARTISAN TAIL SUB-ORDER VARIES BY COUNTY
  (sweep finding): soil & water usually before school
  (Boone/Campbell/Daviess/Kenton) but Fayette prints it LAST among
  nonpartisan races; school usually before city but Kenton 2024 and
  Laurel print CITY before school; Fayette inserts URBAN COUNTY COUNCIL
  between judicial and school — consistent with clerk discretion under
  31 KAR 2:010, so model the tail as county-variable. CLOSED by the
  sweep: Supreme/Circuit placement (Kenton 2022 full chain);
  single-county corroboration (now 17 counties incl. Jefferson + Fayette
  via Common Crawl). Still open: odd-year executive internal order
  (needs a 2023 ballot; matters only for Nov 2027); no SBE directive for
  the top-of-ballot sequence (would upgrade the B leg); Kenton 2024
  filing doc lists Circuit Clerk before Commonwealth's Attorney —
  filing-list artifact, NOT ballot order (explicitly not relied on).
  ODD-YEAR EXECUTIVES CLOSED (late child agent, 2026-08-16): five 2023
  general ballots (Campbell + Rowan + Harrison via
  https://<county>.countyclerk.us/wp-content/uploads/2023/09/<County>-2023G.pdf;
  Fayette + Kenton SOS copies via Common Crawl CC-MAIN-2024-10) print an
  IDENTICAL executive sequence: STRAIGHT PARTY → GOVERNOR and LIEUTENANT
  GOVERNOR (one combined contest, "Vote for One") → SECRETARY of STATE →
  ATTORNEY GENERAL → AUDITOR of PUBLIC ACCOUNTS → STATE TREASURER →
  COMMISSIONER of AGRICULTURE — then county/nonpartisan tails vary by
  county as usual (Fayette 2023 = general AND special election, State Rep
  93rd prints after the nonpartisan school block). Two more 2024 counties
  confirm the even-year skeleton (Bourbon + Campbell via the same
  countyclerk.us CDN pattern .../2024/10/<County>-2024G.pdf; McCracken
  independently re-derived as a pipeline self-check). Child also reports
  — NOT re-verified against fetched reg text, treat as pointers:
  straight-party-first codified at 31 KAR 5:026 §1, and full-term vs
  unexpired-term grouping at KRS 118.115(5); if confirmed, the
  straight-party slot upgrades from practice to authority. Anti-
  fabrication note: the child's %PDF magic check caught a Wayback error
  page masquerading as Fayette 2023G before it could become a fake
  ballot.

### OR — Oregon (FIPS 41) — GRADE A
- Authority: contest order set by SECRETARY OF STATE DIRECTIVE, not statute.
  ORS 246.110 (SOS = chief elections officer) + ORS 246.120 ("A county clerk
  affected thereby shall comply with the directives or instructions").
  Operative documents: SOS Directive 2024-05 "Official Ballot Statements"
  (Sept 5, 2024; Nov 5 2024 general),
  https://records.sos.state.or.us/ORSOSWebDrawer/Record/12855764/File/document,
  and Directive 2022-06 (Sept 8, 2022; Nov 8 2022 general),
  https://records.sos.state.or.us/ORSOSWebDrawer/Record/12855756/File/document,
  and Directive 2020-3 (Sept 3, 2020; Nov 3 2020 general — scanned, no
  text layer, OCR'd two ways + page visually confirmed by the verify
  pass; signed Bev Clarno by Stephen N. Trout),
  https://records.sos.state.or.us/ORSOSWebDrawer/Record/12855744/File/document
  — each with a "BALLOT ARRANGEMENT" section. WebDrawer unlock: ORMS
  titles these "Certified Ballot" (not "Official Ballot Statements") and
  WebDrawer ID = 12855643 + N for record EPD/25/N; ORMS holds election
  directives only for 2020-2024 (EPD/25/98-121) — a 2018 directive
  appears never to have been published (domain-wide Wayback CDX: earliest
  "directive" URL = 2020; index-level evidence, replay was down). Supporting statutes: ORS
  254.145(7) (measures "printed after the list of candidates"), ORS
  254.125(2)(a) (contested judicial before unopposed), ORS 254.135
  (general-ballot contents), ORS 254.155 + OAR 165-010-0090 (random-alphabet
  name order), ORS 250.115 (state measure numbering), ORS 254.108 (local
  measure county-prefix numbers). All accessed 2026-08-16. Directives carry
  edition/cycle — re-pull per cycle.
- Office order — the directives are PER-CYCLE and list only offices up
  that cycle, so NO single directive carries the whole ladder (verify-pass
  correction: the earlier presentation merged both). Union across 2024-05
  / 2022-06 / real ballots: 1. Federal Office (2024-05 lists US President
  + US Representative, no Senator; 2022-06 lists US Senator + US
  Representative, no President; President → US Senator → US Rep confirmed
  on Lincoln County 2020) → 2. State Office (2022: Governor; 2024: SOS →
  Treasurer → AG) → 3. Legislative (State Senator → State Representative)
  → 4. County Office if partisan → 5. City Office if partisan →
  6. Nonpartisan Office: (a) state — BOLI in 2022, "None at this
  election" in 2024, (b) state judicial, (c) District Attorney (2024-05
  ONLY — 2022-06's tier 6 runs a-e with no DA rung), (d) nonpartisan
  county, (e) nonpartisan city, (f) special district → 7. Ranked Choice
  Voting (2024-05 only; separate card or alternate position, county
  discretion) → 8. Measures (2022-06 numbers Measures as tier 7).
  Governor after US House — baseline spine holds. BOLI (nonpartisan
  statewide) does NOT print with executives — it heads the nonpartisan
  block after all partisan offices.
- Judicial: nonpartisan block slot 6(b) — AFTER every partisan office and
  BOLI, BEFORE District Attorney, nonpartisan county/city, special
  districts. Hierarchy Supreme → Appeals → Tax → Circuit — sourcing
  refined twice by the verify pass: 2024-05 lists Supreme + Circuit only
  and 2022-06 lists Appeals + Circuit only, but the recovered GENERAL
  Directive 2020-3 lists Supreme AND Appeals together ("Contested Supreme
  Court (None at this election) / Uncontested Supreme Court / Contested
  Court of Appeals (None…) / Uncontested Court of Appeals / Contested
  Circuit…"), so Supreme-before-Appeals is general-directive text; only
  the TAX COURT rung still rests on the May-2024 primary directive
  2024-01. Supreme → Appeals → Circuit → DA also ballot-confirmed on
  Lincoln County 2020. Contested before uncontested within each court (directive
  + ORS 254.125(2)(a)) — scoped "for the same office type", so a
  contested Appeals race never rises above an uncontested Supreme one.
  "Incumbent" printed per ORS 254.135(3)(c).
- Measures: last. ORS 254.145(7). Internal order per directive: State →
  County → City → Multi-County Special District → Single-County Special
  District. State measures numbered (= ordered) by ORS 250.115(1):
  legislative constitutional amendments → legislative Acts referred →
  initiative/referendum petitions (ballots print "State Legislative
  Measures Referred…" before "State Initiative Measures…"). Local measures
  carry county prefix (ORS 254.108; Multnomah = 26-).
- County discretion: narrow — arrangement binding via ORS 246.120; each
  directive ends with a contact-us escape for "undue administrative or
  printing problems". Express county options: RCV contests (separate
  card/alternate position — Multnomah 2024 used a wholly separate Portland
  RCV card) and Precinct Committee Person (primaries).
- School/special: school districts, ESDs, community colleges, ordinary
  special districts = ORS 255.012 "districts" electing "in each
  odd-numbered year on the third Tuesday in May" (ORS 255.335(1)) — SCHOOL
  BOARDS ABSENT from November even-year ballots. Exceptions on the general:
  soil & water conservation districts (ORS 568.530(1), 568.560(10)) +
  urban flood safety & water quality districts — both in tier 6(f) after
  judicial and county/city, before measures. Community college / Metro
  MEASURES can ride November at the district rung.
- Corroboration: Multnomah 2024 general
  (https://multco-web7-psh-files-usw2.s3-us-west-2.amazonaws.com/s3fs-public/96-1-4101-1-S-NON-EN.pdf)
  — Federal → State (SOS/Treasurer/AG) → Legislative → Nonpartisan State
  Judiciary (contested Circuit Pos 38 with 5 candidates BEFORE uncontested
  Pos 20/21/33) → SWCD → flood district → state legislative measures
  115-117 → initiatives 118-119 → Portland measures 26-249..253; separate
  RCV card (multco.us/file/2801-1-rcv-non-en.pdf/download) with Portland
  Mayor/Auditor/Councilor, county commissioner after judiciary on regular
  card; Multnomah 2022 general
  (https://multco.us/file/4101-1-s-2022-11.pdf/download) — Governor under
  State Offices → BOLI under State Nonpartisan Office → judiciary (Appeals
  Pos 10/11, Circuit 3/8/37) → county → city → SWCD → measures
  state→county→city→Metro. Both PyMuPDF block-geometry extractions,
  curl+UA. Directive-vs-ballot match tier-for-tier both cycles; only
  cosmetic header variance (2022 folded legislators under State Offices
  label). SECOND COUNTY (verify pass): Marion County official Voters'
  Pamphlet composite sample ballots, 3 cycles — 2024
  (https://www.co.marion.or.us/CO/elections/Documents/VoterRegistrationNumbers/2024/FINAL%20November%205%202024%20General%20VP_REV2(ONLINEVER).pdf),
  2022 (.../Final%20-%20November%208th%2c%202022%20VP.pdf), 2020
  (.../Results/Documents/20201103/November%202020%20Final%20Voter%20Pamphlet.pdf)
  — top-level sequence stable all three: Federal → Statewide Partisan →
  Countywide Partisan → Statewide Nonpartisan (judges) → Countywide
  Nonpartisan (sheriff/clerk) → Cities (alphabetical, mayor before
  councilor) → Soil & Water → State Measures → Local Measures. Marion
  2020 CLOSES Supreme-vs-Appeals: "Judge of the Supreme Court Position 4"
  prints ABOVE "Judge of the Court of Appeals Position 9" in the same
  column. Marion 2020 also prints state measures grouped "Proposed by
  Legislative Assembly" (107-108) then "Proposed by Initiative Petition"
  (109-110) — ORS 250.115 order observed. CAVEAT: pamphlet composites,
  not printed precinct ballots (grouping/sequence authoritative, per-voter
  ballot is a subset); and Marion 2020's executive run printed SOS → AG →
  Treasurer vs the 2024 directive's SOS → Treasurer → AG — the internal
  executive order is per-cycle directive, never claim it fixed. THIRD
  COUNTY (verify pass, verifier re-derived both): Lincoln County 2024
  general
  (https://www.co.lincoln.or.us/DocumentCenter/View/5816/November-5-2024-General-Election-Sample-Ballot)
  — Federal → State (SOS → Treasurer → AG) → Legislative → Nonpartisan
  (Supreme Pos 1, 7) → Nonpartisan County (Commissioner, Sheriff) —
  directive 2024-05 tier-for-tier; Lincoln 2020 general (county voters'
  pamphlet .../View/284, sample ballot pp. 25-26) — President → US
  Senator → US Rep 5th (federal-tier order pinned) and Supreme Pos 4 →
  Appeals Pos 9 → Circuit 17th Pos 2 → District Attorney (judicial chain
  + DA rung on a real ballot) → nonpartisan county → 7 cities → SWCD → 3
  PUDs → state measures → local fire-district measure 21-198. Both
  curl+UA → PyMuPDF line-level, column edges from the x0 distribution
  (the 2020 ballot is FOUR columns — a 3-column split silently merges
  the federal and judicial columns).
- Baseline delta: SUBSTANTIAL — (1) judicial NOT late: prints before
  county/city/special-district contests (opposite of baseline tail);
  (2) county/city offices SPLIT around the judicial block by partisanship
  (partisan before, nonpartisan after — most OR county/city offices are
  nonpartisan, so they land after the judges); (3) BOLI leaves the
  statewide-executive block, heads nonpartisan section; (4) school boards
  absent from November entirely; (5) Governor-after-US-House and
  measures-last match baseline.
- Notes: all vote-by-mail. Name order = single statewide random alphabet
  per election (ORS 254.155), no precinct rotation (informational).
  Primary (one line): per-party nominating ballots + nonpartisan ballot +
  PCP tier. No-filer offices still print with write-in lines. Directive
  PDFs vanish from sos.oregon.gov (404) — recover via SOS WebDrawer
  records.sos.state.or.us. CLOSED by the verify pass: Supreme-vs-Appeals
  (Directive 2020-3 text + Lincoln 2020 ballot); multi-county
  corroboration (Multnomah + Marion + Lincoln, 2020/2022/2024); the 2020
  directive itself (found + OCR'd). Still open: no OAR arranges contests
  (165-010-0090 = name order only; full ch. 165 index not enumerable —
  ballot-design rule elsewhere not fully excluded); nonpartisan-county
  example lists in tier 6 are "such as the following" EXAMPLES whose
  internal order differs per cycle (2022: Clerk→Assessor→…; 2024: County
  Judge→JP→Sheriff→…) — never treat as a fixed sub-order; 2018 directive
  existence airtight only via Wayback replay or an SOS records request.

### OK — Oklahoma (FIPS 40) — GRADE A
- Authority: 26 O.S. § 6-105 (separate General Election ballot sections),
  § 6-106 (printing; "the officers in the order in which they are set out
  by the Constitution and statutes"; SEB Secretary sets off sections),
  § 6-103/-104 (state/county boards print "in the order they appear in the
  statutes"), § 6-113 (state-question wording), §§ 11-108/-109 (judicial
  ballots nonpartisan; retention wording), § 6-102 (unopposed candidates
  never printed), § 13A-103 (school = Feb/April cycle), § 3-101(B)(5)
  (November allowable for subdivision specials) — official complete-title
  PDF https://www.oklegislature.gov/OK_Statutes/CompleteTitles/os26.pdf
  (curl+UA → PyMuPDF, accessed 2026-08-16). State-officer internal order
  from Okla. Const. art. VI § 1
  (https://oksenate.gov/sites/default/files/2022-05/oc6.pdf) + art. IX
  § 15 (Corporation Commission, .../oc9.pdf). No SEB admin rule prescribes
  contest order (OAC 230 ch. 25 = separate ballots/lot order/quantities;
  official host rules.ok.gov 403s — ch. 25 text via elaws mirror + SEB
  proposal PDFs, those claims capped B).
- Office order (AS PRINTED, 5 real ballots across 3 cycles): STRAIGHT
  PARTY VOTING → Electors for President/VP (presidential years) →
  STATEWIDE EXECUTIVES BEFORE US SENATE/HOUSE, in constitutional order:
  Governor → Lt Governor → Auditor & Inspector → AG → Treasurer →
  Superintendent of Public Instruction → Labor Commissioner → Insurance
  Commissioner → Corporation Commissioner → US Senator (full term) → US
  Senator (unexpired) → US Representative → State Senator → State
  Representative → county offices (DA sits in this statutory block,
  § 6-105(7) — not directly observed) → District/Associate District Judge
  (nonpartisan, contested) → appellate RETENTION block (Supreme → Criminal
  Appeals → Civil Appeals) → State Questions LAST. Secretary of State =
  appointed, never on ballot (art. VI § 1(B)). County questions,
  municipal, school, special districts = SEPARATE BALLOTS.
- Judicial: late block, split — contested district/associate district
  judge races after county offices, before appellate retention; retention
  Supreme → Criminal Appeals → Civil Appeals ("Shall [NAME] … be retained
  in Office?", § 11-109); all before State Questions. Civil Appeals =
  retention, not contested.
- Measures: numbered State Questions dead last on the state/federal
  ballot, ascending number (SQ 833 → 834 in 2024), § 6-113 wording. County
  questions = separate ballot at the biennial general (Lincoln 2024
  county EMS-district proposition ballot verified; OAC 230:25-3-3
  proposal text says county questions "shall not be printed on the state
  ballot for the biennial General Election" — recorded vs elaws mirror's
  space-permitting wording, both B-capped).
- County discretion: none over order — § 6-104 binds county boards to
  statutory order; SEB programs ballots centrally (MESA entry +
  state-generated proofs, OAC 230:25-3-3). Discretion = quantities +
  whether a county question needs its own ballot.
- School/special: school + technology-center board elections = February
  primary / April general (§ 13A-103(A)) — NO school races in November.
  November specials allowed (§ 3-101(B)(5)) but ride SEPARATE ballots
  (OAC 230:25-13-1.1; SEB Precinct Official Manual "Multiple Elections on
  Same Date" = multiple ballots per voter).
- Section banners (printed grey bands, Delaware + Woodward 2024/2020):
  STRAIGHT PARTY → PRESIDENTIAL → STATE OFFICERS → CONGRESSIONAL OFFICERS →
  LEGISLATIVE, DISTRICT, AND COUNTY OFFICERS → JUDICIAL RETENTION ("Vote
  separately on each justice or judge; they are not running against each
  other.") → STATE QUESTIONS. The STATE OFFICERS band precedes the
  CONGRESSIONAL OFFICERS band on every banner ballot — the exec-first
  finding is structural, not incidental. Full 9-executive internal order
  OBSERVED on Osage 2018 (text PDF, coordinates): Governor → Lt Governor →
  State Auditor & Inspector → AG → Treasurer → Superintendent → Labor →
  Insurance Commissioner → Corporation Commissioner — exactly the art. VI
  § 1 enumeration (SOS skipped, appointed).
- Corroboration: Lincoln County 2022 general
  (https://lincolncountyok.org/file/ballots_and_results/general_election_sample_ballot_pct._1_2_3_4_21_22_110822_ballot_33.pdf)
  — Governor → LtGov → AG → Treasurer → Supt → Labor → Corporation
  Commissioner → US Senator ×2 → US Rep 05 → State Senator 28 → County
  Commissioner D1 → DISTRICT JUDGE D23 Office 1 (no party labels) →
  Supreme + Civil Appeals retention; Lincoln 2024 state/federal
  (...general_election_state_federal_sample_ballot_ballot_78.pdf) —
  President → Corporation Commissioner → US Rep → 3-court retention → SQ
  833 → SQ 834; Lincoln 2024 COUNTY ballot
  (...lincoln_county_sample_ballot_11.5.24_ballot_77.pdf) — sole EMS
  proposition; Osage 2022 pcts 570108 + 570305
  (https://osage.okcounties.org/file/ballots_and_results/pct_108_ballot_344.pdf,
  .../pct_305_ballot_361.pdf) — same sequence independently; Osage 2020
  pct 570107 (.../pct_107_ballot_256.pdf) — President → Corporation
  Commissioner → US Senator → US Rep → retention → SQ 805 → SQ 814. All
  curl+UA → PyMuPDF. Zero conflicts across five ballots.
- Baseline delta: (1) statewide executives before US Senate/House
  (President still first); (2) judicial block split (contested trial after
  county; appellate retention after that) and BEFORE measures; (3) county
  questions/municipal/school/special = separate ballots, outside the
  sequence entirely; (4) straight-party control heads the ballot; (5)
  State Questions last matches baseline.
- Notes: the apparent statute-vs-print conflict DISSOLVES on verification
  (adversarial pass): § 6-105 is a SECTION INVENTORY, not a sequence —
  its own list puts "6. State questions" ahead of "7. State Senators …
  county officers," which no ballot does; the text says the ballot "shall
  contain a separate section for the following". § 6-103/-104 are
  duty-to-print provisions whose operative clause is "in the order they
  appear in the statutes" (§ 6-104 carries the clause with no enumeration
  at all). The LAYOUT provision is § 6-106, and it matches print exactly:
  "The name of the office entitled to the first place, preceded by the
  word 'for', shall appear in bold type, as 'For Governor'" — the
  statute's own first-place example is Governor, and both non-presidential
  ballots open with FOR GOVERNOR; officers named "in the order in which
  they are set out by the Constitution and statutes" (Constitution first
  = art. VI § 1 executive order); "sections … set off with well-defined
  lines or by other means as prescribed by the Secretary of the State
  Election Board" = the printed banners. So the authority chain is
  § 6-106 + art. VI § 1, corroborated 4 elections × 4 counties.
  Observation-only legs: the federal band's position relative to the
  state band (§ 6-106 doesn't sequence federal vs state) and presidential
  electors printing first in presidential years. Unopposed candidates
  omitted entirely (§ 6-102) — explains missing offices per cycle (Osage
  2018 supplies the full 9-exec run). General election: NO rotation —
  recognized parties in lot order as prescribed by the SEB Secretary,
  then unrecognized parties, then independents (§ 6-106); rotation only
  at primary/runoff (§ 6-109). Primary (one line): per-party colored
  ballots (§ 6-110); OAC 230:25-3-3 contains BOTH sentences consecutively
  — county question may ride the state ballot at PRIMARY/runoff "if there
  is adequate space" but "shall not be printed on the state ballot for
  the biennial General Election" (primary = space-permitting, general =
  flat ban; the earlier mirror-vs-proposal "conflict" was a misread).
  DA SLOT PINNED (verify-pass child): Delaware County June 2022 GOP
  primary ballot
  (https://delaware.okcounties.org/file/ballots_and_results/5_7_8_9_12_14_15_16_18_20_rep_ballot_101.pdf)
  prints FOR DISTRICT ATTORNEY DISTRICT 13 (contested) after State
  Senator + State Representative and before County Sheriff, inside the
  same "LEGISLATIVE, DISTRICT, AND COUNTY OFFICERS" banner the generals
  use — primary-cycle evidence, but the banner and grouping are verbatim
  identical on the Nov 2022 Delaware generals (which also show the whole
  section DROPPING OUT when nothing in it is contested, precincts
  1-2-3-5… ballot_107 — Oklahoma omits empty sections rather than
  printing empty tiers). Contested DA on a November general remains
  unobserved (only 2022 case = Oklahoma County, which publishes no
  ballot PDFs).
  Additional corroborators (child sweep): Delaware County 2024 (2 ballot
  styles incl. the legislative-band style,
  https://delaware.okcounties.org/file/ballots_and_results/1_2_3_4_5_6_7_8_9_12_14_15_16_18_19_20_21_22_23_ballot_135.pdf
  + .../19_22_23_ballot_134.pdf, scans read at 110 dpi), Woodward 2024 +
  2020 (.../sample_ballot_ballot_49.pdf,
  .../november_election_ballot_ballot_25.pdf), Osage 2020 pct 201 + 2018
  pct 101 text PDFs with coordinates
  (https://osage.okcounties.org/file/ballots_and_results/pct_201_ballot_263.pdf,
  .../pct_101_ballot_151.pdf; siblings verified) — 4 general elections
  (2018/2020/2022/2024) × 4 counties (Lincoln, Osage, Delaware, Woodward),
  zero conflicts. Osage 2018 supplies the
  once-missing Auditor + Insurance Commissioner positions (observed, not
  just constitutional inference). Open: DA slot unobserved (no contested
  DA on any of 13 ballots); Associate District Judge same-slot assumption
  untested; district-judge home = between county block and retention
  (Lincoln 2022 observed; 2024 banner wording "LEGISLATIVE, DISTRICT, AND
  COUNTY OFFICERS" is consistent but no judge race printed under it);
  OKC/Tulsa/Cleveland county boards publish no ballot PDFs (address-gated
  portal; Cleveland 2024 files purged, Wayback replay 503).

### CT — Connecticut (FIPS 09) — GRADE A
- Authority: Conn. Gen. Stat. § 9-251 "Order of office on ballots" (ch.
  147), https://www.cga.ct.gov/current/pub/chap_147.htm (accessed
  2026-08-16 — cga.ct.gov TLS chain broken; `curl -sk` works, r.jina.ai
  proxy also works). Supporting: § 9-249a (party row order), § 9-250 (form),
  § 9-249b (>9 rows / SOTS multi-column), § 9-242(a) (≥9 party capacity),
  § 9-3 (SOTS = Commissioner of Elections, written instructions
  presumptively correct), § 9-181 (Gov/LtGov single vote), § 9-164
  (municipal = odd-year), § 45a-18(a) (probate quadrennial 1974+4k).
  Currency verified: 2026 Supplement to ch. 147 touches only §§ 9-239,
  9-261, 9-264 — order sections unamended.
- Office order (GRID ballot — offices are COLUMNS left→right, parties are
  ROWS; § 9-251 verbatim, note the opening scope limiter): "In the
  preparation of ballots for use at a state election, precedence shall be
  given to the offices … in the following descending order: Presidential electors, Governor and
  Lieutenant Governor, United States senator, representative in Congress,
  state senator, state representative, Secretary of the State, Treasurer,
  Comptroller, Attorney General and judge of probate." GOV/LTGOV = COLUMN
  2, ABOVE US SENATE AND US HOUSE; the other four statewide executives
  (SOS, Treasurer, Comptroller, AG) sit BELOW state senate + state house —
  executive block split around the legislature. Gov/LtGov one column, one
  vote (§ 9-181). Registrar of Voters (not in § 9-251; elected at state
  elections, § 9-190a) prints as the final column after judge of probate.
  Municipal-election order: "as prescribed by the Secretary of the State
  … so far as practicable … uniform throughout the state" (no published
  SOTS list located).
- Judicial: Supreme/Superior/lower courts APPOINTED (Conn. Const. art. V
  §§ 2-3 — governor nominates, General Assembly appoints). Only JUDGE OF
  PROBATE elected (art. V § 4; § 45a-18(a) "at the state election in
  1974, and every four years thereafter") — QUADRENNIAL for REGULAR
  terms: on ballot 2022 + 2026, NOT 2024. Prints last among statutory
  offices, before registrar. VACANCY carve-out: vacancy contests can
  appear in any state-election year and OUT of § 9-251 sequence — SOTS
  places them by ad hoc directive (Plainville Nov 2025 probate-vacancy in
  column 1; Mansfield 2024 probate-vacancy AFTER registrar). JPs removed
  from § 9-251 by P.A. 74-109.
- Measures: NO placement statute — § 9-369 governs numbering only
  (municipal clerk numbers per appearance order; SOTS numbers
  constitutional amendments by resolution date); explanatory text goes on
  polling-place posters, not the ballot (§ 9-369b). Placement VARIES BY
  TOWN: right-hand panel (Bridgeport/Greenwich/West Hartford/New
  Haven/New Britain), full-width top band (Hartford/Norwalk/Danbury),
  hybrid (Waterbury 2024), back of ballot (Stamford 2024). This sub-leg =
  C-grade town discretion.
- County discretion: NO COUNTY TIER EXISTS — counties are geographic only
  since 1960 (OLR 2015-R-0274); § 9-1(i) municipality = city/borough/town;
  last county-ish office (sheriff) deleted from § 9-251 by P.A. 00-99
  (eff. Dec 2000). Order centrally prescribed (statute + SOTS); town
  discretion = question placement + charter variations in which offices
  are elected (§ 9-185).
- School/special: Board of Education = municipal office (§ 9-185(8)),
  municipal elections ODD-YEAR (§ 9-164(a)(1)(A)) — NO school contests in
  even-year November. Odd-year ballots confirm: Hartford Nov 2025 = BOE
  only; West Hartford Nov 2025 = Town Council (cols 1-6) then BOE (cols
  7-9) — school after town legislative body. Charter exception CONFIRMED
  (verify pass): Stamford Charter § C1-80-5(b) requires an ANNUAL BOE
  election ("Annually at the regular election to be held in Stamford on
  the Tuesday after the first Monday in November, three members of the
  Board of Education shall be elected for terms of three years", per
  S.A. No. 467 of 1951) — in even years that November election IS the
  state election, so 3 BOE seats print on the state ballot (Stamford 2024
  side 2 cols 7-9 "Vote for Three"; electionhistory.ct.gov shows the same
  Full Term contest in 2022). Statutory basis: § 9-203 savings clause
  ("shall not be construed to repeal or affect any special act relating
  to a town which elects the members of its board of education in a
  different manner or for different terms") + § 9-185's "Unless otherwise
  provided by special act or charter" opening. NOT a § 9-164 escape —
  § 9-164 permits only May-of-odd-years variance; Stamford's even-year
  BOE = municipal offices riding the STATE election. Charter fetched from
  library.municode.com (JS SPA — browser render + innerText; curl and
  r.jina.ai get empty shells).
- Corroboration: Bridgeport Nov 2022 amended ballot
  (https://portal.ct.gov/-/media/sots/electionservices/town_ballots/2022/bridgeport-amended-11322.pdf?rev=167e5310b60a452983aeacab1900fb8c&hash=E6CF5DB9F3C597828FD7EDA5C761C9DA,
  curl+UA → PyMuPDF coordinates, researcher-parsed): columns 1-11 =
  Governor and Lieutenant Governor | US Senator | Representative in
  Congress | State Senator | State Representative | Secretary of the
  State | Treasurer | Comptroller | Attorney General | Judge of Probate |
  Registrar of Voters — exact § 9-251 match + registrar appended; rows
  A-I, row I = WRITE-IN VOTES; question in right panel. 18 further
  ballots from the official SOTS town-ballot archive
  (https://portal.ct.gov/sots/election-services/town-ballots/ballots; 10 ×
  Nov 2024, 8 × Nov 2022 across Bridgeport/New Haven/Hartford/Stamford/
  Waterbury/Norwalk/Danbury/New Britain/West Hartford/Greenwich) all
  match — SUBAGENT-SOURCED with only the index URL retained (no
  per-ballot URLs; supporting only, the Bridgeport parse is the verified
  anchor); 2024 STATE-OFFICE BLOCK = 6 columns (Presidential Electors For
  | US Senator | US Rep | State Senator | State Rep | Registrar), with
  charter-driven town offices appended where they exist (Stamford 2024
  BOE cols 7-9); no "PROBATE" string in any of the 10 SAMPLED 2024 PDFs —
  regular quadrennial cycle confirmed (Mansfield 2024 carried a probate
  VACANCY contest, see Judicial).
- Baseline delta: SUBSTANTIAL — (1) Gov/LtGov above US Senate/House;
  (2) SOS/Treasurer/Comptroller/AG below the legislature (executive split);
  (3) no county tier; (4) judicial = single probate column (last statutory
  office), no supreme→appeals→trial block; (5) measures not reliably last
  (town placement, incl. back of ballot); (6) Registrar of Voters after
  the judicial slot; (7) no municipal/school/local offices in even-year
  November (charter exceptions aside).
- Notes: cells labeled column-number + row-letter (1A, 2A…); write-in
  always bottom row; § 9-242(a) nine-party capacity. Party row order
  (one line, § 9-249a): top gubernatorial vote-getter's party first row,
  descending, then minors, then petitioning. Ballots bilingual EN/ES in
  sampled towns. Nov 2026 = gubernatorial AND probate year → the 2022
  Bridgeport 11-column structure is the correct template, not 2024.
  Follow-up sweep (child agent) closed several legs: RCSA 9-242a ALL 28
  sections harvested (eregulations.ct.gov JS shell but full text sits in a
  `var jsonData` blob — plain curl+UA works; r.jina.ai Cloudflare-403s
  there) — nothing on office order; Sec. 9-242a-4 puts printing "in
  accordance with the ballot layout established by the municipal clerk"
  (SOTS-approved). Registrar-last CONFIRMED twice over: SOTS form ED-101
  ("Offices to be Filled at a State Election", § 9-254 instrument,
  https://portal.ct.gov/-/media/sots/electionservices/lead_communications/2014/032014ed101officestobefilledatstateelectionpdf.pdf)
  lists Registrars of Voters after Judge of Probate, and 4 towns' 2022
  ballots print the identical 11-column order. VACANCY WRINKLE: SOTS
  places vacancy offices by ad hoc directive (ROVAC 2024 manual p.100 —
  private association source; corroborated: Plainville Nov 2025 put
  "Judge of Probate / To Fill a Vacancy" in column 1; Mansfield 2024 put
  a probate vacancy AFTER registrar) — vacancy contests can break the
  § 9-251 order. Question sub-rules that ARE stable: statewide
  constitutional amendment always Question 1 ahead of local questions;
  questions always on the office face (reverse = instructions). For state
  elections SOTS transmits binding "town grids" ("exactly as it appears
  on the enclosed list, with no variations" — 2016 Merrill letter); no
  municipal counterpart exists in the LEAD archive (263 items enumerated;
  2015 municipal year has no grid letter). Municipal order = § 9-254
  ED-102 returned-list mechanism, NOT public (form irretrievable; soft-404
  everywhere); de facto municipal shape from 30 Nov-2025 ballots
  (Mayor/First Selectman → Selectmen → Town Clerk → Treasurer → Tax
  Collector → Bd of Finance → Bd of Education → BAA → P&Z → ZBA →
  Regional BOE → Constables) but the legislative-body slot MOVES by town
  (Danbury/Stamford/Newington differ) — never model as fixed. Registrar
  variance CLOSED (verify pass): § 9-185's "Unless otherwise provided by
  special act or charter" is the mechanism — towns with charter/special
  acts appoint instead of elect, echoed by § 9-190 ("in each municipality
  in which registrars of voters are elected"), § 9-190a, and ED-101's
  "*Cross out if not elected" footnote. Stamford 2024 BOE mechanism
  CLOSED — see School/special.

### UT — Utah (FIPS 49) — GRADE A
- Authority: Utah Code § 20A-6-110(6)-(7) "Master ballot position list … —
  Ballot order" (current text, amended by Ch. 329, 2026 Gen. Sess.),
  https://le.utah.gov/xcode/Title20A/Chapter6/C20A-6_1800010118000101.pdf
  (accessed 2026-08-16); identical ladder in force for Nov 2024 as
  § 20A-6-305(6) + (7)(a)-(b) (effective 5/12/2020, renumbered 5/7/2025 —
  subsection (6) byte-identical; the 2020 text LACKS current (7)(c)
  (joint Gov/LtGov ticket) and (7)(d) (joint Pres/VP ticket), so the
  joint-ticket cites are current-law only,
  https://le.utah.gov/xcode/Title20A/Chapter6/C20A-6-S305_2020051220200512.pdf).
  Supporting: § 20A-6-107 (proposition/amendment headings + LG-assigned
  sequential numbering), § 20A-6-301/-304 (manual/mechanical ballots;
  nonpartisan + propositions after the candidate list), § 20A-12-201(3)(b),
  (4) (retention in nonpartisan section + judges.utah.gov statement),
  § 20A-14-104.1(2) (SBOE partisan), § 17B-1-306(7)(b)(i) (special
  districts in nonpartisan section). All from official le.utah.gov PDF
  endpoints (HTML routes are JS shells), curl+UA → pypdf.
- Office order (§ 20A-6-110(6), "each ticket … shall appear separately, in
  the following order"): federal (President/VP → US Senate → US House) →
  state (Governor and Lt Governor as single joint ticket → AG → State
  Auditor → State Treasurer → State Senate → State House → State Board of
  Education member) → county (executive → legislative body → assessor →
  county/district attorney → auditor → clerk → recorder → sheriff →
  surveyor → treasurer → LOCAL SCHOOL BOARD member) → municipal (mayor →
  council) → elected planning & service district council member → judicial
  retention questions → ballot propositions. Governor immediately after US
  House. No Secretary of State exists (LG runs elections); no separate LG
  contest (§ 20A-6-110(7)(c)). Combined offices take the earliest subsumed
  position (§ 20A-6-110(7)(a)).
- Judicial: ALL judges = unopposed retention questions, tier (f) — after
  every candidate contest incl. local school board, before propositions.
  "In the nonpartisan section of the ballot" (§ 20A-12-201(3)(b)(i)) with
  mandatory judges.utah.gov statement (§ 20A-12-201(4)(a)(i)). Exempt from
  the randomized master-list ordering (§ 20A-6-110(5)(b)). Court order NOT
  prescribed — 2024 practice, by county: Salt Lake printed Supreme →
  Appeals → District → Juvenile (no Justice Court questions on that
  style); Utah County printed District → Juvenile → Justice Court
  (high-to-low consistent, but no single ballot shows the full chain).
- Measures: last, tier (g). "Constitutional Amendment ___" (letter) vs
  "Proposition #___" (number); numbers assigned sequentially statewide by
  the LG (§ 20A-6-107(2)(b)(ii) — hence Utah County's gappy #5/#9/#10/#11/
  #13/#14). INTERNAL ORDER NOT PRESCRIBED and counties disagree: Utah
  County printed Amendments A-D then local propositions; Salt Lake printed
  county proposition + bond FIRST, Amendments A-D LAST. Recorded both ways.
- County discretion: minimal on office order (mandatory list). Real
  discretion: combined-office collapse, mechanical ballots "approximately"
  the manual order (§ 20A-6-304(1)(a)), device layout (§ 20A-6-102(3)),
  and the unlegislated measures sub-order. All-mail state
  (§ 20A-3a-202(1)(a)).
- School/special: SBOE = partisan, LAST IN STATE BLOCK (after State House)
  — not a late school tier. Local school board = last in COUNTY block,
  before municipal, printed without party. Special district trustees = "in
  the nonpartisan section" (§ 17B-1-306(7)(b)(i)) with no numbered tier;
  distinct tier (e) only for planning & service district councils.
- Corroboration: Salt Lake County 2024 general official ballot type 1
  (proof watermark 2024-09-17; two sheets:
  https://www.saltlakecounty.gov/globalassets/1-site-files/clerk/elections/2024-general-election/24g-ballots/ballot-type-1---english---page-1-of-2.pdf
  + …page-2-of-2.pdf) — FEDERAL → STATE (Gov/LtGov → AG → Auditor →
  Treasurer → State House 41 → State School Board 7) → COUNTY (Mayor →
  Council At-Large C → Council D6 → Assessor → Recorder → Surveyor →
  Treasurer) → SCHOOL BOARD (Canyons D3) → JUDICIAL RETENTION (statement +
  Supreme → Appeals → District → Juvenile) → countywide propositions →
  Constitutional Amendments A-D; Utah County 2024 general sample
  (https://vote.utahcounty.gov/cms/uploads/2024_General_Sample_Ballot_22344cdbf0.pdf;
  5-page variant independently at https://www.utah.gov/pmn/files/1175207.pdf)
  — same federal/state sequence, school boards closing the county block,
  retention, then Amendments A-D → Propositions. Method correction
  (verify pass): the Salt Lake PDFs carry a full text layer (readable
  directly); only the Utah County PDF is imageless — rendered at 150-400
  dpi and read as images. No office-order conflicts.
- Baseline delta: (1) SBOE inside the state PARTISAN block after State
  House — not a school tier; (2) local school board at END of county
  block, BEFORE municipal (baseline: school after municipal); (3) planning
  & service district tier between municipal and judicial; (4) judicial =
  single retention block after ALL candidate contests (no competitive
  judges); (5) measures last matches baseline but internal order
  county-variable.
- Notes: name order = statewide randomized alphabet drawn by the LG each
  even year (§ 20A-6-110(1)-(4)); no precinct rotation; single-candidate
  races + retention exempt — informational. Primaries use the same
  ordering (§ 20A-6-203(1)(b)). Municipal tier dormant in even-year
  generals (odd-year municipal elections) — statutory position unconfirmed
  by ballot. Utah County sample prints "County Surveyor" header twice
  (county document typo, verified at 400 dpi — the duplicate header sits
  over the TREASURER candidate). Admin-rule negative CLOSED (verify
  pass): R623-9 "Ballot Printing, Handling, and Envelope Standards"
  fetched in full via
  https://r.jina.ai/https://adminrules.utah.gov/public/rule/R623-9/Current%20Rules
  (plain curl 404s — JS shell; proxy renders it; last changed 2025-10-23,
  authorized by §§ 20A-6-108, 20A-3a-106) — covers vendor facility
  security, chain of custody, mailing/IMb, envelope standards; ZERO
  contest-order provisions. 2027: § 20A-6-301(5) removes clerk's name
  from masthead (no order impact).

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
