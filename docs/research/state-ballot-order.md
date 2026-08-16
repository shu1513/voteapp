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

### CA — California (FIPS 06) — PENDING

### TX — Texas (FIPS 48) — GRADE B (partial)
- Authority: Tex. Elec. Code §52.092,
  https://statutes.capitol.texas.gov/Docs/EL/htm/EL.52.htm (chapter file;
  section text verified in-browser 2026-08-16 — the page is script-rendered, a
  plain fetch shows only the site nav)
- Office order: federal (president → US senator → US rep) → statewide
  (governor → lt. governor → AG → comptroller → land commissioner →
  agriculture → railroad commissioner → supreme court → court of criminal
  appeals) → district (SBOE → state senator → state rep → court of appeals →
  district judge …) → county → precinct.
- Judicial: within-level, verbatim from §52.092(c)–(d) — statewide courts
  print inside the statewide block after the executives; appellate/district
  judges inside the district block. NOT a late block. Minority pattern.
- Baseline delta: REAL — judicial late block (ranks 82–90) is wrong for TX;
  sample corroboration needed before an override.
- Notes: measures/county-discretion/school questions PENDING.

### FL — Florida (FIPS 12) — PENDING

### NY — New York (FIPS 36) — PENDING

### PA — Pennsylvania (FIPS 42) — PENDING

### IL — Illinois (FIPS 17) — PENDING

### OH — Ohio (FIPS 39) — GRADE B (partial)
- Authority: Ohio RC 3505.04, https://codes.ohio.gov/ohio-revised-code/section-3505.04
  (accessed 2026-08-16)
- Judicial: nonpartisan ballot section after every partisan office — late block
  confirmed. But INSIDE that section the statute puts judicial FIRST: "county
  judicial offices shall be listed first on the ballot, followed by municipal
  and township offices, and by offices of member of a board of education".
- Baseline delta: REAL — baseline prints school (80) before judicial (82–90);
  Ohio's nonpartisan section is judicial → municipal/township → school. Needs a
  within-tail reorder once grade A.
- Notes: §3505.04 speaks to county-level judicial; supreme/appeals placement on
  the nonpartisan ballot needs its own cite in the full pass. Other schema
  questions PENDING.

### GA — Georgia (FIPS 13) — PENDING

### NC — North Carolina (FIPS 37) — GRADE B (partial)
- Authority: NC GS §163-165.6 ("Arrangement of official ballots"),
  https://www.ncleg.gov/EnactedLegislation/Statutes/HTML/BySection/Chapter_163/GS_163-165.6.html
  (accessed 2026-08-16)
- Judicial: within-level (minority pattern) — same caveat as TX.
- Baseline delta: judicial late block wrong for NC; needs grade-A completion.

### MI — Michigan (FIPS 26) — GRADE B (partial)
- Authority: Michigan Ballot Production Standards (Dept. of State, Sept 2024
  edition), https://www.michigan.gov/-/media/Project/Websites/sos/01mcalpine/BallotStandards.pdf
  (accessed 2026-08-16)
- Judicial: nonpartisan section after partisan offices — late block confirmed.
  Inside the nonpartisan section the prescribed order is Judicial → Community
  College → Intermediate School District → City → Township Library → Village →
  Local School District → Metropolitan District → District Library — judicial
  FIRST, before city/township/school.
- Baseline delta: REAL — same shape as OH: baseline's school-before-judicial is
  inverted in MI's nonpartisan tail. Needs a within-tail reorder once grade A.
- Notes: standards republish per cycle — carry the edition forward. Other schema
  questions PENDING.

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
