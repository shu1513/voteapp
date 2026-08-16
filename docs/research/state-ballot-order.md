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

```
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
- Authority: Tex. Elec. Code §52.092 (accessed 2026-08-15, #719 research round)
- Judicial: within-level — judicial offices ordered inside their level of
  government, NOT a late block. Minority pattern.
- Baseline delta: judicial late block (ranks 82–90) is wrong for TX; grade-A
  completion + sample corroboration needed before an override.
- Notes: only the judicial question verified; other 6 questions PENDING.

### FL — Florida (FIPS 12) — PENDING

### NY — New York (FIPS 36) — PENDING

### PA — Pennsylvania (FIPS 42) — PENDING

### IL — Illinois (FIPS 17) — PENDING

### OH — Ohio (FIPS 39) — GRADE B (partial)
- Authority: Ohio RC 3505.04 (accessed 2026-08-15, #719 research round)
- Judicial: nonpartisan ballot section after every partisan office — matches the
  baseline late block.
- Baseline delta: none expected for judicial; other questions PENDING.

### GA — Georgia (FIPS 13) — PENDING

### NC — North Carolina (FIPS 37) — GRADE B (partial)
- Authority: NC GS §163-165.6 (accessed 2026-08-15, #719 research round)
- Judicial: within-level (minority pattern) — same caveat as TX.
- Baseline delta: judicial late block wrong for NC; needs grade-A completion.

### MI — Michigan (FIPS 26) — GRADE B (partial)
- Authority: Michigan ballot-form standards (SOS) (accessed 2026-08-15, #719
  research round — pin exact document + edition during full pass)
- Judicial: nonpartisan section after partisan offices — matches baseline.
- Baseline delta: none expected for judicial; other questions PENDING.

## Batch 2

### NJ — New Jersey (FIPS 34) — PENDING

### VA — Virginia (FIPS 51) — PENDING

### WA — Washington (FIPS 53) — GRADE B (partial)
- Authority: RCW 29A.36.161(3) (accessed 2026-08-15, #719 research round)
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
- Authority: Minn. Rule 8250.1810 subp 5 (accessed 2026-08-15, #719 research
  round); statute backdrop MN 204D.13
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
- Authority: 21-A §601 (accessed 2026-08-15, #719 research round — re-verify
  the exact cite during full pass)
- Office order: Governor prints before US House — deviates from the baseline's
  federal-first tiers.
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
