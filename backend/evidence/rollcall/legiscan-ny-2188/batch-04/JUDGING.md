# New York batch-04 — A 10710 and A 10711, the two measures held for the user

## Why this batch exists

Both bills set who decides New York's childhood vaccine standards, and an earlier session marked
them `deferred:user-direction` rather than judge them. On 2026-09-05 the user chose: **import both
with a neutral, factual description and no area or direction tag.**

## How "no direction tag" was recorded

`parseRollCallLabels` refuses an empty label list, and a stance area with a null stance fails
validation ("requires stance"). The only label that carries a record with no direction is the
non-stance `general` area, so both measures carry `[{"slug": "general"}]` — the same shape Ohio HB
116, Maine LD 613 and Missouri SB 4 used. The 2026-09-02 label rule that keeps `general` off
roll-call records (Delaware CODE-FINDINGS section 6) is set aside here on the user's explicit
direction for these two measures only; nothing else in this state uses it.

## What the bills do (from the enacted text, chapters 114 and 115 of 2026)

- **A 10710** amends Insurance Law sections 3216, 3221 and 4303. Plans had to cover
  immunizations recommended by the federal Advisory Committee on Immunization Practices. The act
  adds immunizations the state health commissioner recommends to the superintendent, using
  generally accepted medical standards and taking into account the recommendations of the AAP,
  AAFP, ACOG, ACP and similar recognized scientific organizations.
- **A 10711** amends Public Health Law 2164, 2165, 2167 and 2803-j, Education Law 6527, 6802,
  6909 and 6801, and Social Services Law 131. The required childhood immunizations (polio, mumps,
  measles, diphtheria, rubella, varicella, Hib, pertussis, tetanus, pneumococcal, hepatitis B,
  meningococcal) no longer have to meet standards approved by the United States Public Health
  Service; they follow regulations issued by the commissioner using generally accepted medical
  standards and considering the AAP, AAFP, ACOG, ACP, ACIP and similar organizations. Pharmacists
  may give COVID-19 shots to patients two and older on a physician or nurse practitioner order
  (COVID-19 moves off the 18-and-older list); the ACIP references in the pharmacist and posting
  provisions are replaced by the state health department, which also sets the newborn schedule and
  the schedule social services districts hand out.

Both were introduced at the request of the Governor and passed on party lines: A 10710 Assembly
99-39 (roll 1687411, 2026-04-21) and Senate 38-22 (roll 1696603, 2026-05-11, substituted for
S 9599); A 10711 Assembly 90-48 (roll 1687413, 2026-04-21) and Senate 39-23 (roll 1689755,
2026-04-21, substituted for S 9598). Signed 2026-05-15.

Source read: the Assembly bill pages (Summary, Actions, Text) through `ny_bill.py`, added language
marked `{NEW ... NEW}`. The sponsor memo was not read.

## Result

4 rolls approved, **106 records** (53 per Assembly roll), 0 errors. The Senate rolls write nothing
because the crosswalk maps one senator and that seat did not vote. Both worklist rows now read
`batch-04`. New York holds 1,243 records.
