# Maryland batch-01 — selection

**11 measures / 20 rolls / 1,599 records across 158 candidates.** Deliberately
the size of the Georgia and Ohio pilots.

## The gate

The pool was the session's **320 divided-and-enacted rolls on 176 measures**
(divided = `LEAST(yeas,nays) >= GREATEST(yeas,nays)/4.0` with at least one nay;
enacted = LegiScan status 4, cross-checked against the bill page's
`Approved by the Governor - Chapter <n>` line). The five standing filters:

1. **divided** — a lopsided vote tells a voter nothing;
2. **became law** — a measure the voter now lives under;
3. **a nameable subject** that maps to one of our research areas;
4. **one roll per measure per chamber**, preferring the roll cast on the
   **enacted text** (see below);
5. **a defensible for/against direction** — anything that would land on
   `general` is dropped rather than imported.

## Filter 4 in Maryland: every selected roll is on the enacted text

Maryland does not roll-call concurrence, but the originating chamber takes a
**second** recorded third-reading vote right after it concurs. So an amended bill
carries three rolls, and the later originating-chamber roll is the one cast on
the text that became law.

Each measure's version shape was read off the **chapter file's suffix** on the
bill page (`…t.pdf` = the Third Reading print was enacted unchanged; `…e.pdf` =
the second chamber amended it), cross-checked against the fiscal note's own
version header:

| measure | chapter | shape | rolls kept |
|---|---|---|---|
| HB 39 | 651 | `t` — unamended | H 1499761, S 1561552 |
| HB 197 | 240 | `t` — unamended | H 1505879, S 1559874 |
| SB 848 | 436 | `t` — unamended | S 1532080, H 1537949 |
| HB 424 | 611 | `e` — Senate amended | H 1546805 (post-concurrence), S 1561850 |
| SB 901 | 431 | `e` — House amended | S 1561754 (post-concurrence), H 1546815 |
| HB 767 | 563 | `e` — Senate amended | H 1547072 (post-concurrence), S 1561714 |
| HB 983 | 278 | `e` — Senate amended | H 1545740 (post-concurrence), S 1565951 |
| HB 1020 | 121 | `e` — Senate amended | H 1546881 (post-concurrence) |
| HB 1222 | 718 | `e` — Senate amended | H 1547029 (post-concurrence), S 1561703 |
| HB 1378 | 104 | `e` — Senate amended | H 1545844 (post-concurrence) |
| HB 1424 | 97 | `e` — conference report | H 1546900, S 1572563 (both post-CCR) |

**Consequence: no measure in this batch needed a per-chamber version split.**
Both rolls of every two-roll measure sit on the same enacted text, so both carry
the same description — unlike Texas SB 379, Illinois SB 3777 or Tennessee SB 336.
The superseded originating-chamber rolls (HB 1222 H 1522475, HB 424 H 1506154,
HB 767 H 1522900, HB 983 H 1522862, HB 1020 H 1508428, HB 1378 H 1544279,
HB 1424 H 1518012, SB 901 S 1532109) were dropped by filter 4, not overlooked.

Two measures contribute a single roll because the other chamber's vote was not
divided: **HB 1020** (Senate 40-3) and **HB 1378** (Senate 36-7).

## Crossfiled twins

Maryland enacts both halves of a crossfiled pair as consecutive, identical
chapters (README §"Crossfiled twins"). Batch-01 takes **exactly one twin per
pair**: HB 424 not SB 357 (Ch. 611/610), SB 848 not HB 930 (Ch. 436/435), HB 39
not SB 356 (Ch. 651/652). Importing both would write two records making the same
claim onto every legislator.

## Selected measures

| measure | chapter | area | yea direction |
|---|---|---|---|
| HB 1222 Maryland Values Act | 718 | immigration | **for** |
| HB 1424 Protect Our Federal Workers Act | 97 | social_programs_and_welfare | for |
| HB 424 Prescription Drug Affordability Board | 611 | healthcare_affordability | for |
| SB 901 packaging producer responsibility | 431 | environment_and_public_health | for |
| HB 767 landlord-tenant eviction notice | 563 | housing_affordability | for |
| SB 848 Public Health Abortion Grant Program | 436 | womens_reproductive_rights | for |
| HB 1378 child sexual abuse damages caps | 104 | civil_rights | **against** |
| HB 39 Carlton R. Smith Act (HIV) | 651 | civil_rights | for |
| HB 983 election language assistance | 278 | election_integrity | for |
| HB 1020 Fair Medical Debt Reporting Act | 121 | corporate_accountability | for |
| HB 197 restorative practices schools | 240 | public_education_quality | for |

Ten areas, and `civil_rights` deliberately carries **both directions** in one
batch (HB 39 for, HB 1378 against) — the Tennessee `gun_control` pattern.

## Dropped under filter 5, after a full read of the fiscal note

- **HB 853 Maryland Second Look Act** (Ch. 96) — resentencing after 20 years for
  people convicted at 18-24. `public_safety_and_crime_control` reads
  "justice system performance" one way and "safety" the other; genuinely
  contested direction. The session's most newsworthy divided-and-enacted
  criminal-justice measure — **expect to be asked.**
- **SB 181 / HB 1123 geriatric and medical parole** — same shape as HB 853.
- **HB 716 statewide rental assistance vouchers** (Ch. 234) — runs both ways in
  its own text: it prioritises vouchers for children, veterans, disabled and
  homeless applicants, but it also **loosens unit inspections from annual to
  biennial**.
- **SB 937 / HB 1035 Next Generation Energy Act** — an energy omnibus whose
  strands pull opposite ways (emissions reductions alongside expedited approval
  for new generation); the Florida SB 700 shape.
- **HB 1503 state-employee paid family and medical leave** (Ch. 606) — real and
  divided, but no research area carries an honest direction for a state
  workforce benefit.
- **HB 260 drug paraphernalia penalties** (Ch. 180) — decriminalisation vs
  enforcement, contested.
- **HB 504 Excellence in Maryland Public Schools Act** (Ch. 237) — Blueprint
  changes; contested direction within `public_education_quality`.
- **HB 872 / SB 606 tenants' right of first refusal** — a narrow liability
  clarification, no direction worth a record.
- **HB 350 Budget Bill / HB 352 BRFA** — appropriations, dropped by standing
  precedent (there is no honest direction on funding the government).
- Speed-monitoring-system bills (HB 182, HB 343, HB 913, HB 1173, SB 338 and
  others) and single-county measures — local/narrow.
