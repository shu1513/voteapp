# Alabama batch-02 — the one-chamber scope

This batch takes the scope Pennsylvania opened: **divided votes on measures that passed one chamber
and then died.** Under the enacted-only gate these votes are invisible, and in a chamber where the
majority passes most things unopposed they are often where a member's position actually shows.

The pool spans both Alabama sessions — 17 divided rolls in the 2025 session and 9 in the 2026
session — so the 2026 half of the batch lives in `../../legiscan-al-2218/batch-02/`.

## The gate, verified rather than assumed

For every measure here: the LegiScan status is not "enacted", the bill history contains no Act
number, no approval by the Governor and no enacted line, and **the second chamber never held a floor
vote**. Each bill's last recorded action is a committee or calendar step. Both sessions have since
adjourned, so these bills are dead rather than pending — which is why the descriptions can say the
bill died when the session ended instead of using Pennsylvania's time-stamped hedge for a live
session.

## Wording rules for this scope

1. **Conditional throughout** — "would have required", never "requires". These bills changed no law.
2. **The tail states the outcome as a completed fact** — the chamber passed it, the other chamber
   never voted, and it died when the session ended.
3. **Version risk mostly disappears.** With no second chamber there are no later amendments, so the
   engrossed text is what the chamber passed. HB 360 was passed without amendment, so its introduced
   text is the voted text; the other four were engrossed after floor amendments and the engrossed
   print was used.

## What is in the batch

Five rolls from 2025 (413 records) and one from 2026 (89 records).

| Measure | Vote | Label |
|---|---|---|
| HB 30 post-election audits | House 63-30 | election_integrity, yes = for |
| HB 7 Laken Riley Act, immigration enforcement | House 74-26 | immigration, yes = against |
| HB 29 unemployment work-search increase | House 76-25 | social_programs_and_welfare, yes = against |
| HB 234 Alyssa's Law school panic buttons | House 58-30 | public_safety_and_crime_control, yes = for |
| HB 247 Gulf of America Act | House 72-26 | general, no stance |
| HB 360 (2026) Second Amendment Sales Tax Holiday | House 72-29 | gun_control, yes = against |

**Two of these came back and became law the next year**, which is the most useful thing this scope
shows: HB 30's post-election audit returned as HB 95 of 2026, and HB 247's Gulf renaming returned as
HB 2 of 2026. Both 2026 versions are in the batch-01 imports, so a reader sees the position twice —
once on the bill that died and once on the one that passed. HB 247's description says so explicitly.

## Dropped from this pool

- **Contested direction inside the only plausible area:** HB 146 (bars youthful-offender status for
  16-year-olds charged with murder), SB 156 (habitual offender resentencing), HB 42 (partial cash
  bail), HB 403 (a gang database whose membership criteria include style of dress, tattoos and
  association), HB 479 (removes the multistate voter-list cross-check and substitutes a state
  database — both sides claim accuracy), HB 541 (closed primaries), HB 363 (crime of disrupting a
  worship service), SB 298 (police staffing floor enforced by a state takeover of a city police
  department, the SB 330 shape).
- **No research area fits:** HB 280 (public nuisance standing), HB 521 (mixed spirit beverages),
  SB 324 (parole board size), SB 5 of 2025 and HB 169 of 2026 (Archives board), SB 91 (withdrawal
  from multijurisdiction authorities), SB 303 (off-road vehicles, and the one failed vote in the
  pool at 43-44), HB 56 (autocycle helmets), HB 559 and HB 168 (a medical-mask exemption to the
  loitering statute).
- **Local act:** SB 262 (Lowndes County wagering).
- **Misfiled roll:** HB 169's roll carries HB 593's printed number (see
  `../../legiscan-al-2218/CODE-FINDINGS.md` §1).
