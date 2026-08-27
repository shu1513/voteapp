# Illinois batch-01 — selection

22 divided floor votes / 11 enacted measures / **1,364 records across 132
candidates**. Deliberately the size of the Ohio and Texas pilots.

## The five filters

Applied in order to the 2,063 kept floor rolls:

1. **Divided** — `LEAST(yeas,nays) >= GREATEST(yeas,nays)/4` and `nays > 0`.
   590 rolls survive.
2. **Became law** — LegiScan `status = 4`. 427 rolls on 248 measures.
3. **Nameable subject that maps to a research area.** Illinois short titles
   are useless for this (see the README's gut-and-replace table); the subject
   was read off the Legislative Reference Bureau synopsis of the version each
   chamber voted.
4. **One roll per measure per chamber, preferring third-reading passage.**
   Where a chamber also has a concurrence roll, the third reading is the one
   selected — dropped on that basis: H.B. 5295 house concurrence (1721612),
   S.B. 3777 senate concurrence (1721228), S.B. 2339 senate concurrence
   (1717513).
5. **Stance-defensible** — the measure has to carry a research-area label
   with an honest for/against direction. Anything that would land on
   `general` was dropped rather than imported.

## Selected

| measure | Public Act | area / yea | rolls |
|---|---|---|---|
| S.B. 8 Safe Gun Storage | 104-0031 | `gun_control` / for | S 1545154 (33-19), H 1580413 (69-40) |
| H.B. 1373 firearm tracing | 104-0030 | `gun_control` / for | H 1540287 (75-40), S 1582371 (43-11) |
| H.B. 5295 Reproductive Health Records Privacy | 104-0471 | `womens_reproductive_rights` / for | H 1721610 (73-34), S 1721611 (38-19) |
| S.B. 3341 minors' consent to contraception | 104-0570 | `womens_reproductive_rights` / for | S 1717532 (37-19), H 1717533 (73-38) |
| H.B. 3489 pharmacist-dispensed contraception | 104-0312 | `womens_reproductive_rights` / for | H 1539817 (77-36), S 1576151 (41-16) |
| H.B. 5095 identification gender designation | 104-0536 | `civil_rights` / for | H 1719534 (72-37), S 1719535 (38-19) |
| S.B. 3777 Human Rights Act disparate impact | 104-0744 | `civil_rights` / for | S 1721226 (38-20), H 1721227 (72-38) |
| S.B. 1976 Workers' Rights and Worker Safety | 104-0161 | `corporate_accountability` / for | S 1575572 (38-19), H 1582212 (80-32) |
| S.B. 2339 Right to Privacy in the Workplace | 104-0455 | `immigration` / for | S 1544480 (35-21), H 1582511 (75-42) |
| S.B. 3772 environmental justice permitting | 104-0827 | `environment_and_public_health` / for | S 1718025 (43-11), H 1718026 (73-39) |
| H.B. 4339 high school voter registration | 104-0549 | `election_integrity` / for | H 1718089 (77-24), S 1718090 (41-12) |

Eight research areas, none of which had any Illinois coverage before this
batch.

## Dropped, with reasons

- **H.B. 3564** (rental fee transparency, `housing_affordability` on its
  face) — the House's first concurrence motion **lost 56-36-2** and the
  successful one came 64-40 two months and eight amendments later, with the
  house third reading eight months upstream of the enacted text. No single
  defensible per-chamber question. Its companion H.B. 5234 is drafted to take
  effect "if and only if House Bill 3564 becomes law", which compounds it.
- **S.B. 1950** (End-of-Life Options for Terminally Ill Patients Act, the
  most newsworthy divided-and-enacted measure of the session) — no research
  area carries an honest direction for medical aid in dying. Filter 5 drops
  it rather than forcing it onto `general`. **Expect to be asked about this
  one.**
- The remaining 405 divided-and-enacted rolls on 237 measures are untouched
  and listed in `../survey/divided-enacted-worklist.tsv` with status
  `pending`.

## Fan-out arithmetic

11 house rolls at a median 92 matched candidates and 11 senate rolls at a
median 33 predicted roughly 1,375 records; the dry run planned 1,364 and the
real run inserted exactly that.
