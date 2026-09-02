# California batch-06 — judging notes

Method as repaired after batch-05, applied from the start: for each measure, extract every change the
Legislative Counsel's Digest enumerates **without truncating any sentence**, read the enacted text
for the exact terms, then check the drafted description against the digest list item by item —
**before** judging or importing.

## Label calls

| measure | label | why this direction |
| --- | --- | --- |
| AB 263 | `environment_and_public_health`/for | Keeps drought-era instream flow protections on two salmon rivers in force until permanent rules exist. |
| SB 25 | `corporate_accountability`/for | Gives the state's antitrust enforcer the same merger filings federal regulators get, with a penalty for not filing. |
| AB 1084 | `civil_rights`/for | Removes the objection mechanism from gender-conforming name changes — "fair treatment under law". Pairs with batch-04's SB 59, which sealed the same records. |
| AB 435 | `environment_and_public_health`/for | A child-restraint safety standard: "community health through standards … and prevention". `public_safety_and_crime_control` was considered and rejected — that area is about policing and the justice system, not vehicle safety. |
| SB 709 | `corporate_accountability`/for | Consumer price transparency: the renter learns the real ceiling before signing. |

## Traps caught while reading

- **AB 263 has an end condition, not just a date.** The emergency rules last until 2031 **or** until
  permanent flow rules are adopted, whichever comes first — the descriptions carry both.
- **SB 25 has a two-part trigger.** It reaches a company with its principal place of business in
  California **or** California sales of the goods or services in the deal of at least 20% of the
  federal filing threshold. Saying "companies merging in California" would have been wrong.
- **SB 25 protects the filings.** The Attorney General may not disclose what is filed; a description
  that stopped at "must file with the state" would read as a disclosure regime, which it is not.
- **AB 1084 is not one rule but four paths** — adults, minors with every parent's signature, minors
  without, and when a hearing happens at all. All four are stated, plus the dropped 30-day
  judgment-filing requirement, which the digest lists separately.
- **AB 435 replaces a height rule with a fit test.** The old law let a child under 8 use a belt at
  4 feet 9 inches; the new standard is the five-part fit test, stated in full because each part is
  the operative rule.

## Runs

| step | result |
| --- | --- |
| completeness audit (pre-import) | 24 digest items across 5 measures, **all covered** |
| plain-language lint | 10 descriptions, **0 warnings** |
| `rollcall:judge --dry-run` → real | 5 `dry_run` → 5 `updated` |
| `rollcall:legiscan:import --dry-run` | **311 planned inserts**, 3,548 unchanged, 0 errors, 0 notified |
| `rollcall:legiscan:import` | **311 inserts**, 0 errors, 0 notified |
| re-run `--dry-run` | **3,859 unchanged**, 0 errors |

**Reconciled three ways.** `candidate_records` 113,684 → 113,995 (+311); the run's predicate
`origin_run_id LIKE 'rollcall:CA:%:2172:%:2026-08-31T06:00:43.395Z'` returns 311 rows; and the dry
run's stamp `2026-08-31T06:00:20.168Z` matches **zero** rows.

California now holds **3,859 roll-call records across 80 candidates** over 54 measures. Prod
untouched.

## Review response (2026-08-31) — SB 25's two-track document rule, 62 records rewritten

**The finding (P1, true).** Section 16782 splits the additional-documentary-material duty in two:
a filer with its principal place of business in California includes the material **with the filing**
(subdivision (b)); a filer covered only by the 20%-of-threshold California sales test provides it
**only on the Attorney General's request, within seven business days** (subdivision (c)). The first
draft said every covered company hands the documents over with the filing — wrong for the
sales-only class. Reading the section in full also surfaced a term the description lacked: the
filing itself is due **within one business day** of the federal filing (subdivision (a)). Both now
stated. 62 records rewritten, re-run 3,859 `unchanged`, lint 0 warnings, row count unchanged.

**Why the pre-flight audit missed it.** The digest item — "would also require the person to file or
provide the additional documentary material required under federal law, **as specified**" — was on
the checklist and the description matched it. But "file **or provide**" IS the two-track split,
compressed into three words, and "as specified" was an unexpanded pointer. The audit checked that
the topic was covered; it did not resolve the pointer to the statute's terms. The batch-05 rule —
every "as specified" inside a digest item must be chased into the statute before check-off —
applies even when the item already appears covered.
