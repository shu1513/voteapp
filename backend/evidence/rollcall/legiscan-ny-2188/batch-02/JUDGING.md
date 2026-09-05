# New York batch-02 — ten measures read, eight kept

**8 measures, 11 rolls, 358 records, 225 area tags, 0 errors.** New York now holds 928 records.
51 measures remain as `candidate:batch-03`, and 2 still await a user decision.

| measure | area | chambers |
| --- | --- | --- |
| A 387 hospital language assistance | civil_rights | Assembly |
| A 1820 discriminatory covenants in deeds | civil_rights | Assembly + Senate |
| S 123 infant walker sale ban | environment_and_public_health | Assembly |
| S 1353 coerced debt | cost_of_living_reduction | Assembly |
| S 2551 fines on convicted companies | corporate_accountability | Assembly + Senate |
| S 4914 shield law for reproductive and gender-affirming care | womens_reproductive_rights | Assembly + Senate |
| S 8311 NYCHA succession policies | housing_affordability | Senate |
| S 8420 disclosing a synthetic performer in an ad | corporate_accountability | Assembly |

## Two dropped

**A 56** on bounced rent check fees moves both ways: it requires the fee to be in the lease and
proved on request, but lets a landlord charge actual costs above the old flat $20.

**S 8789** is a clean-up bill riding on a separate 2025 chapter whose text is not in the feed. It
widens "employer" to include state and local government while deleting the high-public-trust
limit, so any position subject to a state-agency background investigation becomes exempt, and its
section 3 repeals part of that 2025 chapter in words that cannot be read from what is available.

## Version handling

New York dates every text version to the bill's introduction date, so dates cannot order
versions. The rule used here is to take the **last `Amended` record by document id**, which is the
version that passed. All eight measures carry status 4.

## Honesty about what these acts do not do

Every description names the limit a reader would otherwise miss:

- **A 387** reaches general hospitals only, not clinics or doctors' offices, and names no penalty.
- **A 1820** does not void the covenants — they are already unenforceable — blocks no sale, and
  carries no penalty.
- **S 123** bans sale and lease only; a parent may still own and use a walker at home.
- **S 1353** leaves the burden on the debtor, and the creditor may still collect from anyone else,
  sue the abuser, and restart collection if it explains why.
- **S 2551** reaches companies only, not people.
- **S 4914** does not change who may get care or what a doctor may do.
- **S 8311** gives nobody a succession right and leaves eligibility to the housing authority.
- **S 8420** falls on whoever makes the ad, not the platform that carries it, and exempts film,
  television, streaming, games and audio-only ads.

## The New York Senate crosswalk is effectively empty

Every Senate roll in this batch resolves to **exactly one** candidate record. Assembly rolls
resolve to 49 to 52. That is why 11 rolls yield 358 records rather than roughly 550, and it makes
a **New York Senate roster campaign the single biggest lever on this jurisdiction** — far larger
than any further judging. Senate rolls are still imported, because a later re-import picks up new
members without duplicating anything.

## Chapter-amendment twins

New York's governor routinely signs a bill on the understanding that a second bill will amend it.
Both become law and both can be divided. `../CODE-FINDINGS.md` section 3 sets the rule: import the
act, drop its chapter amendment. Four pairs are visible by title in the remaining pool —
A 1820 with S 8760, S 73 with S 8832, S 7416 with S 8887, and A 584 with A 9452. **A 1820 is the
act half and is imported here; S 8760 must be dropped as its twin** when batch-03 reaches it.

## Checks

| check | result |
| --- | --- |
| Plain-language lint | 22 descriptions, **0 warnings** |
| Reading-level and style checker | **0 problems** |
| Flesch-Kincaid grade | median **7.6**, worst **8.8** |
| Stated tallies against the stored vote row | **11 of 11 match** |

## Reconciliation

Predicted independently before touching the database: **358 records and 225 area tags**. Dry run
358 insert, real run 358 insert with 0 errors, database 358 and 225, re-run all 358 unchanged. The
dry-run stamp `2026-09-05T04:20:07.933Z` matched zero rows; the real stamp is
`2026-09-05T04:20:10.217Z`.

## Still open

**A 10710 and A 10711 set the childhood vaccine schedule.** An earlier session marked them
`deferred:user-direction` rather than judging them, and that decision is left standing. They need
the user's direction before anyone writes a description for them.

## Review revisions (2026-09-05)

One description was corrected after PR review, re-judged, and re-imported (49 rows rewritten,
re-run all 358 unchanged).

- **S 8420** — the first draft made the expressive-works exemption blanket. Subdivision 4
  exempts ads for films, television, streaming content and video games only where the
  synthetic performer's use in the ad is consistent with its use in the work itself; a
  synthetic performer built just for the ad is not exempt. The description now carries that
  condition.
