# New York batch-03 — the last 51 measures

## New York is finished, except two the user must decide

**Every row in `../survey/divided-enacted-worklist.tsv` now carries a disposition, apart from two
that are deliberately held for the user.** A 10710 and A 10711 set the childhood vaccine schedule.
An earlier session marked them `deferred:user-direction` rather than judging them, and that
decision stands. They need direction before anyone writes a description.

**7 measures, 8 rolls, 209 records, 158 area tags, 0 errors.** New York now holds **1,137
records**.

| measure | area | direction |
| --- | --- | --- |
| A 584 Trapped at Work Act | corporate_accountability | for |
| A 6558 allergen labeling on prepacked food | environment_and_public_health | for |
| S 745 ammunition dealers and the merchant category code | gun_control | yea = against |
| S 1069 no oil and gas leases on state forests | environment_and_public_health | for |
| S 3294 medical cannabis | healthcare_affordability | for |
| S 4906 victim statements at the workplace | public_safety_and_crime_control | for |
| S 6997 pole attachment safety | public_infrastructure | for |

## The finding that matters most: chapter amendments hide behind their titles

`../CODE-FINDINGS.md` section 3 already recorded that New York enacts an act and then a second
bill amending it, and set the rule — import the act, drop the amendment. What this batch adds is
**how to find them**, because titles do not reveal them.

Six of the measures read here turned out to be chapter amendments, and **two are amendments of
bills this very campaign imported**:

- **S 8807 amends S 4914**, the reproductive and gender-affirming care shield law imported in
  batch-02.
- **S 8836 amends S 1069**, the state-forest drilling ban imported in *this* batch.

Neither appeared on the twin list built from titles. The reliable signal is the **effective-date
clause**: a New York bill whose effective date reads "on the same date and in the same manner as"
another chapter is a chapter amendment. Check that phrase, not the title. A 9516, S 824, S 815,
S 805 and S 8763 were all caught the same way.

This matters beyond bookkeeping. S 8807 *lowers* the shield law's penalty from $15,000 to $10,000
per violation while widening the Attorney General's power to sue, and S 8836 *carves an exemption*
into the drilling ban for leases already in place. Importing either alongside its parent act would
have given the same legislator two records making opposite-leaning claims about one policy.

## What else was dropped, and why

Of the 51: 7 imported, 1 excluded as an omnibus, and 43 dropped. Eleven were read in full; the
rest were set aside on the title and digest with an individual reason recorded in the worklist.

The recurring reasons are the same three as everywhere in this campaign: **no research area covers
the subject** (electronic wills, model management registration, airport commission appointments,
ballot drop-box mechanics, a single yeshiva's tax exemption); **the measure is a chapter
amendment**; and **it moves both ways inside one area** — S 550 requires a child-abuse caller to
give their name while adding confidentiality for that caller, and S 8012 lowers what solar and
wind projects pay while shielding assessors from cost awards.

A 1894 is worth noting separately: it repeals a Public Health Law section in two lines, and a
repeal bill does not reprint what it repeals, so what the removed duty actually required cannot be
stated from the text.

## Checks

16 descriptions, **0 plain-language lint warnings**, 0 checker problems, Flesch-Kincaid median
grade **8.1**, worst **8.8**, longest sentence 23 words. **All 8 stated tallies match the stored
vote row.**

## Reconciliation

Predicted independently before touching the database: **209 records and 158 area tags**. Dry run
209 insert, real run 209 insert with 0 errors, re-run all 209 unchanged.

## Review revisions (2026-09-05)

Two descriptions were corrected after PR review, re-judged, and re-imported (52 + 49 rows
rewritten, re-run all 209 unchanged).

- **S 4906** — the first draft applied the second-degree-assault condition to the whole right.
  The new subdivision 6 gives any assault victim at a qualifying workplace the choice to give
  their statement there; the Penal Law 120.05 condition gates only the officers' duty to tell
  the victim about that choice. The description now keeps the right and the notice apart.
- **S 6997** — the first draft promised a week to cure before any fine. Section 119-e(4)(b)(i)
  lets the commission shorten or extend that week by the nature and severity of the violation,
  and (4)(b)(ii)–(c) carve out one-touch make-ready violations, which draw a fine of up to
  $20,000 with no cure period at all. The description now says both.
