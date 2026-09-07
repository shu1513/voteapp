# Batch-03 judging notes

## HB 1107 is a gutted bill — the second one this session

The feed calls it "measures to increase access to services in facilities that
provide medical care". The enrolled act does one thing: it creates a dementia
care services information form, requires licensed dementia facilities to
complete, publish and produce it, and lets civil fines already collected under
the assisted living statute pay for building the form. Nothing in it touches
access to medical care generally.

This is the same trap as SB 124 in batch-01. **Triage Colorado 2026 on the
`description` field, and then still read the act.**

## HB 1411 dropped — the act cuts, it does not add

The bill is titled as "changes to health insurance benefits for certain
low-income individuals who are not eligible for medical assistance due to their
immigration status". Reading it, every change is a restriction on the existing
state-funded program:

- an annual $1,100 cap on dental services from 1 July 2026;
- behavioral health on a fee-for-service basis only from 2027;
- no accountable care collaborative and no managed care from 2027;
- long-term services, supports and ongoing home health cut off for children not
  already receiving them by the end of 2026;
- an enrollment cap of 25,000 children for 2026-27 if enrollment or spending
  crosses a trigger.

The House passed it 41-20 during the budget. A yes vote is not "for immigrant
health coverage" and it is not plainly "against" it either — it keeps a program
alive by shrinking it, and the no votes come from both directions. There is no
defensible single stance, so the measure is dropped rather than given one. This
follows the campaign's contested-direction rule.

## HB 1002 does two separate things, and only one is the headline

The headline is provider directory accuracy. The quieter half extends
Colorado's timely credentialing statute — which until now spoke only of
"physicians" — to mental health providers, substance use providers and
psychiatric nurses, and adds a duty to reimburse a participating provider for
medically necessary care furnished by a **pre-licensed** provider under
supervision. Out-of-network plans owe the same for supervised out-of-network
trainees.

## SB 32 is about who decides, not about a mandate

The act does not require anyone to be vaccinated. It creates a Colorado
schedule of recommended adult immunizations set by the state board of health,
which must take the federal ACIP into account **alongside** the American Academy
of Pediatrics, the American Academy of Family Physicians, the American College
of Obstetricians and Gynecologists and the American College of Physicians. It
also changes the HPV coverage mandate from "all females" to "all individuals"
for whom the vaccine is recommended, and adds a fallback: if ACIP stops
recommending HPV vaccination, the commissioner may still require coverage.

## SB 113 leaves a lot out

The licensing duty covers a residence that bans alcohol and non-prescribed
drugs, promotes independent living, and provides recovery support to people with
a primary behavioral health diagnosis. It expressly does **not** reach family
homes, licensed residential treatment, permanent supportive housing, shelters
and stays averaging under three days, health facilities, or reentry programs
that serve people regardless of diagnosis. The penalty is a civil fine of $50 to
$100, not a criminal one.

## SB 167's credit has real limits

The credit is capped at what the same drug would have cost in network under the
plan. It does not apply if the patient sends no proof within 90 days, if the drug
is off the plan's formulary and no exception is granted, or if the patient
skipped the plan's prior authorization or step therapy.

## SB 178 is a borrowing authority, not a new subsidy

The enterprise may issue up to $100 million in bonds on or after 1 January 2027.
For 2027 onward the allocation is at least 20% to state subsidies, at least 50%
to reinsurance, at least 25% to reducing individual plan costs, up to 3%
administration, and then Hyde Amendment compliance costs. A study due by 1 July
2027 must evaluate a federal basic health program.

## Reconciliation

492 ledger inserts = 492 rows across 55 candidates. 378 rows are yea-side and
there are exactly 378 tags. The convergence run reports all 492 `unchanged`. The
dry run wrote nothing: 1,102 records before it and 1,102 after.

No existing record was flagged as related, and nothing was retired.

## A checker bug found and fixed

The British-spelling guard in the judgments builder listed `analys` as a stem,
which flagged the American word "analysis" in HB 1425. It now lists `analysed`
and `analysing`, and "analyse" was deliberately left out because "analyses" is
also an ordinary American noun. The fix was verified in both directions before
being trusted: analysis, analyses, analyst, enroll, fulfill, offense, license
and defense pass; analysed, analysing, enrol, fulfil, behaviour, licence,
organisation, practising, favours, offence, recognise, centre, programme,
defence, colour and labour are all still caught. This is the same class of
false positive that stopped batch-11 of the 2025 session.
