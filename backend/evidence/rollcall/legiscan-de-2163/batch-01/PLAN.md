# Delaware batch-01 — what was selected and why

**14 measures / 25 rolls.** First imported at 177 records across 14 candidates; after
the House roster campaign grew the crosswalk, re-imported at **346 records across 29
candidates** with no re-judging. Every roll is its
chamber's last kept floor vote on the measure, so every one is on the text that became
law. Local `voteapp` only; production holds no Delaware roll-call records.

## The five filters

1. **Divided.** The losing side is at least a quarter of the winning side. Delaware's
   minority caucus is large enough for this to work unchanged: Republicans hold roughly
   a third of the House and a quarter of the Senate, so the gate is not miscalibrated
   the way it was in Kentucky. 158 of the 1,278 stored floor rolls are divided.
2. **Became law.** 91 of the 158 are on measures the Governor signed. The other 67 are
   dispositioned `out-of-gate` or `excluded` in `survey/divided-worklist.tsv`.
3. **A nameable subject that maps to a research area.** Delaware's titles are formulaic
   ("An Act To Amend Title 26 ... Relating To Public Utility Rates"), so the subject came
   from each bill's own synopsis and then from the enacted text.
4. **One roll per measure per chamber, the last one.** Delaware re-votes in the
   originating chamber after the other chamber amends, so the last roll is the vote on
   the enacted text. Seven earlier rolls are marked `superseded`.
5. **A defensible for-or-against direction.** Sixteen rolls on measures that cleared
   filters 1 to 4 were dropped after a full read; the reasons are in the worklist and
   in JUDGING.md.

## What is in

| measure | rolls | area | yea | reach |
| --- | --- | --- | --- | --- |
| HB 37 — equal accommodations reaches the government | H 26-12, S 15-6 | civil_rights | for | 14 |
| HB 105 — pay ranges in job ads | H 28-12, S 16-4 | reduce_wealth_gap | for | 14 |
| HB 119 — Freedom to Read Act | H 27-10, S 14-6 | civil_rights | for | 14 |
| HB 205 — shield for lawful health care | H 29-11, S 15-6 | womens_reproductive_rights | for | 14 |
| HB 210 — Pollution Accountability Act | H 28-12, S 15-6 | environment_and_public_health | for | 14 |
| HB 344 — campaign finance enforcement | H 24-14, S 16-5 | anti_corruption | for | 14 |
| HB 427 — 16- and 17-year-olds shooting unsupervised | H 28-9, S 14-6 | gun_control | **against** | 14 |
| SB 23 — Housing for Every Delawarean Act | S 14-6, H 29-11 | housing_affordability | for | 14 |
| HB 70 — lead paint certification for older rentals | S 15-6, H 26-11 | environment_and_public_health | for | 14 |
| HB 116 — low-income utility rate | S 15-6, H 26-14 | social_programs_and_welfare | for | 14 |
| HB 50 — Delaware Energy Fund | H 27-13, S 15-6 | social_programs_and_welfare | for | 14 |
| SB 82 — five-year lethal violence protective orders | S 15-6 | gun_control | for | 9 |
| HB 154 — immunity for giving away gun locks | S 15-6 | gun_control | for | 9 |
| HB 444 — Delaware John Lewis Voting Rights Act | H 29-11 | civil_rights | for | 5 |

Nine areas. **`gun_control` carries both directions on purpose**: HB 427 loosens a
supervision rule and scores `against`, while SB 82 and HB 154 score `for`. Tennessee
and Maryland set that precedent.

`election_integrity` was deliberately not used for HB 444. That area is defined as
elections being secure, accurate and auditable; HB 444 is about access and
anti-discrimination, which is `civil_rights` under the rule California settled.

## Reach

When this batch was selected, only 8 of 41 House districts were rostered and a House
roll reached 5 candidates against a Senate roll's 9 — the smallest fan-out of any state
in this campaign, and the reason three of the fourteen measures are one-chamber and why
Senate rolls were worth more than House rolls here.

**That has since changed.** The House roster campaign reached 26 of 41 districts, 15
more sitting members became reachable, and a re-import on 2026-09-04 took this batch
from 177 records to **346 across 29 candidates** — House fan-out median 5 to 19 — with
no re-judging and nothing duplicated, because the fan-out keys on the roll-call URL.
Ledger `import-crosswalk-extension-report.json`. Expect to repeat this as the remaining
15 House districts are rostered.

## What was left for later

43 rolls are marked `candidate:batch-02` — they cleared the divided-and-enacted gate
but have not been read in full. The named one worth an early look is **SB 10**, the
Richard "Mouse" Smith Act on sentence modification.

**The pool will grow.** The session is still open and Delaware's signing lags: 40
divided rolls sit on bills that had passed both chambers but had not been signed when
the dataset was cut on 2026-08-30. One bill in this very batch passed on 24 June and
was signed on 20 August.
