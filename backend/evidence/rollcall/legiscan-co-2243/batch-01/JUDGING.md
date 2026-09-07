# How this batch was judged

Every measure was judged from its **enrolled act**. No claim here comes from a
bill summary, and none was carried over from the 2025 version of the same idea.
That discipline is the direct result of the review of Colorado batch-11, which
found ten of eighteen descriptions needed correcting because they rested on
summary text or quietly widened a bill's scope.

It paid for itself immediately. **Five claims I was ready to write turned out to
be wrong**, and each was caught by reading the operative text:

| I was going to say | the act says |
|---|---|
| HB 1001 covers land owned by faith groups and schools | school districts, colleges, boards of cooperative services, housing authorities, transit districts and affordable-housing nonprofits — no faith organizations |
| HB 1001 protects buildings up to 45 feet | **38 feet** |
| HB 1134 caps municipal jail terms at the state equivalent | it does not; that was the vetoed 2025 bill. This one sets 48-hour hearings, counsel for defendants who cannot pay, and flat-fee limits |
| HB 1424 requires fingerprint background checks | it does not; that was the vetoed 2025 bill. This one moves the existing check to the company, every six months |
| HB 1139 requires a person to approve a denial of care | it requires the insurer to **disclose whether** a person must approve one |

## HB 1322 says outright that it creates nothing new

Subsection (6) reads: this section does not create a new cause of action. So the
description says the act "sets out" that a person harmed by conversion therapy
can sue and removes the time limit — not that it creates a new right to sue.
Getting that wrong is the same class of error the batch-11 review caught on
SB 185.

## SB 124 was dropped: the vehicle-bill trap, arriving through the title

SB 124 is titled **"Colorado Survivor Justice Act"** in the feed. Its enrolled
act is titled *Concerning information related to the automated protection order
notification system*, and the whole act amends what that system records — adding
whether a background check denied the restrained person a firearm, and repealing
one existing item.

The `description` field matches the act; only `title` is stale. The measure is
administrative and pulls in both directions, so it is dropped rather than given
a stance it does not have. **The reusable rule: triage Colorado 2026 on
`description`, never on `title`.**

## The version check had to be rebuilt for this session

96% of this session's bill texts carry the epoch date `1969-12-31`, so the
print-in-force-on-the-vote-date method is impossible. The action history is
correctly dated, so the check asks instead whether any text-changing action
follows the chamber's selected roll. Concurring in the other chamber's
amendments **accepts** text and is not a change; refusing to concur, adhering,
passing with amendments and conference reports are. All 21 rolls came back clean.

## Seven rolls were the wrong kind until the judge said so

Seven of the House rolls are `Senate Amendments Repass` votes with a same-day
`Concur` peer, not plain third readings. The first judge run refused the batch
and named the peer. All seven were corrected to repassage wording and the
same-day concurrence acknowledged.

## A hand-written record that was wrong about a member's vote

The dry run flagged five existing records as related. Four are the familiar
same-date, different-bill false positive. The fifth is not:

> "Voted Yes on House concurrence for HB26-1144 … the vote passed 40-23."

**Dusty Johnson voted Nay on both House votes on that bill** — the concurrence,
which carried 44-19, and the repassage, which carried 40-23. The record also
attaches the word "concurrence" to the repassage tally. It is retired with that
reason in `retirements.json`, and the imported record cites the roll call itself.

## Wording

First draft measured a Flesch-Kincaid median of 7.7; after rewriting the four
hardest measures it is 7.5. The plain-language lint reports 0 warnings across
all 42 descriptions. Reading grade is computed separately, because the lint only
counts words per sentence.
