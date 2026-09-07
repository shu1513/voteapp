# Batch-15 judging notes

## SB 135 dropped — the feed does not say what happened to it

SB 135 is the K-12 funding bill. Its history ends:

    2026-05-12 S Senate Considered House Amendments - Result was to Concur - Repass
    2026-05-20 S Signed by the President of the Senate
    2026-05-20 H Signed by the Speaker of the House

There is no "Sent to the Governor" and no governor action of any kind, and the
bill carries an Enrolled text but no Chaptered one. LegiScan's status is 3,
enrolled. Whether it became law, was vetoed, or became law without signature is
not knowable from this dataset.

Every description in this scope must either assert that a bill did not become
law or, in the enacted scope, that it did. SB 135 supports neither claim, so it
is left unjudged and flagged for external verification against the Colorado
Secretary of State's session laws. Its two divided rolls — House 42-21 on 9 May
and Senate 23-12 on 12 May — remain available if that check is done.

This is the second measure this session whose LegiScan status could not be taken
at face value; the first was SB 80, which the feed marked failed and which was
actually signed (batch-12).

## SB 48 is the one bill here that its own chamber killed twice over

On 13 May the Senate concurred in the House amendments 18-17 — exactly the
constitutional majority — and then, on the vote to repass the bill under those
amendments, rejected it 11-24. Seven senators who had just voted to concur voted
against passage. The tail of the description states both numbers so a reader is
not left thinking 11-24 was the whole story.

## HB 1281 narrows a charge, and the description says so first

Under current law, causing the death of another person through conduct showing
extreme indifference to human life is first degree murder. The bill would have
kept that only where the conduct caused **more than one** death, or the death of
a child under 12, or the death of an on-duty peace officer, emergency medical
provider or firefighter, or one death plus serious bodily injury to two or more
others by a deadly weapon. A single death by extreme indifference would have
dropped to second degree murder. It would also have made each person killed or
seriously injured a separate violation, and created aggravated vehicular
homicide for drivers with prior convictions, drivers eluding police and drivers
fleeing another felony.

A reader told only that the bill "aligned homicide offenses" would not learn
that its main effect is to reduce a charge.

## HB 1273's cap has three parts

A take rate above 20 percent of the consumer fare would have been barred; the
driver would have had to receive at least 80 percent of that fare; and a
company could not have charged a driver a per-task fee where the take plus the
fee exceeded 20 percent. Pass-through amounts would have had to reach the driver
in full.

## SB 43 covers unfinished barrels

A "firearm barrel" would have included any forging, casting, printing,
extrusion or machined body far enough along to be readily completed into one, or
marketed as such. A barrel permanently attached to a firearm is excluded. A
first offense is an unclassified misdemeanor of up to $500 and 30 days; a second
is a class 2 misdemeanor. Possession with intent to sell, by anyone who is not a
federally licensed dealer, would also have been unlawful.

## Reconciliation

516 ledger inserts = 516 rows across 55 candidates. 292 rows are yea-side and
there are exactly 292 tags. The convergence run reports all 516 `unchanged`. The
dry run wrote nothing: 6,926 records before it and 6,926 after.

No existing record was flagged as related, and nothing was retired.
