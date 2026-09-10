# Nebraska batch-04 judging

One measure judged, one dropped. Both were read from the Final Reading text —
the version the roll call actually voted on — with the markup reader, never
from a committee statement and never from the Introducer's Statement of Intent.

## Sources

- The Final Reading print of each act, read through `ne_text.py`, which asks
  the PDF where its drawn lines sit so that struck-through words and underlined
  words stay apart. A plain text dump runs the old rule and the new rule
  together and can say the opposite of what the act does.
- The bill's own action history on the Nebraska Legislature site, for the
  tally, the Governor's approval, and the list of bills folded in.

The committee statement was not used for either measure. Nebraska's committee
statement describes the bill the committee sent out, not the act, and it
disagreed with the enacted text seven times across the earlier batches.

## LB 415, narrowing the paid sick leave law — social_programs_and_welfare, against

Nebraska voters adopted a paid sick leave law by initiative in November 2024.
The act changes it before most of it had taken effect. Read against the Final
Reading print, it does all of the following:

- Takes independent contractors, individual owner-operators, workers in
  seasonal or other temporary agricultural jobs, and anyone under sixteen out
  of the definition of employee.
- Raises the floor for a covered employer from one worker to eleven.
- Requires eighty hours of work in continuous employment before paid sick time
  starts to accrue, where the old text began accrual at the start of
  employment. (Eighty hours worked, not an eighty-hour stretch: the state
  Department of Labor's FAQ reads the clause the same way.)
- Strikes the subsection that let an employee sue an employer for a violation
  and recover the judgment plus costs and attorney's fees, and strikes the
  four-year limitation period that went with it.
- Says an employer whose own paid leave policy equals or exceeds the standard
  owes nothing more and need not let time carry over beyond that policy.
- Says nothing requires an employer to pay out unused paid sick time when
  employment ends.

The second directional strand extends the Gambling Winnings Setoff for
Outstanding Debt Act. The state already took casino, sports wagering and
parimutuel winnings to satisfy tax debts; the act adds unemployment benefit
overpayments to what can be taken, and brings the Department of Labor and the
Department of Motor Vehicles into the collection system. The word "tax" is
struck out of the phrase "outstanding state tax liability" throughout, which is
what widens it.

The third strand rewrites the Conveyance Safety Act: which standards editions
apply to elevators and lifts, what an equivalency or variance is, and what an
application for an elevator mechanic or contractor license must contain. It is
administrative and takes no side.

The area is `social_programs_and_welfare` on the line already set by LB 229 in
batch-02 — no labor area exists, and the campaign has put worker safety-net
coverage here since West Virginia HB 4005. Yes is `against`: the act removes
coverage, removes a remedy, and takes money back out of benefits already paid.
The no side takes no tag, since a no vote could rest on any one of the three
strands.

The description names the initiative because that is the fact a reader needs:
this is the Legislature changing something the voters passed, which is also why
the vote was close.

## LB 530, motor vehicle homicide and road safety — dropped

Dropped under filter 5. The act has no defensible direction. PLAN.md sets out
the changes that raise penalties and the changes that lower them; they sit side
by side in one policy area, so neither `for` nor `against` can be written
honestly. The rule is the one that dropped LB 921.

Two things are worth recording for a future operator. First, the eight folded
bills are only visible in the history, dated after the Governor's signature,
and their titles alone were enough to raise the question — but only the full
read settled it. Second, the reader has to be careful about direction in § 3.
The first draft of PLAN.md read the driving-ban change backwards, as a
mandatory one-to-fifteen-year revocation becoming a discretionary two-year
suspension. Review against the enrolled act and current 28-306 showed the
opposite: drunk- or drugged-driving homicide now carries a flat fifteen-year
ban, and the lower grades gain a discretionary ban where they had none. That
correction moves the largest item out of the "penalties go down" list, so the
act leans harder toward tougher penalties than the first read suggested. It is
still dropped: the probation-fee waiver, the extend-rather-than-revoke rule,
and the new five-day juvenile review pull the other way inside the same act,
and filter 5 asks whether the strands conflict, not which side is heavier.

## Quality

Reading level: grade 8.6, longest sentence 24 words, 13 sentences. The builder
ran the 45-word check, the comma splice check and the British spelling check
before writing the file, and the spelling check was proved to fire on a
deliberately wrong string first.

Repository lint over all Nebraska descriptions: **34 descriptions, 0 warnings.**
