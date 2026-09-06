# How this batch was judged

Every measure was read twice: the Legislative Council Staff final fiscal note
for the shape of the act, then the enrolled act itself for what it actually
says. Where the two disagreed, the act won.

## The source trap, in a new form

The campaign already knew that Colorado's bill feed can list an initial fiscal
note and omit the final one. **HB 25B-1001 is a sharper version of the same
problem.** The feed lists a supplement dated 8 September 2025 at the `_f1`
address — the address that holds the final note for every other bill in this
session — but the document there is headed `Version: Initial Fiscal Note` and
states that it "reflects the introduced bill".

So the rule holds and pays off again: **read the note's own version header
before using it**, never the file name and never the feed's date. HB 25B-1001
was judged from its enrolled act alone.

## Colorado hides its deletions, and one measure turns on one

Colorado prints new statutory language in capitals and deletions struck
through. Text extraction renders a strike-through as ordinary text, so an
extracted read shows repealed law as if it were still in force.

**HB 25B-1001 is entirely a deletion.** Its one operative sentence amends the
add-back rule, and the whole effect is the removal of the words "but before
January 1, 2026". Extracted text shows those words present, which reads as no
change at all. The page was rendered as an image to confirm the strike-through.
The end date is gone, so the add-back is permanent.

**HB 25B-1002 needed the same treatment.** Its section 3 appears in extracted
text to keep an exclusion that denies a subtraction for dividends from tax-haven
corporations. Rendered as an image, that exclusion is struck through: it is
repealed. That is a giveback sitting inside a bill that otherwise raises
corporate tax, so it was worth confirming rather than assuming. It is a
conforming change — the same money is now captured by the new add-back — and
the act's stated purpose, and its net effect, remain reducing tax avoidance.
The description does not mention it, because it changes no one's stance.

## SB 25B-004 is a title trap

The act is called "Increase Transparency for Algorithmic Systems". It increases
nothing. Every section does one thing: replace the date "February 1, 2026" with
"June 30, 2026" in Colorado's 2024 artificial-intelligence law. Nine dated
duties move, and the only other edits are the words "promulgated" to "adopted"
and "made".

So a yes vote postpones consumer protections against algorithmic
discrimination, and the label is `civil_rights` with **`yea: against`**. That
follows the precedent set by SB 25-208 in the regular session, which delayed
free prison phone calls by a year and was scored the same way.

The nay side carries no tag, as always: an unstated nay is silence, not the
opposite claim.

## SB 25B-002 is not what its shape suggests

The act pays clinics with state money when a July 2025 federal law blocks their
Medicaid payments — in Colorado, chiefly Planned Parenthood. It would be easy
to describe that as the state paying for abortions. It is not.

The act carves out "those services covered pursuant to section 25.5-2-106".
That section, added by SB 25-183 in the regular session, is titled *State-funded
abortion care* and already requires abortion care to be reimbursed from state
funds only. So abortion care was already state-paid, and what SB 25B-002
rescues is the rest of the Medicaid care those clinics give: exams, screenings,
birth control, infection treatment. The description says so plainly.

## HB 25B-1006 is conditional, and says so

The $100 million only moves if Congress does not extend the enhanced federal
premium tax credit. That is a real condition, not a funding contingency of the
kind this campaign drops — the act commits the money either from tax credit
sales or, failing that, from the general fund. The description states the
condition rather than hiding it.

## Version check

For every one of the 13 selected rolls, the last print in force on the vote
date was diffed against the enrolled act. All matched. The only recurring
difference is an enrollment habit: pre-enrollment prints spell numbers out and
the enrolled act uses numerals, so HB 25B-1003's "fifteen of the eighteen
insurers" becomes "15 of the 18 insurers". Nothing substantive moved between
the votes and the law in any measure.

## Wording

The descriptions were measured, not eyeballed. The first draft came out at a
Flesch-Kincaid median of 9.6, above the 7th-grade target, so every measure was
rewritten into shorter sentences and plainer words; the rewrite measures 6.8
median and 8.3 at worst, in line with the regular session's batches.

After the rewrite each description was read back against the act, because
shortening is where qualifiers get lost. That pass kept the $500,000 and
$1 million thresholds and the farming exemption in HB 25B-1001, and the
"only when federal money cannot be used" limit in SB 25B-002.

The plain-language lint ran over all 26 descriptions before the import: 0
warnings. The lint only counts words per sentence, so reading grade was
computed separately.
