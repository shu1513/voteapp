# Hawaii 2025 — batch 01 judging notes

## Sources

Every description was written from the enacted text, read top to bottom with the statutory markup
kept (`<u>` new text, `<s>` deleted text), fetched by document id through the LegiScan bulk API and
hash-checked against the dataset. Committee reports (the conference committee report, or the last
standing committee report) were read as a neutral index of what the final draft does. Testimony
was not read: it is advocacy.

## Version check, per roll

The draft named on each passage line was matched to the bill's text versions by name.

- HB 137: Senate third reading names no draft; the Senate passed HD 1 unchanged and HD 1 is the
  last text. The House's 47-1 third reading was on the same draft.
- SB 1433, SB 897: both chambers' final readings name CD 1, the last text.
- SB 97: both chambers name CD 2. The dataset lists the House floor amendment print
  (`SB97_CD1_HFA4_.HTM`) after `SB97_CD2_.HTM`, so the CD 2 text was fetched by name (finding 7).

## Members

The fetcher reconstructed every aye list. Session 2175's seat file removes Gene Ward (HD-018)
from April 1, 2025, so the April 30 and May 2 House rolls were taken by 50 members: SB 1433 is
36-13 with 1 excused = 50, SB 897 is 39-10 with 1 excused = 50. The Senate rolls all equal the
printed aye count. The importer re-derived every list and found no difference.

## Measure notes

**HB 137.** The old subsection already made a felon's gun possession a class B felony; the act adds
that when the prior felony was a crime of violence "the defendant shall be sentenced to an
indeterminate term of imprisonment as provided by law." The description says the prison term set
by state law becomes mandatory and does not name a number of years, because the act does not.

**SB 1433.** Needs-based distribution replaces one-for-one exchange (§325-113(c)). Non-injection
drug users may receive services "exclusive of syringes and needles" (§325-113(d)). The liability
section shields program staff (on duty) and participants (at a program visit) for needles, syringes and "authorized objects"
(cookers, cottons, ties), and shields participants from residue-based possession charges for two
months after a program visit; it applies only to exchanges between participants and staff, and a
good-faith arrest carries no civil liability (§325-114). The two-month window and the
participant-to-staff limit are both stated.

**SB 897.** The cap applies to "qualifying damages," defined as economic damage to real or
personal property and excluding bodily or emotional harm; a catastrophic wildfire substantially damages or destroys more
than 500 structures (50 for a cooperative). The PUC sets the maximum payable amount by rule with
the Governor's approval, and a utility may assert the cap only with an approved wildfire
mitigation plan being implemented on schedule and full compliance with the rules' conditions.
Joint and several liability is abolished for these claims. Securitization is capped at $500
million per corporate family, repaid by a nonbypassable charge on all customers. Executive
compensation may not rise unless the mitigation compliance reports are approved for five
consecutive years (§ -3(i)). The act also appropriates study money; not described. Direction: the
dominant provisions limit what a utility pays for harm it may have caused, so
corporate_accountability, yes = against; the mitigation-plan condition is stated rather than
treated as a counterweight of equal size.

**SB 97.** A third excessive-speeding offense within five years becomes a misdemeanor with a
mandatory 30-day minimum, license revocation of 90 days to six months, and forfeiture "if the
court so orders" (§291C-105(d)). The automated-camera section (§291L-5) removes the "not less than
five miles per hour" threshold, so a camera citation follows any violation of §291C-108.

## Writing checks

Builder `hi_build.py` refused to write on any comma splice, missing tally, sentence over 45 words,
or British spelling, and verified its spelling check fires on a known-bad string. Plain-language
lint: 0 warnings over 10 descriptions. Flesch-Kincaid grade, no-side text: median 8.3, worst 10.8
(SB 897, driven by the defined terms). Descriptions run five to eight short sentences instead of
two to four; the qualifiers are what earlier review rounds in this campaign were lost on, so
length was traded for completeness on purpose.

## Result

Judge: 5 updated. Dry run: 62 planned inserts. Real run 2026-09-10T00:03:38.012Z: 62 inserts, 0
errors, 0 notifications, 0 related flags. Convergence: 62 unchanged. Database: 62 records on 30
candidates, 44 tags (yes side only; every no side is null). Wider sweep for hand-written records
naming these bills on Hawaii candidates: none.
