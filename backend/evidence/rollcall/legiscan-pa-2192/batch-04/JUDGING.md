

## Plain-language pass 2 (2026-08-30) — the whole campaign measured, not assumed

Batches 03, 04 and 05 were written after the batch-01/02 rewrite and were
never held to the same standard, so every PA description was scored rather
than eyeballed: Flesch-Kincaid grade, longest sentence, and a scan for terms
of art left bare.

The median was already fine; the tail was not. 45 of the 179 measures came in
at grade 8 or above, or carried bare jargon, with the worst at grade 10.5.
Those 44 bodies were rewritten (HB 1042 appears twice, once per chamber).

| | before | after |
| --- | --- | --- |
| median grade | 6.8 | **6.4** |
| worst grade | 10.5 | **9.0** |
| measures with bare jargon | 20 | **0** |
| longest sentence | 42 words | 38 words |

Every remaining hit on the jargon scan is a term explained inside its own
sentence, which is the point rather than a failure: "a felony, the most
serious class of crime"; "a land bank, a public agency that takes over vacant
property to get it back in use"; "the prevailing wage law sets minimum pay on
public construction jobs"; "towns can borrow against the future tax growth a
development creates, a tool called tax increment financing". Crime grades
lost their Latin numbering — "a third-degree felony" is now "a serious crime,
at the lowest felony level", and "raised several crimes by one grade" is now
"one level more serious". "Released on their own recognizance" became
"go free on a promise to appear".

**No fact moved.** A machine check compared every numeric token, roll number,
date, chamber, review status and label across all 179 measures before and
after: zero differences. 5,837 records rewritten in place across the five
batches; all five convergence runs report everything unchanged; PA totals
stay 26,098 live records across 194 candidates. The insert ledgers are
untouched — this run is `import-plain-language-2-report.json` in each batch.

Residual: SB 471 sits at grade 9.0 with an 11.8-word average sentence. The
score is driven by unavoidable long words (prosecutor, immigration,
defendant, citizen), not by structure, and shortening them further would cost
accuracy.
