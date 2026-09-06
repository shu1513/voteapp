# integrity_and_ethics retag — 2026-09-06

## Why

Auto-pick treats every `integrity_and_ethics` tag as a strike (`autoPick.ts` `effectiveStance` returns −1) and the "Skip candidate if negative record" veto excludes any tagged candidate. An audit of all 860 live tagged records found that most were not negative: dismissed complaints, acquittals, unofficial allegations, the candidate's own ethics-reform bills, bios, and statements were all counting as strikes.

## Rule applied (now in `candidateRecordAreaLabelPrompt.ts` and the manual-research `records-import.md`)

Keep `integrity_and_ethics` ONLY for an adverse action against the candidate personally by an official body: conviction, plea, arrest, indictment or pending charge, censure, reprimand, suspension, fine, sanction, license discipline, substantiated ethics or misconduct finding, probable-cause vote, impeachment, court order against them. Formal written warnings count; staff reminders do not. Charges later dropped, dismissed, or acquitted are untagged along with the dismissal record. A plea or admission under a diversion or deferred-prosecution deal stays tagged even if the charge was later reduced or vacated on completion (Whitaker 181, Flores 240, Emil Jones 249, Paxton 472) — not an exoneration. An administrative license suspension for lapsed registration with no misconduct finding is not discipline (Ramirez 73, O'Brien 505).

Untag everything else. The candidate's own reform work moves to `anti_corruption` with a stance.

## Result (local DB)

- 860 tagged records → 236 kept, 624 untagged.
- 539 flagged candidates → 169. 370 candidates lost the strike entirely.
- 36 untagged records identified for `anti_corruption` (31 for, 5 against). 10 added via `ai:candidate-records:relabel --labels-file` (Nov 2026 candidates whose office allows the area). 4 Nov 2026 county-office candidates have no `anti_corruption` in their office allowlist — left untagged. 22 have no upcoming election, so the relabel tool cannot reach them — left untagged.

## Files

- `untags.json` — the applied manifest (`npm run manual:records:untag -- --untags-file ... --apply`). Keep it: promote to prod by re-running it against prod (`ALLOW_REMOTE_DB_WRITES=1`, dry-run first) — `research:promote` never deletes target-only tags.
- `decisions.txt` — every record's decision (`K` keep, `U` untag, `A:for|against` untag + anti_corruption) with a one-line reason. Index = position in the audit dump (sorted by candidate name, event date).
- `edge-cases.txt` — the 68 judgment calls, flagged for review.
- `anti-corruption-adds.json` — the 36 reform records, with which ones the relabel tool could reach.
