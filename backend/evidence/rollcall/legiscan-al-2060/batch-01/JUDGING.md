# Alabama 2023 second special session batch-01 — judging notes

## Source

Judged from the **enrolled Act**, read in full, fetched through the LegiScan bulk API and verified
against the recorded byte length and MD5 hash. The Act amends Section 17-14-70 of the Code of Alabama
to redraw the state's seven congressional districts for the 2024 general election and afterwards,
and adds a section of legislative findings.

## Roll-attribution and date audit

This session prints no roll call numbers in its descriptions, so the attribution check is vacuous.
The imported roll matches its bill history line exactly: 1 of 1.

## Version check

The imported roll is the Senate's vote on the conference committee report — the text that became
Act 2023-563. Its earlier passage vote of 2023-07-19 is a different question on a different text and
is dispositioned as superseded, not acknowledged, because it precedes the imported roll.

## Label reasoning: `general`, no stance

Redistricting gets no stance, and the campaign has now settled this three times the same way:
Missouri imported its 2025 redistricting special session under `general`, the Alabama 2026 special
session's HB 1 and SB 1 were imported the same way, and this is the map those later Acts refer to by
name.

No research area in this taxonomy describes drawing legislative boundaries. `election_integrity` —
"secure, accurate, auditable, and trusted" — is about how an election is run, not how its districts
are shaped, and using it here would score a partisan map fight on an administrative axis.

The description states the context that a reader needs and can verify from the record: the special
session followed the Supreme Court's decision in *Allen v. Milligan*, this map was the Legislature's
answer, and it became Act 2023-563. It stops there.

## Duplicates

The precise sweep found none for this roll. Hand-written records exist for the House vote on HB 5,
the competing map, and are handled in `../batch-02`.

## Import and reconciliation

- Dry run: 1 file, 0 errors, 24 planned inserts.
- Real run (stamp `2026-09-02T16:44:01.189Z`): **24 inserts, 0 errors, 0 notified.**
- Reconciled three ways: report totals (24); run-stamp predicate (24 rows, 24 distinct candidates —
  every mapped senator who voted); and the session total, 133 records carrying a 2060 run id,
  matching 24 + 109.
- Convergence: a follow-up dry run reports all 24 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings over 2 descriptions, 4 sentences each, no sentence
over 45 words, British-spelling scan clean. Flesch-Kincaid 9.1.
