# Indiana batch tools

Five small scripts the Indiana batches use. They read a LegiScan dataset under
`/Users/shu/legiscan-data/` and fetch public PDFs from `iga.in.gov`. None of them touches a
database and none calls an AI provider.

## Pick the session first

These tools serve both Indiana sessions. Only the dataset directory and the year segment of an
`iga.in.gov` URL differ, and `session.py` holds both.

    IN_SESSION=2143 python3 getbills.py SB0450      # 2025 regular session, the default
    IN_SESSION=2234 python3 memberchk.py rolls.json # 2026 regular session

Set `IN_SESSION` on every invocation for the 2026 session. Leaving it unset gives 2025, which
will silently look for the wrong bill and fetch the wrong roll-call PDF. Adding a third session
means one entry in `session.py` and nothing else.

- `annot.py` — renders a bill PDF to text with every ADDED word wrapped in `<<...>>` and
  every DELETED word wrapped in `[[...]]`. Indiana prints additions in bold, existing statute
  text in roman, and deletions in roman with a struck rule; `pdftotext` flattens all three.
  Use this instead. Needs `pdfplumber`. See `../CODE-FINDINGS.md` section 4.
  `python3 annot.py bill.pdf out.txt`
- `getbills.py` — downloads every printed version of a bill using the dated `state_link`
  entries in the LegiScan bill JSON, then writes both plain and annotated text. The dates
  give the exact roll-to-version mapping. See `../CODE-FINDINGS.md` section 5.
  `python3 getbills.py SB0450 SB0002`
- `rolls.py` — maps a LegiScan roll to Indiana's own journal roll-call number using the bill
  history, and says whether the two tallies agree.
  `python3 rolls.py selected.json`, where the file is `{"SB0450": [1484850, 1524703]}`
- `memberchk.py` — compares a roll's LegiScan member list name by name against the official
  Indiana roll-call PDF. This is the mandatory step Indiana adds to the recipe.
  `python3 memberchk.py rollmap.json`, where the file is a list of
  `[bill, roll_id, journal_number, origin_chamber, is_resolution?]`.
- `session.py` — holds the dataset directory and URL year for each session, selected by the
  `IN_SESSION` environment variable. The other four import it; it is not run directly.

**On `memberchk.py`, the most important of the five.** Across both Indiana sessions the
worklist's tally-mismatch flag has been a perfect predictor: six flagged rolls checked, six
found with a member on the wrong side, and no unflagged roll has ever failed. Run it on every
roll a batch uses anyway, because the cost of a wrong member list is a wrong candidate record.

## Working files

Downloads land under a directory named for the session, so the two sessions never collide:
`2143/pdf`, `2143/txt`, `2143/annot`, `2143/rc`, and the same under `2234/`. Those directories
are working files and are not committed.

## Two things learned the hard way

**A raw count of differing runs in a version comparison is not evidence of a policy change.**
SB 277's comparison flagged 299 differing runs on a 299-section act, of which 136 were one
running footer. Read the runs.

**Strip anything the printer appends after the bill body before comparing versions.** Indiana
prints committee reports and floor motions after the enacted text, and a long committee report
reads as a large policy change if it is left in.
