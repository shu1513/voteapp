# Indiana batch tools

Four small scripts the Indiana batches use. They read the LegiScan dataset at
`/Users/shu/legiscan-data/in-2143/` and fetch public PDFs from `iga.in.gov`. None of them
touches a database and none calls an AI provider.

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

`memberchk.py` writes its downloaded PDFs to `rc/` beside itself, and `getbills.py` writes
to `pdf/`, `txt/` and `annot/`. Those directories are working files and are not committed.
