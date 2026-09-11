# Iowa batch-06 — plan

## Scope
The last batch of the divided-and-enacted pool. Batch-05 left 15 measures on 23 slots
deferred, eight of them long omnibus acts. All 15 were read in this batch: 3 imported
(4 rolls, 187 candidate records) and 12 dispositioned without import, each with a written
reason in `../survey/divided-enacted-worklist.tsv`. Nothing in the pool is deferred now.

## Selected
HF 316, SF 378, HF 2670. The reasoning is in `JUDGING.md`.

## How the long acts were read
For each omnibus act the division list was pulled first, then the operative text of each
division that could carry a direction. An act was imported only if at least one strand had a
clean direction and no strand in the same research area pulled the other way.

## Dispositioned without import
- **HF 2676** (91,000 characters, Iowa Make America Healthy Again Act): over-the-counter
  ivermectin with immunity for pharmacists pulls against school meal ingredient rules and
  student physical activity requirements, all on environment_and_public_health.
- **SF 2490** (oil and gas): a six percent severance tax for water quality comes with forced
  pooling and preemption of local regulation.
- **HF 954** (nine-division elections act): bans ranked choice voting and raises the
  minor-party threshold. Under the Oregon HB 3908 precedent, a counting method and a party
  threshold carry no honest direction on election_integrity or civil_rights.
- **HF 2754** and **SF 2231** (education omnibus acts): both carry school-choice financing,
  education savings accounts in one and a textbook credit for religious materials in the other.
  The standing precedent gives that no defensible direction.
- **SF 603**: unemployment insurance and public employee compensation; the labor gap.
- **SF 2488**: an administrative restructuring of early childhood services; filter 3.
- **SF 606**: tax filing machinery; filter 3. **HF 2781**: a fund distribution formula;
  appropriations.
- **HF 395**, **HF 766**, **HF 2215**: school bus training, window tint and a natural
  resources grab-bag; no research area carries an honest direction.

## Audits
Every roll matches Iowa's own journal tally line, none is held, none carries the date skew,
and each is its chamber's final roll on the text that became law. The builder refused one
49-word sentence on the first attempt, before anything was written; it was split and rebuilt.
Plain-language lint: 0 warnings over 8 descriptions, longest sentence 41 words.

## Import and reconciliation
- Judge dry run 4, real run 4. Import dry run 187 inserts, real run 187 inserts, zero errors.
- Run stamp `2026-09-10T06:34:25.892Z`.
- Reconciled three ways: report 187, rows matching the run stamp 187 (100 candidates, 126
  tags), table 2,817 to 3,004.
- Duplicate sweep: the four hand-written records sharing a candidate and a date are about
  other measures. Nothing retired.
