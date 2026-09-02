# Maryland 2026 Regular Session — LegiScan roll calls

This directory holds the full LegiScan dataset evidence for Maryland session
2240 and the first reviewed candidate-record batch.

- Survey: 2,675 bills, 2,732 roll calls, 217 raw people records; zero file or
  vote-parse errors.
- Measured final actions: 2,449 third-reading passage rolls, two Senate
  conference-report rolls, and two veto-override rolls. The source registry
  uses `MD-2240` to select this session while storing records under `MD`.
- `crosswalk.json` reuses the prior session's person IDs. Five 2026 members
  were reviewed: Derrick Coley, Gabriel Moreno, and Darrell Odom map to their
  official 2026 candidate records; Kevin Anderson and Alexander Harlan remain
  explicit nulls because they have no official 2026 House candidate row.
- `batch-01/` contains five enacted measures, ten final rolls, the DLS-based
  judgments, dry-run ledger, real import ledger, and real convergence ledger.

All work used the local `voteapp` database and no AI provider. Production is
untouched.
