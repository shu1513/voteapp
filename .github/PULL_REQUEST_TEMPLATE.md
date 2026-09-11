## What changed and why

<!-- One or two sentences. Link the issue if there is one: "Closes #123". -->

## Checks

- [ ] The checks listed in [CONTRIBUTING.md](../blob/main/CONTRIBUTING.md#pull-request-flow) pass for every package I touched
- [ ] No automatic AI provider calls added; `aiCallGuard.ts` untouched
- [ ] Any new migration is a new numbered file in `db/migrations/`, no existing file renumbered or edited
- [ ] New candidate data has a primary source
- [ ] Commit message uses a Conventional Commits prefix (`feat:`, `fix:`, `chore:`, `docs:`)
