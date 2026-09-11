## What changed and why

<!-- One or two sentences. Link the issue if there is one: "Closes #123". -->

## Checks

- [ ] `npm run typecheck` and `npm test` pass in every package I touched (`frontend/` also `npm run lint`)
- [ ] No automatic AI provider calls added; `aiCallGuard.ts` untouched
- [ ] Any new migration is a new numbered file in `db/migrations/`, no existing file renumbered or edited
- [ ] New candidate data has a primary source
- [ ] Commit message uses a Conventional Commits prefix (`feat:`, `fix:`, `chore:`, `docs:`)
