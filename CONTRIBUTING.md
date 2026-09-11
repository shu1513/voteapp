# Contributing

Thanks for helping. This page covers how to get a change merged. Setup steps are in the [README](README.md#running-locally).

## What we take

- Bug fixes, tests, accessibility and performance work, docs.
- New data sources (a state's campaign-finance adapter, a legislature's roll-call feed). Open an issue first so we agree on scope.
- Corrections to election data. Data lives in the database, not in this repo, so **file a [data correction issue](https://github.com/shu1513/electionssimplified/issues/new?template=data_correction.yml) with a source link** instead of a pull request.

Please open an issue before starting anything larger than a small fix. It saves both of us from work that will not merge.

## Ground rules

- **No automatic AI provider calls.** `backend/src/ai/aiCallGuard.ts` denies by default. Any command that needs a model call takes `AI_API_CALLS_ALLOWED=true` inline on that command. Never put it in `.env`, and never add code that bypasses the guard.
- **Migrations are append-only.** Add a new numbered file in `db/migrations/`; never renumber or edit one that is already on `main`. Postgres identifiers must be 63 characters or fewer.
- **Feature flags:** flags that cost money (paid APIs, model calls) default off. Free read-side flags default on.
- **No photos, ratings, or endorsements** of candidates. See the design choices in the README before proposing a UI change that touches candidate presentation.
- Every candidate record needs a primary source. No unsourced claims.

## Pull request flow

1. Fork the repo and branch from `main`.
2. Make the change. Keep one topic per pull request.
3. Run the checks for whatever you touched (each line is its own subshell, so the block can be pasted whole):

   ```bash
   (cd backend && npm run typecheck && npm test)
   (cd frontend && npm run typecheck && npm run lint && npm test)
   (cd mobile && npm run typecheck && npm run lint)
   (cd infra/cloudflare && npm test)
   ```

4. Commit with a Conventional Commits prefix: `feat(scope):`, `fix(scope):`, `chore(scope):`, `docs:`.
5. Open the pull request against `main` and fill in the template. CI must pass before review.

A maintainer reviews every pull request. Expect questions; small changes usually merge within a few days.

## License

The code is licensed under the [AGPL-3.0-only](LICENSE). By contributing you agree that your contribution is licensed under the same terms. There is no separate contributor agreement.

## Questions

Use [Discussions](https://github.com/shu1513/electionssimplified/discussions) for questions and ideas, [Issues](https://github.com/shu1513/electionssimplified/issues) for bugs and data corrections, and `contact@electionssimplified.com` for anything private. Security reports go through [SECURITY.md](SECURITY.md).
