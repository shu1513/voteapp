# Elections Simplified

Source code for [electionssimplified.com](https://electionssimplified.com): a voter guide that shows what candidates actually did, so people can pick based on their own priorities instead of ads.

Most of us know the least about the races where our vote counts the most (city council, school board, county offices) and the most about the one race where it counts the least (president). This app tries to fix that with plain-language track records for every candidate on your ballot.

## Design choices

These are deliberate. If you are reading the code and wondering "why", this is why.

**No candidate photos.** A face triggers snap judgments about age, race, gender, and looks before a single fact is read. The database has an unused `photo_url` column from the first migration; nothing writes to it and nothing renders it. Candidates are shown by name, office, and record only.

**Records, not opinions.** A candidate entry is a list of things they did (votes, sponsored bills, public positions, finance) with a source for each. We do not rate, grade, endorse, or rank candidates. The app's "auto-pick" feature only matches the user's own stated priorities against those records, and shows its work.

**Neutral on purpose.** No single person or small group knows what is best for everyone else. The job is to lay out facts so people can decide for themselves. See the in-app [mission page](frontend/src/pages/MissionPage.tsx) for the longer version.

**Not a nonprofit.** A nonprofit is a legal structure, not a guarantee of good behavior. It comes with a board, formal governance, and a steady stream of paperwork, and the board seats and donor relationships are exactly where an organization can be captured over time. We run as a small company instead: simpler to operate, harder to infiltrate, and the accountability comes from this repository being public, not from a legal form. Every prompt, every algorithm, and every data pipeline is here for anyone to inspect.

**Open source for transparency, and as a template.** Publishing the code lets voters check how the information was produced: if a claim in the app looks wrong, you can trace it to the pipeline that wrote it. It is also MIT licensed on purpose. If you want to build the same thing for your own country, fork it. The data pipelines are US-specific, but the model (records with sources, no photos, no ratings, user-chosen priorities) travels.

**No AI calls unless a human says so.** AI models are used to research and summarize public sources, but every model call is behind a default-deny guard (`backend/src/ai/aiCallGuard.ts`). Nothing calls a model on a timer or as a side effect. Research runs are started by a person, and results are validated across methods and models before they are written.

**Money only runs the service.** Member contributions pay for hosting and AI usage. They do not go to any candidate, campaign, party, or cause.

## Repository layout

- `backend/` — Node + TypeScript API, research/import pipelines, schedulers, CLI scripts.
- `frontend/` — React Router + Vite web app.
- `mobile/` — Expo app.
- `packages/` — shared API client and types.
- `db/migrations/` — numbered SQL migrations (see `DB_DEPLOYMENT.md`).
- `docs/` — deploy runbooks, plan docs (`docs/plans/`), research notes, legal.
- `infra/`, `render.yaml` — deployment.

## Running locally

Requires Node, Redis (`redis://localhost:6379`), and Postgres **with the [pgvector](https://github.com/pgvector/pgvector) extension** at `postgresql://localhost:5432/voteapp` (migration 234 runs `CREATE EXTENSION vector`; plain Postgres fails there). The quickest matching database is the same image CI uses:

```bash
docker run -d --name voteapp-pg -p 5432:5432 -e POSTGRES_USER=$USER -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=voteapp pgvector/pgvector:pg16
```

Or on macOS: `brew install postgresql@16 pgvector`, then `createdb voteapp`.

```bash
cd backend && cp .env.example .env && npm install && npm run db:migrate && npm run address:api
```

```bash
cd frontend && npm install && npm run dev
```

Checks: `npm run typecheck` and `npm test` in `backend/` and `frontend/`; `npm run lint` in `frontend/`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Wrong data in the app? File a [data correction](https://github.com/shu1513/electionssimplified/issues/new?template=data_correction.yml) with a source link.

## Contact

contact@electionssimplified.com

## License

MIT. See [LICENSE](LICENSE).
