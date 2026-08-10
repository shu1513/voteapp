# VoteApp

Election / candidate info app. Monorepo:

- `backend/` — Node + TypeScript. `src/api` (HTTP API), `src/pipeline` (research/import pipelines per domain: elections, candidates, finance per state), `src/scripts` (CLI entrypoints, wired as npm scripts), `src/scheduler` (Redis-stream schedulers/workers), `src/contracts` (payload contracts + validators), `src/ai` (AI enrichers, guarded).
- `frontend/` — React Router + Vite. `src/pages`, `src/components`, `src/routes`.
- `mobile/` — Expo app (see `mobile/CLAUDE.md`).
- `db/migrations/` — numbered SQL migrations. Postgres identifiers must be ≤63 chars.
- `docs/`, `infra/`, `render.yaml` — deploy (Render).

## Commands

Backend (run in `backend/`): `npm run typecheck` (tsc --noEmit), `npm test` (vitest run).
Frontend (run in `frontend/`): `npm run typecheck`, `npm test`, `npm run lint` (oxlint), `npm run dev`.
Local services: Postgres `postgresql://localhost:5432/voteapp`, Redis `redis://localhost:6379`.

## Hard rules

- **No automatic AI provider calls.** `backend/src/ai/aiCallGuard.ts` is default-deny. AI calls require inline `AI_API_CALLS_ALLOWED=true` on the command — never put it in `.env`.
- Feature flags: free read-side flags stay ON in `backend/.env`; flags that cost money stay off unless the user enables them.
- Manual research (rosters, profiles, records, finance, results) goes through the `voteapp-manual-research` skill and its reference docs — invoke the skill; don't re-derive payload contracts from source.
- Worktree discipline: edits, tests, and commits all happen in the same checkout. Never touch the main checkout during a worktree session.

## Token / speed discipline

- Prefer Grep/Read/Glob tools over `bash grep/sed/cat`; use absolute paths instead of `cd` chains.
- Trim output at the source: `LIMIT` on psql queries, `| tail` on test runs, `curl -o file` then grep the file. Never dump full test/build logs.
- Browser verification: use `read_page` / console / network tools first; screenshot only as final proof (images are expensive to keep in context).
- Long research runs: keep run state in scratchpad files and prefer a fresh session per task over one giant session. Checkpoint progress to the state file every batch; once context passes ~300k tokens, stop and tell the user to continue from the checkpoint in a fresh session (a near-full context makes every remaining turn slower and more expensive).
- Never poll with foreground `sleep`/`until` loops. Run the long command with `run_in_background` and let the task notification wake you.
- Fan wide reads out to subagents: raw pages, logs, and file dumps should land in a subagent's context, not the main session's.
