# Open-source readiness

Status as of 2026-09-10. Scope: what has to be true about this repository
before it can be published.

## Secrets scan (done)

Two independent full-history scans on 2026-09-10: a pattern scan over all
4,918 commits and a Gitleaks 8.30 pass over history, commit messages, and
tree snapshots.

- No VoteApp-owned credential was ever committed. Every secret value in the
  current `.env` files was checked against the full history and none appear.
  Nothing needs rotating.
- The only real finding reachable from `main` was a 9 MB Postgres dump,
  `tmp_voteapp_backup_before_live_ai_20260511.sql`, tracked 2026-05-12 to
  2026-09-10. Its users table was empty; staging payloads carried the
  operator's public IP. Removed from HEAD in PR #1302. It stays in history
  until the rewrite below.
- Two signed county document URLs flagged by Gitleaks (Harris County sample
  ballot, Sangamon County notes) serve the same PDF without the signature.
  They are public share links, not credentials. Left as is.
- 425 saved third-party web pages under `scratch/records-verify-*` exist only
  on two local, never-pushed branches. They embed other sites' API keys and
  tracking tokens. See "Local branches" below.
- `backend/node_modules` (1,570 files) sits in the initial commit
  `680a02f82` and was untracked in `d0ea208a8`. Bloat only.

## Ignore rules (done)

`.gitignore` now blocks database dumps (`tmp_*.sql`, `*.dump`, `*.pgdump`)
and the raw captures under `scratch/records-verify-*/` (any `src/` folder,
HTML, PDF, XML, jina, bin). The notes beside them (`evidence.json`,
`labels.json`, `records.json`, Markdown, `stored.txt`) stay trackable.

## Local branches (done 2026-09-10)

`claude/records-verify-major-city-mayor` (7 commits) and
`claude/records-verify-state-governor` (16 commits) change nothing outside
`scratch/`. Their history was rewritten on 2026-09-10 to drop the captures:
65 and 596 files removed, all 140 and 282 notes kept, commit counts and
messages unchanged, no capture left anywhere in either history. Pre-scrub
backups: `refs/backup/records-verify-major-city-mayor-pre-scrub-20260910`
and `refs/backup/records-verify-state-governor-pre-scrub-20260910`.

The recipe, for any future capture branch. Run from a checkout with a clean
working tree, one branch at a time:

```bash
FILTER='git ls-files -z | /usr/bin/grep -zE "^scratch/records-verify-[^/]+/(.*/)?src/|^scratch/records-verify-[^/]+/.*\.(html?|pdf|xml|jina|bin)$" | git update-index --force-remove -z --stdin; true'
FILTER_BRANCH_SQUELCH_WARNING=1 git filter-branch -f --prune-empty -d /tmp/fb --index-filter "$FILTER" -- main..claude/records-verify-major-city-mayor
```

Then verify, using the backup ref as the "before" side:

```bash
git diff --stat refs/backup/records-verify-major-city-mayor-pre-scrub-20260910 claude/records-verify-major-city-mayor | tail -1
git ls-tree -r --name-only claude/records-verify-major-city-mayor | /usr/bin/grep -cE 'records-verify.*(/src/|\.(html?|pdf|xml|jina|bin)$)'
```

The diff must show only deletions, and the second command must print `0`.
Use `/usr/bin/grep` explicitly; the shell's `grep` is ugrep, where `-z`
means something else.

## Privacy review (done 2026-09-10)

What the app holds about a person, checked against the schema: account
email and first name, district ids (never the address), follows, research
interests, and the picks table (`user_election_choices`: candidate or
measure position per race). Picks are the most sensitive rows in the
database. `user_pick_card_shares` holds one row per shared date: the random
token behind `/picks/<token>`, the election date, and whether the owner's
first name shows on the public card; the picks themselves are read live from
the choices table. A row lives until the owner revokes it or deletes the
account (the user foreign key cascades). IPs live only in the Redis rate
limiter; analytics events carry no user id, IP, or cookie; Ask questions are
stored anonymously for 90 days; account deletion cascades.

Two gaps closed in the same PR as this section:

- Privacy policy 1.5 → 1.6. The old "preferences" paragraph let promotional
  email content be selected from choices that include picks and follows.
  Now: picks are named as stored data, promotional content (if ever) is
  scoped to research-area interests only, shared pick cards are described,
  and section-into-view is listed under analytics.
- Shared pick cards can be revoked. `GET`/`DELETE /api/me/pick-card-shares`
  plus a "Stop sharing" control on `/me/picks`; the public URL 404s once
  revoked and a later Share mints a fresh token.

## Publishing (pending, after the decision)

Build the public repository from a filtered fresh clone rather than
force-pushing this one, so the old commit hashes never become reachable on
the public side:

```bash
git clone --no-local git@github.com:shu1513/electionssimplified.git electionssimplified-public
cd electionssimplified-public
git filter-repo --invert-paths \
  --path tmp_voteapp_backup_before_live_ai_20260511.sql \
  --path backend/node_modules \
  --path-glob 'scratch/records-verify-*/**/src/**' \
  --path-glob 'scratch/records-verify-*/**/*.html' \
  --path-glob 'scratch/records-verify-*/**/*.htm' \
  --path-glob 'scratch/records-verify-*/**/*.pdf' \
  --path-glob 'scratch/records-verify-*/**/*.xml' \
  --path-glob 'scratch/records-verify-*/**/*.jina' \
  --path-glob 'scratch/records-verify-*/**/*.bin'
```

Do this only after the open PR branches have merged; a rewrite invalidates
every branch based on the old history.

Still to decide before publishing, none of it a scrub:

- A read-through of the internal planning notes (`plan-*.md` at the root,
  `docs/plans`, `docs/research`) for tone. They are not secrets.
- License (AGPL suggested, so a tilted fork must publish its changes) and a
  trademark on the app name.
- A public "how we decide" methodology page, published before the code so the
  framing is ours first.
