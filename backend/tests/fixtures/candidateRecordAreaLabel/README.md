# Area-labeler prompt benchmark

27 candidate-record descriptions, each source-verified during the 2026-08 records
repair campaign, used to check `buildCandidateRecordAreaLabelPrompt` for stance
overclaims before a prompt change ships. Nothing here runs in CI: scoring needs a
model, and the repo forbids automatic provider calls.

Fields per record:

- `fail_any_stance: true` — the record states no position; any stance label is a failure.
- `fail: [...]` — `slug/stance` pairs production emitted that the source does not support.
- `expect: [...]` — the correct label when the production one had the wrong direction.
- `require: [...]` — keep-controls: a real stance that must still be emitted (regression guard).

Re-run by hand (no `AI_API_CALLS_ALLOWED`, no API key):

```bash
npx tsx tests/fixtures/candidateRecordAreaLabel/render.ts > /tmp/labeler-prompt.txt
```

Paste the rendered prompt into a model session, score the JSON against the fields
above, and run it twice — single runs flip on one or two records. Baseline from
PR #816 over the first 25 records: old prompt 10 failures per run (6/8 no-position
rows stanced); new prompt 4–5 (0–1/8). Controls missed 2/5 on both.
