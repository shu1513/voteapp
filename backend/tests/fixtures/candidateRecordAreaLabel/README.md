# Area-labeler prompt benchmark

27 candidate-record descriptions, each source-verified during the 2026-08 records
repair campaign, used to check `buildCandidateRecordAreaLabelPrompt` for stance
overclaims before a prompt change ships. Nothing here runs in CI: scoring needs a
model, and the repo forbids automatic provider calls.

Fields per record:

- `fail_any_stance: true` — the record states no position; any stance label is a failure. The only valid stance-free outputs are `general` and `integrity_and_ethics`; any other slug without a stance is rejected by `candidateRecordAreaLabelPayloadContract` before it reaches the writer, so it is a contract error, not a benchmark pass.
- `fail: [...]` — `slug/stance` pairs production emitted that the source does not support.
- `expect: [...]` — the correct label when the production one had the wrong direction.
- `require: [...]` — keep-controls: a real stance that must still be emitted (regression guard).

`fail` is a floor, not the full overclaim set: it lists the pairs production actually
emitted. Score every emitted stance label that is not in `expect` or `require` by
hand against the description; an unsupported one is a failure even if it is not
listed.

Re-run by hand (no `AI_API_CALLS_ALLOWED`, no API key):

```bash
npx tsx tests/fixtures/candidateRecordAreaLabel/render.ts > /tmp/labeler-prompt.txt
```

Paste the rendered prompt into a model session, score the JSON against the fields
above, and run it twice — single runs flip on one or two records. Baseline from
PR #816 over the first 25 records: old prompt 10 failures per run (6/8 no-position
rows stanced); new prompt 4–5 (0–1/8). Controls missed 2/5 on both.
