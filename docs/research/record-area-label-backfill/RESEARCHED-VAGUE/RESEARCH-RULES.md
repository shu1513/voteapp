# Research the bill, then label the record

These records were skipped earlier because the description says a law was "amended", "revised", "modified" or "changed" without saying WHICH WAY. Your job is to find out what the law actually did, then label the record.

You are proposing labels only. You do not write to the database. Another session validates and imports.

## First read the general label rules
/private/tmp/claude-501/-Users-shu-voteApp--claude-worktrees-caveman-ultra-5231aa/133176e8-bcfd-4d8c-b838-a8b31307f3fe/scratchpad/record-labels/LABEL-RULES.md

All of those rules still apply. This file only adds the research step.

## The research step
For each record, find out what the bill or measure actually did.

1. Start with the record's own `source_url`. Open it.
2. If that does not say which way the law moved, search the web for the bill number plus the state, for example "Ohio HB 96 2025 what it does". Good sources: the state legislature's own bill page, LegiScan, Ballotpedia, established news.
3. If a page blocks plain fetching, try the browser tools. Anti-bot blocks are common on Ballotpedia and some state sites, and they usually load fine in a browser.
4. Stop when you can answer one question: did this law expand or restrict the thing it touches, and who does that help or hurt?

Keep it to a few minutes per record. These are small bills; most resolve from the bill page alone.

## Then label
- If the research shows a clear direction, label it with the areas it directly affects, each with its own stance, using only that candidate's `allowed_areas`.
- If after real research the bill still has no direction a voter could weigh — a definitions cleanup, a renaming, a date change, a study — keep it skipped and say what you found.
- Never guess. "I could not determine the direction" is an acceptable and useful answer.

## Output
Same shape as before, at the output path you are given:

{"proposals": [
  {"candidate_id": "...",
   "labels": [{"record_id": "...", "research_area_slug": "...", "stance": "for"}],
   "skipped": [{"record_id": "...", "reason": "what you found, and why it still has no direction"}],
   "retire_candidates": [{"record_id": "...", "reason": "..."}]}
]}

Every record in your input must appear exactly once. Add a `"found"` field to each label or skip entry with one short sentence saying what the bill actually did and where you learned it.

## Hard limits
- Do not rewrite any record description. The description stays as it is; you are only adding labels.
- Do not call any AI provider API and never run a command with AI_API_CALLS_ALLOWED.
- Do not write to the database and do not run npm scripts.

## Writing style
Plain, simple English. Short sentences. No filler or headers. Your final message: how many you could resolve with research, how many stayed unresolved, and the most common reason they stayed unresolved.
