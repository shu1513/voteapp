# Deciding the `nay` stance on old roll-call judgments

These roll calls were judged before the judgment format had a `nay` field. Right now the people who voted NO get no tag at all. Your job is to decide, for each stance label on each roll call, what a NO vote actually shows — or that it shows nothing.

You are proposing decisions only. You do not write to the database.

## What you get
A JSON list of roll calls. Each one has:
- `vote_id`, `jurisdiction`, `chamber`, `measure_id`, `vote_date`, `exact_question`, `result`, `tally`
- `yea_description` and `nay_description`: the sentences already written for each side. Read both. They are the evidence.
- `stance_labels`: the research areas already on this roll call, each with the `yea` stance already decided.

## What you produce
One JSON file at the output path you are given:

{"decisions": [
  {"vote_id": "...",
   "labels": [{"slug": "public_education_quality", "yea": "for", "nay": null, "why": "..."}]}
]}

Include every roll call and every one of its `stance_labels`. Keep `yea` exactly as given — never change it. Only `nay` is yours to decide. `why` is one short sentence.

## The rule for `nay`
`nay` is the stance a NO vote **actually evidences**, or `null` when it evidences none.

- `null` is the correct answer most of the time, and it is not a failure. Set it whenever a no vote could have many reasons: cost, timing, drafting, one provision, wanting a stronger version, or procedure.
- Only set "for" or "against" when voting no on THIS measure, as the two descriptions describe it, can only mean one direction on that area's goal.
- Never set `nay` by flipping `yea`. A no vote on one election bill is not "Opposes Election Integrity". That inversion is exactly the mistake this field exists to prevent.
- `nay` may never equal `yea`. The system rejects that.
- A stance renders to readers as "Supports X" or "Opposes X" — a claim about the person's position on the whole area, not on one bill. If a no vote would only support a narrower claim, use `null`.

Worked example. A bill raises school funding; `yea` is `public_education_quality: for`. A no vote might be about the tax that pays for it, so `nay` is `null`. But if the bill's whole content is repealing a school-funding guarantee and the descriptions say so, a no vote is the only thing standing against the repeal, and `nay: "for"` is honest.

## Hard limits
- Do not change any description text.
- Do not call any AI provider API. Do not run any command with AI_API_CALLS_ALLOWED.
- Do not write to the database and do not run npm scripts.
- You may look up the bill with your research tools when the two descriptions leave you unsure. Keep it brief.

## Writing style
Write in plain, simple English a non-specialist can follow. Short sentences. No filler, no headers, no tables. Your final message should say how many labels you gave a real stance, how many you left null, and anything that surprised you.
