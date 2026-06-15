# Historical Competitiveness

This feature adds a simple historical signal to ballot summaries for races where past
election results are useful context. The app imports MIT Election Lab / MEDSL data into
`public.historical_contest_margins`, then ballot summaries read local rows from that table.

It does not call MIT or Dataverse during normal API requests.

## Supported Offices

Historical competitiveness is currently linked for:

- President of the United States
- United States Senator
- United States Representative
- Governor
- State Senator
- State lower chamber legislator

The lookup uses the office canonical name plus district identity:

- President, U.S. Senate, and Governor use the statewide district key.
- U.S. House uses the congressional district key.
- State Senate uses the state upper chamber district key.
- State House / Assembly uses the state lower chamber district key.

## API Shape

Ballot summaries expose this field on office-race summaries:

```json
{
  "historical_competitiveness": {
    "display_label": "Historically somewhat competitive",
    "display_description": "Based on weighted margins from 2024 and 2022 U.S. House results.",
    "source": "MIT_2024",
    "source_url": "https://...",
    "election_year": 2024,
    "winner_party": "DEMOCRAT",
    "runner_up_party": "REPUBLICAN",
    "margin_percent": 11.45,
    "competitiveness_label": "somewhat_competitive",
    "stale_after_redistricting": false,
    "method": "weighted_last_3",
    "weights": [0.625, 0.375],
    "election_years": [2024, 2022],
    "contests_used": [
      {
        "source": "MIT_2024",
        "source_url": "https://...",
        "election_year": 2024,
        "winner_party": "DEMOCRAT",
        "runner_up_party": "REPUBLICAN",
        "margin_percent": 9.2,
        "competitiveness_label": "competitive",
        "stale_after_redistricting": false,
        "weight": 0.625
      },
      {
        "source": "MIT_2022",
        "source_url": "https://...",
        "election_year": 2022,
        "winner_party": "DEMOCRAT",
        "runner_up_party": "REPUBLICAN",
        "margin_percent": 14.3,
        "competitiveness_label": "somewhat_competitive",
        "stale_after_redistricting": false,
        "weight": 0.375
      }
    ]
  }
}
```

`election_year`, `winner_party`, and `runner_up_party` describe the most recent contest.
`margin_percent` and `competitiveness_label` are the weighted result.

Use `display_label` and `display_description` for user-facing copy. Do not call this
field a prediction; it is historical context from comparable prior elections.

## Weighted Method

The current method is `weighted_last_3`.

Weights are:

- Most recent contest: `50%`
- Second most recent contest: `30%`
- Third most recent contest: `20%`

If fewer than three contests are available, the available weights are normalized:

- Two contests: `62.5% / 37.5%`
- One contest: `100%`

Competitiveness labels use total-vote margin, not two-party margin:

- `toss_up`: margin <= 2
- `very_competitive`: margin <= 5
- `competitive`: margin <= 10
- `somewhat_competitive`: margin <= 15
- `safe`: margin > 15

Example: a `45-40-15` race has a `5` point margin because the denominator is all votes.

## Redistricting Guard

District-level races are only compared within the current redistricting cycle.

For a 2026 U.S. House or state-legislative election, lookup ignores rows before 2022.
For a 2032 U.S. House or state-legislative election, lookup ignores rows before 2032.

Statewide races do not use this redistricting floor.

## Verified Import Sources

The verified importer currently includes these source presets:

- `medsl-2024-president-state`
- `medsl-2024-senate-state`
- `medsl-2024-house-precinct`
- `medsl-2024-state-precinct`
- `medsl-2022-precinct`
- `medsl-2020-precinct-by-state`
- `medsl-2018-precinct-by-state`

The 2024 president and senate sources are aggregate CSVs from the MEDSL GitHub
repository. The 2024 House, 2024 state-office, 2022, 2020, and 2018 sources are
Dataverse files.

The 2020 and 2018 Dataverse files require a guestbook response before download.
The verified importer handles this by requesting a signed Dataverse download URL.
Configure these optional values for the submitted guestbook response:

```bash
DATAVERSE_GUESTBOOK_NAME="VoteApp Historical Contest Importer"
DATAVERSE_GUESTBOOK_EMAIL=data-import@example.invalid
DATAVERSE_GUESTBOOK_INSTITUTION=VoteApp
DATAVERSE_GUESTBOOK_POSITION="Data importer"
```

## Commands

Run from `backend/`.

Dry-run one verified source:

```bash
npm run competitiveness:import:verified -- --preset=medsl-2024-president-state --dry-run
```

Import one verified source:

```bash
npm run competitiveness:import:verified -- --preset=medsl-2022-precinct
```

Import one older guestbook-gated Dataverse source:

```bash
npm run competitiveness:import:verified -- --preset=medsl-2020-precinct-by-state
```

Import every verified source and then print status:

```bash
npm run competitiveness:refresh
```

Print import status:

```bash
npm run competitiveness:status
```

Importing all verified sources can take time because the precinct presets include many
state files. Use `--dry-run` first when adding or changing source definitions.

## Idempotency

Imports upsert rows by:

- source
- election year
- state
- office type
- district type
- district key

Re-running the same verified import updates the existing rows instead of creating
duplicates.

## Source Scope

The lookup does not call remote sources. It reads whichever verified rows have already
been imported locally.

If we later add non-MIT sources, add an explicit source preference before mixing them
with MIT/MEDSL rows. Today the source catalog is MIT/MEDSL-only.
