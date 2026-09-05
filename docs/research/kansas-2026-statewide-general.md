# Kansas statewide general election, 3 November 2026

Manual research run (no AI provider calls). Elections stage and roster stage of the
`voteapp-manual-research` cascade for the Kansas statewide district
(`districts.id = 0902b454-2eff-46cb-9440-bd4dd5e28257`). Written to the local database only;
nothing here has been promoted to production.

## Source of truth

The Kansas Secretary of State's candidate list for the 2026 General election
(<https://sos.ks.gov/elections/elections_upcoming_candidate.aspx>, "2026 General" in the
election drop-down). The page is an ASP.NET postback, so the list is reached by posting the
form with `ddlElections=36`; a plain GET returns the empty selector page.

Supporting sources: the Kansas SOS important-election-dates page (general election date, and
the 1 September deadline for the State Board of Canvassers to certify the primary results),
<https://portal.kansas.gov/government/> (current officeholders, for incumbency),
<https://www.senate.gov/senators/index.htm> (Senate class), and OpenFEC (federal candidate IDs).

## Elections written

Six office shells, all `election_date = 2026-11-03`, `election_stage = general`,
`race_type = office`, `is_partisan = true`, `seats_to_fill = 1`, every one with a resolved
`office_id`:

| Ballot title | Contest family | Office |
| --- | --- | --- |
| Governor / Lieutenant Governor | non_judicial_office | Governor |
| Attorney General | non_judicial_office | Attorney General |
| Secretary of State | non_judicial_office | Secretary of State |
| State Treasurer | non_judicial_office | State Treasurer |
| Commissioner of Insurance | non_judicial_office | Commissioner of Insurance |
| United States Senator | us_senate | United States Senator |

The Senate shell also carries `election_senate_metadata`: class II, term end year 2033.
Ballot titles repeat the titles already used by the 4 August 2026 primary shells, so the
office matcher resolves them the same way.

## Rosters written

15 candidate/election links, all with status `declared`. Governor and Lieutenant Governor run
as one ticket in Kansas, so each Governor row carries `running_mate_candidate_id`.

| Office | Candidate | Party | Incumbent |
| --- | --- | --- | --- |
| Governor / Lieutenant Governor | Cindy Holscher (running mate KC Ohaebosim) | Democratic | no |
| Governor / Lieutenant Governor | Ty Masterson (running mate Jeffrey Klemp) | Republican | no |
| Attorney General | Chris Mann | Democratic | no |
| Attorney General | Kris Kobach | Republican | yes |
| Secretary of State | Jennifer Day | Democratic | no |
| Secretary of State | Pat Proctor | Republican | no |
| Secretary of State | Scott E. Morgan | United Kansas | no |
| State Treasurer | Juan C. Luengo | Democratic | no |
| State Treasurer | Steven Johnson | Republican | yes |
| State Treasurer | Eric Lund | Libertarian | no |
| Commissioner of Insurance | Dinah Sykes | Democratic | no |
| Commissioner of Insurance | Daniel Hawkins | Republican | no |
| Commissioner of Insurance | Ric Koehn | Libertarian | no |
| United States Senator | Adam Hamilton | Democratic | no |
| United States Senator | Roger Marshall | Republican | yes |

Governor, Secretary of State and Commissioner of Insurance are open seats: Laura Kelly is term
limited, and Scott Schwab and Vicki Schmidt both ran for governor instead of re-election.

Fourteen of the eighteen people already existed from the primary cascade and were matched on
their stored campaign or official website; only Scott E. Morgan, Eric Lund and Ric Koehn are
new candidate rows.

## Excluded, with reasons

- **David C Graham (Libertarian, United States Senator)** is on the certified Kansas general
  ballot but has no current-cycle FEC candidate ID. OpenFEC returns only `S2KS00154` from the
  2022 cycle for him. The federal roster policy skips such rows, so he is recorded in
  `staging_items.ai_raw_debug.roster_skipped_no_fec_id` and is not linked. Re-check OpenFEC for
  a late registration before the election; if one appears, re-inject the roster with his
  `fec_ids`.

## Gap ledger

| Item | Outcome |
| --- | --- |
| David C Graham, no current-cycle FEC ID | blocked_by_contract (policy exclusion, name tracked in staging) |
| Eric Lund, no campaign website | confirmed_null (none on the SOS list or the LPKS candidate pages) |
| Ric Koehn, no campaign website | confirmed_null (same check) |
| Ric Koehn, `has_held_public_office` | confirmed gap: no cited source covers his office history |
| Eric Lund, Ric Koehn, Scott E. Morgan, Chris Mann, Jennifer Day, Juan C. Luengo, Adam Hamilton, current office | confirmed_null (none hold public office now) |
| Roger Marshall, Adam Hamilton, Cindy Holscher summaries | repaired: the stored primary-stage text carried campaign-status wording, replaced with source-backed biography |
| KC Ohaebosim and Jeffrey Klemp summaries | still name the lieutenant governor role; left alone, factual and not horse-race |

The district's open elections deferral ("awaiting primary certification") is resolved.

## Not done here

Candidate records, record area labels and campaign finance for these contests are later stages
and were not part of this run.
