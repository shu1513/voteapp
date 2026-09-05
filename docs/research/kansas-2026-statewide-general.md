# Kansas statewide general election, 3 November 2026

Manual research run (no AI provider calls). Elections, roster and profile stages of the
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

## Profiles written

Every person on the six rosters has a candidate profile. That is 17 people: the 15 candidates
who hold a `candidate_elections` link, plus the two running mates, who are stored as their own
candidate rows and are reached through the Governor rows' `running_mate_candidate_id`.

| Office | Person | Party | Website stored | Current office stored |
| --- | --- | --- | --- | --- |
| Governor / Lieutenant Governor | Cindy Holscher | Democratic | yes | Kansas State Senator, District 8 |
| Governor / Lieutenant Governor | KC Ohaebosim (running mate) | Democratic | yes | Kansas State Representative, District 89 |
| Governor / Lieutenant Governor | Ty Masterson | Republican | yes | Kansas State Senator, District 16 and President of the Senate |
| Governor / Lieutenant Governor | Jeffrey Klemp (running mate) | Republican | yes | Kansas State Senator, District 5 |
| Attorney General | Chris Mann | Democratic | yes | none |
| Attorney General | Kris Kobach | Republican | yes | Attorney General of Kansas |
| Secretary of State | Jennifer Day | Democratic | yes | none |
| Secretary of State | Pat Proctor | Republican | yes | Kansas State Representative, District 41 |
| Secretary of State | Scott E. Morgan | United Kansas | yes | none |
| State Treasurer | Juan C. Luengo | Democratic | yes | none |
| State Treasurer | Steven Johnson | Republican | yes | Kansas State Treasurer |
| State Treasurer | Eric Lund | Libertarian | no | none |
| Commissioner of Insurance | Dinah Sykes | Democratic | yes | Kansas State Senator, District 21 |
| Commissioner of Insurance | Daniel Hawkins | Republican | yes | Kansas State Representative, District 100 |
| Commissioner of Insurance | Ric Koehn | Libertarian | no | none |
| United States Senator | Adam Hamilton | Democratic | yes | none |
| United States Senator | Roger Marshall | Republican | yes | United States Senator from Kansas |

Fourteen of these profiles were carried over from the August primary cascade and matched on a
stored hard identifier, usually the campaign website. Three profiles were new at the general
stage: Scott E. Morgan, Eric Lund and Ric Koehn, the three minor-party candidates who were
nominated by party convention and so never appeared on a primary ballot.

## Profile re-verification, 4 September 2026

A second pass re-read the Kansas Secretary of State's 2026 General candidate list and compared
it row by row against the database. Results:

- The official list holds exactly 18 names across the six statewide contests, including the two
  running mates. All 17 importable people have a profile with a summary and at least two
  profile sources; none is missing or empty. No new profile was needed.
- David C Graham was re-checked on OpenFEC. He still has only the 2022 candidate id
  `S2KS00154`, whose status is `N` (not a current candidate), so the federal exclusion still
  applies and he remains unlinked.
- The two blank websites were re-confirmed against the official filing rows: the Secretary of
  State's "Web Address" column is empty for both Eric Lund and Ric Koehn, and neither has a site
  listed on the Libertarian Party of Kansas candidate pages. Both stay confirmed nulls.
- Ric Koehn's `has_held_public_office` stays `null`. No cited source carries his office history.
  He was nominated as a Libertarian presidential elector in 2024 but was not elected (Kansas
  chose the Republican slate), so that nomination is not office-holding and does not settle the
  question either way.

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

## Candidate records, 4 September 2026

The records stage of the cascade for the same six contests. Records are candidate-wide, so the
baseline audit first checked what each of the 15 linked candidates already had.

Twelve of the fifteen already carried a finished, evidence-backed sweep from the primary-stage
runs in July and August (a `candidate_record_sweep_confirmations` row plus a
`last_records_searched_at` stamp), so they were left alone. The two running mates, KC Ohaebosim
and Jeffrey Klemp, were swept on 19 August and were likewise left alone. Only the three
minor-party nominees who entered at the general stage had no sweep at all.

Per-candidate record counts, before and after this run:

| Office | Candidate | Records before | Records after | Sweep |
| --- | --- | ---: | ---: | --- |
| Attorney General | Chris Mann | 6 | 6 | already complete, 2026-07-23 |
| Attorney General | Kris Kobach | 17 | 17 | already complete, 2026-07-24 |
| Commissioner of Insurance | Daniel Hawkins | 9 | 9 | already complete, 2026-07-23 |
| Commissioner of Insurance | Dinah Sykes | 11 | 11 | already complete, 2026-07-23 |
| Commissioner of Insurance | Ric Koehn | 0 | 3 | new, this run |
| Governor | Cindy Holscher | 20 | 20 | already complete, 2026-07-23 |
| Governor | Ty Masterson | 14 | 14 | already complete, 2026-07-23 |
| Secretary of State | Jennifer Day | 6 | 6 | already complete, 2026-07-23 |
| Secretary of State | Pat Proctor | 9 | 9 | already complete, 2026-07-29 |
| Secretary of State | Scott E. Morgan | 0 | 2 | new, this run |
| State Treasurer | Eric Lund | 0 | 2 | new, this run |
| State Treasurer | Juan C. Luengo | 0 | 0 | already complete, 2026-07-23, confirmed `only_general_labels` |
| State Treasurer | Steven Johnson | 17 | 17 | already complete, 2026-08-01 |
| United States Senator | Adam Hamilton | 6 | 6 | already complete, 2026-07-23 |
| United States Senator | Roger Marshall | 58 | 58 | already complete, 2026-08-01 |

Seven records were written, all through `manual:candidate-records:write` with the per-question
evidence ledger attached. Each write's insert/update split matched the prediction (3/0, 2/0,
2/0).

### Ric Koehn (Libertarian, Commissioner of Insurance)

Routed never-held: no source shows him holding public office, and his 2024 Libertarian
presidential elector nomination was a nomination, not service. Three treasurer roles, each
pinned to him by the P.O. Box 468 Cimarron address, phone and email on the filing: the Bleeding
Kansas Advocates PAC (medical marijuana advocacy, 2019-04-25), the Libertarian Party of Kansas
(2024-07-29), and the Libertarian Porcupine Club of Kansas PAC (2026-01-09). All three are
neutral background, so the write carries `candidate_records.only_general_labels`.

### Scott E. Morgan (United Kansas, Secretary of State)

Routed officeholder on his two Lawrence school board terms. Two records: his vote in the 6-1
majority that closed Wakarusa Valley School over a budget shortfall (2011-03-28), labelled
`government_efficiency: for`; and co-founding the Free State Party and becoming executive
director of United Kansas after the April 2026 merger (2026-04-28). His Senate, Federal Election
Commission and Hayden-administration staff roles are biography only - no accessible source
carries a dated action from any of them.

### Eric Lund (Libertarian, State Treasurer)

Routed officeholder on his stored California park-district service. Two records, both party
organisation roles: chairperson of the Libertarian Porcupine Club of Kansas PAC (2026-01-09) and
secretary of the Libertarian Party of Kansas (reported after the 2026-04-25 state convention).
Neutral only, so the write carries `candidate_records.only_general_labels`.

### Records gap ledger

| Item | Outcome |
| --- | --- |
| Ric Koehn, career phases as options trader and as accountant | confirmed_null (no employer, client, filing or dated action in accessible sources) |
| Ric Koehn / Eric Lund / Scott E. Morgan, court and legal records | unresolved (Kansas case search is a login/JS portal; logged as an access gap, not as "no issues") |
| Scott E. Morgan, Lawrence school board 1999-2003 votes | unresolved (no online minutes or vote-level coverage for that term) |
| Scott E. Morgan, Morgan Quitno Press founding and 2007 sale | blocked_by_contract (no dated source both names him and carries the claim; Wikipedia's company page does not name him) |
| Scott E. Morgan, 2007 criticism of his company's city crime rankings | unresolved (sources naming him - thecrimereport.org, contexts.org - fail TLS verification; the reachable Wikipedia page does not name him) |
| Scott E. Morgan, Free State Party signature drive | unresolved (only www2.ljworld.com carries it and that host timed out for the validator) |
| Eric Lund, Cordova Recreation and Park District board era | unresolved (his service predates the district's online archive; the county roster lists only current directors) |
| Eric Lund, 2024 Miami County Clerk candidacy | blocked_by_contract (a prior candidacy is pure_candidacy; the one local article is region-blocked, so no canvass result was reachable) |
| Endorsements, all three candidates | confirmed_null (no dated, cycle-pinned endorsement for any of them) |

## Not done here

Campaign finance for these contests is a later stage and was not part of this run. Nothing on
this page has been promoted to production.
