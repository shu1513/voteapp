# Maryland 2026 roster review

Scope: resolve the 31 explicit nulls in the 2025-session LegiScan crosswalk.

Source: Maryland State Board of Elections, [2026 General Election State
Candidates List](https://elections.maryland.gov/elections/2026/general_candidates/2026_GG_statewide_candidatelist.html), opened 2026-08-29. The page reported `Last updated: 08/28/2026 04:00 PM`; its [Senate CSV](https://elections.maryland.gov/elections/2026/general_candidates/2026_GG_statesenatorbydistrict_candidatelist.csv) and [House CSV](https://elections.maryland.gov/elections/2026/general_candidates/2026_GG_houseofdelegatesbydistrict_candidatelist.csv) were read raw. Incumbency for the two matches was corroborated on the official Maryland General Assembly member pages.

The State Board list has no candidate row in the stated state-legislative race for the 29 people below. They remain explicit nulls: assigning their 2025 votes to a different 2026 candidate would be false attribution.

| LegiScan people_id | 2025 member and seat | 2026 result | crosswalk action |
| --- | --- | --- | --- |
| 4527 | Pamela Beidle, SD-032 | absent from Senate District 32 | keep null |
| 4531 | Frank Conaway, HD-040 | absent from House District 40 | keep null |
| 4562 | Joanne Benson, SD-024 | absent from Senate District 24 | keep null |
| 4607 | Adrienne Jones, HD-010 | absent from House District 10 | keep null |
| 4624 | Anne Healey, HD-022 | absent from House District 22 | keep null |
| 4641 | Susan McComas, HD-034 | absent from House District 34A/34B | keep null |
| 4682 | Bryan Simonaire, SD-031 | absent from Senate District 31 | keep null |
| 4686 | Katherine Klausmeier, SD-008 | absent from Senate District 8 | keep null |
| 4718 | Nancy King, SD-039 | absent from Senate District 39 | keep null |
| 11768 | Bonnie Cullison, HD-019 | absent from House District 19 | keep null |
| 12078 | Charles Otto, HD-038 | absent from House District 38A/38B/38C | keep null |
| 17235 | Michael Jackson, SD-027 | absent from Senate District 27 | keep null |
| 17243 | Chris West, SD-042 | absent from Senate District 42 | keep null |
| 17400 | Kevin Hornberger, HD-035 | absent from House District 35A/35B | keep null |
| 17420 | Vanessa Atterbeary, HD-013 | absent from House District 13 | keep null |
| 17553 | Barrie Ciliberti, HD-004 | absent from House District 4 | keep null |
| 17917 | Pamela Queen, HD-014 | absent from House District 14 | keep null |
| 19271 | Jazz Lewis, HD-024 | absent from House District 24 | keep null |
| 20868 | Jen Terrasa, HD-013 | absent from House District 13 | keep null |
| 20930 | Dalya Attar, SD-041 | absent from Senate District 41 | keep null |
| 20932 | Brian Crosby, HD-029 | absent from House District 29A/29B/29C | keep null |
| 20933 | Nino Mangione, HD-042 | absent from House District 42A/42B/42C | keep null |
| 20955 | Arthur Ellis, SD-028 | absent from Senate District 28 | keep null |
| 21255 | Stephanie Smith, HD-045 | absent from House District 45 | keep null |
| 21426 | Nicole Williams, HD-022 | absent from House District 22 | keep null |
| 24710 | Adrian Boafo, HD-023 | absent from House District 23 | keep null |
| 24711 | Christopher Bouchat, HD-005 | absent from House District 5 | keep null |
| 24715 | Jim Hinebaugh, HD-001 | absent from House District 1A/1B/1C | keep null |
| 24820 | Joshua Stonko, HD-042 | absent from House District 42A/42B/42C | keep null |

Two rows were actual roster gaps. The State Board House CSV lists both people in House District 31, and each current-member page confirms the same office, district, and party:

| LegiScan people_id | official 2026 ballot name | candidate id | corroboration | crosswalk action |
| --- | --- | --- | --- | --- |
| 20530 | Brian A. Chisholm | `565faa5c-9dcd-4a71-8109-3a72b0434b52` | [MGA member page](https://mgaleg.maryland.gov/mgawebsite/Members/Details/chisholm01?ys=2026RS) | mapped |
| 26325 | LaToya Marie Caldwell-Nkongolo | `d8161e29-493c-4ecb-827e-77ee1e22fad8` | [MGA member page](https://mgaleg.maryland.gov/mgawebsite/Members/Details/nkongolo01?ys=2026RS) | mapped; ballot surname changed |

District 31's full six-candidate official roster was staged and fanned out through the manual writer. Only the two verified sitting legislators received profile writes, because they are the only newly mappable voters for this roll-call campaign. No candidate-record worker or AI provider was called.
