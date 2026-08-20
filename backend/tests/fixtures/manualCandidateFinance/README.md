# Mississippi manual-finance fixtures

These fixtures transcribe official Mississippi Secretary of State filings linked by each
`source_url`. Filing IDs, dates, public candidate/committee names, directions, and totals
match the filings. VoteApp candidate/election UUIDs are synthetic. Contributor identities
and addresses are omitted. Empty `itemized_receipts` arrays mean the schedules were not
transcribed into the sanitized fixture; they do not mean the filing reported zero receipts.
The Brandon Presley fixture retains one anonymized receipt solely to exercise separate
occupation and employer fields.

## House District 22 acceptance cohort

The smallest coherent pilot cohort is the November 4, 2025 House District 22 special
election. It joins both candidate pre-election reports to one IE filing with two directions:

| Role | Report date | Portal filed | Filing ID |
| --- | --- | --- | --- |
| Jon Lancaster candidate report | 2025-10-28 | 2025-10-28 | `14AA19F9-0D75-4FBE-B083-91FF72DE1E36` |
| Justin Crosby candidate report | 2025-10-28 | 2025-10-29 | `3D0B9211-4F04-4C8D-AA5A-4E58BADBD801` |
| Improve Mississippi PAC IE report | 2025-10-28 | 2025-10-28 | `D2EE3D0C-08D1-4E87-9959-34ADADDEBA0C` |

The IE form's handwriting appears to say "Jen Lancaster." The official Secretary of State
candidate record and [sample ballot](https://www.sos.ms.gov/content/documents/elections/11-4-2025%20Sample%20Ballot.pdf)
identify the candidate as Jon Lancaster, so the payload uses the canonical public name. A
second Improve Mississippi PAC IE filing dated October 28 was checked and covers Senate
District 45; it is a separate filing, not an amendment of the House District 22 report.

`amends_filing_id: null` means filing history was checked and filing is not an
amendment. An amendment must name the superseded filing ID; unknown amendment
status must not be represented as `null`.
