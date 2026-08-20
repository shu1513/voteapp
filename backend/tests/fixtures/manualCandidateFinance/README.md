# Mississippi manual-finance fixtures

These fixtures transcribe official Mississippi Secretary of State filings linked by each
`source_url`. Filing IDs, dates, public candidate/committee names, directions, and totals
match the filings. VoteApp candidate/election UUIDs are synthetic. Contributor identities
and addresses are omitted; the candidate fixture retains one anonymized receipt solely to
exercise separate occupation and employer fields.

`amends_filing_id: null` means filing history was checked and filing is not an
amendment. An amendment must name the superseded filing ID; unknown amendment
status must not be represented as `null`.
