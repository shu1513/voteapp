# Hawaii — findings recorded, not fixed

1. **LegiScan carries no Hawaii floor votes.** Both the 2025 and 2026 datasets hold committee
   votes only (largest tally 17 against a 51-seat House). The floor votes are text in
   `bill.history[]`. This is why Hawaii has its own pipeline and is listed in
   `JURISDICTIONS_WITH_DEDICATED_PIPELINES` in `legiscanStateConfigs.ts`. Registering a LegiScan
   config for Hawaii could never queue a record.

2. **Hawaii names only the dissenters.** The journal prints who voted no, who voted aye with
   reservations, and who was excused. The Senate adds an aye count; the House prints none. The aye
   list is inferred (every sitting member not named). The inference is checked against the seat
   count and, in the Senate, the printed aye count. The House cannot detect a vacancy from its text,
   so `seats.json` is required and must come from official sources.

3. **The 2026 dataset repeats 2025 votes.** Bills carry over within the biennium, so the 2026
   histories include 603 floor votes cast in 2025 that the 2025 dataset already holds. The fetcher
   files a vote under the session whose `year_start` is the vote's year and reports the rest as
   `prior_session`.

4. **LegiScan's people file keeps replaced members.** The 2026 file lists 53 representatives and 26
   senators for 51 and 25 seats. `seats.json` gives each replaced member's last day and each
   appointee's first day: Gene Ward (to 2025-03-31) and Joe Gedeon (from 2025-05-28), HD-018;
   Daniel Holt (to 2026-02-13) and Michael Ratcliffe (from 2026-04-14), HD-028; Henry Aquino (to
   2025-11-30) and Rachele Lamosao (from 2026-01-21), SD-019; Daisy Hartsfield (from 2026-01-21),
   HD-036.

5. **Journal surnames differ from LegiScan surnames.** `Belatti` is LegiScan's `au Belatti`,
   `Lamosao` is `Fernandez Lamosao`, and `Dela Cruz` is LegiScan's `Cruz`. The resolver matches the
   whole surname first, then the last token of the printed surname against the last token of the
   person's last name; a printed initial (`Lee, M.`) must open the person's first name or nickname.
   No two members of one chamber share a surname in either session, and every printed name in both
   sessions resolved to exactly one person.

6. **The Senate spells a zero two ways.** `0 No(es): none.` and `Noes, 0 (none).`, and likewise for
   the excused. Five 2025 lines use the count-first form with names (`1 No(es): Senator(s)
   McKelvey.`). The parser accepts both.

7. **SB 97's last text in the feed is not the draft voted.** The final-reading lines say `CD 2`,
   and the dataset's `texts[]` holds `SB97_CD2_.HTM`, but the last entry by position is the House
   floor amendment print (`SB97_CD1_HFA4_.HTM`). Match the draft named on the passage line to the
   text version by name, never by list position.

8. **The bill status page and its documents sit behind Cloudflare.** `capitol.hawaii.gov` answers
   403 to curl and to the fetch tool. The LegiScan bulk API serves the same documents by id
   (`getBillText` for text versions, `getSupplement` for committee reports), hash-checked. The
   Browser pane reads the HTML pages but not the PDF committee reports.

9. **Bill HTML marks changes with tags the plain text loses.** New statutory text is `<u>`,
   deleted text is `<s>` inside brackets. `hi_text.py` (outside the repo, in
   `/Users/shu/legiscan-data/hi-work/`) prints them as `<<new>>` and `[[deleted]]`. A
   brand-new section is printed plain. The first version of that script stripped its own markers
   as tags; it now uses control characters until the tags are gone.

10. **The Senate reach is structural.** Only 3 of 25 senators map to a candidate in the local
    roster, so a Senate roll writes 2 to 3 records. A roster campaign for the Senate seats on the
    2026 ballot would raise it; the House reaches 28 of 51.
