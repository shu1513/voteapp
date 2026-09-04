import type { LegislativeVoteChamber } from "./legislativeVotes.js";

// Per-session configuration for the LegiScan roll-call pipeline, the phase-4
// rollout source (docs/plans/roll-call-vote-import.md §5 phase 4). LegiScan's
// `desc` field has no national convention (`Read 3rd time`, `House Passed`,
// `Passage: House Vote #243`, `FP`, TX prints bare `RV#105`), so which
// descriptions are final-action floor votes is a PER-STATE fact that must be
// measured before it is encoded: run
//   npm run rollcall:legiscan:fetch -- --state <ST> --dataset-dir <dir> --survey
// against the state's dataset, read the desc histogram it writes, and only
// then add the state's entry here. A state without an entry cannot be
// fetched, judged, or imported — the registry is the list of states whose
// vocabulary a human has actually looked at. Never guess a pattern from
// another state's conventions.
//
// Patterns are tested against the LOWERCASED, whitespace-collapsed desc, so
// write them in lowercase. `excludedQuestions` are checked first — they are
// the specific carve-outs (a "refused to concur" must not match a kept
// "concur" pattern).
//
// The federal pipeline has its own fetcher, so "US" (which LegiScan also
// covers) never belongs in this registry.

export type LegiscanQuestionClass = "passage" | "concurrence" | "conference_report" | "veto_override";

export type LegiscanStateConfig = {
  // Postal abbreviation, e.g. "TX"; also the legislative_votes.jurisdiction.
  jurisdiction: string;
  // The LegiScan session this state's Nov-2026 campaign runs on. Every bill
  // in the dataset must carry this session_id; it is also the
  // legislative_votes.session key (as a string).
  sessionId: number;
  // Seats per chamber, the denominator of the floor-vs-committee tally
  // check. Unicameral states (NE) name only `senate`.
  chamberSizes: Partial<Record<LegislativeVoteChamber, number>>;
  // Final-action descriptions this state prints on floor votes we keep.
  keptQuestions: readonly { pattern: RegExp; questionClass: LegiscanQuestionClass }[];
  // Floor-but-procedural descriptions, excluded by rule (never stored as
  // kept). Checked before keptQuestions.
  excludedQuestions: readonly RegExp[];
  // Roll calls the survey proved WRONG (LegiScan's tally or member list
  // disagrees with the state's own record), keyed by roll_call_id with the
  // reason. Stored and surfaced (is_floor_vote null) but never queued, so
  // they cannot be approved by mistake. Remove an entry only after the
  // roll has been re-verified against the state's record.
  heldRollCallIds?: Readonly<Record<number, string>>;
};

// Registered per surveyed session. A state's first configured session keeps
// its postal key for backwards-compatible CLI use (for example `MD`), while
// later sessions use `<ST>-<LegiScan session id>` (for example `MD-2240`).
// The key selects a source session only; `config.jurisdiction` remains the
// stored legislative_votes jurisdiction.
// NOTE: legiscan.com pages sit behind a Cloudflare challenge (probed
// 2026-08-24, curl answers 403 "Just a moment..."); the record validator
// accepts that (allowStatusCodes [403] in
// verifyUniqueCandidateRecordSourceUrls), and a human viewer passes the
// challenge in a browser, so the roll-call page stays a valid source_url.
// Missouri's floor-question vocabulary, shared by its regular session and the
// 2025 2nd Extraordinary Session below. Both sessions were surveyed
// separately and print the SAME families, so they share one definition
// rather than two copies that could drift apart.
const MISSOURI_KEPT_QUESTIONS: LegiscanStateConfig["keptQuestions"] = [
  // House third reading, under any of its calendars: the regular
  // `HBs FOR THIRD READING` / `SBs FOR THIRD READING`, the informal
  // calendar (`HBs 3rd READ - INFORMAL`), the consent calendar
  // (`HBs 3rd READING - CONSENT`), the constitutional-amendment
  // calendar (`HJRs FOR THIRD READING`) and the appropriations
  // calendar (`HABs FOR THIRD READING`). The trailing space is
  // required so the heading is never matched without its bill chain.
  {
    pattern: /^house: (?:hbs|sbs|hjrs|habs) (?:for third reading|3rd read - informal|3rd reading - consent) /,
    questionClass: "passage",
  },
  // The House taking up its own bill as returned with Senate
  // amendments — Missouri's concurrence question.
  { pattern: /^house: (?:hbs|hjrs) with senate amendments /, questionClass: "concurrence" },
  // `House: BILLS IN CONFERENCE CCS HCS SS SCS SBS 81 & 174, E.C`.
  { pattern: /^house: bills in conference /, questionClass: "conference_report" },
  // The Senate's two substantive spellings.
  { pattern: /^senate: third reading$/, questionClass: "passage" },
  { pattern: /^senate: conference committee report adoption$/, questionClass: "conference_report" },
];
const MISSOURI_EXCLUDED_QUESTIONS: LegiscanStateConfig["excludedQuestions"] = [
  // Perfection: the House's amend-and-engross stage, not passage.
  /^house: (?:hbs|hjrs) (?:for perfection|perfection - informal) /,
  // The motion for the previous question (debate cutoff).
  /^house: general pq$/,
  // Floor adoption of a Senate substitute, and the separate vote on a
  // bill's emergency clause.
  /^senate: adopt substitute$/,
  /^senate: emergency clause$/,
];

// Alabama's MODERN floor-question vocabulary, shared by the 2025 and 2026
// regular sessions and the 2026 first special session. Each was surveyed
// separately and prints the SAME families, so they share one definition
// rather than three copies that could drift apart. The 2026 session adds two
// spellings, both harmless no-ops against 2025 (verified: zero 2025 rolls
// match either).
//
// ⚠ THIS VOCABULARY DOES NOT DESCRIBE ALABAMA BEFORE 2025. The feed rewrote
// its captions twice. The 2023 sessions print a third, older set, and 2024
// prints BOTH systems side by side inside one session. Two further
// definitions below cover them, and the same string can mean opposite things
// across them: `Passed House Of Origin` is the 2024 feed's PASSAGE vote,
// while 2025's `SBIR: Passed by House of Origin` is a Budget Isolation
// Resolution. Never reuse one session's patterns on another without
// surveying it first.
const ALABAMA_KEPT_QUESTIONS: LegiscanStateConfig["keptQuestions"] = [
  // Conference-report votes. Listed first so the concurrence rule below
  // cannot claim them.
  { pattern: /concur[ -]in and adopt conference committee report/, questionClass: "conference_report" },
  // `<sponsor> Concur In and Adopt - Roll Call 1188`, `Concur In and Senate
  // Amendment`, `Senate Concurs In House Amendment`, `<sponsor> Motion to
  // Concur In and Adopt`, and the 2026 session's hyphenated
  // `<sponsor> Concur-In and Adopt Executive Amendment` — the vote to accept
  // a change the GOVERNOR sent back with the bill.
  { pattern: /\bconcur(?:s)?[ -]in\b/, questionClass: "concurrence" },
  // Alabama's passage question, with or without a sponsor prefix and with or
  // without ` as Amended`.
  { pattern: /\bmotion to read a third time and pass\b/, questionClass: "passage" },
];
const ALABAMA_EXCLUDED_QUESTIONS: LegiscanStateConfig["excludedQuestions"] = [
  // THE BUDGET ISOLATION RESOLUTION, under all FOUR of its captions. The
  // first two are the pair every taken-up bill carries (identical tally and
  // member list — see the AL entry's note). The last two are how a FAILED
  // Budget Isolation Resolution prints, and the 2026 spelling is the
  // dangerous one: the desc says only `Lost in House of Origin`, which reads
  // like a failed passage vote, while the bill history line that records the
  // same action says `BIR Lost in House of Origin` (HB 583 of 2026, 47-37 —
  // it needed three fifths). Always take the history's word, not the desc's.
  /^[hs]bir:/,
  /^third reading in (?:house of origin|second house)$/,
  /\bmotion to adopt bir\b/,
  /^lost in (?:house of origin|second house)$/,
  // Floor adoption of an amendment or substitute — `<sponsor> motion to
  // Adopt - Roll Call 27 F2Z4DCC-1`. Written without a start anchor because
  // the sponsor prefix is not optional in practice, and with a word boundary
  // so `motion to Concur In and Adopt` is untouched.
  /\bmotion to adopt\b/,
  // Tabling an amendment, and the two debate-cutoff motions.
  /\bmotion to table\b/,
  /\bpetition to (?:cease|close) debate\b/,
  /\bprevious question\b/,
  // Housekeeping: adding a cosponsor, and the local-bill certification
  // resolution (spelled with and without spaces).
  /\badd cosponsor\b/,
  /local ?certification ?resolution/,
  // Procedural steps around a conference: sending a bill to one, refusing to
  // concur, and reconsidering a completed vote.
  /^in conference committee$/,
  /\bnon-concur\b/,
  /\breconsider\b/,
];

// Alabama's 2023 vocabulary, shared by the 2023 regular session and both
// 2023 special sessions. Surveyed separately 2026-09-02; all three print the
// same families and nothing is left unmatched.
//
// Two things make it unlike the modern one. First, the passage question
// carries NO `Motion to` prefix — it is plainly `Read a Third Time and Pass`,
// in four casings and with or without ` as Amended`. Applying the modern
// patterns here matches almost nothing and reports a false empty pool.
// Second, THERE ARE NO BUDGET ISOLATION RESOLUTION ROLL CALLS AT ALL. The
// bill history records `On Third Reading in House of Origin` as a stage line
// with no vote attached, so 2023 took those resolutions by voice. The only
// `Passed by House of Origin` rolls in 2023 are on SPECIAL ORDER CALENDAR
// resolutions, a procedural question, and they are excluded by name.
const ALABAMA_2023_KEPT_QUESTIONS: LegiscanStateConfig["keptQuestions"] = [
  // `Read a Third Time and Pass`, `Read A Third Time And Passed As Amended`,
  // `Read Again a Third Time and Pass as Amended`, `READ A THIRD TIME AND
  // PASSED`. Anchored at the end so a tabling or reconsideration motion
  // naming the same stage cannot match.
  { pattern: /\bread (?:again )?a third time and pass(?:ed)?(?: as amended)?$/, questionClass: "passage" },
  // The second chamber accepting the other's changes: `Concur In and Adopt`,
  // the 2023 first special session's `House Concur and Adopt`, and the bare
  // `Concur`.
  { pattern: /^(?:house )?concur(?: in)? and adopt$/, questionClass: "concurrence" },
  { pattern: /^concur$/, questionClass: "concurrence" },
];
const ALABAMA_2023_EXCLUDED_QUESTIONS: LegiscanStateConfig["excludedQuestions"] = [
  // Refusing to concur and sending the bill to a conference committee.
  // Listed first so the concurrence rule above cannot claim it.
  /\bnon concur\b/,
  // Adoption of a SPECIAL ORDER CALENDAR resolution — the chamber setting
  // its own order of business, not a vote on a measure.
  /^passed by (?:house of origin|second house)$/,
  // Floor adoption of an amendment or substitute (`Adopt`, `Adopt 4XDG33-1`)
  // and tabling one. Start-anchored, so `Concur In and Adopt` is untouched.
  /^adopt\b/,
  /^table\b/,
  // Housekeeping and procedure.
  /\badd cosponsor\b/,
  /^accede$/,
  /^local certification resolution$/,
  /\bprevious question\b/,
  /\bpetition to cease debate\b/,
  /\bcarry over to the call of the chair\b/,
  /\breconsider\b/,
];

// Alabama's 2024 vocabulary. THIS SESSION USES TWO CAPTION SYSTEMS AT ONCE,
// which is the single most important thing to know about it. Surveyed
// 2026-09-02 over 1,229 bills and 2,147 roll calls; 111 families, nothing
// left unmatched.
//
// System A is the older style, with no roll call number in the desc:
//   `Third Reading House of Origin`   = the Budget Isolation Resolution
//   `Passed House Of Origin`          = THE PASSAGE VOTE
// System B is the modern style, with ` - Roll Call <n>` in the desc:
//   `Third Reading in House of Origin`          = the Budget Isolation Resolution
//   `Motion to Read a Third Time and Pass`      = the passage vote
//
// The two BIR captions differ by one word (`in`), and the System A passage
// caption looks like a stage marker. Proof, from SB 47: the bill history
// records `Third Reading in House of Origin` and then `Motion to Read a
// Third Time and Pass - Adopted Roll Call 108`, while the stored rolls are
// captioned `Third Reading House of Origin` (34-0) and `Passed House Of
// Origin` (34-0). The passage vote is there; only its caption changed.
// Reading `Passed House Of Origin` as a Budget Isolation Resolution — which
// is what it is in 2025 — hides 176 real passage votes and understates the
// divided pool by more than half.
const ALABAMA_2024_KEPT_QUESTIONS: LegiscanStateConfig["keptQuestions"] = [
  // Conference-report votes, in four spellings. Listed first so the
  // concurrence rule below cannot claim them.
  {
    pattern: /\bconcur in and adopt (?:conference committee report|conf rpt|concurrence request)/,
    questionClass: "conference_report",
  },
  // `Reed Concur In and Adopt House Amendment`, plain `Concur In and Adopt`.
  { pattern: /\bconcur in and adopt\b/, questionClass: "concurrence" },
  // Accepting a change the Governor sent back with the bill.
  { pattern: /\bmotion to concur in executive amendment\b/, questionClass: "concurrence" },
  // System B passage.
  { pattern: /\bmotion to read (?:again )?a third time and pass(?: as amended)?\b/, questionClass: "passage" },
  // System A passage. End-anchored: nothing else in the session ends this
  // way, and the anchor keeps it from swallowing a longer caption.
  { pattern: /^passed (?:house of origin|second house)$/, questionClass: "passage" },
];
const ALABAMA_2024_EXCLUDED_QUESTIONS: LegiscanStateConfig["excludedQuestions"] = [
  // Refusing to concur. First, so the concurrence rules cannot claim it.
  /\bnon concur\b/,
  // THE BUDGET ISOLATION RESOLUTION under both of this session's captions.
  // The optional `in` is the whole difference between them.
  /^third reading (?:in )?(?:house of origin|second house)$/,
  // Floor adoption of an amendment or substitute, and tabling one. System B
  // spells these `<sponsor> motion to Adopt`; System A spells them
  // `<sponsor> amendment <code>`, `<sponsor> substitution <code>` and
  // `Instrument Change[ Tabled]`.
  /\bmotion to adopt\b/,
  /\bmotion to table\b/,
  /^[a-z.'-]+ (?:amendment|substitution)\b/,
  /^instrument change\b/,
  // Housekeeping and procedure.
  /^local_?certification/,
  /^in conference committee$/,
  /\bsuspend rule\b/,
];

export const LEGISCAN_STATE_CONFIGS: Readonly<Record<string, LegiscanStateConfig>> = {
  // Georgia General Assembly, 2025-2026 Regular Session (both years, sine
  // die 2026-04-03). Vocabulary measured from the full dataset survey
  // 2026-08-26: 5,480 bills, 2,520 roll calls, 242 people (180 House + 56
  // Senate seats plus mid-biennium turnover).
  //
  // What the survey established:
  // - Georgia stamps EVERY desc with a unique chamber vote number
  //   (`Passage: House Vote #804`, `Local Calendar : House Vote #270` —
  //   note the space before the colon), so every pattern has to tolerate a
  //   trailing ` : <chamber> vote #<n>`. 1,696 raw descs collapse to 155
  //   families once that suffix is folded.
  // - Passage is worded `Passage`, `Passage By Substitute` and `Passage As
  //   Amended`; the second chamber's concurrence is always worded `Agree
  //   To …` (23 variants, incl. abbreviations like `Agree To Sam To Hsub`),
  //   so the concurrence rule matches the `agree to ` stem rather than
  //   enumerating them. Conference reports come as `Adopt Conference
  //   Committee Report` plus two one-off spellings (`Adopt CCR`, `Adopt
  //   Conference Comm. Report`).
  // - LOCAL CALENDARS ARE EXCLUDED BY RULE. Georgia passes local
  //   legislation (single-county/city bills) in en-bloc consent calendars:
  //   one roll call is attached to up to ten different bills (`Local
  //   Consent Calendar` 331, `Local Calendar` 319, plus supplemental
  //   variants). Those rolls have no single measure, and none is divided.
  // - The dataset carries NO committee votes at all: every tally is a
  //   whole-chamber tally (House 175-180, Senate 54-56), so nothing lands
  //   in the small-tally or committee buckets.
  // - Georgia proposes CONSTITUTIONAL AMENDMENTS as resolutions
  //   (`HR 251`, `SR 838`), which LegiScan types `R` — a type the shared
  //   kept-types list drops before this config is consulted. Two enacted
  //   Nov-2026 ballot amendments (HR 251, HR 1243) therefore cannot be
  //   queued today; see evidence/rollcall/legiscan-ga-2167/CODE-FINDINGS.md.
  //   Keeping `R` wholesale is NOT the fix: 3,239 of the session's 5,480
  //   "bills" are resolutions, mostly commendations, and several of those
  //   are divided (a Trump commendation split 31-18).
  GA: {
    jurisdiction: "GA",
    sessionId: 2167,
    chamberSizes: { house: 180, senate: 56 },
    keptQuestions: [
      {
        pattern: /^passage(?: by substitute)?(?: as amended)?(?:\s*:?\s*(?:house|senate) vote ?#\d+)?$/,
        questionClass: "passage",
      },
      { pattern: /^agree to /, questionClass: "concurrence" },
      { pattern: /^adopt (?:conference committee report|conference comm\.? report|ccr)/, questionClass: "conference_report" },
    ],
    excludedQuestions: [
      // `Motion To Table`, `Motion To Engross`, `Motion For The Previous
      // Question`, `Motion To Withdraw And Commit`, `Motion To Adjourn` —
      // scheduling and debate motions. Engrossment motions also name a
      // whole calendar of bills in the desc.
      /^motion /,
      // Floor amendment votes: `Adoption Of Amendment #1 By The Senator
      // From The 38th` and its `Adoption Of The Amendment … As Amended`
      // spelling. Deliberately NOT matching `adoption of constitutional
      // amendment`, which is a final question (currently unreachable, see
      // the resolution note above).
      /^adoption of amend/,
      /^adoption of the amendment/,
      /^reconsider/,
      /\brecon\b/,
      // En-bloc local and study-committee calendars (one roll, many bills).
      /local consent calendar/,
      /local calendar/,
      /consent calendar/,
      /uncontested house resolutions/,
      /^immediately transmit/,
      /^table(?:\s*:?\s*(?:house|senate) vote ?#\d+)?$/,
      /shall the ruling of the chair/,
      /^point of order/,
    ],
  },
  // Illinois 104th General Assembly (2025-2026, both years in one dataset).
  // Vocabulary measured from the full dataset survey 2026-08-26: 12,073 bills,
  // 9,077 roll calls, 181 people (121 distinct HD + 60 distinct SD holders over
  // the biennium; the chambers seat 118 + 59).
  //
  // What the survey established:
  // - Illinois passes bills on THIRD READING and prints the plainest desc
  //   vocabulary of any state surveyed so far: 104 distinct descs, no per-roll
  //   id suffix (Texas), no ` : House Vote #<n>` suffix (Georgia).
  // - **Every committee desc ends in the literal word `Committee`** — including
  //   the mis-typed `House Police & Fire Committee Committee` — so committee
  //   votes are excluded by RULE here, not left to the tally heuristic alone.
  // - **Each floor family is printed in TWO SPELLINGS, split by date and never
  //   overlapping**: `Third Reading in House` (2025-04..2025-05) became
  //   `House Third Reading` (2025-10..2026-05) when LegiScan changed its
  //   formatting mid-dataset; same for Concurrence and Motion. Both spellings
  //   are required. Verified they never describe the same physical vote: over
  //   all 9,077 rolls there is no (chamber, bill, date, tally, member-list)
  //   group carrying more than one desc, so keeping both cannot double-count.
  // - Deliberately NOT excluded, so they stay surfaced for a human: the
  //   `Motion in House/Senate` + `House/Senate Motion` family (213 rolls). It
  //   is a garbage bucket — it holds genuine third-reading passages, motions to
  //   reconsider, Note Act motions AND the amendatory-veto votes (Illinois's
  //   distinctive override question), and the desc alone cannot separate them.
  //   `Agreed Bill List` (2) and `House Amendments` (1) are surfaced for the
  //   same reason; the latter is the only JRCA floor roll in the dataset
  //   (HJRCA 28, House 74-38, never voted by the Senate, so no constitutional
  //   amendment from this GA reached the ballot).
  // - Resolution ADOPTION motions (`Senate Motion To Adopt`, `Motion To Adopt
  //   in Senate`, 31 rolls) attach only to JR/R measures. Illinois joint
  //   resolutions are never presented to the governor, so they can never clear
  //   the campaign's became-law filter; excluded by rule to keep the surfaced
  //   queue readable.
  // Connecticut General Assembly, 2025 session. Connecticut files ONE
  // LegiScan dataset per calendar year (unlike Georgia/Illinois/Tennessee,
  // whose two-year assemblies come in a single file), so session 2174 holds
  // the 2025 regular session AND the special sessions that ran under it —
  // including the November 2025 housing session (HB 8002, HB 8003). The
  // 2026 session is a separate dataset (2244), not yet surveyed.
  //
  // Vocabulary measured from the full dataset survey 2026-08-29: 4,073
  // bills, 2,625 roll calls, 211 people (151 House + 36 Senate seats plus
  // turnover). What the survey established:
  // - **1,774 of the 2,625 roll calls are JOINT COMMITTEE tallies printed
  //   with chamber `J`** (all chamber_id 108, all `… Vote Tally Sheet
  //   (Joint Favorable…)`). Connecticut is the joint-committee legislature:
  //   its standing committees seat both chambers, so a committee vote
  //   belongs to no chamber. They are rejected before the queue on the
  //   chamber code (LEGISCAN_COMMITTEE_CHAMBER_CODES in legiscanRollCall.ts)
  //   — nothing in this entry can reach them. The remaining 851 rolls are
  //   ALL floor votes: House totals 150-151, Senate 36. Five Senate rolls
  //   list only the members present (21, 21, 22, 25, 27); the two at 21
  //   fall under the 0.6 floor ratio and surface for a human rather than
  //   being silently kept.
  // - Every desc carries the chamber's own sequential vote number
  //   (`House Roll Call Vote 54 AS AMENDED`, `Senate Roll Call Vote 268 `,
  //   note the trailing space), so every pattern has to tolerate it. That
  //   number is also the `Sequence Number` on the state's own vote-record
  //   PDF (cga.ct.gov/2025/VOTE/<H|S>/PDF/2025<H|S>V-<nnnnn>-R00<BILL>-<H|S>V.PDF).
  // - The House NAMES its question: a bare vote is passage, ` AS AMENDED`
  //   is passage of the amended bill, ` CONSENT CALENDAR` is passage on the
  //   consent calendar, ` EMERGENCY CERTIFICATION` is passage of a bill that
  //   skipped committee, and ` HOUSE AMD <letter>` is a vote on that
  //   amendment. The amendment suffix can follow either of the others
  //   (`… AS AMENDED HOUSE AMD E`, `… EMERGENCY CERTIFICATION HOUSE AMD A`)
  //   and the QUESTION IS ALWAYS THE AMENDMENT — verified against the bill
  //   history for SB 7's roll 225, which the desc calls "AS AMENDED HOUSE
  //   AMD E" and the House journal records as rejecting Amendment Schedule
  //   E 48-99. Excluded patterns run before kept ones, so the amendment
  //   rule wins wherever both would match; that ordering is load-bearing.
  // - Consent-calendar rolls are NOT Georgia's en-bloc calendars: no roll
  //   call in the dataset is attached to more than one bill (checked over
  //   all 2,625), all 29 are on joint resolutions, and none is divided.
  // - **⚠ THE SENATE DESC DOES NOT NAME THE QUESTION.** All 438 Senate
  //   rolls read `Senate Roll Call Vote <n>` and nothing else, whether the
  //   question was passage or a floor amendment (HB 7042 alone carries 18
  //   rejected Senate amendments, every one of them 11-25 and therefore
  //   "divided"). The class below is this pipeline's DEFAULT, not
  //   Connecticut's claim — the same caveat Florida's entry carries. Ground
  //   truth is the bill-status page's ordered action trail
  //   (cga.ct.gov/asp/cgabillstatus/cgabillstatus.asp?selBillType=Bill&bill_num=<BILL>&which_year=2025),
  //   which names every `Senate Rejected Senate Amendment Schedule <X>` and
  //   the single `Senate Passed …`; the vote PDF gives only the tally.
  //   BATCH SELECTION MUST READ THAT TRAIL for every Senate roll it keeps.
  //   The feed's `passed` flag narrows the problem but does not solve it:
  //   81 Senate rolls are `passed:0` (failed amendments), and 17 bills carry
  //   two `passed:1` Senate rolls, so an adopted amendment still looks like
  //   a passage here.
  CT: {
    jurisdiction: "CT",
    sessionId: 2174,
    chamberSizes: { house: 151, senate: 36 },
    keptQuestions: [
      {
        pattern: /^house roll call vote \d+(?: as amended| consent calendar| emergency certification)?$/,
        questionClass: "passage",
      },
      { pattern: /^senate roll call vote \d+$/, questionClass: "passage" },
    ],
    excludedQuestions: [
      // Floor amendment votes. Matched anywhere in the desc so the
      // concatenated spellings (`… AS AMENDED HOUSE AMD E`, `… EMERGENCY
      // CERTIFICATION HOUSE AMD A`) are excluded too — see the note above.
      /\bhouse amd [a-z]\b/,
    ],
  },
  IL: {
    jurisdiction: "IL",
    sessionId: 2176,
    chamberSizes: { house: 118, senate: 59 },
    keptQuestions: [
      { pattern: /^(?:third reading in (?:house|senate)|(?:house|senate) third reading)$/, questionClass: "passage" },
      { pattern: /^(?:concurrence in (?:house|senate)|(?:house|senate) concurrence)$/, questionClass: "concurrence" },
    ],
    excludedQuestions: [/committee$/, /^(?:senate motion to adopt|motion to adopt in senate)$/],
  },
  // Tennessee 114th General Assembly (2025 + 2026 regular sessions are one
  // LegiScan dataset). Vocabulary measured from the full dataset survey
  // 2026-08-27: 9,159 bills, 15,468 roll calls, 136 people (= 99 House +
  // 33 Senate + 4 members who resigned or were replaced mid-term).
  //
  // What the survey established:
  // - Tennessee is the first state whose feed LABELS floor votes: every
  //   floor desc starts `FLOOR VOTE:` and every committee desc starts with
  //   the committee's name (`HOUSE JUDICIARY COMMITTEE: Rec. for pass…`).
  //   0 of the 6,111 non-`FLOOR VOTE:` rolls carry a floor-sized tally, so
  //   the patterns anchor on that prefix and the tally check only
  //   corroborates.
  // - The House prints the CALENDAR and every motion that preceded the
  //   question into one desc (`REGULAR CALENDAR MOTION TO ADOPT AMENDMENT
  //   # 12 BY WILLIAMS PASSAGE ON THIRD CONSIDERATION`), so the trailing
  //   `PASSAGE ON THIRD CONSIDERATION` is calendar context, NOT the
  //   question — amendment and previous-question rolls carry it too. Both
  //   families are excluded, and each of the 106 previous-question rolls
  //   and all 296 amendment rolls was verified to sit beside a plain
  //   passage roll on the same bill, chamber and day.
  // - Ground truth: each bill's own history prints `Passed H., as am.,
  //   Ayes 82, Nays 8` lines. HB0487 pins the reading — the plain roll is
  //   82-8 (`Passed H.`), the previous-question roll beside it is 69-20.
  // - The Senate passes on `Third Consideration` / `as Amended Third
  //   Consideration`, and takes its CONSENT calendar and every resolution
  //   as a bare `Motion to Adopt` (3,509 rolls, only 2 divided; SB0462's
  //   32-0 `Motion to Adopt` is the Consent Calendar 2 vote its history
  //   leaves untallied). That is a real final question, so it is kept as
  //   passage; the House spells the same thing `CONSENT CALENDAR PASSAGE
  //   ON THIRD CONSIDERATION`.
  // - Minority conference reports (4 rolls) are excluded: they are the
  //   losing alternative to the conference report, never the measure.
  // - 2 rolls stay unknown by design (one desc that is only the bare
  //   `FLOOR VOTE:` prefix with nothing after it — a real, non-empty
  //   string that parses fine — and one bare `REGULAR CALENDAR 2`), so
  //   they surface instead of being guessed.
  TN: {
    jurisdiction: "TN",
    sessionId: 2161,
    chamberSizes: { house: 99, senate: 33 },
    keptQuestions: [
      // House final passage, under any calendar (regular, consent 1-4,
      // appropriations, message, unfinished business).
      { pattern: /^floor vote: .*passage on third consideration$/, questionClass: "passage" },
      // Senate final passage.
      { pattern: /^floor vote: (?:as amended )?third consideration$/, questionClass: "passage" },
      // Adoption of a resolution, and the Senate consent calendar.
      {
        pattern: /^floor vote: .*motion to adopt(?: as amended)?(?: third consideration| third and final reading)?$/,
        questionClass: "passage",
      },
      // `Motion to Concur House Amendment # 1`, `MESSAGE CALENDAR CONCUR IN
      // SENATE AMENDMENT # 1`, `REGULAR CALENDAR AS AMENDED MOTION TO
      // CONCUR`. Tennessee never prints a refusal-to-concur question.
      { pattern: /^floor vote: .*concur/, questionClass: "concurrence" },
      { pattern: /^floor vote: .*conference committee report(?: \d+)?$/, questionClass: "conference_report" },
    ],
    excludedQuestions: [
      // The motion for the previous question (debate cutoff), which the
      // House and Senate both print with the pending question's calendar
      // text appended.
      /^floor vote: .*previous question/,
      // Floor amendment votes: `MOTION TO ADOPT AMENDMENT # 4 BY WILLIAMS`,
      // `MOTION TO CONSIDER AMENDMENT # 10 BY HARDAWAY`, the bare
      // `AMENDMENT # 2 BY JONES J` spelling, and the Senate's `amend# 2 by
      // senator yarbro`. The lookahead keeps concurrence descs, which name
      // the amendment being concurred in, out of this rule.
      /^floor vote: (?!.*concur).*amend(?:ment)?\s*#\s*\d+/,
      /^floor vote: .*lay on the table/,
      /^floor vote: .*motion to (?:table|defer|adjourn|recall|reconsider|amend|suspend the rules)/,
      /^floor vote: .*minority conf/,
      /^floor vote: .*re-?refer to committee/,
      /^floor vote: .*refer to committee$/,
      /^floor vote: .*division of question/,
      /^floor vote: .*appoint conference committee/,
      /^floor vote: test motion entry$/,
      /^floor vote: motion$/,
    ],
  },
  // Texas 89th Legislature, Regular Session (sine die). Vocabulary measured
  // from the full dataset survey 2026-08-24: 11,503 bills, 9,726 roll
  // calls, 181 people (= 150 House + 31 Senate). Registry pins the regular
  // session; the two 2025 special sessions (LegiScan 2221, 2223 — the
  // redistricting fight) would need their own entries later.
  //
  // What the survey established:
  // - Texas passes bills on THIRD READING; the House stamps every desc
  //   with a unique roll id (`Read 3rd time RV#3832`), so patterns must
  //   tolerate a trailing ` rv#<n>`.
  // - The chambers word constitutional-amendment passage DIFFERENTLY:
  //   Senate prints `Read 3rd time`, the House prints `Adopted RV#<n>`
  //   (all 24 House 2025 CA passages) — hence the second passage pattern.
  // - The Senate publishes summary-only tallies (no member positions) on
  //   non-record votes — 2,701 rolls incl. 1,223 `Read 3rd time`. Those
  //   are skipped as unrecorded; the divided votes this campaign wants
  //   tend to be record votes, so the target set keeps its positions.
  // - The excluded list covers the measured floor-sized PROCEDURAL
  //   families (~3,200 rolls: second readings, rule suspensions,
  //   amendment steps, journal statements, scheduling), which would
  //   otherwise flood the surfaced-null queue and bury real unknowns.
  //   Deliberately NOT excluded, so they stay surfaced: bare `RV#<n>`
  //   descs (~200, could be anything) and `Record vote` rows.
  TX: {
    jurisdiction: "TX",
    sessionId: 2160,
    chamberSizes: { house: 150, senate: 31 },
    keptQuestions: [
      { pattern: /^read 3rd time(?: rv#\d+)?$/, questionClass: "passage" },
      { pattern: /^adopted(?: as amended)?(?: rv#\d+)?$/, questionClass: "passage" },
      { pattern: /^(?:house|senate) concurs in (?:senate|house) amendment/, questionClass: "concurrence" },
      { pattern: /adopts conference committee report/, questionClass: "conference_report" },
    ],
    excludedQuestions: [
      /^read 2nd time/,
      // `Rules suspended-Regular order of business`, `Three day rule
      // suspended`, `Printing rule suspended` — scheduling motions, not
      // the federal-style "suspend the rules AND PASS" (passage always
      // gets its own third-reading row in Texas).
      /rules? suspended/,
      /^amend/,
      // `Vote recorded in journal` / `Statement(s) of vote recorded in
      // journal`: post-hoc journal entries, not questions.
      /vote recorded in journal/,
      /^laid out/,
      /^point of order/,
    ],
  },
  // Florida 2025 Regular Session (sine die 2025-06-16, after the budget
  // extension). Vocabulary measured from the full dataset survey
  // 2026-08-26: 1,960 bills, 3,003 roll calls, 219 people (the chambers
  // seat 120 + 40; the surplus are mid-session replacements and members
  // who appear only on committee rolls).
  //
  // What the survey established:
  // - Florida's floor vocabulary is the cleanest measured so far: EXACTLY
  //   two desc shapes, `House: Third Reading RCS#<n>` (401 rolls) and
  //   `Senate: Third Reading RCS#<n>` (382). Every other desc in the feed
  //   is a literal committee name. The feed carries no concurrence,
  //   conference-report or veto-override desc at all, so `passage` is the
  //   only question class Florida can produce.
  // - The House stamps a unique RCS number on every vote (401 distinct
  //   descs for 401 votes); the Senate recycles RCS#1..61 across days.
  //   Patterns tolerate the suffix, as in Texas.
  // - Tallies separate cleanly: floor rolls total 119-120 of 120 (House)
  //   and 38-39 of 40 (Senate), while every House committee tops out at
  //   30 (25%). The one exception is `Senate Rules`, a 25-member committee
  //   = 62% of the chamber, which clears the committee-tally cut and would
  //   otherwise sit in the surfaced-null queue for all 154 of its rolls.
  //   It is excluded by NAME because its tally cannot distinguish it.
  // - Unlike Texas, every Florida roll carries a member list (0
  //   summary-only tallies), and no roll_call_id is reused for a second
  //   identical floor action (102 duplicate-identity groups exist, all
  //   committee, all rejected before the queue).
  // - Constitutional amendments ride JOINT RESOLUTIONS here (bill_type JR,
  //   27 in session), already a kept type — Florida needs no second
  //   passage pattern the way Texas did.
  // - Florida also prints FAILED floor votes under the same desc (HB 1205
  //   lost a Senate vote 10-26 and a House vote 27-82 before passing), so
  //   selection must pick the decisive roll per chamber; classification
  //   neither can nor tries to.
  FL: {
    jurisdiction: "FL",
    sessionId: 2135,
    chamberSizes: { house: 120, senate: 40 },
    keptQuestions: [{ pattern: /^(?:house|senate): third reading(?: rcs#\d+)?$/, questionClass: "passage" }],
    excludedQuestions: [
      // The Senate Rules COMMITTEE (always exactly 25 of 40 senators), not
      // a floor motion: too big for the committee-tally cut, so it has to
      // be named. No House committee comes close to that cut.
      /^senate rules$/,
    ],
  },
  // California 2025-2026 Regular Session (LegiScan 2172). Vocabulary
  // measured from the full dataset survey 2026-08-26: 5,057 bills, 19,942
  // roll calls, 160 people (81 Assembly + 40 Senate districts + 39
  // committee pseudo-people, which carry no district).
  //
  // What the survey established:
  // - California prints the bill number and its author INSIDE the desc, so
  //   almost every desc string is unique (`SB 586 Jones Senate Third
  //   Reading By Jeff Gonzalez`). The question is a phrase within the desc,
  //   never the whole of it — hence unanchored patterns in the Assembly,
  //   where the desc STARTS with the measure, and `^` anchors in the
  //   Senate, where it starts with the question.
  // - The two chambers word the same question differently: the Assembly
  //   writes `Third Reading` and `Concurrence in Senate Amendments`, the
  //   Senate writes `3rd Reading` and `Unfinished Business … Concurrence`.
  //   Both spellings are kept for both chambers; each is measured.
  // - Committee votes are worded as RECOMMENDATIONS that name the same
  //   destinations (`Do pass. To consent calendar`, `Be adopted. Ordered to
  //   third reading`, `Placed on suspense file`), which is why the kept
  //   patterns require the chamber word or the `^` anchor. As written,
  //   every one of the 5,291 kept-type matches is floor-sized and no
  //   committee-sized roll matches any kept pattern (measured).
  // - Urgency bills (a 2/3 vote) get their own wording in both chambers —
  //   `Third Reading Urgency` with no chamber word, and `Concurrence -
  //   Urgency Added` — so both need their own alternative.
  // - CA 2172 has NO conference-report and NO veto-override roll calls
  //   (zero descs mention either), so those classes go unused here.
  // - Feed health: 0 duplicate roll_call_id identity groups (the Texas 9.4%
  //   collapse is a verified no-op in California), 0 summary-only rolls, 0
  //   tally or member-list mismatches.
  CA: {
    jurisdiction: "CA",
    sessionId: 2172,
    // The Assembly is the lower chamber; LegiScan prints its rolls as
    // chamber `A`, which parseLegiscanRollCall maps to `house`.
    chamberSizes: { house: 80, senate: 40 },
    keptQuestions: [
      // Assembly floor passage: `AB 111 Gabriel Assembly Third Reading`,
      // `SB 586 Jones Senate Third Reading By Jeff Gonzalez` (an Assembly
      // vote on a Senate bill), `AB 40 Bonta Third Reading Urgency`.
      { pattern: /(?:assembly|senate) third reading|third reading urgency/, questionClass: "passage" },
      // Senate floor passage: `Senate 3rd Reading SB680 Rubio`, `Assembly
      // 3rd Reading AB123 BUDGET (Gabriel) By Wiener`.
      { pattern: /^(?:senate|assembly) 3rd reading\b/, questionClass: "passage" },
      // Consent calendars are floor passage of measures no member objected
      // to; the Assembly says `Consent Calendar Second Day Regular
      // Session`, the Senate `Consent Calendar 2nd` / `Special Consent`.
      { pattern: /consent calendar (?:first|second) day/, questionClass: "passage" },
      { pattern: /^consent calendar\b/, questionClass: "passage" },
      { pattern: /^special consent\b/, questionClass: "passage" },
      // `W/O Ref. To File SB48 Gonzalez`: the file-section label on a
      // SUBSTANTIVE vote taken up without reference to file — the waiver
      // itself is granted by unanimous consent and has NO roll call (the
      // history prints `Consent granted to take up without reference to
      // file` with no tally). The recorded roll matches the substantive
      // action's tally exactly: SB 48's 27-5 is `Assembly amendments
      // concurred in. (Ayes 27. Noes 5.)`, SB 166's 29-9 and SB 694's 25-6
      // likewise. Of the 36 kept-type instances, 32 are the Senate's
      // concurrence in Assembly amendments — including every divided one —
      // so the class is concurrence; the remaining 4 are near-unanimous
      // second-chamber ACA passages, for which this label is nominal (the
      // class is report metadata only, and the judge reads the bill
      // history before writing a description either way).
      { pattern: /^w\/o ref\. to file\b/, questionClass: "concurrence" },
      { pattern: /concurrence in (?:senate|assembly) amendments|concurrence - urgency added/, questionClass: "concurrence" },
      { pattern: /^unfinished business\b.*\bconcurrence\b/, questionClass: "concurrence" },
    ],
    excludedQuestions: [
      // Second reading in California is where amendments are taken up; the
      // urgency-clause votes held there are not passage.
      /\b2nd reading\b/,
      /\bsecond reading\b/,
      // Procedural motions that reuse a kept question's wording. A desc
      // ending in a bare ` Reconsider` is the vote GRANTING reconsideration,
      // not the question itself: SB 627's `Unfinished Business … Concurrence
      // Reconsider` 30-10 is the "Reconsideration granted" line of the bill
      // history, sitting between the 27-10 concurrence it undid and the
      // operative 28-11 concurrence that followed. 15 floor rolls, all
      // Senate; the 91 committee `reconsideration granted` rolls were
      // already below the tally line. Anchored at the end so it does not
      // also swallow the committee `Reconsideration granted` families,
      // which the tally cut already rejects before the queue.
      /\breconsider$/,
      /motion to lay on the table/,
    ],
  },
  // Pennsylvania General Assembly, 2025-2026 Regular Session (LegiScan
  // 2192). Vocabulary measured from the full dataset survey 2026-08-29:
  // 4,935 bills, 5,038 roll calls, 260 people (203 House + 50 Senate seats
  // plus mid-biennium turnover). The session is still LIVE (sine_die 0,
  // dataset_date 2026-08-23), so this dataset grows.
  //
  // What the survey established:
  // - Pennsylvania NAMES THE VENUE in every desc. A floor vote reads
  //   `House Floor: HB 1431 PN 1746, FINAL PASSAGE`; a committee vote reads
  //   `House Judiciary: Report Bill As Committed` / `Senate Appropriations:
  //   Re-Reported as Committed`. Every one of the 1,660 floor-sized rolls
  //   carries the literal `Floor:`, and no committee-sized roll does, so
  //   every kept pattern anchors on that token (the Tennessee `FLOOR VOTE:`
  //   shape). The chamber WORD in the desc is not reliable — four Senate
  //   rolls are captioned `House Floor: PN1030, Concur in House Amendments`
  //   — so no pattern reads it.
  // - The desc puts the measure and its PRINTER'S NUMBER between the venue
  //   and the question (`SB 246 PN 1009`, sometimes just `PN1225`, and
  //   sometimes an amendment number too), which makes 3,881 of the 5,038
  //   descs distinct. The question is always the comma-delimited TAIL, so
  //   patterns match `floor:.*,` then anchor the question at `$`.
  // - Passage is worded `FINAL PASSAGE`. Reconsidered passage votes are
  //   spelled four different ways (`Final Passage - Reconsideration`,
  //   `Final Passage-Reconsidered`, `Final Passage Reconsidered`,
  //   `Reconsideration - Final Passage`, 9 rolls) and one two-thirds vote is
  //   `Final Passage Constitutional 2/3 Vote`; all are genuine passage
  //   votes on the bill. `Motion to Reconsider bill on final passage` also
  //   ENDS in `final passage` but is the motion, not the question — it is
  //   excluded by rule below, which is why the exclusions must run first.
  // - Second-chamber agreement is `CONCURRENCE` in the House and
  //   `Concur in House Amendments` (plus `Concurrence in House Amendments as
  //   Amended` and `Concur in House Amendments to Senate Amendments`) in the
  //   Senate. 50 rolls in all.
  // - PA 2192 has NO conference-report and NO veto-override roll calls
  //   (zero descs mention either), so those classes go unused here.
  // - Amendment votes are the biggest floor family (373 House, ~90 Senate).
  //   The House prints the amendment number as the whole tail
  //   (`, 2025 A1363`) or glues it to the printer's number
  //   (`PN1936 A02188`); the Senate names the offering senator
  //   (`Brooks Amendment No. A-1422`). All are excluded by rule, along with
  //   every `Motion to ...`, `Uncontested Calendar`, `Second Consideration`,
  //   `CONSTITUTIONALITY` (a PA-specific point-of-order vote) and
  //   `Re-referred`/`Recommit` roll. Without those exclusions ~795 floor
  //   rolls would land in the surfaced queue.
  // - With this entry the whole dataset classifies with NOTHING surfaced:
  //   861 kept (811 passage / 50 concurrence, all bill type B), 795
  //   excluded, 3,134 committee-sized, and 248 floor-sized rolls that are
  //   all type `R` resolutions, dropped by the shared kept-types list before
  //   this config is consulted.
  // - Feed health: 0 duplicate roll_call_ids (the Texas 9.4% collapse is a
  //   verified no-op here), 0 summary-only rolls, 0 tally mismatches; 7
  //   identity-duplicate extras, which the shared identity key collapses.
  PA: {
    jurisdiction: "PA",
    sessionId: 2192,
    chamberSizes: { house: 203, senate: 50 },
    keptQuestions: [
      // `House Floor: HB 1431 PN 1746, FINAL PASSAGE`,
      // `Senate Floor: PN1936 A02188, Final Passage`.
      { pattern: /floor:.*,\s*final passage$/, questionClass: "passage" },
      // The four reconsidered-passage spellings and the 2/3 vote.
      { pattern: /floor:.*,\s*final passage\s*-?\s*reconsider(?:ation|ed)$/, questionClass: "passage" },
      { pattern: /floor:.*,\s*reconsideration\s*-\s*final passage$/, questionClass: "passage" },
      { pattern: /floor:.*,\s*final passage constitutional 2\/3 vote$/, questionClass: "passage" },
      // House second-chamber agreement.
      { pattern: /floor:.*,\s*concurrence$/, questionClass: "concurrence" },
      // Senate second-chamber agreement, all three spellings.
      {
        pattern: /floor:.*,\s*concur(?:rence)? in house amendments(?: to senate amendments| as amended)?$/,
        questionClass: "concurrence",
      },
    ],
    excludedQuestions: [
      // Every procedural motion: table, suspend rules, appeal the ruling of
      // the chair, proceed, postpone, previous question, and the
      // `Motion to Reconsider bill on final passage` that would otherwise
      // match the passage pattern's `$` anchor.
      /\bmotion\b/,
      // Senate amendment votes name the offering senator.
      /\bamendment no\./,
      // House amendment votes: `, 2025 A1363` as the tail, or the amendment
      // number glued to the printer's number (`PN1936 A02188`).
      /,\s*(?:19|20)\d{2}\s+a\d+$/,
      /\ba\d{3,}$/,
      // A PA point of order on whether the bill is constitutional, taken
      // before passage; not a vote on the measure.
      /,\s*constitutionality$/,
      // Consent-calendar and pre-passage stages.
      /\buncontested calendar\b/,
      /\bsecond consideration\b/,
      /\b2nd consideration\b/,
      /,\s*third consideration as amended$/,
      // Committal and housekeeping motions worded without the word "motion".
      /\bre-referred\b/,
      /\brecommit\b/,
      /\breconsideration of request to go over$/,
      /\btabled$/,
      /,\s*reconsidered$/,
    ],
  },
  // Maine 132nd Legislature, 2025-2026 Regular Session (both years, sine
  // die). Vocabulary measured from the full dataset survey 2026-08-29:
  // 2,454 bills, 1,580 roll calls, 188 people (151 House + 35 Senate seats
  // plus mid-biennium turnover).
  //
  // What the survey established:
  // - Maine decides most questions by an unrecorded division; a roll call
  //   happens only when members demand one. The consequence is the
  //   opposite of a coverage problem: 1,450 of the 1,580 recorded rolls
  //   are DIVIDED, because the contested bills are exactly the ones that
  //   get a roll. Nothing here is a consent calendar.
  // - Every desc, in BOTH chambers, ends with a unique ` RC #<n>` (the
  //   clerk's roll number), so no pattern may anchor on `$` without
  //   tolerating it. 1,579 raw descs fold to 227 families.
  // - MAINE PASSES A BILL BY ACCEPTING ITS COMMITTEE REPORT. The
  //   substantive floor question is `Accept Majority Ought To Pass As
  //   Amended Report` (Senate) / `Acc Maj Otp As Amended Rep` (House) and
  //   ~40 further spellings of the same act (majority/minority, Report
  //   "A"/"B"/"C", `Otp-am By Ca "a"`, `Acceptance Of The Otp-am
  //   Report`). Enumerating them is hopeless; the rule instead keeps any
  //   desc carrying an ought-to-pass token (`otp`, `otp-am`, `ought to
  //   pass`), with the ought-NOT-to-pass tokens excluded FIRST so an
  //   `Ontp` report acceptance can never fall through to it.
  // - The later stages each have their own question: `Passage To Be
  //   Engrossed`, then `Enactment` (Maine's true final passage, also
  //   spelled `Enactment - Emer`, `Enactment - Bond Issue`, `Enact-Emer
  //   2/3 Elect`, `Final Enactment`, `Final Passage`). All are passage.
  // - `Recede And Concur` (and the bare `Recede`) is how a chamber gives
  //   up its position and takes the other's — the concurrence analog.
  // - Veto questions are `Veto Override (2/3)` in the Senate and
  //   `Reconsideration - Veto` in the House ("shall the bill become law
  //   notwithstanding the objections of the Governor"). The plain
  //   `Reconsider` motion is a different question and is excluded, so the
  //   exclusion carries a lookahead for the veto spelling.
  // - OUGHT-NOT-TO-PASS ACCEPTANCES ARE EXCLUDED BY RULE (352 rolls, the
  //   two largest families after OTP). They are votes to KILL a bill, so
  //   the yea sentence would have to be inverted, and the measured value
  //   is small: only 8 of them are divided votes on a measure that became
  //   law anyway. Indefinite postponement, tabling, commitment,
  //   reference, insistence, rule suspensions and the ~145 amendment
  //   families (`Adopt Hah-963 To Cah-959`, `Indef Pp Hbh-3 To Cah-1`)
  //   are excluded for the same procedural reason.
  // - 19 rolls stay SURFACED on purpose: `Accept Report`, `Acceptance Of
  //   Report` and `Acc Majority Report` do not say WHICH report, so
  //   whether a yea passes or kills the bill cannot be read off the desc.
  //   10 of the 16 `Accept Report` rolls are divided votes on enacted
  //   measures, so they are worth a human's eyes, not a guess.
  // - The dataset carries NO committee votes at all: every tally is a
  //   full-chamber tally (House 149-151, Senate 35), and no roll is
  //   summary-only, so nothing lands in the committee or unrecorded
  //   buckets.
  ME: {
    jurisdiction: "ME",
    sessionId: 2181,
    chamberSizes: { house: 151, senate: 35 },
    keptQuestions: [
      // Acceptance of an ought-to-pass committee report, in all ~40 of the
      // spellings the two chambers use. Ought-NOT-to-pass tokens are
      // excluded above, so this token test cannot invert a question.
      { pattern: /\botp\b|ought to pass/, questionClass: "passage" },
      // `Enactment`, `Enactment - Emer`, `Enactment - Bond Issue`,
      // `Enactment - Mandate`, `Enact-Emer 2/3 Elect`.
      { pattern: /^enact/, questionClass: "passage" },
      { pattern: /^final (?:enactment|passage)/, questionClass: "passage" },
      // `Passage`, `Passage To Be Engrossed[ As Amended]`, `Passage Of
      // Emergency Measure`.
      { pattern: /^passage/, questionClass: "passage" },
      // Adoption of a joint resolution — for a JR, adoption IS the final
      // passage question. Verified over the dataset: all 8 bare-`Adoption`
      // rolls sit on JR measures; amendment adoptions are never worded
      // this way (they all begin `Adopt <designator>` — `Adopt Cah-1`,
      // `Adopt Hah-963 To Cah-959` — and are excluded above), so the
      // end-anchored bare word cannot reach an amendment vote.
      { pattern: /^adoption(?: rc ?#\d+)?$/, questionClass: "passage" },
      // `Recede And Concur` — the chamber gives up its own position and
      // takes the other chamber's. A BARE `Recede` is different: the
      // chamber recedes from an earlier action of its OWN (LD 209's Senate
      // `Recede`, 12-20, is supplemental-budget amendment machinery, not a
      // concurrence), so the 12 bare-`Recede` rolls surface as unknown
      // instead of being auto-kept.
      { pattern: /^recede and concur/, questionClass: "concurrence" },
      { pattern: /^reconsideration ?- ?veto/, questionClass: "veto_override" },
      { pattern: /^veto override/, questionClass: "veto_override" },
    ],
    excludedQuestions: [
      // A vote to accept an ought-not-to-pass report kills the bill; see
      // the note above. Both the House abbreviation and the Senate's
      // long form, in every report letter.
      /\bontp\b/,
      /ought not to pass/,
      // Maine's other kill motion, and the amendment-scoped spelling
      // (`Indefinitely Postpone Senate Amendment (sas-1)`, `Indef Pp
      // Hbh-3 To Cah-1`, `Ipp Hah-489`, `Ha "a" Be Indef Pp`).
      /^indef/,
      /^ipp /,
      /\bindef pp\b/,
      // Floor and committee amendment adoptions: `Adopt Cah-1`, `Adopt
      // Hah-963 To Cah-959`, `Adopt Senate Amendment (s-292) To Ld 1519`.
      // The trailing space keeps the bare `Adoption` question out.
      /^adopt /,
      // The motion to reconsider a completed action, which is not the
      // question it reopens. The lookahead preserves the House's veto
      // question, spelled `Reconsideration - Veto`.
      /^reconsider(?!ation ?- ?veto)/,
      /^recon of /,
      // Scheduling, referral and debate motions.
      /^table/,
      /^commit(?: rc ?#\d+)?$/,
      /^reference/,
      /^insist/,
      /^suspen/,
      /^dispens/,
      /^move the previous question/,
      /^appeal/,
      /^committee of the whole/,
      /^rule comm/,
      /^forthwith/,
      /^\d(?:st|nd|rd|th) reading/,
      // Referral questions worded as report acceptances, which would
      // otherwise be read as passage by the ought-to-pass token rule.
      /^accept majority to refer/,
      /^accept to reject report/,
      // The motion to swap a committee report for a joint resolution: a
      // question about what the chamber debates next, not its passage.
      /^substitute joint res/,
    ],
  },
  // Missouri 103rd General Assembly, 2025 Regular Session (LegiScan session
  // 2169; the two 2025 special sessions, 2216 and 2226, are separate
  // datasets and would need their own entries). Vocabulary measured from
  // the full dataset survey 2026-08-29: 2,673 bills, 557 roll calls, 197
  // people (163 House + 34 Senate seats plus mid-session turnover).
  //
  // What the survey established:
  // - The two chambers write their descs in COMPLETELY different styles.
  //   The Senate prints the bare question and nothing else — only five
  //   spellings exist in the whole session. The House prints its CALENDAR
  //   HEADING followed by the bill's substitute chain, so every House desc
  //   is unique (`House: HBs WITH SENATE AMENDMENTS SS SCS HB 225, A.A.,
  //   E.C.`); 275 raw House descs fold to 16 calendar families. House
  //   patterns therefore match the heading and let the chain trail.
  // - `Senate: Third Reading` (172 rolls) is BOTH the first-round third
  //   reading and the Truly Agreed To And Finally Passed vote — Missouri
  //   prints no separate TAFP wording — so the second chamber's vote on
  //   the enacted text lands in this family too.
  // - MISSOURI PERFECTION IS NOT PASSAGE. `HBs FOR PERFECTION` /
  //   `HBs PERFECTION - INFORMAL` / `HJRs FOR PERFECTION` (36 rolls) are
  //   the House's amend-and-engross stage, the second-reading analog that
  //   Texas and California also exclude; third reading is the passage
  //   question and every perfected bill gets one.
  // - `Senate: Adopt Substitute` (13) is the floor adoption of an SS/SCS
  //   (nine of them on SB 98 in a single day) and `Senate: Emergency
  //   Clause` (8) is the separate vote on whether the act takes effect at
  //   once — neither is a vote on the measure, so both are excluded.
  // - `Senate: Adoption` (10) is DELIBERATELY LEFT UNMATCHED so it
  //   surfaces: nine of the ten are ceremonial concurrent/simple
  //   resolutions (rejected earlier as excluded measure types) but the
  //   tenth is the Senate adopting the HB 595 conference committee report
  //   (22-11). One desc, two questions, so it is reviewed rather than
  //   guessed.
  // - The dataset carries NO committee votes at all: every tally is a
  //   whole-chamber tally (House 161-163, Senate 31-34).
  // - A HOUSE DESC NAMES THE CALENDAR, NOT THE QUESTION. On one calendar
  //   day the House prints the previous-question motion (the debate
  //   cutoff), the concurrence ("House Adopts"), the Truly Agreed To And
  //   Finally Passed vote and the emergency clause under the SAME desc —
  //   HB 225 on 2025-05-08 is one string over 92-45 / 89-32 / 88-26 /
  //   88-34, and only the last two are votes on the measure. Nothing in
  //   the feed separates them, so the kept patterns classify the FAMILY
  //   and `questionClass` here is report metadata, not ground truth. The
  //   queue is a review queue — a stored roll fans out only after a human
  //   judgment — and Missouri selection must match every roll to its
  //   official House roll-call PDF (documents.house.mo.gov/billtracking/
  //   bills251/rollcalls/<day>.<n>.pdf), whose header names any
  //   non-passage question and stamps the LR number of the text on the
  //   floor; see evidence/rollcall/legiscan-mo-2169/CODE-FINDINGS.md.
  //   Classifying these headings as unknown instead would park ~293 House
  //   rolls in the surfaced queue, where the approval CHECK can never
  //   accept them, while relocating the same per-roll PDF check nowhere.
  // - Only the calendar headings measured in this session are listed. A
  //   heading this session never printed (an `SJRs FOR THIRD READING`, a
  //   Senate concurrence wording) surfaces as unknown rather than being
  //   guessed from another state.
  // - Feed health: 0 repeated roll_call_ids (the Texas 9.4% collapse is a
  //   verified no-op here), 0 summary-only rolls, 0 tally mismatches, 0
  //   file errors. 49 rolls collapse under the shared identity key, and
  //   in Missouri they are NOT all reprints of one action: back-to-back
  //   DISTINCT actions can carry identical data — HB 594's "House Adopts
  //   SS#2" (official roll 066.003) and its Truly Agreed To And Finally
  //   Passed vote minutes later (066.004) are both 102-41 with the same
  //   member list, so the key folds them and the lowest roll_call_id
  //   survives as the class representative. No member position is lost
  //   (the lists are identical, and the fan-out imports one roll per
  //   measure per chamber regardless), but a judgment's description must
  //   be worded to hold for the whole class unless the roll-call PDF pins
  //   the specific action. Disabling the collapse would be worse: the
  //   fan-out dedupes on ls:<id> URL keys, so two stored ids for one
  //   identical vote would let a double judgment write every member's
  //   record twice.
  MO: {
    jurisdiction: "MO",
    sessionId: 2169,
    chamberSizes: { house: 163, senate: 34 },
    keptQuestions: MISSOURI_KEPT_QUESTIONS,
    excludedQuestions: MISSOURI_EXCLUDED_QUESTIONS,
  },

  // Missouri's 2025 SECOND EXTRAORDINARY SESSION (LegiScan session 2226),
  // surveyed 2026-08-30: 13 bills, 8 roll calls, 194 people.
  //
  // ⚠ THE REGISTRY KEY IS NOT THE JURISDICTION HERE. Every other entry is
  // keyed by its postal code because a state has had one session in scope;
  // Missouri has two. `jurisdiction` is a separate field that every database
  // write already uses, so a compound KEY pins the second session while the
  // rows, the evidence filenames (`ls-mo-…`) and the run ids all stay `MO`.
  // Nothing looks a config up by a row's jurisdiction — all four scripts
  // resolve it from the `--state` flag — so the two entries cannot collide.
  // Run this one with `--state MO-2226`.
  //
  // What the survey established:
  // - The vocabulary is a SUBSET of the regular session's and needs no new
  //   pattern: `HJRs/HBs FOR THIRD READING` and `Senate: Third Reading` are
  //   kept, `HJRs/HBs FOR PERFECTION` excluded. Hence the shared constants.
  // - Both substantive measures are divided in both chambers: HB 1, the
  //   congressional redistricting map (enacted), and HJR 3, the "Protect
  //   Missouri Voters" amendment (finally passed; defeated at the ballot
  //   as Amendment 4, August 2026). 5 kept floor votes, 3 excluded
  //   perfection votes.
  // - House tallies run to 159 of 163 seats and the Senate to 34 of 34, so
  //   the floor cut is unchanged.
  // - Feed health: 0 repeated roll_call_ids, 0 summary-only rolls, 0 tally
  //   mismatches, 0 file errors, and no roll_call_id collides with session
  //   2169's.
  //
  // Missouri's 2025 FIRST Extraordinary Session (LegiScan 2216) is
  // deliberately NOT registered: LegiScan files the REGULAR session's SB 4
  // utility votes under that session's unrelated SB 4 (a housing trust fund
  // bill), with roll_call_ids that do not collide with 2169's, so importing
  // it would attach utility votes to a housing bill. See
  // evidence/rollcall/legiscan-mo-2169/README.md.
  "MO-2226": {
    jurisdiction: "MO",
    sessionId: 2226,
    chamberSizes: { house: 163, senate: 34 },
    keptQuestions: MISSOURI_KEPT_QUESTIONS,
    excludedQuestions: MISSOURI_EXCLUDED_QUESTIONS,
  },

  // Maryland General Assembly, 2025 Regular Session (Jan 8 - Apr 7 2025;
  // Maryland sits in ANNUAL sessions, so this dataset is one year only —
  // the 2026 Regular Session is its own LegiScan session, 2240).
  // Vocabulary measured from the full dataset survey 2026-08-29: 2,617
  // bills, 2,494 roll calls, 216 people (141 Delegates + 47 Senators plus
  // mid-term turnover).
  //
  // What the survey established:
  // - Maryland's vocabulary is the SMALLEST of any state surveyed so far:
  //   2,494 rolls collapse to 15 desc families, and 2,295 of them are the
  //   single literal string `Third Reading Passed`. Passage of a version
  //   the other chamber amended is worded `Third Reading Passed with
  //   Amendments` in the House and `Third ReadingS Passed with Amendments`
  //   (plural) in the Senate — the same question, two spellings, so the
  //   pattern makes the `s` optional. All 14 of those are ordinary
  //   single-bill second-chamber passages, not en-bloc calendars.
  // - The session has exactly ONE conference-report roll (SB 338, Senate,
  //   30-17), worded `Conference Committee Report 903525/1 Adopted` — the
  //   number is the amendment's filing id, so the pattern tolerates digits
  //   and a slash rather than naming it.
  // - Maryland does NOT roll-call concurrence: no desc in the session
  //   mentions concurring. The originating chamber's agreement to the
  //   other chamber's amendments is taken without a recorded vote, so for
  //   an amended bill the only recorded votes are each chamber's own third
  //   reading — which means the two chambers routinely voted DIFFERENT
  //   TEXT. Every judgment on a `with Amendments` measure has to name the
  //   version each roll actually was.
  // - The dataset carries NO committee votes at all (as in Georgia): every
  //   tally is whole-chamber (House 137-141, Senate 45-47), so nothing
  //   reaches the small-tally or committee buckets and no roll is left
  //   surfaced. Every desc in the session matches a kept or an excluded
  //   pattern — 2,310 kept, 184 excluded, 0 unmatched (measured).
  // - Excluded families are all floor-sized and all procedural: floor
  //   amendments (152 rejected, 2 adopted), committee amendments adopted
  //   on the floor (12), and motions (previous question, suspend the rules
  //   for late introduction / two readings the same day / to refer,
  //   special order). Amendment descs name the sponsoring member in
  //   parentheses, so they are anchored at the start of the string.
  // - Feed health is the best of any phase-4 state, tied with Georgia: 0
  //   repeated roll_call_ids (the Texas 9.4% collapse is a verified no-op
  //   here), 0 identity-duplicate groups, 0 summary-only rolls (every roll
  //   carries a member list), 0 tally mismatches, 0 file errors.
  // - Only bill types B (2,605) and JR (12) appear; Maryland proposes
  //   constitutional amendments as ordinary BILLS, so the Georgia
  //   resolution-typed-amendment gap does not recur.
  // - 414 of the kept rolls fall on sine die (2025-04-07); per the Illinois
  //   date-skew finding, audit a selected roll's date against the official
  //   Maryland General Assembly bill page before importing it.
  MD: {
    jurisdiction: "MD",
    sessionId: 2164,
    chamberSizes: { house: 141, senate: 47 },
    keptQuestions: [
      // `Third Reading Passed`, `Third Reading Passed with Amendments`,
      // `Third Readings Passed with Amendments`. Anchored at both ends:
      // the excluded amendment and motion families never share this
      // wording, and nothing else in the session's vocabulary does either.
      { pattern: /^third readings? passed(?: with amendments)?$/, questionClass: "passage" },
      // `Conference Committee Report 903525/1 Adopted` (SB 338).
      { pattern: /^conference committee report [\d/]+ adopted$/, questionClass: "conference_report" },
    ],
    excludedQuestions: [
      // `Floor Amendment 273422/1 (Delegate Hornberger) Rejected`, and the
      // handful worded without a filing number.
      /^floor amendment\b/,
      // `Committee Amendment (Senator Beidle) Adopted` — the floor vote
      // adopting a committee's amendment, not the question on the bill.
      /^committee amendment\b/,
      // `Motion Vote Previous Question (...) Adopted`, `Motion Rules
      // Suspend for Late Introduction (...) Adopted`, `Motion Special
      // Order until Later This Session (...) Rejected`, `Motion Rules
      // Suspend to Refer (...) Rejected`.
      /^motion /,
    ],
  },

  // Maryland General Assembly, 2026 Regular Session (Jan 14 - Apr 13 2026,
  // the constitutional 90-day adjournment; April 6 was the budget deadline,
  // not sine die). The LegiScan session also carries a 32-roll August 2026
  // sitting under the same session id — the source of the `Overridden`
  // veto-override rolls; the regular session's own last votes fall on
  // April 13. Vocabulary measured from the full dataset survey 2026-08-29:
  // 2,675 bills, 2,732 roll calls, and 217 people. The 2,449 final floor
  // votes use the same three third-reading spellings as 2025. Two Senate
  // conference-report votes and the veto overrides appeared after the
  // initial final-passage histogram and are explicitly listed below.
  // Every roll has a member list and whole-chamber tally (House 141, Senate
  // 47); the remaining 279 rolls are procedural.
  "MD-2240": {
    jurisdiction: "MD",
    sessionId: 2240,
    chamberSizes: { house: 141, senate: 47 },
    keptQuestions: [
      { pattern: /^third readings? passed(?: with amendments)?$/, questionClass: "passage" },
      { pattern: /^conference committee report(?: [\d/]+)? adopted$/, questionClass: "conference_report" },
      { pattern: /^overridden$/, questionClass: "veto_override" },
    ],
    excludedQuestions: [
      /^floor amendment\b/,
      /^committee amendment\b/,
      /^favorable with amendments\b/,
      /^motion /,
      /^decision of the chair upheld$/,
    ],
  },

  // Kentucky General Assembly, 2025 Regular Session (the 30-day short
  // session; sine die 2025-03-28). Vocabulary measured from the full
  // dataset survey 2026-08-31: 1,441 bills, 701 roll calls, 138 people —
  // exactly the 100 House and 38 Senate seats.
  //
  // What the survey established:
  // - EVERY desc carries a per-roll sequence suffix, and the two chambers
  //   spell it DIFFERENTLY: the House prints ` RCS# <n>` (Roll Call
  //   Sequence) and the Senate ` RSN# <n>`. All 701 descs carry one, so
  //   the patterns REQUIRE it — a future desc without a suffix surfaces
  //   for review instead of silently classifying. Folding the suffix
  //   collapses 701 raw descs to 36 families.
  // - Every tally is a whole-chamber tally (exactly 100 House, 38 Senate;
  //   `total` counts the seats, not the votes cast). The dataset holds NO
  //   committee votes at all, so nothing lands in the small-tally bucket.
  // - The Senate writes its VERSION CHECK into the desc: `Senate: Third
  //   Reading W/scs1 sfa1 scta1` names the committee substitute and floor
  //   amendments folded into the text being voted on. Only Pennsylvania's
  //   printer's numbers match this.
  // - Kentucky's veto override needs only a SIMPLE MAJORITY of each
  //   chamber, so the legislature overrides routinely: 28 bills were
  //   vetoed and overridden in this session. Both chambers' override rolls
  //   are in the feed for 25 of them (HB 2, HB 4 and HB 6 are House-only).
  //
  // *** THE `desc` IS LEGISCAN'S CLAIM ABOUT THE QUESTION, NOT KENTUCKY'S,
  // AND IT IS WRONG. *** `House: Veto Override` appears 7 times and NOT ONE
  // of them is a veto override. Checked against Kentucky's own vote record
  // (see below), the seven are: Previous Question (SB 2 RCS# 308, HB 495
  // RCS# 304, HB 695 RCS# 306), Reconsider (SB 120 RCS# 283, SB 65
  // RCS# 333), Strike Enacting Clause (HB 398 RCS# 85) and a floor
  // amendment (HJR 15 RCS# 36). The REAL override votes are worded `Third
  // Reading` in both chambers, exactly like an ordinary passage. So this
  // config excludes `veto override` BY RULE — which also removes seven
  // procedural votes that would otherwise be stored as passages — and the
  // stored `questionClass` on every kept Kentucky roll is report metadata
  // only, never a claim to show a reader.
  //
  // GROUND TRUTH for what a roll decided is Kentucky's own vote record:
  // https://apps.legislature.ky.gov/record/25rs/<bill>/vote_history.pdf
  // — one PDF per bill giving every roll's RCS#/RSN#, the question in plain
  // words (`Final Passage`, `Reconsider`, `Previous Question`, `Override
  // Veto Final Passage`, `Strike enacting clause`), the date and time, and
  // the full member lists. Question AND version in one document, which is
  // better than Florida's equivalent. Verify every selected roll against it.
  //
  // Two more measured facts, recorded so a later session does not re-derive
  // them: 672 of the 701 roll tallies appear verbatim in their own bill's
  // history, and the 29 that do not are all procedural votes for which
  // Kentucky prints no tally — not a data defect. And the bill history's
  // `chamber` label is INVERTED on the override lines (HB 4's House
  // override, 79-19, is filed under S; its Senate override, 32-6, under H),
  // so a roll's chamber must be read from the roll, never from the history.
  KY: {
    jurisdiction: "KY",
    sessionId: 2179,
    chamberSizes: { house: 100, senate: 38 },
    keptQuestions: [
      // The House's only substantive floor wording, covering passage,
      // concurrence in the other chamber's changes, and veto override
      // alike — Kentucky does not distinguish them here, and the official
      // vote record is what tells the three apart.
      { pattern: /^house: third reading rcs# \d+$/, questionClass: "passage" },
      // A joint resolution is ADOPTED, not passed. Bare `Adopt` is the
      // resolution question; simple and concurrent resolutions carry it
      // too, but the shared kept-types list drops those before this
      // config is consulted, so only joint resolutions survive.
      { pattern: /^house: adopt rcs# \d+$/, questionClass: "passage" },
      // `Senate: Third Reading RSN# 3362`, and the same question with the
      // adopted committee substitute and floor amendments named:
      // `Senate: Third Reading W/scs1 sfa1 scta1 RSN# 3501`.
      { pattern: /^senate: third reading(?: w\/[a-z0-9 ]+?)? rsn# \d+$/, questionClass: "passage" },
    ],
    excludedQuestions: [
      // Floor and committee amendments, in both chambers: `House: Adopt
      // HFA 1`, `House: Adopt HCS 1`, `House: Adopt SCS 1`, `Senate:
      // Adopt SFA 5`. Checked before the bare `Adopt` kept pattern.
      /^(?:house|senate): adopt (?:hfa|hcs|hfta|scs|sfa|sfta|scta) ?\d+ (?:rcs|rsn)# \d+$/,
      // Scheduling and debate motions.
      /^house: suspend the rules rcs# \d+$/,
      /^house: table rcs# \d+$/,
      /^house: lay on the clerks desk rcs# \d+$/,
      // NOT a veto override — see the block comment above. Seven rolls,
      // every one of them a previous question, a reconsideration, a
      // motion to strike the enacting clause, or a floor amendment.
      /^house: veto override rcs# \d+$/,
    ],
  },

  // Kentucky General Assembly, 2026 Regular Session (the 60-day session;
  // adjourned April 2026, dataset dated 2026-07-12 and complete).
  // Vocabulary measured from its own full dataset survey 2026-08-31: 1,737
  // bills, 917 roll calls, 138 people. 917 raw descs fold to 19 families.
  //
  // *** THIS SESSION'S PATTERNS ARE NOT THE 2025 PATTERNS, AND THE
  // DIFFERENCE IS NOT COSMETIC. *** LegiScan's Kentucky desc vocabulary
  // FLIPS between sessions: in 2026 `House: Veto Override` is the House's
  // DOMINANT family at 415 rolls — its label for every substantive House
  // floor vote, passage and concurrence and genuine override alike — while
  // `House: Third Reading` falls to 29. Applying the 2025 entry's rules
  // here would drop 415 House votes and keep 29 duplicates. Verified
  // against Kentucky's own record: HB 398 RCS# 46 is `Pass`, RCS# 373 is
  // `Final Passage`, HB 2 RCS# 455 is `Veto Override` — all three arrive
  // under the single label `House: Veto Override`. Never carry a Kentucky
  // desc rule across sessions; survey each one.
  //
  // The 2026 feed also holds 31 DUPLICATE rolls that 2025 does not: the
  // same (chamber, sequence number) under two roll_call_ids with identical
  // bill, date and tally, each pair naming `House: Veto Override` plus one
  // of `House: Third Reading` (29), `House: Adopt HFA 1` (1) or `House:
  // Co-Sponsor` (1). The shared identity key includes `desc`, so it does
  // NOT collapse them. Excluding the three partner spellings resolves 29 of
  // the 31 by rule. Of the other two, the co-sponsor vote sits on a simple
  // resolution that the shared kept-types list drops before this config is
  // consulted, and the floor amendment (HB 84, RCS# 40) is excluded below by
  // its sequence number. Recorded in the evidence directory's
  // CODE-FINDINGS.md. 2025 has ZERO duplicates (701 distinct pairs).
  //
  // Ground truth is the same document as 2025 under the 26rs path:
  // https://apps.legislature.ky.gov/record/26rs/<bill>/vote_history.pdf
  "KY-2247": {
    jurisdiction: "KY",
    sessionId: 2247,
    chamberSizes: { house: 100, senate: 38 },
    keptQuestions: [
      // The House's substantive floor family for this session, despite the
      // name. `questionClass` stays report metadata: this label covers
      // passage, final passage and genuine overrides indiscriminately.
      { pattern: /^house: veto override rcs# \d+$/, questionClass: "passage" },
      { pattern: /^house: adopt rcs# \d+$/, questionClass: "passage" },
      { pattern: /^senate: third reading rsn# \d+$/, questionClass: "passage" },
      // The Senate, unlike the House, does name its override question in
      // this session: 30 of these 36 rolls sit on a bill whose history
      // records an override.
      { pattern: /^senate: veto override rsn# \d+$/, questionClass: "veto_override" },
      // Joint resolution adoption, with the adopted committee substitute
      // and amendments named: `Senate: Adopt W/ SCS 1, SCA 1 (T)`.
      { pattern: /^senate: adopt(?: w\/ [a-z0-9 ,()]+?)? rsn# \d+$/, questionClass: "passage" },
    ],
    excludedQuestions: [
      /^house: adopt (?:hfa|hcs|hfta|scs|sfa|sfta|scta) ?\d+ rcs# \d+$/,
      // The stale duplicate copy of an already-kept House roll — see the
      // block comment. In THIS session only; in 2025 it is the kept family.
      /^house: third reading rcs# \d+$/,
      // A vote to add co-sponsors, on a simple resolution.
      /^house: co-sponsor rcs# \d+$/,
      // HB 84's RCS# 40 is the ONE duplicate the spelling rules above cannot
      // reach: the feed carries it both as `House: Adopt HFA 1 RCS# 40`
      // (excluded) and as `House: Veto Override RCS# 40` (kept by the broad
      // rule), and Kentucky's own record says it is the adoption of House
      // Floor Amendment 1, 81-8, not a passage. Excluded by sequence number,
      // and pinned by a test, so a re-fetch cannot store it as a floor vote.
      /^house: veto override rcs# 40$/,
    ],
  },

  // Indiana General Assembly, 2025 Regular Session (124th General Assembly,
  // first regular session, sine die 2025-04-25; the long budget session).
  // Vocabulary measured from the full dataset survey 2026-08-31: 1,489
  // bills, 1,010 roll calls, 151 people (100 House + 50 Senate seats plus
  // one mid-session replacement).
  //
  // What the survey established:
  // - Every desc is prefixed with the chamber and a dash (`House - Third
  //   reading`), so every pattern here carries that prefix. The chamber
  //   word in the prefix is the voting chamber; a bill's own chamber of
  //   origin never appears there. 155 raw descriptions fold to 21 families
  //   once the amendment sponsor names and numbers are folded away.
  // - The dataset holds NO committee votes at all: every roll's total is
  //   exactly 100 (House) or 50 (Senate), so nothing lands in the
  //   committee-tally or small-tally buckets.
  // - Amendment descs name their sponsor (`House - Amendment #1 (Burton)
  //   failed`), which is why the exclusion matches the `amendment #<n> `
  //   stem rather than enumerating 155 spellings.
  // - Constitutional amendments ride JOINT RESOLUTIONS (`SJR0017`), which
  //   LegiScan types `JR` — already a kept type, so Georgia's
  //   resolution-typed amendment gap does not recur here.
  // - `Rules Suspended.` is a scheduling prefix Indiana prints on
  //   end-of-session concurrences and conference reports. It does not
  //   change the question, so both kept patterns tolerate it.
  //
  // TWO HAZARDS, both recorded in
  // evidence/rollcall/legiscan-in-2143/CODE-FINDINGS.md:
  // (1) Ten House rolls carry a BLANK question (the desc is the literal
  //     `House -` with nothing after the dash). Resolved against the bill
  //     histories, they are five third readings, two concurrences, one
  //     failed amendment and two appeals of the chair's ruling — so the
  //     question genuinely is not in the desc and no pattern can recover
  //     it. They stay unmatched and surface for a human, which costs two
  //     divided kept votes (SB0178's 74-20 third reading and HB1460's
  //     59-18 concurrence).
  // (2) LegiScan's Indiana member lists disagree with the official journal
  //     on 30 of the 1,010 rolls. Verified against the state's own
  //     roll-call PDF on HB1155 roll 83: LegiScan records Rep. Jim Lucas
  //     as a yea, the official roll call records him as a nay (LegiScan
  //     89-2, journal 88-3). Every roll selected for a batch must
  //     therefore have its member list checked against
  //     iga.in.gov/pdf-documents/124/2025/<chamber>/bills/<BILL>/rollcalls/<BILL>.<n>_<H|S>.pdf
  //     before it is judged.
  IN: {
    jurisdiction: "IN",
    sessionId: 2143,
    chamberSizes: { house: 100, senate: 50 },
    keptQuestions: [
      // Indiana's final passage question in both chambers.
      { pattern: /^(?:house|senate) - third reading$/, questionClass: "passage" },
      // `House - House concurred with Senate amendments`, and the one
      // end-of-session spelling that carries the scheduling prefix.
      {
        pattern: /^(?:house|senate) - (?:rules suspended\. )?(?:house|senate) concurred with (?:house|senate) amendments$/,
        questionClass: "concurrence",
      },
      // A concurrence motion that drew a majority of those voting but not
      // the 26 votes an Indiana Senate measure needs to pass. It is a
      // recorded vote on the measure, so it is kept, not excluded.
      {
        pattern: /^(?:house|senate) - concurrence failed for lack of constitutional majority$/,
        questionClass: "concurrence",
      },
      // `Senate - Conference Committee Report 1`, `House - Rules
      // Suspended. Conference Committee Report 2`.
      {
        pattern: /^(?:house|senate) - (?:rules suspended\. )?conference committee report \d+$/,
        questionClass: "conference_report",
      },
    ],
    excludedQuestions: [
      // Floor amendments, both outcomes (`failed`, `prevailed`).
      /^(?:house|senate) - amendment #\d+ /,
      // A vote on whether the presiding officer's ruling stands, not on
      // the measure.
      /^(?:house|senate) - appeal the ruling of the chair/,
      // Second reading is Indiana's amend-and-engross stage.
      /^(?:house|senate) - second reading$/,
      // `House - Referred to Committee on Education pursuant to House
      // Rule 126.4` — a motion to send a bill back to committee.
      /^(?:house|senate) - referred to committee on /,
    ],
  },

  // Indiana General Assembly, 2026 Regular Session (LegiScan session 2234).
  // The 124th General Assembly's short session: it convened 2025-11-18 and
  // adjourned sine die 2026-03-12, so the session is closed and the dataset
  // is final. Vocabulary measured from the full dataset survey on 2026-09-02:
  // 935 bills, 689 roll calls, 152 people, 131 distinct descriptions. The
  // survey is written up in
  // backend/evidence/rollcall/legiscan-in-2234/README.md.
  //
  // Indiana needs a SECOND entry rather than a new sessionId on `IN` because
  // the 2025 batches must stay re-runnable. Records still land under
  // `jurisdiction: "IN"`, and nothing collides: evidence filenames carry the
  // session, and a legislative_votes row is keyed by jurisdiction, chamber,
  // session and roll.
  //
  // The feed is shaped exactly like 2025 and, unlike Kentucky, the kept
  // vocabulary does carry across: `Third reading` is still final passage in
  // both chambers, `<chamber> concurred with <chamber> amendments` is still
  // the second chamber's agreement, `Conference Committee Report <n>` is
  // still numbered, and `Rules Suspended. ` is still a scheduling prefix that
  // does not change the question. There are again no committee votes at all —
  // every total is 100 in the House and 49 or 50 in the Senate.
  //
  // What DID change is the procedural vocabulary, and one change is a trap.
  //
  // *** `House - Concurrence defeated` IS EXCLUDED, NOT KEPT. *** 2025 spelt
  // a failed concurrence `Concurrence failed for lack of constitutional
  // majority` and that spelling is kept above, because it is a recorded vote
  // on the measure. 2026's one occurrence looks like the same question under
  // a shorter name and is not safe to treat that way: on HB 1368 roll 399
  // (2026-02-26) the House DEFEATED the motion 48-42 — an Indiana concurrence
  // needs a constitutional majority of 51 — but LegiScan sets `passed: 1`,
  // because its flag is a bare-majority check. fetchLegiscanRollCallVotes
  // writes `result` straight from that flag, so keeping this desc would store
  // a defeated vote as "Passed". It is the ONLY roll in the session whose
  // flag disagrees with the constitutional-majority rule; the 2025
  // counterpart carries `passed: 0` and stored correctly. Nothing is lost by
  // excluding it: a defeated concurrence can never be the final action on a
  // bill that became law, and this one was superseded the next day by roll
  // 420, which concurred 57-40 and is kept. Same defect class as Montana's
  // eight two-thirds rolls — never trust LegiScan's `passed` flag against a
  // chamber's own majority rule.
  //
  // The other four new families are all procedural and all excluded. Written
  // against the classification measured on 2026-09-02, which reconciles
  // exactly: 689 dataset rolls = 536 floor + 139 excluded question + 12 on
  // excluded measure types (11 CR, 1 R) + 2 surfaced.
  // - `Committee report`, `Rules Suspended. Committee report, adopted` — the
  //   full chamber voting to accept a committee's recommendation. It is a
  //   pre-passage stage like second reading, and each of the three rolls sits
  //   on a bill whose own passage vote is kept separately.
  // - `Motion to postpone indefinitely, failed` — a motion to kill the bill.
  // - `Recommitted to Committee on ... pursuant to House Rule 126.4` — 2026's
  //   spelling of the 2025 `Referred to committee on ` exclusion, a motion to
  //   send the bill back to committee. Both verbs are covered below.
  // - `First reading` — DEFENSIVE ONLY. The session's one occurrence sits on
  //   SCR 1, a concurrent resolution, so the shared kept-types list drops it
  //   before this config is consulted and the pattern never fires today. It
  //   is written down because for a bill Indiana's first reading is a
  //   referral with no vote, so a bill-typed roll under this desc would be
  //   procedural, not passage.
  //
  // TWO HAZARDS carry over from 2025, both in
  // evidence/rollcall/legiscan-in-2143/CODE-FINDINGS.md:
  // (1) The blank-question defect recurs. Two House rolls carry the literal
  //     desc `House -` with nothing after the dash (HB 1002 and SB 0076).
  //     They stay unmatched and surface for a human, exactly as in 2025.
  // (2) LegiScan's Indiana member lists still disagree with the official
  //     journal. Five of the worklist's 95 divided-and-enacted rolls report a
  //     tally with no exact match in the bill history, and HB 1032's `House -
  //     Committee report` is a sixth (LegiScan 63-23, journal 63-24). All five
  //     were then checked against the official PDF and ALL FIVE disagree, each
  //     with a member on the wrong side. Every roll selected for a batch must
  //     be checked name by name against
  //     iga.in.gov/pdf-documents/124/2026/<chamber>/bills/<BILL>/rollcalls/<BILL>.<n>_<H|S>.pdf
  //     before it is judged. Note the `2026` path segment.
  "IN-2234": {
    jurisdiction: "IN",
    sessionId: 2234,
    chamberSizes: { house: 100, senate: 50 },
    keptQuestions: [
      // Final passage in both chambers: 196 House rolls and 200 Senate.
      { pattern: /^(?:house|senate) - third reading$/, questionClass: "passage" },
      // The second chamber's agreement, with and without the scheduling
      // prefix: 45 + 39 + 1 + 1 rolls.
      {
        pattern: /^(?:house|senate) - (?:rules suspended\. )?(?:house|senate) concurred with (?:house|senate) amendments$/,
        questionClass: "concurrence",
      },
      // 26 + 16 + 11 + 1 rolls. Every conference report in this session is
      // numbered 1.
      {
        pattern: /^(?:house|senate) - (?:rules suspended\. )?conference committee report \d+$/,
        questionClass: "conference_report",
      },
    ],
    excludedQuestions: [
      // Floor amendments, both outcomes (`failed`, `prevailed`).
      /^(?:house|senate) - amendment #\d+ /,
      // A vote on whether the presiding officer's ruling stands.
      /^(?:house|senate) - appeal the ruling of the chair/,
      // Indiana's amend-and-engross stage.
      /^(?:house|senate) - second reading$/,
      // 2025 said `referred`, 2026 says `recommitted`; both are a motion to
      // send the measure back to committee, and both carry the rule citation.
      /^(?:house|senate) - (?:referred|recommitted) to committee on /,
      // The full chamber accepting a committee's recommendation, a
      // pre-passage stage. Covers the bare and scheduling-prefixed spellings.
      /^(?:house|senate) - (?:rules suspended\. )?committee report(?:, adopted)?$/,
      // A motion to kill the measure.
      /^(?:house|senate) - motion to postpone indefinitely/,
      // Referral for a bill, adoption for a resolution; never passage of a
      // measure that can become law.
      /^(?:house|senate) - first reading$/,
      // See the block comment: LegiScan's `passed` flag is wrong on this
      // roll, so the vote must not be stored from this feed.
      /^(?:house|senate) - concurrence defeated$/,
    ],
  },

  // Montana Legislature, 2025 Regular Session (convened January 6, adjourned
  // sine die April 30 2025). Montana's legislature meets only in odd years,
  // so this one closed session is the entire dataset available to the
  // November 2026 campaign. Vocabulary measured from the full dataset survey
  // on 2026-08-31: 1,761 bills, 9,209 roll calls, 151 people, and 266
  // distinct descriptions.
  //
  // Montana separates floor votes from committee votes more cleanly than any
  // other state in this registry. Every description opens with the chamber in
  // parentheses, `(H) ` or `(S) `. Every committee description names the
  // committee and its question joined by a double hyphen — `(H) Judiciary--Do
  // Pass`, `(S) Finance and Claims--Be Concurred In` — and no committee roll
  // reports a total above 23. No floor description contains a double hyphen,
  // and every floor roll reports the whole chamber (House 100, Senate 50),
  // because Montana counts absent and excused members in `total`. The
  // floor-versus-committee tally check therefore rejects every committee roll
  // before the review queue, and none of the patterns below has to name a
  // committee.
  //
  // Montana votes each measure twice on the floor. Second reading is the
  // committee of the whole, where floor amendments are taken; third reading
  // is final passage. Only third reading is kept, the same call Texas,
  // California and Missouri made about their own pre-passage floor stages.
  // `Concurred` is Montana's word for the SECOND chamber acting on the other
  // chamber's measure, so `3rd Reading Concurred` is that chamber's own
  // passage vote. The originating chamber's later vote on the text the other
  // chamber amended is worded `3rd Reading Passed as Amended by Senate` (or
  // `by House`).
  //
  // Data notes (both written up in
  // backend/evidence/rollcall/legiscan-mt-2159/CODE-FINDINGS.md):
  // - 42 roll calls in this dataset fail to parse because their reported
  //   tallies are multiples of their own member lists (one claims 500 votes
  //   in a 100-seat chamber). ALL 42 are committee rolls, so none could ever
  //   be queued; the fetch run reports them and exits non-zero. That exit
  //   code is a signal, not a rollback — every valid roll is stored and the
  //   import is unaffected.
  // - LegiScan's `passed` flag is a bare majority check, so on the eight
  //   rolls where a constitutional-amendment bill won a majority but missed
  //   Montana's two-thirds requirement (HB 316, HB 821, HB 822, HB 921,
  //   SB 185) the stored `result` says Passed while the desc — Montana's own
  //   words — says Failed. The desc is right. `result` mirrors LegiScan's
  //   claim; treat it like Florida's question fields and never trust it in a
  //   judgment — the official action trail is the ground truth.
  MT: {
    jurisdiction: "MT",
    sessionId: 2159,
    chamberSizes: { house: 100, senate: 50 },
    keptQuestions: [
      // Final passage where the measure started — `(H) 3rd Reading Passed`
      // (694 rolls), `(S) 3rd Reading Passed` (398) — and final passage in
      // the second chamber, `(S) 3rd Reading Concurred` (588),
      // `(H) 3rd Reading Concurred` (306).
      { pattern: /^\([hs]\) 3rd reading (passed|concurred)$/, questionClass: "passage" },
      // The same question, lost: `3rd Reading Failed` (15 House, 23 Senate)
      // and `3rd Reading Failed; 2nd House Vote Required` (6). Kept so the
      // audit trail and the superseded-stage gate see a chamber's whole
      // third-reading record, not only the votes that carried.
      { pattern: /^\([hs]\) 3rd reading failed(?:; 2nd house vote required)?$/, questionClass: "passage" },
      // The originating chamber voting on the text the other chamber
      // amended: `(H) 3rd Reading Passed as Amended by Senate` (146),
      // `(S) 3rd Reading Passed as Amended by House` (86), and the three
      // House rolls that refused it.
      {
        pattern: /^\([hs]\) 3rd reading (passed|not passed) as amended by (senate|house)$/,
        questionClass: "concurrence",
      },
      // `3rd Reading Conference Committee Report Adopted` (9 House, 9
      // Senate), the free-conference spelling (6 each), and the one Senate
      // rejection.
      {
        pattern: /^\([hs]\) 3rd reading (free )?conference committee report (adopted|rejected)$/,
        questionClass: "conference_report",
      },
      // Montana lets the governor return a measure with recommended
      // amendments; the chamber then votes on adopting them, which is a vote
      // on the text that becomes law (11 rolls in each chamber).
      { pattern: /^\([hs]\) 3rd reading governor's proposed amendments adopted$/, questionClass: "concurrence" },
    ],
    excludedQuestions: [
      // Second reading is the committee of the whole — the amendment stage,
      // not final passage. One pattern covers `2nd Reading Passed`,
      // `2nd Reading Concurred`, `2nd Reading Motion to Amend Carried` and
      // `Failed`, `2nd Reading Indefinitely Postponed`, the
      // amendment-concurrence spellings, and the conference-report and
      // governor's-amendment votes taken at second reading.
      /^\([hs]\) 2nd reading\b/,
      // Scheduling and reconsideration motions: `Motion Failed`, `Motion
      // Carried`, `Motion to Reconsider Failed`, `Taken from Committee;
      // Placed on 2nd Reading`, `Reconsidered Previous Action; Placed on 2nd
      // Reading`, and `Reconsidered Previous Action; Remains in 3rd Reading
      // Process`.
      /^\([hs]\) motion\b/,
      /^\([hs]\) taken from committee\b/,
      /^\([hs]\) reconsidered previous action\b/,
      // Simple resolutions are adopted under their own wording. All but one
      // are LegiScan type R, which never reaches this config; the exception
      // is one House joint resolution adopted 100-0.
      /^\([hs]\) resolution (adopted|failed)$/,
    ],
  },

  // North Carolina General Assembly, 2025-2026 Regular Session (both years in
  // one dataset; the session was still sitting when the dataset was cut on
  // 2026-08-30). Vocabulary measured from the full dataset survey 2026-08-31:
  // 2,338 bills, 1,493 roll calls, 180 people (120 House + 50 Senate seats
  // plus mid-biennium turnover).
  //
  // What the survey established:
  // - North Carolina takes its recorded floor vote on SECOND READING. A bill
  //   needs three readings; the roll call is called on the second, and the
  //   third reading passes without a roll call unless a member objects. So
  //   `Second Reading` (415 House / 253 Senate) is the passage question and
  //   `Third Reading` (32 / 36) is the same question taken again on the days
  //   somebody objected. Both are kept as passage; the campaign's
  //   one-roll-per-chamber rule and the judge's own final-vote gate pick the
  //   later of the pair.
  // - THE QUESTION CAN BE A SUFFIX, so every pattern is anchored at both
  //   ends and the exclusions run first. `A1 Blackwell Second Reading` is a
  //   floor amendment, `Second Reading M4 Previous Question` is the motion to
  //   cut off debate, and `Second Reading Motion 1 To Table` is a motion to
  //   kill — all three carry the passage wording and none of them is passage.
  // - The two chambers word the same questions differently. Concurrence in
  //   the other chamber's changes is `M11 Concur` in the House and `Motion 9
  //   To Concur` in the Senate; conference reports are `C RPT Adoption`
  //   (House) and `Conference Report Motion 8 To Adopt` plus the readings on
  //   the report (Senate).
  // - VETO OVERRIDES ARE A REAL POOL HERE, not an edge case. North Carolina
  //   has a Republican legislature and a Democratic governor, so 26 of the
  //   kept rolls are override votes on 14 bills — `Veto Override` in the
  //   House, `Motion 11 Veto Override` in the Senate. The threshold is three
  //   fifths of the members present, and an override that carries enacts the
  //   bill over the veto (nothing forbids a unanimous override; whether a
  //   roll is divided is measured per roll, never assumed). The House also
  //   prints `Veto Override M4 Previous Question`, which is the debate-cutoff
  //   motion taken during an override debate and is excluded.
  // - The House prefixes a question with `R2 Ruled Mat&#x27;l` / `R3 Ruled
  //   Mat&#x27;l` when the presiding officer has ruled the matter material
  //   under House Rule 2 or 3 (the vote is still on the concurrence or the
  //   conference report, and the tallies are whole-chamber). LegiScan leaves
  //   the apostrophe HTML-escaped, so the pattern matches the escape.
  // - Feed health is in the cleanest tier: 0 repeated roll_call_ids, 0
  //   identity-duplicate rolls, 0 summary-only rolls (every roll carries a
  //   member list), 0 INTERNAL tally mismatches (each roll's printed counts
  //   match its own member list — a feed-consistency check, not a check
  //   against the official journal), 0 committee votes, 0 file or parse
  //   errors, and NOTHING left surfaced — all 1,493 rolls match a kept or an
  //   excluded pattern, or sit on an excluded instrument type.
  // - Internal consistency is NOT official accuracy: on the three House
  //   override rolls of 2026-06-24 (1711513, 1711515, 1711527) LegiScan
  //   drops two unaffiliated members and prints 71-46 where the official
  //   transcripts (RCS 738, 740, 736) record 71-47. Those rolls are held out
  //   of import until the importer can cite an official tally over the feed;
  //   see backend/evidence/rollcall/legiscan-nc-2189/CODE-FINDINGS.md.
  // - Only bill types B (2,284), JR (23) and R (31) appear. North Carolina
  //   proposes constitutional amendments as ordinary BILLS, so Georgia's
  //   resolution-typed-amendment gap does not recur; the 31 resolutions are
  //   dropped by the shared kept-types list before this config is read.
  NC: {
    jurisdiction: "NC",
    sessionId: 2189,
    chamberSizes: { house: 120, senate: 50 },
    keptQuestions: [
      // Passage. Anchored: the amendment, previous-question and table
      // families all end in these same two words.
      { pattern: /^(?:second|third) reading$/, questionClass: "passage" },
      // House concurrence: `M11 Concur`, `M11 Concur Sen. Amd. 1`, and the
      // same question under a materiality ruling. `M11 Not Concur` is
      // excluded below — refusing to concur is not passage.
      { pattern: /^(?:r[23] ruled mat&#x27;l )?m11 concur(?: sen\. amd\. \d+)?$/, questionClass: "concurrence" },
      // Senate concurrence, with or without a reading named in front of it.
      { pattern: /^(?:(?:second|third) reading )?motion 9 to concur(?: house amend)?$/, questionClass: "concurrence" },
      // House conference report, in its three orderings.
      {
        pattern: /^(?:r[23] ruled mat&#x27;l )?c rpt adoption(?: r2 ruled mat&#x27;l)?$/,
        questionClass: "conference_report",
      },
      // Senate conference report: the motion to adopt it, and the readings
      // the Senate then takes on the report itself.
      { pattern: /^conference (?:report|rpt) motion 8 to adopt$/, questionClass: "conference_report" },
      { pattern: /^conference rpt (?:second|third) reading$/, questionClass: "conference_report" },
      // The override votes, one spelling per chamber.
      { pattern: /^veto override$/, questionClass: "veto_override" },
      { pattern: /^motion 11 veto override$/, questionClass: "veto_override" },
    ],
    excludedQuestions: [
      // House floor amendments: `A1 Blackwell Second Reading`, `A34 Prather
      // Second Reading`, `A5 Brown, G. Third Reading` — the amendment number
      // and its sponsor lead, the reading trails.
      /^a\d+ .*\b(?:second|third) reading\b/,
      // Senate floor amendments and the motions to table them:
      // `Amendment 3`, `Amendment 3 Motion 1 To Table`.
      /^amendment \d+/,
      // Debate and scheduling motions, wherever they appear in the desc:
      // `M4 Previous Question`, `Motion 3 Previous Question`, `M3 To Lay On
      // The Table`, `Motion 1 To Table`, `M6 Reconsider`, `M8 Re-Refer
      // Appropriations`, `Motion 12/Divide`, `Motion 11 To Adjourn`.
      /\bprevious question\b/,
      /\bto lay on the table\b/,
      /^(?:second reading )?motion 1 to table\b/,
      /\breconsider\b/,
      /\bre-refer\b/,
      /\bmotion 12\//,
      /^motion 11 (?:to adjourn|to substitute|divide question)$/,
      /^motion 1 to table motion 11 to postpone$/,
      // Refusing to concur sends the bill to conference; it is a real vote
      // but not a vote on passing the measure.
      /\bnot concur\b/,
    ],
  },

  // Alabama Legislature, 2025 Regular Session (Feb 4 - May 14 2025).
  // Alabama sits in ANNUAL sessions, so each later one is a separate
  // LegiScan session with its own compound key (the MO/MD pattern): the
  // 2026 Regular Session is `AL-2218` and the 2026 First Special Session
  // is `AL-2262`, both registered below and both sharing this state's one
  // vocabulary definition.
  // Vocabulary measured from the full dataset survey 2026-08-31: 1,449
  // bills, 2,851 roll calls, 139 people (105 House + 35 Senate seats).
  //
  // What the survey established:
  // - 1,439 of the 2,851 descs carry a trailing ` - Roll Call <n>` —
  //   every passage desc does, but the BIR / `Third Reading` captions and
  //   a few concurrences do not. The suffix is sometimes followed by an
  //   amendment code or a bill-page URL, so no kept pattern anchors at the
  //   end. Passage descs also carry an OPTIONAL sponsor-name prefix (SB 54
  //   roll 164 is `Roberts motion to Read a Third Time and Pass as
  //   Amended`), so the passage pattern does not anchor at the start
  //   either. 1,453 raw desc rows fold to 513 families.
  // - THE BUDGET ISOLATION RESOLUTION IS PRINTED TWICE, AND ITS SECOND
  //   CAPTION LOOKS LIKE PASSAGE. Alabama's constitution bars most bills
  //   from being taken up before the budgets pass unless the chamber first
  //   adopts a Budget Isolation Resolution by a three-fifths vote. LegiScan
  //   files that one vote as TWO roll calls: `HBIR:`/`SBIR: Passed by House
  //   of Origin|Second House` AND `Third Reading in House of Origin|Second
  //   House`. All 698 pairs in this session are identical in tally AND in
  //   member list (1 lone BIR, 0 lone `Third Reading`), so the
  //   `Third Reading ...` caption is never a vote on the bill and BOTH
  //   families are excluded. The vote that passes an Alabama bill is
  //   `Motion to Read a Third Time and Pass[ as Amended]`.
  // - Concurrence in the other chamber's amendments is `<sponsor> Concur In
  //   and Adopt`, `Concur In and Senate Amendment` or `Senate Concurs In
  //   House Amendment`; the session holds exactly one conference-report
  //   vote (`Concur In and Adopt Conference Committee Report`), which is
  //   listed before the concurrence rule so it keeps its own class.
  // - `<sponsor> motion to Adopt` is AMENDMENT adoption (417 of 422 sit on
  //   bills), so the family is excluded. The 5 that sit on joint
  //   resolutions are ceremonial commendations, every one unanimous, so
  //   nothing the campaign would judge is lost.
  // - LOCAL ACTS ARE NOT FILTERED HERE, AND NO TALLY RULE CATCHES THEM.
  //   Alabama passes county bills on the votes of that county's delegation
  //   alone, but the roll still lists the whole chamber with everyone else
  //   recorded as not voting, so `total` is floor-sized and the row is
  //   stored as a floor vote (SB 314, Shelby County: 10-3 with 90 not
  //   voting, total 103). They are 175 of the 917 kept rolls but only 2 of
  //   the 34 divided ones, and selection drops them on the
  //   nameable-subject filter, not here.
  // - The dataset carries NO committee votes: every whole-chamber tally is
  //   34-35 (Senate) or 103-105 (House). Feed health is the cleanest tier —
  //   0 repeated roll_call_ids, 0 summary-only rolls, 0 tally mismatches.
  // - Alabama proposes CONSTITUTIONAL AMENDMENTS as ordinary bills (the
  //   dataset holds only types B, JR and R), so Georgia's resolution-typed
  //   amendment gap does not recur here.
  AL: {
    jurisdiction: "AL",
    sessionId: 2148,
    chamberSizes: { house: 105, senate: 35 },
    keptQuestions: ALABAMA_KEPT_QUESTIONS,
    excludedQuestions: ALABAMA_EXCLUDED_QUESTIONS,
  },

  // Alabama Legislature, 2026 Regular Session — a separate LegiScan session
  // from 2025 because Alabama sits in annual regular sessions. Vocabulary
  // measured from the full dataset survey 2026-09-01: 1,531 bills, 3,541
  // roll calls, 140 people. It classifies with the shared Alabama
  // vocabulary above and NOTHING left unmatched once the two 2026-only
  // spellings were added (the hyphenated executive-amendment concurrence and
  // the failed Budget Isolation Resolution's `Lost in House of Origin`).
  //
  // Feed health matches 2025's, the cleanest tier: 0 repeated roll call ids,
  // 0 summary-only rolls, 0 tally mismatches, and no roll call id collides
  // with the 2025 session. 1,080 kept floor votes, of which 27 are divided
  // and 18 of those are on measures that became law.
  //
  // The people file overlaps 2025 by 135 of 140, so the 2025 crosswalk
  // carries over and only the five new members needed review.
  "AL-2218": {
    jurisdiction: "AL",
    sessionId: 2218,
    chamberSizes: { house: 105, senate: 35 },
    keptQuestions: ALABAMA_KEPT_QUESTIONS,
    excludedQuestions: ALABAMA_EXCLUDED_QUESTIONS,
  },

  // Alabama Legislature, 2026 First Special Session (May 4 - May 12 2026),
  // called to redraw districts. Tiny and entirely conventional: 9 bills, 9
  // roll calls, and a people file identical to the 2026 regular session's,
  // so that session's crosswalk covers it unchanged and no roll call id
  // collides with either regular session. Surveyed 2026-09-02 — every
  // description matches a kept or excluded pattern already defined above,
  // with NOTHING left unmatched, which is why this entry adds no rules of
  // its own.
  //
  // Only 3 of the 9 rolls are floor votes on a measure, and all three are
  // divided and on measures that became law: the House and Senate votes on
  // HB 1 and the Senate vote on SB 1, both authorising special primary
  // elections after redistricting. The other six are amendment adoptions
  // and previous-question motions, all correctly excluded.
  //
  // ⚠ FEED GAP: SB 1's HOUSE passage vote is missing from the dataset. The
  // bill history records `Motion to Read a Third Time and Pass - Adopted
  // Roll Call 4` in the House on 2026-05-08, but no such roll call exists
  // in the vote files; what the dataset holds for that chamber and day is
  // the previous-question motion (Roll Call 3). Missouri's feed had the
  // same shape. Nothing here can recover the missing vote, so SB 1 is
  // represented by its Senate vote only.
  "AL-2262": {
    jurisdiction: "AL",
    sessionId: 2262,
    chamberSizes: { house: 105, senate: 35 },
    keptQuestions: ALABAMA_KEPT_QUESTIONS,
    excludedQuestions: ALABAMA_EXCLUDED_QUESTIONS,
  },

  // Alaska Legislature, 34th Legislature 2025-2026 Regular Session (both
  // years in one dataset; still sitting when the dataset was cut on
  // 2026-08-30). Vocabulary measured from the full dataset survey on
  // 2026-09-03: 848 bills, 1,068 roll calls, 85 people (40 House seats and
  // 20 Senate seats, plus turnover across the two years).
  //
  // What the survey established:
  // - EVERY roll call in the dataset is a whole-chamber vote. `total` is 40
  //   on every House roll and 20 on every Senate roll, with three
  //   exceptions, all veto overrides (below). There are no committee votes
  //   in the feed at all, so the tally cut never has to separate them.
  // - Every desc is `House: ` or `Senate: ` followed by the question, so
  //   the chamber word is in the desc as well as the `chamber` field.
  // - Alaska annotates a passage desc with the OTHER questions the journal
  //   settled at the same time, so `Senate: Third Reading - Final Passage
  //   Effective Date(s)` (91 rolls) is the PASSAGE vote, not a vote on the
  //   effective date. The bill history proves it: the roll's tally matches
  //   the journal's `PASSED Y18 N2` line and the effective-date line right
  //   below it reads `EFFECTIVE DATE(S) SAME AS PASSAGE` with no separate
  //   roll. A genuine effective-date vote wears the question at the FRONT
  //   (`House: Third Reading Effective Date`, `Senate: Effective Date
  //   Clause(s)`), which is why every exclusion here is anchored at the
  //   start of the desc and the annotations are allowed to trail.
  // - Alaska's bill history carries the tally on its own action lines
  //   (`PASSED Y18 N2`, `AM NO 1 ADOPTED Y15 N5`, `CONCUR AM OF (H) Y15
  //   N5`), so the dataset itself holds the oracle for what question a roll
  //   asked. That is what mapped each desc family below to a journal
  //   action.
  //
  // VETO OVERRIDES ARE EXCLUDED BY RULE, and this is the Alaska-specific
  // decision (written up in
  // backend/evidence/rollcall/legiscan-ak-2171/CODE-FINDINGS.md). Alaska
  // overrides a veto in a JOINT SESSION of both chambers, not chamber by
  // chamber. The feed holds five override rolls and none of them can be
  // stored honestly:
  // - Two are the joint session itself, with 60 members (44 House + 16
  //   Senate) filed under `chamber: "H"`. Storing one would record a vote
  //   of the whole legislature as a House floor vote, and it is the only
  //   place in the feed where `total` exceeds the chamber size.
  // - The other three report tallies that do not match the joint-session
  //   result the bill history prints. HB 314's `House: Veto Override` says
  //   29-11 where the history says `GOVERNOR VETO OVERRIDDEN Y45 N15`.
  // The passage and concurrence rolls on those same bills are unaffected
  // and stay in the pool.
  AK: {
    jurisdiction: "AK",
    sessionId: 2171,
    chamberSizes: { house: 40, senate: 20 },
    keptQuestions: [
      // Final passage, however the chamber reached it: `House: Third
      // Reading Final Passage` (158), `Senate: Third Reading - Final
      // Passage Effective Date(s)` (91), `Senate: Third Reading - Final
      // Passage` (72), `Senate: Final Passage` (50), `House: Second Reading
      // Final Passage Special Order of Business` (30), `House: Third
      // Reading Final Passage Reconsideration` (18, the same question taken
      // again the next day), and the court-rule and budget-reserve
      // annotations. Alaska can pass a bill in second reading after the
      // chamber advances it by special order, so the reading number is
      // optional here.
      {
        pattern: /^(?:house|senate): (?:(?:second|third) reading\s*-?\s*)?final passage\b/,
        questionClass: "passage",
      },
      // Passage taken up again on reconsideration, worded without the
      // `Final Passage` phrase: `Senate: Third Reading - On
      // Reconsideration` and its effective-date and budget-reserve
      // annotations (7 rolls). The journal calls these `PASSED ON
      // RECONSIDERATION`.
      { pattern: /^(?:house|senate): third reading\s*-\s*on reconsideration\b/, questionClass: "concurrence" },
      // The House votes on the Senate's amendments under the bare word
      // `Concur` (52 rolls; the journal line is `CONCUR AM OF (S)`, or
      // `FAILED CONCUR (S) AM` when it loses).
      { pattern: /^house: concur$/, questionClass: "concurrence" },
      // The Senate spells the same question out and names the bill inside
      // the desc, so this pattern is not anchored at the end: `Senate:
      // Shall the Senate Concur in the House Amendment(s) to CSSB 200(RES)
      // am Effective Date(s)` (29 rolls across as many spellings).
      {
        pattern: /^(?:house|senate): shall the (?:senate|house) concur in the (?:house|senate) amendment/,
        questionClass: "concurrence",
      },
      // Conference reports: `Senate: Shall the Senate Adopt the Conference
      // Committee Report` and its annotated spellings (6), and the House's
      // bare `Adopt` (7), which the journal records as `CC REPORT ADOPTED`.
      {
        pattern: /^(?:house|senate): shall the (?:senate|house) adopt the conference committee report/,
        questionClass: "conference_report",
      },
      { pattern: /^house: adopt$/, questionClass: "conference_report" },
    ],
    excludedQuestions: [
      // Floor amendments and the motions around them, all taken in second
      // reading: `Second Reading Amendment No. 1` (38) down to `Amendment
      // No. 67`, `Second Reading Amendment No. 1 to Amendment No. 2`,
      // `Second Reading Adopt Finance HCS`, `Second Reading Move to bottom
      // of the calendar`, `Second Reading Withdraw Amendment No. 1`, and
      // the third-reading amendment votes.
      /^(?:house|senate): (?:second|third) reading (?:amendment|amend amendment|rescind|withdraw amendment|adopt|move|advance|table)\b/,
      /^(?:house|senate): second reading(?: -)? final passage reconsideration$/,
      /^(?:house|senate): second reading$/,
      /^(?:house|senate): second reading\\/,
      /^(?:house|senate): third reading (?:return to|take up reconsideration|amendment)\b/,
      /^(?:house|senate): take up reconsideration\b/,
      /^(?:house|senate): rescind previous action\b/,
      // The effective-date clause is its own question in Alaska: a bill
      // whose effective date falls sooner than the default needs a separate
      // two-thirds vote. `House: Third Reading Effective Date` (17),
      // `Senate: Effective Date Clause(s)` (9), `House: Effective Date
      // Concur` (8), `House: Adopt Effective Date` (3). These are votes on
      // when the law starts, not on the law, so they are not the member's
      // position on the measure.
      /^(?:house|senate): (?:third reading )?effective date\b/,
      /^(?:house|senate): adopt effective date$/,
      // Appropriations from the Constitutional Budget Reserve need a
      // three-quarters vote and are taken separately from passage:
      // `Senate: Adopt Budget Reserve Fund Section(s)` (4) and its
      // constitutional spellings, `House: Constitutional Budget Reserve
      // Appropriations Concur`.
      /^(?:house|senate): (?:adopt |shall the (?:senate|house) adopt the )(?:constitutional )?budget reserve\b/,
      /^house: constitutional budget reserve appropriations concur$/,
      // Veto overrides — see the note above this entry. `House: Veto
      // Override` (2), `House: Veto Override SENATE` (1), `Senate: Veto
      // Override` (1), and one Senate roll that opens with a list of member
      // names before the words `SENATE SB 183 Veto Override`.
      /^(?:house|senate): veto override\b/,
      /\bveto override$/,
      // Points of order and appeals of the chair's ruling.
      /^(?:house|senate): point of order/,
      /sustain ruling of the chair/,
      // Scheduling and referral motions.
      /^(?:house|senate): (?:bill to calendar|return to rules|adjourn|refer to|withdraw bill|suspend uniform|discharge|advance from|amendment deadline|set amendment deadline)/,
    ],
  },

  // South Carolina General Assembly, 126th (2025-2026 Regular Session; both
  // years sit in one dataset). Vocabulary measured from the full survey on
  // 2026-09-02: 4,032 bills, 2,054 roll calls, 185 people (124 House + 46
  // Senate seats plus mid-term turnover).
  //
  // What the survey established:
  // - The two chambers name their final vote differently. The HOUSE prints
  //   `House: Passage Of Bill` (377 rolls) and `House: Passage Of Joint
  //   Resolution` (18). The SENATE's substantive vote is SECOND reading
  //   (`Senate: 2nd Reading`, 294 rolls); it takes a recorded third reading
  //   too (`Senate: 3rd Reading`, 59), so both are kept as passage and the
  //   judge's superseded-stage gate picks the chamber's last one.
  // - Concurrence is `House: Concur In Senate Amendments` / `Senate: To
  //   Concur`; conference reports come as `Adopt Conference Report` and
  //   `Adopt Free Conference Report` (`To Adopt The …` in the Senate);
  //   vetoes are overridden by `House: Override Veto By The Governor` /
  //   `Senate: To Override The Veto`.
  // - THE BUDGET IS VOTED SECTION BY SECTION AND ALL OF IT IS EXCLUDED. The
  //   House votes each part of the appropriations act on its own
  //   (`House: Adopt Section 5, Part 1B` 211 rolls, `House: Passage Of
  //   Section 33, Part 1A` 198) and the Senate votes each agency's section
  //   (`Senate: To Adopt Section 22 - Corrections, Department Of`, one per
  //   agency). None of those is a vote on the measure. The appropriations
  //   act's own conference-report vote still classifies as a kept
  //   conference report; the campaign's own gate drops appropriations.
  // - Every desc names its chamber and question in full, so the whole
  //   session classifies with NOTHING unmatched and nothing surfaced.
  // - Feed health is the cleanest tier: 0 repeated roll_call_ids, 0
  //   identity duplicates, 0 summary-only rolls, 0 tally mismatches, 0
  //   committee votes (every tally is whole-chamber; the one exception is a
  //   single 0-8 Senate second reading, which the tally cut rejects).
  // - South Carolina proposes CONSTITUTIONAL AMENDMENTS as joint
  //   resolutions, a type the shared kept-types list already keeps, so
  //   Georgia's resolution gap does not recur here.
  SC: {
    jurisdiction: "SC",
    sessionId: 2194,
    chamberSizes: { house: 124, senate: 46 },
    keptQuestions: [
      // Final passage. The House names the instrument; the Senate names the
      // reading.
      { pattern: /^house: passage of (?:bill|joint resolution)$/, questionClass: "passage" },
      { pattern: /^senate: (?:2nd|3rd) reading$/, questionClass: "passage" },
      // Accepting the other chamber's amendments.
      { pattern: /^house: concur in senate amendments$/, questionClass: "concurrence" },
      { pattern: /^senate: to concur$/, questionClass: "concurrence" },
      // Conference and free-conference reports.
      { pattern: /^house: adopt (?:free )?conference report$/, questionClass: "conference_report" },
      { pattern: /^senate: to adopt the (?:free )?conference report$/, questionClass: "conference_report" },
      // Veto overrides.
      { pattern: /^house: override veto by the governor$/, questionClass: "veto_override" },
      { pattern: /^senate: to override the veto$/, questionClass: "veto_override" },
    ],
    excludedQuestions: [
      // Floor amendment votes, adopted and tabled alike. South Carolina
      // repeats the amendment number in the desc
      // (`House: Table Amendment 6 Amendment Number 6`), and the Senate
      // spells its own several ways
      // (`Senate: To Lay On The Table Amendment No. 3`,
      // `Senate: To Adopt Amendment Number Rfh-1`).
      /^house: (?:adopt|table) amendment/,
      /^senate: to (?:adopt|lay on the table) amendment/,
      /^senate: to adopt [a-z&' ]+ committee amendment$/,
      /^senate: to (?:allow|consider|take up|carry over) amendment/,
      // The appropriations act, voted one section or part at a time, plus
      // single-proviso votes.
      /^house: adopt section/,
      /^house: passage of section/,
      /^house: proviso /,
      /^senate: to adopt section/,
      // Scheduling, debate and other procedural motions.
      /^house: table\b/,
      /^house: (?:motion to )?(?:recommit|commit|continue|recede|reconsider)/,
      /^house: recommit bill$/,
      /^house: reconsider the vote$/,
      /^house: adjourn/,
      /^house: (?:table )?cloture$/,
      /^house: invoke the previous question/,
      /^house: waive rule/,
      /^house: grant free conference powers$/,
      /^senate: to lay on the table$/,
      /^senate: (?:motion to )?suspend rule/,
      /^senate: rule /,
      /^senate: cloture motion$/,
      /^senate: to grant free conference powers$/,
      /^senate: to (?:continue the bill|recede)/,
      /^senate: to set for special order$/,
      /^senate: to take up the order of the day$/,
      // Simple and concurrent resolutions, whose types the shared kept-types
      // list already drops; listed so a stray one cannot reach a kept rule.
      /^house: adopt (?:house resolution|concurrent resolution)$/,
      /^senate: to adopt the resolution$/,
    ],
  },

  // Nevada Legislature, 83rd Session (2025). Nevada meets in odd years only,
  // so this one regular session is the whole campaign apart from the 36th
  // Special Session (LegiScan 2233), which is surveyed separately.
  //
  // NEVADA TIES NEW YORK FOR THE SMALLEST FLOOR VOCABULARY IN THIS REGISTRY.
  // The survey of 2026-09-02 read all 1,333 roll calls and found exactly TWO
  // descriptions: `Senate Final Passage` (670) and `Assembly Final Passage`
  // (663). There is no third spelling to exclude, which is why
  // `excludedQuestions` is empty — not an oversight. LegiScan carries no
  // concurrence, conference-report or veto-override roll for Nevada at all,
  // and no committee vote: every Assembly roll lists all 42 members and
  // every Senate roll lists all 21 but one (see the SB 26 note below).
  //
  // ⚠ WHAT THE EMPTY VOCABULARY COSTS: because the feed holds only final
  // passage, a bill the second chamber amended has NO roll on the first
  // chamber accepting that amendment. The first chamber's only recorded vote
  // can therefore predate the text that became law. Nevada gives no version
  // check in the description, so every selected roll needs its version
  // confirmed against the bill history on the Legislature's own site
  // (leg.state.nv.us) before it is judged.
  //
  // ⚠ NINE bill-and-chamber pairs carry TWO `Final Passage` rolls. They are
  // reconsider-and-revote pairs, mostly the same day with consecutive roll
  // call ids (AB 123 Senate 14-7 then 13-8; AB 44 Senate 13-8 then 14-7),
  // and one where the second vote FAILED (AB 500 Assembly 25-17 then 20-22).
  // The superseded-stage gate in `rollcall:judge` catches these; the bill
  // history says which vote stands.
  //
  // Feed health is the cleanest tier: 0 repeated roll call ids, 0 identity
  // duplicates, 0 summary-only rolls, 0 tally mismatches, 0 parse errors and
  // 0 committee-chamber rolls. Two data notes, both recorded in the
  // campaign's CODE-FINDINGS.md and neither fixed here:
  //   1. 46 Nevada bills carry an `A` letter after the number (`SB88A`,
  //      `AJR6A`). The dataset parser rejects those file names, so the
  //      survey reports 46 file errors and a non-zero exit. All 46 are dead
  //      bills with ZERO roll calls, so nothing reachable is lost, but the
  //      non-zero exit on a Nevada run is expected, not a failure.
  //   2. One Senate roll lists only 2 of 21 senators (SB 26, roll 1550268,
  //      recorded 2-0). The small-tally guard classifies it null and
  //      surfaces it rather than queueing it, which is the wanted outcome.
  //
  // Pool measured before any batch was promised: 292 divided floor votes,
  // of which 104 rolls on 73 measures are on bills that became law. Nevada's
  // government is divided — a Democratic legislature and a Republican
  // governor — so a further 145 divided rolls on 79 measures sit on bills he
  // vetoed. Those are outside the standard divided-and-enacted gate.
  NV: {
    jurisdiction: "NV",
    sessionId: 2144,
    chamberSizes: { house: 42, senate: 21 },
    keptQuestions: [
      // The Assembly's only floor question. LegiScan files Assembly rolls
      // under chamber code `A`, which `parseLegiscanRollCall` maps to
      // `house` (the mapping added for California; verified on all 663
      // Nevada Assembly rolls).
      { pattern: /^assembly final passage$/, questionClass: "passage" },
      // The Senate's only floor question.
      { pattern: /^senate final passage$/, questionClass: "passage" },
    ],
    // Nothing to exclude: the survey left NOTHING unmatched, and a Nevada
    // description that is neither of the two above has never been seen. A
    // future one would fall through to `unknown_question` and surface, which
    // is the behaviour we want over a guessed rule.
    excludedQuestions: [],
  },

  // Alabama Legislature, 2023 Regular Session. The same legislators as the
  // 2025 and 2026 sessions: Alabama elects its whole legislature to
  // four-year terms, so the members elected in November 2022 sit through
  // 2026 and are the people on the November 2026 ballot. Vocabulary measured
  // from the full dataset survey 2026-09-02: 1,255 bills, 1,485 roll calls,
  // 140 people, nothing left unmatched.
  //
  // This session predates the caption rewrite, so it uses the 2023
  // definitions above, NOT the modern ones. It also has no Budget Isolation
  // Resolution roll calls at all, which is why it stores far fewer votes
  // than 2024 or 2025 while passing a comparable number of bills.
  //
  // 1,003 kept floor votes, of which 28 are divided and 21 of those are on
  // measures that became law — the largest divided-and-enacted pool of any
  // Alabama session in scope.
  "AL-2014": {
    jurisdiction: "AL",
    sessionId: 2014,
    chamberSizes: { house: 105, senate: 35 },
    keptQuestions: ALABAMA_2023_KEPT_QUESTIONS,
    excludedQuestions: ALABAMA_2023_EXCLUDED_QUESTIONS,
  },

  // Alabama Legislature, 2023 Second Special Session (July 2023), called to
  // redraw the congressional map after Allen v. Milligan. Surveyed
  // 2026-09-02: 39 bills, 26 roll calls, 138 people, nothing unmatched under
  // the 2023 definitions.
  //
  // Only 10 kept floor votes, 4 divided, and 2 of those on the one measure
  // that became law: SB 5, the reapportionment act, which the Senate passed
  // 24-8 and then re-passed 24-6 after the House amended it.
  //
  // The 2023 FIRST special session (LegiScan 2048) was surveyed the same day
  // and is deliberately NOT registered: 32 bills, 6 roll calls, 6 kept floor
  // votes and ZERO divided ones, so it can never contribute a record.
  "AL-2060": {
    jurisdiction: "AL",
    sessionId: 2060,
    chamberSizes: { house: 105, senate: 35 },
    keptQuestions: ALABAMA_2023_KEPT_QUESTIONS,
    excludedQuestions: ALABAMA_2023_EXCLUDED_QUESTIONS,
  },

  // Alabama Legislature, 2024 Regular Session — the transition year, and the
  // only Alabama session that prints two caption systems at once. See the
  // 2024 vocabulary above for what that means and why it matters. Vocabulary
  // measured from the full dataset survey 2026-09-02: 1,229 bills, 2,147
  // roll calls, 139 people, nothing left unmatched across 111 families.
  //
  // 838 kept floor votes, of which 31 are divided and 10 of those are on
  // measures that became law.
  "AL-2103": {
    jurisdiction: "AL",
    sessionId: 2103,
    chamberSizes: { house: 105, senate: 35 },
    keptQuestions: ALABAMA_2024_KEPT_QUESTIONS,
    excludedQuestions: ALABAMA_2024_EXCLUDED_QUESTIONS,
  },

  // New York, 2025-2026 General Assembly (both years in one dataset;
  // surveyed 2026-09-02 over 25,313 bills / 14,737 roll calls / 221 people).
  //
  // New York has the smallest floor vocabulary of any state surveyed so far:
  // EXACTLY TWO descriptions are floor votes, and both say so in words --
  // `Senate Floor Vote - Final Passage` (3,614 rolls, total 61-63 of 63
  // seats) and `Assembly Floor Vote - Final Passage` (1,856 rolls, total
  // 148-150 of 150). Every one of the other 212 description families names a
  // committee (`Senate Health Committee Vote`, `Assembly Codes Committee:
  // Favorable refer to committee Rules`), and NOT ONE of them reaches even
  // 40 votes, so the tally check rejects them all before the queue and this
  // entry needs no exclusion rules at all. The largest committee is Assembly
  // Rules at 31 of 150 seats.
  //
  // New York prints no concurrence, conference-report or veto-override
  // question: a bill must pass both houses in identical form, and the second
  // house substitutes its own companion bill and votes the SAME bill number,
  // so 234 of the 236 divided-and-enacted measures carry one floor vote per
  // chamber under a single bill id.
  //
  // Constitutional amendments are ordinary bills here (type B), so the
  // Georgia resolution gap does not recur. The four floor votes on type CR
  // are the joint sessions that ELECT REGENTS of the University of the State
  // of New York plus the sine-die resolution -- not measures, and dropped
  // before this config is read because CR is not a kept bill type.
  //
  // Feed health is the cleanest tier: 0 repeated roll call ids, 0
  // summary-only rolls, 0 tally mismatches against a roll's own member list,
  // 68 identity-duplicate extras that the fetcher collapses. ONE roll is a
  // permanent parse error -- roll 1473007 (S 824, Senate, 2025-01-22) says
  // yea 35 while its member list holds 36 yes votes, so it can never be
  // stored; a non-zero fetch exit for that single row is expected here.
  NY: {
    jurisdiction: "NY",
    sessionId: 2188,
    chamberSizes: { house: 150, senate: 63 },
    keptQuestions: [
      { pattern: /^assembly floor vote - final passage$/, questionClass: "passage" },
      { pattern: /^senate floor vote - final passage$/, questionClass: "passage" },
    ],
    excludedQuestions: [],
  },

  // New Mexico Legislature, 2025 Regular Session (60 days, January 21 to
  // March 22 2025). Vocabulary measured from the full dataset survey
  // 2026-09-02: 1,328 bills, 571 roll calls, 128 people (70 House seats and
  // 42 Senate seats plus turnover).
  //
  // NEW MEXICO HAS THE SMALLEST VOCABULARY OF ANY STATE IN THIS REGISTRY:
  // every roll call in the session carries one of exactly two descriptions,
  // `House Final Passage` (310) and `Senate Final Passage` (254). There is
  // nothing to exclude, so `excludedQuestions` is deliberately empty, and
  // both patterns are anchored at both ends so any new spelling in a future
  // session surfaces for review instead of being classified silently.
  //
  // What the survey established:
  // - The feed carries FINAL PASSAGE ONLY. No bill has more than one roll
  //   call in the same chamber anywhere in the session (checked over all
  //   571), so there are no amendment votes, no concurrence votes and no
  //   conference-report votes to classify or exclude. ⚠ That is a JUDGING
  //   hazard, not a convenience: when the other chamber amends a bill, the
  //   vote that accepts the change is simply absent, so a chamber's only
  //   recorded vote can be on text that is not the text that became law.
  //   Every selected roll needs its own version check against nmlegis.gov.
  // - There are no committee votes at all. Every House tally totals 69 or
  //   70 and every Senate tally totals exactly 42, so nothing lands in the
  //   small-tally or committee buckets.
  // - New Mexico proposes CONSTITUTIONAL AMENDMENTS as joint resolutions
  //   (type JR, which the shared kept-types list already keeps), so the
  //   Georgia resolution gap does not recur. A joint resolution goes to the
  //   VOTERS and never to the governor, so a description of one must never
  //   say it became law.
  // - MEMORIALS (types M and JM, 88 and 14 bills) also take final-passage
  //   votes and are dropped before this config by the kept-types list. They
  //   express the legislature's opinion and change no law.
  //
  // Feed health is the cleanest tier: 0 repeated roll call ids, 0 identity
  // duplicates, 0 summary-only rolls. Three defects are recorded in
  // evidence/rollcall/legiscan-nm-2187/CODE-FINDINGS.md and none is fixable
  // here: seven House rolls of 2025-02-27 drop the same member from their
  // member lists while their header tallies stay right (they fail the parser
  // and never reach the queue); one Senate roll is stamped 2024-02-10 inside
  // a 2025 session; and roll 1496261 (House vote on SB 3) stores 42-23 where
  // the official sheet reads 44-23, with the header and the 69-member list
  // agreeing with each other, so no parser check can see it. ⚠ That last one
  // means a clean parse proves nothing about the tally: every roll picked
  // for a batch is checked against its nmlegis.gov official sheet (date,
  // yeas, nays, present-not-voting, absent + excused) before judging, and a
  // mismatch holds the roll — survey/tally-audit.json records the check.
  NM: {
    jurisdiction: "NM",
    sessionId: 2187,
    chamberSizes: { house: 70, senate: 42 },
    keptQuestions: [
      { pattern: /^house final passage$/, questionClass: "passage" },
      { pattern: /^senate final passage$/, questionClass: "passage" },
    ],
    excludedQuestions: [],
  },
  // Kansas Legislature, 2025-2026 Regular Session. Kansas files both years
  // of the biennium in ONE LegiScan dataset (session 2178, dated
  // 2026-06-28), so this single entry covers the whole current term; there
  // is no separate 2025 or 2026 session to register. Vocabulary measured
  // from the full dataset survey 2026-09-02: 1,483 bills, 1,435 roll calls,
  // 218 people (125 House + 40 Senate seats plus mid-term turnover).
  //
  // What the survey established:
  // - ⚠ EVERY KANSAS DESCRIPTION ENDS WITH ITS OWN TALLY, spelled
  //   ` - Yea: <n> Nay: <n>` (1,433 of 1,435 rolls; the two exceptions are
  //   line-item veto rolls whose description is truncated at 250
  //   characters). That suffix makes almost every description unique, so
  //   the RAW histogram is useless: 729 distinct descriptions fold to just
  //   130 families once it is removed. No pattern here may be anchored at
  //   the end of the string.
  // - ⭐ THE EMBEDDED TALLY IS A FREE PER-ROLL CHECKSUM, and no other state
  //   in this campaign offers one. Comparing it to the roll's own `yea` and
  //   `nay` fields across all 1,433 rolls that carry it found exactly ONE
  //   disagreement: roll 1661569 (House Substitute for SB 229, 2026-03-12)
  //   is captioned `Yea: 85` while the structured fields and the member
  //   list both say 83 yeas (83 + 36 nay + 5 absent = 124 of 125 seats).
  //   That bill did not become law, so it is outside the campaign gate, but
  //   run the comparison over any batch before judging it.
  // - Kansas takes TWO recorded floor votes on a bill. `Committee of the
  //   Whole` is the amend-and-debate stage (floor amendments, rulings of
  //   the chair, motions to rerefer), the analog of a second reading in
  //   Texas, California and Missouri; `Final Action` is passage. Only Final
  //   Action and the later stages are kept.
  // - `Emergency Final Action` is Final Action taken on the same day the
  //   bill is reported, not a distinct question, so it is kept under the
  //   same rule. `Senate Consent Calendar Passed` is likewise real passage.
  // - ⭐ VETO OVERRIDES ARE A FIRST-CLASS POOL HERE, not an oddity.
  //   Kansas has a Republican supermajority legislature and a Democratic
  //   governor, and the legislature overrode her 69 times in this
  //   biennium (`Motion to override veto prevailed`, House 34 / Senate 35).
  //   An override needs two thirds of each chamber, so these votes are
  //   divided by definition. They are kept as `veto_override`. ⚠ A
  //   prevailing override in ONE chamber is NOT enactment: SB 79's Senate
  //   override prevailed 29-11 on 2025-04-10, the House never took it up,
  //   and the veto stood (`No motion to reconsider vetoed bill; Veto
  //   sustained`). A description may say the measure became law over the
  //   veto only when the bill's own history shows BOTH chambers overrode
  //   it (LegiScan `status` 4, `Passed`).
  // - ⚠ LINE-ITEM VETO OVERRIDES ARE EXCLUDED BY RULE. They are worded
  //   `Motion to override line item veto prevailed; Line item veto 88(k),
  //   88(m) overridden` and every one sits on an appropriations bill. A
  //   line-item override is a vote on the vetoed ITEMS, not on the act, so
  //   describing it as a vote on the bill would be a false claim — the same
  //   trap that removed Kentucky's HB 2 from its 2026 pool.
  // - The dataset carries NO committee votes at all: every roll's `total`
  //   is the whole chamber (125 House, 40 Senate), so nothing lands in the
  //   small-tally or committee buckets. Feed health is the cleanest tier —
  //   0 repeated roll_call_ids, 0 summary-only rolls, 0 internal tally
  //   mismatches, 1 identity-duplicate extra.
  // - ⚠⚠ ELEVEN ROLLS DISAGREE WITH KANSAS'S OWN TALLY. The bill history
  //   prints the state's count on every floor action; comparing it to
  //   LegiScan's `yea`/`nay` found 11 rolls off by one to six votes. The
  //   one checked against the state's published roll call (SB 63 House
  //   override, roll 1491886) has Rep. Bob Lewis recorded NAY where he
  //   voted YEA. All 11 are listed in `heldRollCallIds` below so they
  //   surface but can never be approved; see CODE-FINDINGS.md finding 5.
  // - Failed final questions (`Final Action - Not passed`, `Conference
  //   Committee Report not adopted`, `Motion to override veto failed`,
  //   `Motion to concur with amendments failed`) are KEPT under their
  //   class, as Montana keeps `3rd Reading Failed`: a chamber's later
  //   rejection must be visible to the judge's superseded-stage gate (HB
  //   2527 passed the House 109-13 in February, then the House rejected
  //   the conference report 46-75 in March). Note LegiScan's `passed` flag
  //   is unreliable on these (one `not adopted` roll carries passed=1); the
  //   description is the truth, and nothing fans out until approved.
  // - ⚠ Kansas proposes CONSTITUTIONAL AMENDMENTS as CONCURRENT
  //   RESOLUTIONS (`HCR 5004`, `SCR 1611`), which LegiScan types `CR` — a
  //   type the shared kept-types list drops before this config is read. Two
  //   adopted amendments are therefore unreachable today. Keeping `CR`
  //   wholesale is NOT the fix: the same type carries ceremonial
  //   resolutions and Article V convention applications, several of them
  //   divided. Recorded in
  //   evidence/rollcall/legiscan-ks-2178/CODE-FINDINGS.md, same shape as
  //   the Georgia finding.
  //
  // Every description in the session classifies: 1,270 kept floor votes
  // (836 passage / 297 conference report / 69 concurrence / 68 veto
  // override), 100 excluded, and only the 11 held rolls surfaced.
  KS: {
    jurisdiction: "KS",
    sessionId: 2178,
    chamberSizes: { house: 125, senate: 40 },
    keptQuestions: [
      // `House Conference Committee Report was adopted - Yea: 121 Nay: 0`,
      // and its rejections (`not adopted`, `Motion to not adopt … passed`).
      // Listed first so the concurrence rule cannot claim it.
      {
        pattern: /^(?:house|senate) (?:conference committee report (?:was|not) adopted|motion to not adopt conference committee report)\b/,
        questionClass: "conference_report",
      },
      // The second chamber accepting the other's changes, either directly
      // or as worked out in conference.
      {
        pattern: /^(?:house|senate) (?:concurred with amendments(?: in conference)?|motion to concur with amendments failed)\b/,
        questionClass: "concurrence",
      },
      // The override of a whole-bill veto, prevailed or failed. Never
      // matches the line-item spellings, which carry `line item veto` and
      // are excluded below.
      { pattern: /^(?:house|senate) motion to override veto (?:prevailed|failed)\b/, questionClass: "veto_override" },
      // Passage, in all of its spellings: plain and `Emergency` Final
      // Action, on a bill or a `Substitute`, `passed` or (for resolutions)
      // `adopted` or their `not` forms, with optional ` as amended` and an
      // optional ` by required 2/3 majority` tail.
      {
        pattern:
          /^(?:house|senate) (?:emergency )?final action - (?:substitute )?(?:not )?(?:passed|adopted)(?: as amended)?(?: by required 2\/3 majority)?\b/,
        questionClass: "passage",
      },
      // The Senate's consent calendar, which is real passage.
      { pattern: /^senate consent calendar passed\b/, questionClass: "passage" },
    ],
    excludedQuestions: [
      // Committee of the Whole: the amend-and-debate stage. Covers floor
      // amendments (`Amendment by Senator Holscher was rejected`), rulings
      // of the chair, motions to rerefer, and the stage's own
      // `Be passed as amended` recommendation.
      /^(?:house|senate) committee of the whole\b/,
      // A floor amendment taken during Emergency Final Action debate.
      /^house efa subject to amendment and debate\b/,
      // Line-item veto overrides, prevailed and failed alike: a vote on the
      // vetoed items, not on the act. See the note above.
      /\bmotion to override (?:selected )?line item veto\b/,
      // Procedural motions: pulling a bill out of committee (spelled
      // `Citing Rule 11(b), motion to withdraw from committee failed` in
      // the Senate and `Motion to withdraw from Committee on Veterans and
      // Military not adopted` in the House), killing a bill by striking its
      // enacting clause, the debate cutoff, and a germaneness ruling.
      /\bmotion to withdraw from committee\b/,
      /\bmotion to strike the enacting clause\b/,
      /\bmotion for previous question\b/,
      /\bquestion of germaneness\b/,
    ],
    // The 11 rolls whose LegiScan tally disagrees with the tally Kansas
    // prints in its own bill history (CODE-FINDINGS.md finding 5). Value =
    // `LegiScan tally vs Kansas tally`.
    heldRollCallIds: {
      1520565: "HB 2007 Senate 2025-03-18: LegiScan 28-12 vs Kansas 27-13",
      1523869: "HB 2060 Senate 2025-03-20: LegiScan 36-4 vs Kansas 35-5",
      1523332: "HB 2164 Senate 2025-03-20: LegiScan 37-3 vs Kansas 38-2",
      1679592: "HB 2164 House 2026-04-09: LegiScan 38-85 vs Kansas 40-83",
      1543581: "HB 2240 House 2025-04-10: LegiScan 87-38 vs Kansas 88-37",
      1666167: "HB 2444 Senate 2026-03-19: LegiScan 35-5 vs Kansas 34-6",
      1666842: "HB 2739 Senate 2026-03-19: LegiScan 39-1 vs Kansas 38-2",
      1491886: "SB 63 House 2025-02-18: LegiScan 84-35 vs Kansas 85-34 (Rep. Bob Lewis listed nay, voted yea)",
      1671816: "SB 197 House 2026-03-27: LegiScan 74-49 vs Kansas 75-48",
      1666091: "SB 254 House 2026-03-19: LegiScan 77-47 vs Kansas 78-46",
      1670877: "SB 356 House 2026-03-26: LegiScan 105-19 vs Kansas 99-25",
    },
  },

  // Delaware General Assembly, 153rd (2025-2026). Both years sit in one
  // LegiScan session. Surveyed 2026-09-02 over the full dataset: 1,296
  // bills, 2,044 roll calls, 65 people (41 House seats + 21 Senate seats,
  // plus mid-term replacements).
  //
  // Feed health is the cleanest tier this campaign has seen: no committee
  // votes at all, no repeated roll call ids, no roll whose parts fail to add
  // up to its own total, and no parse errors. Every recorded roll reports a
  // whole chamber (House 41 or 40 with one seat vacant, Senate 21 or 19), so
  // the floor-versus-committee tally check never has to decide anything.
  // 350 House rolls carry no member list at all: Delaware records a voice
  // vote as a roll with every count zero, almost always on a concurrent
  // resolution. The fetcher already skips those as unrecorded votes.
  //
  // ⚠⚠ THE WHOLE DELAWARE VOCABULARY IS TWO STRINGS, AND NEITHER NAMES THE
  // QUESTION. Every one of the 2,044 rolls reads exactly `House Third
  // Reading` or `Senate Third Reading`. Final passage, a vote on an
  // amendment, the originating chamber's later vote on the other chamber's
  // version, and a procedural motion all wear the same words. Florida and
  // Connecticut had the same defect in one chamber; Delaware has it in both,
  // for every roll, with no second spelling anywhere to fall back on.
  //
  // So `questionClass: "passage"` on a Delaware row is this feed's claim,
  // not Delaware's, exactly as it is in Florida. Never show it to a reader
  // and never let it pick a roll. The question comes from the bill history,
  // which spells it out:
  //   `Passed By House. Votes: 31 YES 5 NO 5 ABSENT`      -> passage
  //   `Amendment SA 3 to HB 445 - Passed By Senate. ...`   -> an amendment
  //   `Defeated By House. Votes: 15 YES 26 NO. Reason Taken: motion to
  //    recess to read amendment ...`                       -> procedural
  // Matching a roll to its history line on (date, chamber, yeas, nays)
  // resolves 1,574 of the 1,694 recorded rolls outright and all but 8 of the
  // 158 divided ones. That match is a selection-time step in the batch
  // recipe, not something a description pattern can do.
  //
  // A roll the match leaves unresolved stays unselected. HB 245's roll
  // 1599336 (House, 2025-08-12, 15-6, `passed: 0`) is the reference case:
  // the feed calls it `House Third Reading`, this entry calls it `passage`,
  // and the history line -- dated 2025-08-13, the day AFTER the roll --
  // reads `Defeated By House ... Reason Taken: Motion to Suspend Rules on
  // HB 245`. A motion to suspend the rules, wearing passage words. Nothing
  // in the fetch, this classifier, or `rollcall:judge` can see that line;
  // only the history match can, so it has to run before any Delaware roll
  // is judged, and a roll it cannot place (the one-day date skew above is
  // one of two such cases in the session) is left out, never guessed.
  //
  // Nothing is excluded here because nothing else exists to exclude: a
  // config exclusion can only read the description, and the description is
  // the same on a passage vote and on an amendment vote. Excluding either
  // spelling would throw away every real passage vote in that chamber.
  //
  // Constitutional amendments ride ordinary bills in Delaware (they pass in
  // two consecutive General Assemblies and never go to voters), so the
  // Georgia resolution gap does not recur and no extra bill type is needed.
  DE: {
    jurisdiction: "DE",
    sessionId: 2163,
    chamberSizes: { house: 41, senate: 21 },
    keptQuestions: [
      { pattern: /^house third reading$/, questionClass: "passage" },
      { pattern: /^senate third reading$/, questionClass: "passage" },
    ],
    excludedQuestions: [],
  },
};

export const LEGISCAN_CONFIG_KEYS: readonly string[] = Object.keys(LEGISCAN_STATE_CONFIGS);

// The distinct `jurisdiction` values the registry can write, which is NOT
// the key list: a state with two sessions in scope is keyed `MO` and
// `MO-2226` but writes only `MO`. Anything validating a jurisdiction a
// human typed (the judge's judgments file) must use this, or it would
// accept `MO-2226` as a jurisdiction and store rows no importer looks for.
export const LEGISCAN_RECORD_JURISDICTIONS: readonly string[] = [
  ...new Set(Object.values(LEGISCAN_STATE_CONFIGS).map((config) => config.jurisdiction)),
];

// States already served by their OWN source pipeline. Importing one of these
// through LegiScan would DUPLICATE every record: the two feeds cite the same
// vote with different URLs, and the fan-out's duplicate scan compares folded
// URL keys — `oh:136:sb56` from Ohio's actions feed never equals `ls:<roll
// call id>` from the LegiScan page — so nothing would recognize the second
// copy as the same vote. Ohio's GA-136 is 1,330 live records across 94
// candidates; a LegiScan re-import would silently double them.
//
// LegiScan remains useful for these states as a read-only CROSS-CHECK. Ohio
// was verified that way on 2026-08-24: all 466 kept floor votes matched
// LegiScan exactly on chamber + date + measure + yea + nay, including all 24
// judged rolls. Remove a state from this set only when its own pipeline is
// being retired and its existing records are migrated or retired first.
const JURISDICTIONS_WITH_DEDICATED_PIPELINES: ReadonlySet<string> = new Set(["OH"]);

export function getLegiscanStateConfig(state: string): LegiscanStateConfig {
  const jurisdiction = state.trim().toUpperCase();
  if (JURISDICTIONS_WITH_DEDICATED_PIPELINES.has(jurisdiction)) {
    throw new Error(
      `${jurisdiction} is served by its own roll-call pipeline (rollcall:oh:*), not LegiScan; ` +
        "importing it here would write a duplicate record for every vote already imported from that source"
    );
  }
  const config = LEGISCAN_STATE_CONFIGS[jurisdiction];
  if (!config) {
    const registered = LEGISCAN_CONFIG_KEYS.length > 0 ? LEGISCAN_CONFIG_KEYS.join(", ") : "none yet";
    throw new Error(
      `no LegiScan state config for ${state} (registered: ${registered}); ` +
        "survey the state's dataset first and add its entry to legiscanStateConfigs.ts"
    );
  }
  return config;
}
