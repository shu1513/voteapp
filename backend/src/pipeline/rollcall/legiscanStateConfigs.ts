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
