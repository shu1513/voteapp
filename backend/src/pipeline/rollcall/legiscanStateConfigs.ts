import type { LegislativeVoteChamber } from "./legislativeVotes.js";

// Per-state configuration for the LegiScan roll-call pipeline, the phase-4
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

// Registered per state as each state's survey is read (data-phase PRs).
// NOTE: legiscan.com pages sit behind a Cloudflare challenge (probed
// 2026-08-24, curl answers 403 "Just a moment..."); the record validator
// accepts that (allowStatusCodes [403] in
// verifyUniqueCandidateRecordSourceUrls), and a human viewer passes the
// challenge in a browser, so the roll-call page stays a valid source_url.
export const LEGISCAN_STATE_CONFIGS: Readonly<Record<string, LegiscanStateConfig>> = {
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
};

export const LEGISCAN_STATE_JURISDICTIONS: readonly string[] = Object.keys(LEGISCAN_STATE_CONFIGS);

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
    const registered = LEGISCAN_STATE_JURISDICTIONS.length > 0 ? LEGISCAN_STATE_JURISDICTIONS.join(", ") : "none yet";
    throw new Error(
      `no LegiScan state config for ${state} (registered: ${registered}); ` +
        "survey the state's dataset first and add its entry to legiscanStateConfigs.ts"
    );
  }
  return config;
}
