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
export const LEGISCAN_STATE_CONFIGS: Readonly<Record<string, LegiscanStateConfig>> = {};

export const LEGISCAN_STATE_JURISDICTIONS: readonly string[] = Object.keys(LEGISCAN_STATE_CONFIGS);

export function getLegiscanStateConfig(state: string): LegiscanStateConfig {
  const config = LEGISCAN_STATE_CONFIGS[state.trim().toUpperCase()];
  if (!config) {
    const registered = LEGISCAN_STATE_JURISDICTIONS.length > 0 ? LEGISCAN_STATE_JURISDICTIONS.join(", ") : "none yet";
    throw new Error(
      `no LegiScan state config for ${state} (registered: ${registered}); ` +
        "survey the state's dataset first and add its entry to legiscanStateConfigs.ts"
    );
  }
  return config;
}
