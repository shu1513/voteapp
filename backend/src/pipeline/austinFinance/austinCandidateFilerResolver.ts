// Candidate → Report Detail filer resolution for Austin (plan Phase 2),
// copy-adapted from denverCandidateCommitteeResolver. Austin has no
// registration list and no filer or committee ids: a candidate's finance
// identity is the exact `filer_name` string on their Report Detail rows
// (the string Phase 3 queries by), so "the filers for an election" are the
// distinct filer names on candidate-form rows tagged with that election
// date, each with the office codes its rows parse to. Resolution is roster
// candidate ↔ filer by the office-code gate plus the shared person-name
// gates (nickname expansion on the VoteApp side only, middle-name evidence,
// generational-suffix veto) — token-based, never substring.
//
// Fail-closed rules:
// - Two filers name-match one candidate → ambiguous. This includes two
//   spellings of one person ("Watson, Kirk P." / "Watson, Kirk P"): they
//   are two exact-match query keys, and picking one silently drops the
//   other's reports — an operator decides.
// - One filer name-matches two roster candidates → ambiguous for both
//   (the same money must never attach to two candidates).
// - A candidate whose election has no office code (a council title with no
//   district number) → unmatched: the office gate is unverifiable.
// - A filer whose rows carry no parsable office code can never match: a
//   same-named filer for a different or unknown seat is not this race.
// Stale election tags (a 2023 report tagged 2026-11-03, plan gotcha 6) are
// harmless here — the roster is the gate, not the tag.

import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import { personNamesMatchWithMiddleEvidence } from "../finance/personNameMiddleEvidence.js";
import { normalizeAustinFinanceTextKey } from "./austinFinanceKeys.js";
import {
  parseAustinOfficeSoughtCode,
  type AustinOfficeCode,
} from "./austinFinanceEligibleOffices.js";
import {
  AUSTIN_CANDIDATE_CORRECTION_FORM_CODES,
  AUSTIN_CANDIDATE_REGULAR_FORM_CODES,
  AUSTIN_CANDIDATE_SPECIAL_FORM_CODES,
  type AustinReportDetailRow,
} from "./austinSocrataClient.js";

export type AustinAppCandidate = {
  candidateId: string;
  displayName: string;
  /** Office code from the roster election; null fails closed. */
  officeCode: AustinOfficeCode | null;
};

/** One distinct Report Detail filer for an election date. */
export type AustinReportFiler = {
  /** Exact Socrata `filer_name` spelling — the sync's query key. */
  filerName: string;
  /** Distinct office codes parsed from the filer's rows (unparsable dropped). */
  officeCodes: readonly AustinOfficeCode[];
  /** Candidate-form rows behind this filer (informational). */
  rowCount: number;
};

export type AustinCandidateFilerResolution = {
  candidate: AustinAppCandidate;
} & (
  | { status: "matched"; filerName: string }
  | { status: "unmatched"; reason: string }
  | { status: "ambiguous"; reason: string }
);

/**
 * Groups candidate-form Report Detail rows (COH / COHFR / CORCOH / COHATX7)
 * by exact filer name. Rows of other forms (PAC reports, dissolutions) and
 * rows without a filer name are not filers. Sorted by name for stable
 * output.
 */
export function collectAustinReportFilers(
  rows: readonly AustinReportDetailRow[],
): AustinReportFiler[] {
  const byName = new Map<string, { codes: Set<AustinOfficeCode>; rowCount: number }>();
  for (const row of rows) {
    if (
      !AUSTIN_CANDIDATE_REGULAR_FORM_CODES.has(row.formTypeCode) &&
      !AUSTIN_CANDIDATE_CORRECTION_FORM_CODES.has(row.formTypeCode) &&
      !AUSTIN_CANDIDATE_SPECIAL_FORM_CODES.has(row.formTypeCode)
    )
      continue;
    const filerName = row.filerName?.trim();
    if (!filerName) continue;
    const entry = byName.get(filerName) ?? { codes: new Set(), rowCount: 0 };
    entry.rowCount += 1;
    const code = parseAustinOfficeSoughtCode(row.officeSought);
    if (code) entry.codes.add(code);
    byName.set(filerName, entry);
  }
  return [...byName.entries()]
    .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
    .map(([filerName, entry]) => ({
      filerName,
      officeCodes: [...entry.codes].sort(),
      rowCount: entry.rowCount,
    }));
}

// Strips exactly the generational suffixes the shared policy recognizes
// (personNameMiddleEvidence GENERATIONAL_SUFFIX_RANK). Bare "V" is NOT one:
// as a trailing token it is far more often a middle initial ("Smith, John
// V"), and stripping it would erase middle evidence — "John V. Smith" would
// then match "Smith, John B." (PR #759 review). A genuine fifth-generation
// name fails to align and gets linked manually instead.
function normalizeAustinPersonName(value: string): string {
  return normalizeAustinFinanceTextKey(value)
    .replace(/\b(JR|SR|II|III|IV)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// Expand nicknames on the VoteApp side ONLY (the personFirstNameNicknames
// design rule): two distinct filed names can never meet at a shared key.
function firstNamesEquivalent(candidateFirst: string, rowFirst: string): boolean {
  return (
    candidateFirst === rowFirst ||
    firstNameVariants(candidateFirst).includes(rowFirst)
  );
}

// Quoted call names → the parenthetical form personNameParseVariants reads
// as an alias. Applied to BOTH sides: roster display names carry them
// ('Zohaib "Zo" Qadri') and so do Austin filer names ('Vela, Jose "Chito",
// III', 'Renteria, Sabino "Pio"', 'Craig, Kenneth O. "Ken", Jr.' — live
// 2026-08-18). Left as quotes, the normalizer would turn the call name into
// a middle token and a roster "Pio Renteria" could never align.
const quotedCallNamesToParens = (value: string): string =>
  value.replace(/"([^"]+)"/g, " ($1) ");

/**
 * Person-name comparison between a Report Detail filer name ("Last, First
 * M.") and a roster display name under the shared gates.
 */
export function austinPersonNameMatchesCandidate(
  filerName: string,
  candidateDisplayName: string,
): boolean {
  return personNamesMatchWithMiddleEvidence({
    candidateName: quotedCallNamesToParens(candidateDisplayName),
    rowNames: [quotedCallNamesToParens(filerName)],
    normalizePersonName: normalizeAustinPersonName,
    firstNamesEquivalent,
  });
}

/**
 * Resolves each roster candidate against the election date's filers.
 * Anything not a clean one-to-one match — no filer, several filers, one
 * filer claimed by two candidates, no office code — fails closed for manual
 * review.
 */
export function resolveAustinCandidateFilers(input: {
  candidates: readonly AustinAppCandidate[];
  filers: readonly AustinReportFiler[];
}): AustinCandidateFilerResolution[] {
  const resolutions: AustinCandidateFilerResolution[] = input.candidates.map(
    (candidate) => {
      if (!candidate.officeCode) {
        return {
          candidate,
          status: "unmatched",
          reason:
            "candidate's election has no office code; cannot verify the office gate",
        };
      }
      // Office gate first: a same-named filer for a different seat is a
      // different race, not a match. Then the person-name gates.
      const matches = input.filers.filter(
        (filer) =>
          filer.officeCodes.includes(candidate.officeCode!) &&
          austinPersonNameMatchesCandidate(filer.filerName, candidate.displayName),
      );
      if (matches.length > 1) {
        return {
          candidate,
          status: "ambiguous",
          reason: `${matches.length} Report Detail filers name-match (${matches
            .map((filer) => JSON.stringify(filer.filerName))
            .join(", ")}); link manually`,
        };
      }
      if (matches.length === 0) {
        return {
          candidate,
          status: "unmatched",
          reason: `no Report Detail filer for ${candidate.officeCode} name-matches`,
        };
      }
      return { candidate, status: "matched", filerName: matches[0]!.filerName };
    },
  );

  // One filer resolving to two candidates would attach the same money to
  // both — fail all of its links closed instead of letting order decide.
  const candidatesByFiler = new Map<string, number>();
  for (const resolution of resolutions) {
    if (resolution.status === "matched") {
      candidatesByFiler.set(
        resolution.filerName,
        (candidatesByFiler.get(resolution.filerName) ?? 0) + 1,
      );
    }
  }
  return resolutions.map((resolution) => {
    if (
      resolution.status === "matched" &&
      (candidatesByFiler.get(resolution.filerName) ?? 0) > 1
    ) {
      return {
        candidate: resolution.candidate,
        status: "ambiguous",
        reason: `filer ${JSON.stringify(resolution.filerName)} resolves to multiple roster candidates; link manually`,
      };
    }
    return resolution;
  });
}
