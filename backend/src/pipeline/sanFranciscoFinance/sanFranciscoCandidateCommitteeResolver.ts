// Candidate → controlled-committee resolution for San Francisco (plan
// Phase 3). The SFEC dashboard manifest names each candidate committee with
// filer_nid + FPPC filer_id, so identity comes from the manifest, not from
// committee-name heuristics. Resolution order, all within one
// already-eligibility-matched contest:
//
//   1. FPPC id: the manifest filer_id appears in a VoteApp candidate's
//      state_filing_ids (the roster stage stores SF Ethics ids there).
//   2. Name: Georgia-style person-name matching — parse variants with
//      first+last alignment and a middle-name-conflict veto, so
//      "MICHAEL NGUYEN" matches "Michael T. Nguyen" (absent middle is weak
//      evidence) while "John A. Smith" never matches "John B. Smith".
//
// Anything else — no match, two candidates matching one manifest row, two
// manifest rows matching one candidate — fails closed for manual review.
// Manifest names are uppercase display names ("DION-JAY (DJ) BROOKTER");
// VoteApp names carry quoted nicknames, parentheticals, suffixes, and
// punctuation ('Emanuel "Manny" Yekutiel', 'Ellsworth "Ell" M. Jennison,
// Jr.', "J.R. Eppler") — all real November 2026 rows, all covered by tests.

import type { SanFranciscoManifestCandidate } from "./sanFranciscoDashboardManifestClient.js";

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string | null | undefined): string {
  return normalizeTextKey(value)
    // Bare "V" is a middle initial, not a suffix (GENERATIONAL_SUFFIX_RANK in
    // finance/personNameMiddleEvidence.ts): stripping it erased middle evidence.
    .replace(/\b(JR|SR|II|III|IV)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeSanFranciscoCandidateNameForStorage(
  value: string,
): string {
  const normalized = normalizePersonName(stripSuffixComma(value));
  return normalized || value.trim().replace(/\s+/g, " ").toUpperCase();
}

// "Ellsworth M. Jennison, Jr." — the comma belongs to the suffix, not a
// "Last, First" form. Stripped before parsing so the comma branch below only
// ever sees genuine surname-first forms.
// Keeps "V" on purpose: it only drops the comma; the token still reaches
// normalizePersonName, which now keeps it as middle evidence.
function stripSuffixComma(value: string): string {
  return value.replace(/,\s*(?:JR|SR|II|III|IV|V)\.?\s*$/i, " ");
}

// `exact` marks parses whose surname boundary is explicit (comma form,
// single token, two tokens) rather than guessed from a space-form split.
type ParsedPersonName = {
  first: string;
  middles: string[];
  last: string;
  exact: boolean;
};

// Comma forms ("Last, First M.") are unambiguous and yield one parse. Space
// forms are ambiguous about where the surname starts, so every split is
// emitted and the pair comparison tries them all — a wrong split can only
// fail to align, never manufacture agreement.
function parsePersonName(raw: string): ParsedPersonName[] {
  const commaIndex = raw.indexOf(",");
  if (commaIndex > 0) {
    const last = normalizePersonName(raw.slice(0, commaIndex));
    const restTokens = normalizePersonName(raw.slice(commaIndex + 1))
      .split(" ")
      .filter(Boolean);
    if (!last || restTokens.length === 0) return [];
    return [
      { first: restTokens[0]!, middles: restTokens.slice(1), last, exact: true },
    ];
  }
  const tokens = normalizePersonName(raw).split(" ").filter(Boolean);
  if (tokens.length === 0) return [];
  if (tokens.length === 1)
    return [{ first: tokens[0]!, middles: [], last: tokens[0]!, exact: true }];
  const parses: ParsedPersonName[] = [];
  for (let lastStart = 1; lastStart < tokens.length; lastStart += 1) {
    parses.push({
      first: tokens[0]!,
      middles: tokens.slice(1, lastStart),
      last: tokens.slice(lastStart).join(" "),
      exact: tokens.length === 2,
    });
  }
  return parses;
}

// The name outside parentheses plus each parenthetical alias parsed on its
// own — "DION-JAY (DJ) BROOKTER" and "Dionjay (DJ) Brookter" disagree on the
// hyphenated form but share the DJ alias variant, which is what matches.
function personNameVariants(value: string): ParsedPersonName[] {
  const raw = stripSuffixComma(value);
  const variants: ParsedPersonName[] = [];
  variants.push(...parsePersonName(raw.replace(/\([^()]+\)/g, " ")));
  for (const match of raw.matchAll(/\(([^()]+)\)/g)) {
    if (match[1]) variants.push(...parsePersonName(match[1]));
  }
  return variants;
}

// Middle-name evidence between two parses whose first and last already agree:
// "strong" when every shared-position middle corroborates (equal, or an
// initial matching the full form), "conflict" when any shared position
// disagrees, "weak" when at least one side has no middle information. All
// shared positions are compared — "MICHAEL ANDREW" vs "MICHAEL BERNARD" is a
// conflict even though the first tokens agree.
function middleNameEvidence(
  a: string[],
  b: string[],
): "strong" | "weak" | "conflict" {
  if (a.length === 0 || b.length === 0) return "weak";
  const shared = Math.min(a.length, b.length);
  for (let index = 0; index < shared; index += 1) {
    const tokenA = a[index]!;
    const tokenB = b[index]!;
    if (tokenA === tokenB) continue;
    if (tokenA.length === 1 && tokenB.startsWith(tokenA)) continue;
    if (tokenB.length === 1 && tokenA.startsWith(tokenB)) continue;
    return "conflict";
  }
  return "strong";
}

// Aggregation across all variant pairs: any strong pair matches. A conflict
// on an EXACT pair — one whose surname boundary is explicit (comma form)
// rather than guessed, which pins the aligned parse on the other side too —
// is authoritative and rejects outright: an ambiguous space-form split
// elsewhere must never override it ("SMITH, JOHN B. A." conflicts with
// "John A. Smith" no matter how a space-form sibling variant re-splits).
// Purely ambiguous weak/conflict evidence is judged only at the LONGEST
// aligned surname — a compound surname emits bogus shorter splits ("Mary Van
// Dyke" vs "MARY B VAN DYKE" aligns correctly on "VAN DYKE" but the
// "DYKE"-surname split reads VAN-vs-B as a middle conflict), and the longest
// alignment is the real one. At that length, any conflict rejects; otherwise
// a first+last agreement with middle information missing on a side matches.
export function sanFranciscoCandidateNameMatches(
  appCandidateName: string,
  manifestCandidateName: string,
): boolean {
  const appVariants = personNameVariants(appCandidateName);
  const evidenceBySurnameLength = new Map<
    number,
    { weak: boolean; conflict: boolean }
  >();
  let sawStrong = false;
  let sawExactConflict = false;
  for (const manifestVariant of personNameVariants(manifestCandidateName)) {
    for (const appVariant of appVariants) {
      if (
        appVariant.first !== manifestVariant.first ||
        appVariant.last !== manifestVariant.last
      )
        continue;
      const evidence = middleNameEvidence(
        appVariant.middles,
        manifestVariant.middles,
      );
      if (evidence === "strong") {
        sawStrong = true;
        continue;
      }
      if (
        evidence === "conflict" &&
        (appVariant.exact || manifestVariant.exact)
      ) {
        sawExactConflict = true;
        continue;
      }
      const surnameLength = appVariant.last.split(" ").length;
      const bucket = evidenceBySurnameLength.get(surnameLength) ?? {
        weak: false,
        conflict: false,
      };
      bucket[evidence] = true;
      evidenceBySurnameLength.set(surnameLength, bucket);
    }
  }
  if (sawStrong) return true;
  if (sawExactConflict) return false;
  if (evidenceBySurnameLength.size === 0) return false;
  const longest = Math.max(...evidenceBySurnameLength.keys());
  const decisive = evidenceBySurnameLength.get(longest)!;
  return decisive.weak && !decisive.conflict;
}

// Stable identity for a manifest outside committee that carries no FPPC id.
// Scoped by the relation row's (candidate, election) uniqueness, so the
// normalized committee name is enough — and the money is never dropped.
export function sanFranciscoSyntheticSpenderId(spenderName: string): string {
  const normalized = normalizeTextKey(spenderName);
  if (!normalized)
    throw new Error("San Francisco outside committee has no usable name");
  return `name:${normalized}`;
}

export type SanFranciscoAppCandidate = {
  candidateId: string;
  displayName: string;
  /** SF Ethics FPPC ids from candidates.state_filing_ids ([] when none). */
  stateFilingIds: readonly string[];
};

export type SanFranciscoManifestCandidateResolution = {
  manifestCandidate: SanFranciscoManifestCandidate;
} & (
  | { status: "matched"; candidateId: string; matchedBy: "fppc_id" | "name" }
  | { status: "unmatched" | "ambiguous"; reason: string }
);

/**
 * Resolves every manifest candidate of one contest against the election's
 * VoteApp candidates. A manifest entry matching nobody is expected, not an
 * error — SFEC lists committee-formers, and some (a withdrawn rival, a
 * primary-only filer) never reach the ballot. The reverse — a VoteApp
 * candidate with no manifest row — simply produces no link.
 */
export function resolveSanFranciscoContestCandidates(input: {
  manifestCandidates: readonly SanFranciscoManifestCandidate[];
  appCandidates: readonly SanFranciscoAppCandidate[];
}): SanFranciscoManifestCandidateResolution[] {
  const resolutions: SanFranciscoManifestCandidateResolution[] =
    input.manifestCandidates.map((manifestCandidate) => {
      const idMatches = input.appCandidates.filter((candidate) =>
        candidate.stateFilingIds.includes(manifestCandidate.fppcId),
      );
      if (idMatches.length > 1)
        return {
          manifestCandidate,
          status: "ambiguous",
          reason: `FPPC id ${manifestCandidate.fppcId} appears on ${idMatches.length} candidates' state filing ids`,
        };
      if (idMatches.length === 1)
        return {
          manifestCandidate,
          status: "matched",
          candidateId: idMatches[0]!.candidateId,
          matchedBy: "fppc_id",
        };
      const nameMatches = input.appCandidates.filter((candidate) =>
        sanFranciscoCandidateNameMatches(
          candidate.displayName,
          manifestCandidate.candidateName,
        ),
      );
      if (nameMatches.length > 1)
        return {
          manifestCandidate,
          status: "ambiguous",
          reason: `Manifest name ${manifestCandidate.candidateName} matches ${nameMatches.length} candidates`,
        };
      if (nameMatches.length === 1)
        return {
          manifestCandidate,
          status: "matched",
          candidateId: nameMatches[0]!.candidateId,
          matchedBy: "name",
        };
      return {
        manifestCandidate,
        status: "unmatched",
        reason: "No VoteApp candidate in this contest matches by FPPC id or name",
      };
    });
  // Two manifest committees resolving to one candidate would fight over the
  // single active link per (candidate, election) — fail both closed instead
  // of letting the upsert order decide which committee wins.
  const byCandidate = new Map<string, number>();
  for (const resolution of resolutions)
    if (resolution.status === "matched")
      byCandidate.set(
        resolution.candidateId,
        (byCandidate.get(resolution.candidateId) ?? 0) + 1,
      );
  return resolutions.map((resolution) => {
    if (
      resolution.status === "matched" &&
      (byCandidate.get(resolution.candidateId) ?? 0) > 1
    )
      return {
        manifestCandidate: resolution.manifestCandidate,
        status: "ambiguous",
        reason:
          "Multiple manifest committees resolve to this candidate; link manually",
      };
    return resolution;
  });
}
