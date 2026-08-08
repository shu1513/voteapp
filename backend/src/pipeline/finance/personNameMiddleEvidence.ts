// Middle-name evidence gate for state finance candidate-name matching.
//
// Every state resolver historically collapsed person names to a first+last
// key and accepted any key overlap, so "John A. Smith" linked to
// "Smith, John B." as an "exact" match whenever office, district, and
// election year agreed — attaching another candidate's finance records.
// Georgia fixed this first (georgiaCandidateCommitteeResolver.ts, commit
// 59e24a0d); this module is the shared port of that fix, shaped as a VETO so
// each state keeps its own recall behavior (nickname expansion, committee-name
// keys, search tokens) and only the middle-conflict false positive is removed:
//
//   match = <state's existing key-overlap match> && !hasMiddleNameConflict(...)
//
// Evidence rules, per Georgia: conflicting middles reject, an initial that
// corroborates the full middle matches, and the first+last fallback is only
// trusted when no aligned pair carried contradicting middle evidence.

export type ParsedPersonName = {
  first: string;
  middles: string[];
  last: string;
  // Marks parses whose surname boundary is explicit (comma form, single
  // token, two tokens) rather than guessed from a space-form split. A middle
  // conflict on a pair with an exact side is authoritative — see
  // collectMiddleEvidence.
  exact: boolean;
};

// States pass their own person-name normalizer so per-state stop-word and
// suffix stripping is preserved. Must return an uppercase, space-separated
// token string ("" when the input carries no name content).
export type NormalizePersonName = (value: string) => string;

// Parses a raw display name into (first, middles, last). Comma forms
// ("Carr, Christopher M.") are unambiguous and yield one parse. Space forms
// are ambiguous about where the surname starts ("Mary Van Dyke"), so every
// split is emitted and the pair comparison tries them all — a wrong split can
// only fail to align, never manufacture evidence.
export function parsePersonNameCandidates(
  raw: string,
  normalizePersonName: NormalizePersonName
): ParsedPersonName[] {
  const commaIndex = raw.indexOf(",");
  if (commaIndex > 0) {
    const last = normalizePersonName(raw.slice(0, commaIndex));
    const restTokens = normalizePersonName(raw.slice(commaIndex + 1))
      .split(" ")
      .filter(Boolean);
    if (!last || restTokens.length === 0) {
      return [];
    }
    return [{ first: restTokens[0]!, middles: restTokens.slice(1), last, exact: true }];
  }
  const tokens = normalizePersonName(raw).split(" ").filter(Boolean);
  if (tokens.length === 0) {
    return [];
  }
  if (tokens.length === 1) {
    return [{ first: tokens[0]!, middles: [], last: tokens[0]!, exact: true }];
  }
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

// Expands a display name into parse variants: the name outside parentheses
// plus each parenthetical alias ("LEE, Bill (Bill Lee)") parsed on its own.
// A single-token parenthetical is a call name substituting the FIRST name
// ("Glenn A. (Mike) Prax", "Robert (Bob) Smith"), not a standalone person, so
// it also yields the outer parses with the first token swapped — keeping the
// outer middles and surname so their evidence survives ("Mike A Prax" still
// contradicts "Prax, Mike B"). Single characters are excluded: a lone letter
// in parentheses is a party or ballot marker, not a call name.
export function personNameParseVariants(
  value: string,
  normalizePersonName: NormalizePersonName
): ParsedPersonName[] {
  const variants: ParsedPersonName[] = [];
  const outerParses = parsePersonNameCandidates(value.replace(/\([^()]+\)/g, " "), normalizePersonName);
  variants.push(...outerParses);
  for (const match of value.matchAll(/\(([^()]+)\)/g)) {
    if (!match[1]) {
      continue;
    }
    variants.push(...parsePersonNameCandidates(match[1], normalizePersonName));
    const aliasTokens = normalizePersonName(match[1]).split(" ").filter(Boolean);
    const callName = aliasTokens.length === 1 && aliasTokens[0]!.length >= 2 ? aliasTokens[0]! : null;
    if (callName) {
      for (const outer of outerParses) {
        if (callName !== outer.first) {
          variants.push({ first: callName, middles: outer.middles, last: outer.last, exact: outer.exact });
        }
      }
    }
  }
  return variants;
}

function middleTokensCorroborate(tokenA: string, tokenB: string): boolean {
  if (tokenA === tokenB) {
    return true;
  }
  if (tokenA.length === 1 && tokenB.startsWith(tokenA)) {
    return true;
  }
  return tokenB.length === 1 && tokenA.startsWith(tokenB);
}

// Middle-name evidence between two parses whose first and last already agree:
// "strong" when the middles corroborate position by position (equal, or an
// initial matching the full form), "conflict" when any shared position
// disagrees ("John A. B. Smith" vs "Smith, John A. C." conflicts on the
// second middle), "weak" when at least one side has no middle information.
// Tokens past the shorter side's length are one-sided and carry no evidence.
export function middleNameEvidence(a: string[], b: string[]): "strong" | "weak" | "conflict" {
  if (a.length === 0 || b.length === 0) {
    return "weak";
  }
  const shared = Math.min(a.length, b.length);
  for (let index = 0; index < shared; index += 1) {
    if (!middleTokensCorroborate(a[index]!, b[index]!)) {
      return "conflict";
    }
  }
  return "strong";
}

// Committee names bury the person name inside designator text ("Citizens for
// Jane B Doe for Governor", "Jane B Doe 2026"). The trailing tokens block
// first+last alignment, so a contradicting middle would produce no evidence
// and slip through a committee-name match. Callers vetoing against a
// committee name expand it into candidate row-name strings first: the raw
// name, the name with 4-digit years removed, and each "for"-delimited
// segment — the person-name segment then aligns cleanly. Extra segments can
// only add aligned evidence, never block alignment that already existed.
export function committeeNameMiddleEvidenceRowNames(raw: string): string[] {
  const names = new Set<string>();
  const trimmed = raw.trim();
  if (!trimmed) {
    return [];
  }
  names.add(trimmed);
  const withoutYears = trimmed
    .replace(/\b(?:19|20)\d{2}\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (withoutYears) {
    names.add(withoutYears);
    for (const segment of withoutYears.split(/\bfor\b/i)) {
      const trimmedSegment = segment.trim();
      if (trimmedSegment) {
        names.add(trimmedSegment);
      }
    }
  }
  return [...names];
}

export type MiddleNameConflictInput = {
  candidateName: string;
  rowNames: readonly string[];
  normalizePersonName: NormalizePersonName;
  // Nickname-aware states (alaska/connecticut/illinois/texas pattern) pass
  // their one-sided candidate→row first-name equivalence here so a middle
  // conflict on "Mike A. Smith" vs "SMITH, MICHAEL B" still registers.
  // Defaults to strict equality.
  firstNamesEquivalent?: (candidateFirst: string, rowFirst: string) => boolean;
};

// Aggregated middle evidence across aligned candidate/row variant pairs.
// A conflict on an EXACT pair — one whose surname boundary is explicit
// (comma form) rather than guessed, which pins the aligned parse on the
// other side too — is authoritative and is collected regardless of surname
// length: an ambiguous space-form split elsewhere must never override it
// ("Smith, John B. A." conflicts with "John A. Smith" no matter how a
// sibling "John B A Smith" row re-splits). Otherwise only pairs aligned on
// the LONGEST matching surname interpretation carry evidence: a shorter
// split misreads part of a compound surname as a middle name and can
// manufacture a conflict ("Mary [VAN] DYKE" vs "Mary [B VAN] DYKE"
// conflicts, while the true "VAN DYKE" alignment is a clean weak fallback).
// The wrong split still can never manufacture AGREEMENT — first and last
// must align token-for-token — so preferring the most-specific alignment
// only discards manufactured contradictions.
function collectMiddleEvidence(input: MiddleNameConflictInput): {
  sawStrong: boolean;
  sawWeak: boolean;
  sawConflict: boolean;
  sawExactConflict: boolean;
} {
  const firstNamesEquivalent =
    input.firstNamesEquivalent ?? ((candidateFirst: string, rowFirst: string) => candidateFirst === rowFirst);
  const candidateVariants = personNameParseVariants(input.candidateName, input.normalizePersonName);
  let surnameTokenCount = 0;
  let sawStrong = false;
  let sawWeak = false;
  let sawConflict = false;
  let sawExactConflict = false;
  for (const rowName of input.rowNames) {
    for (const rowVariant of personNameParseVariants(rowName, input.normalizePersonName)) {
      for (const candidateVariant of candidateVariants) {
        if (candidateVariant.last !== rowVariant.last) {
          continue;
        }
        if (!firstNamesEquivalent(candidateVariant.first, rowVariant.first)) {
          continue;
        }
        const evidence = middleNameEvidence(candidateVariant.middles, rowVariant.middles);
        if (evidence === "conflict" && (candidateVariant.exact || rowVariant.exact)) {
          sawExactConflict = true;
          continue;
        }
        const pairSurnameTokenCount = rowVariant.last.split(" ").length;
        if (pairSurnameTokenCount < surnameTokenCount) {
          continue;
        }
        if (pairSurnameTokenCount > surnameTokenCount) {
          surnameTokenCount = pairSurnameTokenCount;
          sawStrong = false;
          sawWeak = false;
          sawConflict = false;
        }
        if (evidence === "strong") {
          sawStrong = true;
        } else if (evidence === "conflict") {
          sawConflict = true;
        } else {
          sawWeak = true;
        }
      }
    }
  }
  return { sawStrong, sawWeak, sawConflict, sawExactConflict };
}

// True when the candidate and row names align on first+last, at least one
// aligned pair carries contradicting middle names, and no aligned pair
// corroborates the middle. Callers apply this AFTER their existing key-overlap
// match, as a rejection gate:
//
//   "John A. Smith" vs "Smith, John B."  -> true  (reject the row)
//   "John A. Smith" vs "Smith, John Andrew" -> false (initial corroborates)
//   "John Smith"    vs "Smith, John B."  -> false (a side lacks middle info)
//
// When no variant pair aligns at all (committee-name keys, nickname keys the
// caller did not surface here) there is no middle evidence either way and the
// state's key-overlap verdict stands.
export function hasMiddleNameConflict(input: MiddleNameConflictInput): boolean {
  const evidence = collectMiddleEvidence(input);
  return !evidence.sawStrong && (evidence.sawConflict || evidence.sawExactConflict);
}

// Full match verdict (the georgia aggregation), for states whose key sets
// carry NO first+last collapse (colorado pattern: full-string and comma-flip
// keys only). Those states have the opposite problem — "John A. Smith" never
// matches "Smith, John" at all — so they call this as a RECALL fallback after
// their exact-key match misses: any strong pair matches; otherwise any middle
// conflict rejects; otherwise a first+last alignment with middle information
// missing on a side matches. No alignment at all is no match — this function
// never widens beyond first+last agreement.
export function personNamesMatchWithMiddleEvidence(input: MiddleNameConflictInput): boolean {
  const evidence = collectMiddleEvidence(input);
  return (
    evidence.sawStrong ||
    (evidence.sawWeak && !evidence.sawConflict && !evidence.sawExactConflict)
  );
}
