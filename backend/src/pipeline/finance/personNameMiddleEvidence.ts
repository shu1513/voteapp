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
    return [{ first: restTokens[0]!, middles: restTokens.slice(1), last }];
  }
  const tokens = normalizePersonName(raw).split(" ").filter(Boolean);
  if (tokens.length === 0) {
    return [];
  }
  if (tokens.length === 1) {
    return [{ first: tokens[0]!, middles: [], last: tokens[0]! }];
  }
  const parses: ParsedPersonName[] = [];
  for (let lastStart = 1; lastStart < tokens.length; lastStart += 1) {
    parses.push({
      first: tokens[0]!,
      middles: tokens.slice(1, lastStart),
      last: tokens.slice(lastStart).join(" "),
    });
  }
  return parses;
}

// Expands a display name into parse variants: the name outside parentheses
// plus each parenthetical alias ("LEE, Bill (Bill Lee)") parsed on its own.
export function personNameParseVariants(
  value: string,
  normalizePersonName: NormalizePersonName
): ParsedPersonName[] {
  const variants: ParsedPersonName[] = [];
  variants.push(...parsePersonNameCandidates(value.replace(/\([^()]+\)/g, " "), normalizePersonName));
  for (const match of value.matchAll(/\(([^()]+)\)/g)) {
    if (match[1]) {
      variants.push(...parsePersonNameCandidates(match[1], normalizePersonName));
    }
  }
  return variants;
}

// Middle-name evidence between two parses whose first and last already agree:
// "strong" when the middles corroborate (equal, or an initial matching the
// full form), "conflict" when both sides carry middles that disagree, "weak"
// when at least one side has no middle information.
export function middleNameEvidence(a: string[], b: string[]): "strong" | "weak" | "conflict" {
  if (a.length === 0 || b.length === 0) {
    return "weak";
  }
  const tokenA = a[0]!;
  const tokenB = b[0]!;
  if (tokenA === tokenB) {
    return "strong";
  }
  if (tokenA.length === 1 && tokenB.startsWith(tokenA)) {
    return "strong";
  }
  if (tokenB.length === 1 && tokenA.startsWith(tokenB)) {
    return "strong";
  }
  return "conflict";
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
  const firstNamesEquivalent =
    input.firstNamesEquivalent ?? ((candidateFirst: string, rowFirst: string) => candidateFirst === rowFirst);
  const candidateVariants = personNameParseVariants(input.candidateName, input.normalizePersonName);
  let sawConflict = false;
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
        if (evidence === "strong") {
          return false;
        }
        if (evidence === "conflict") {
          sawConflict = true;
        }
      }
    }
  }
  return sawConflict;
}
