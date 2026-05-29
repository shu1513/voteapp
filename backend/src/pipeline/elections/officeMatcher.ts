import type { PoolClient } from "pg";

import { getStateNameByAbbreviation } from "../../constants/usStates.js";
import type { ElectionDistrictType } from "../../types/election.js";
import { normalizeElectionTitleKey } from "../../utils/normalizeElectionTitleKey.js";

type OfficeAliasRow = {
  office_id: string;
  normalized_alias: string;
};

type OfficeRow = {
  id: string;
  canonical_name: string;
};

type OfficeCandidate = {
  id: string;
  canonicalName: string;
  canonicalMatcherKey: string;
  canonicalTokens: string[];
};

type OfficeMatchInput = {
  scope: ElectionDistrictType;
  districtName: string;
  state: string;
  officialBallotTitle: string;
};

type OfficeMatchMethod = "alias_exact" | "deterministic_fallback" | "none" | "ambiguous";

export type OfficeMatchResult = {
  officeId: string | null;
  method: OfficeMatchMethod;
  confidence: number;
  normalizedAlias: string;
  aliasMemoryKey: string;
  shouldPersistAlias: boolean;
};

const MIN_CONFIDENCE = 0.56;
const MIN_MARGIN = 0.12;
// Tuned so a plain partial token overlap (around F1 ~= 0.5) is rejected unless boosted by stronger
// canonical phrase agreement. This keeps deterministic fallback conservative.
const STOPWORDS = new Set([
  "of",
  "the",
  "and",
  "for",
  "in",
  "to",
  "primary",
  "general",
  "runoff",
  "special",
  "election",
  "vacancy",
  "unexpired",
  "term",
]);

function normalizeMatcherText(value: string): string {
  return value
    .toLowerCase()
    .replace(/\bu\.?\s*s\.?\b/g, "united states")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function stripJurisdictionPrefixes(value: string, input: { districtName: string; state: string }): string {
  let next = value;

  const districtName = normalizeMatcherText(input.districtName);
  if (districtName.length > 0) {
    const districtPattern = new RegExp(`\\b${escapeRegExp(districtName)}\\b`, "g");
    next = next.replace(districtPattern, " ");
  }

  const stateLower = input.state.trim().toLowerCase();
  if (stateLower.length > 0) {
    if (/^[a-z]{2}$/.test(stateLower)) {
      const stateName = getStateNameByAbbreviation(stateLower);
      if (stateName) {
        const normalizedStateName = normalizeMatcherText(stateName);
        const stateNamePattern = new RegExp(`\\b${escapeRegExp(normalizedStateName)}\\b`, "g");
        next = next.replace(stateNamePattern, " ");
      }
      const stateAbbrevPattern = new RegExp(`\\b${escapeRegExp(stateLower)}\\b`, "g");
      next = next.replace(stateAbbrevPattern, " ");
    } else {
      const normalizedStateName = normalizeMatcherText(stateLower);
      if (normalizedStateName.length > 0) {
        const stateNamePattern = new RegExp(`\\b${escapeRegExp(normalizedStateName)}\\b`, "g");
        next = next.replace(stateNamePattern, " ");
      }
    }
  }

  next = next
    .replace(/\bstate of\b/g, " ")
    .replace(/\bcounty of\b/g, " ")
    .replace(/\bcity of\b/g, " ");

  return next.replace(/\s+/g, " ").trim();
}

function stripSeatSuffixes(value: string): string {
  return value
    .replace(/\boffice no \d+\b/g, " ")
    .replace(/\bdistrict \d+[a-z]{0,2}\b/g, " ")
    .replace(/\b\d+(st|nd|rd|th)\s+district\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function toMatcherTokens(value: string): string[] {
  return value
    .split(" ")
    .map((token) => token.trim())
    .filter((token) => token.length > 0 && !STOPWORDS.has(token));
}

function toMatcherKeyFromBallotTitle(input: OfficeMatchInput): string {
  const normalized = normalizeMatcherText(input.officialBallotTitle);
  const withoutJurisdiction = stripJurisdictionPrefixes(normalized, {
    districtName: input.districtName,
    state: input.state,
  });
  const withoutSeat = stripSeatSuffixes(withoutJurisdiction);
  return withoutSeat.length > 0 ? withoutSeat : normalized;
}

function toMatcherKeyFromCanonicalName(canonicalName: string): string {
  const normalized = normalizeMatcherText(canonicalName);
  const withoutSeat = stripSeatSuffixes(normalized);
  return withoutSeat.length > 0 ? withoutSeat : normalized;
}

function hasPhrase(text: string, phrase: string): boolean {
  if (!text || !phrase) {
    return false;
  }
  return new RegExp(`\\b${escapeRegExp(phrase)}\\b`).test(text);
}

function scoreOfficeMatch(titleMatcherKey: string, titleTokens: string[], office: OfficeCandidate): number {
  if (titleTokens.length === 0 || office.canonicalTokens.length === 0) {
    return 0;
  }

  const titleSet = new Set(titleTokens);
  let intersectionCount = 0;
  for (const token of office.canonicalTokens) {
    if (titleSet.has(token)) {
      intersectionCount += 1;
    }
  }

  if (intersectionCount === 0) {
    return 0;
  }

  const precision = intersectionCount / titleTokens.length;
  const recall = intersectionCount / office.canonicalTokens.length;
  const f1 = (2 * precision * recall) / (precision + recall);

  let score = f1;

  if (titleMatcherKey === office.canonicalMatcherKey) {
    score += 0.25;
  } else if (hasPhrase(titleMatcherKey, office.canonicalMatcherKey)) {
    score += 0.12;
  }

  if (hasPhrase(titleMatcherKey, "lieutenant governor")) {
    if (hasPhrase(office.canonicalMatcherKey, "lieutenant governor")) {
      score += 0.2;
    } else if (hasPhrase(office.canonicalMatcherKey, "governor")) {
      score -= 0.35;
    }
  }

  if (hasPhrase(titleMatcherKey, "united states senator")) {
    if (hasPhrase(office.canonicalMatcherKey, "state senator")) {
      score -= 0.4;
    }
  }

  if (hasPhrase(titleMatcherKey, "state senator")) {
    if (hasPhrase(office.canonicalMatcherKey, "united states senator")) {
      score -= 0.4;
    }
  }

  if (hasPhrase(titleMatcherKey, "united states representative")) {
    if (hasPhrase(office.canonicalMatcherKey, "state representative")) {
      score -= 0.4;
    }
  }

  if (hasPhrase(titleMatcherKey, "state representative")) {
    if (hasPhrase(office.canonicalMatcherKey, "united states representative")) {
      score -= 0.4;
    }
  }

  return score;
}

export class OfficeMatcher {
  private readonly aliasByScope = new Map<ElectionDistrictType, Map<string, string>>();
  private readonly officesByScope = new Map<ElectionDistrictType, OfficeCandidate[]>();

  constructor(private readonly client: Pick<PoolClient, "query">) {}

  private async loadAliases(scope: ElectionDistrictType): Promise<Map<string, string>> {
    const cached = this.aliasByScope.get(scope);
    if (cached) {
      return cached;
    }

    const result = await this.client.query<OfficeAliasRow>(
      `
        SELECT office_id, normalized_alias
        FROM public.office_title_aliases
        WHERE scope = $1
      `,
      [scope]
    );

    const aliasMap = new Map<string, string>();
    for (const row of result.rows ?? []) {
      aliasMap.set(row.normalized_alias, row.office_id);
    }
    this.aliasByScope.set(scope, aliasMap);
    return aliasMap;
  }

  private async loadOffices(scope: ElectionDistrictType): Promise<OfficeCandidate[]> {
    const cached = this.officesByScope.get(scope);
    if (cached) {
      return cached;
    }

    const result = await this.client.query<OfficeRow>(
      `
        SELECT id, canonical_name
        FROM public.offices
        WHERE scope = $1
      `,
      [scope]
    );

    const offices = (result.rows ?? []).map((row) => {
      const matcherKey = toMatcherKeyFromCanonicalName(row.canonical_name);
      return {
        id: row.id,
        canonicalName: row.canonical_name,
        canonicalMatcherKey: matcherKey,
        canonicalTokens: toMatcherTokens(matcherKey),
      };
    });

    this.officesByScope.set(scope, offices);
    return offices;
  }

  rememberAlias(scope: ElectionDistrictType, normalizedAlias: string, officeId: string): void {
    if (!normalizedAlias) {
      return;
    }
    const existing = this.aliasByScope.get(scope);
    if (!existing) {
      return;
    }
    existing.set(normalizedAlias, officeId);
  }

  async resolve(input: OfficeMatchInput): Promise<OfficeMatchResult> {
    const normalizedAlias = normalizeElectionTitleKey(input.officialBallotTitle);
    if (normalizedAlias.length === 0) {
      return {
        officeId: null,
        method: "none",
        confidence: 0,
        normalizedAlias,
        aliasMemoryKey: "",
        shouldPersistAlias: false,
      };
    }

    const aliases = await this.loadAliases(input.scope);
    const titleMatcherKey = toMatcherKeyFromBallotTitle(input);
    let exactOfficeId = aliases.get(normalizedAlias);
    if (!exactOfficeId && titleMatcherKey.length > 0 && titleMatcherKey !== normalizedAlias) {
      exactOfficeId = aliases.get(titleMatcherKey);
    }
    if (exactOfficeId) {
      return {
        officeId: exactOfficeId,
        method: "alias_exact",
        confidence: 1,
        normalizedAlias,
        aliasMemoryKey: titleMatcherKey.length > 0 ? titleMatcherKey : normalizedAlias,
        shouldPersistAlias: false,
      };
    }

    const offices = await this.loadOffices(input.scope);
    if (offices.length === 0) {
      return {
        officeId: null,
        method: "none",
        confidence: 0,
        normalizedAlias,
        aliasMemoryKey: titleMatcherKey,
        shouldPersistAlias: false,
      };
    }

    const titleTokens = toMatcherTokens(titleMatcherKey);
    const scored = offices
      .map((office) => ({
        officeId: office.id,
        score: scoreOfficeMatch(titleMatcherKey, titleTokens, office),
      }))
      .sort((a, b) => b.score - a.score);

    const top = scored[0];
    if (!top || top.score < MIN_CONFIDENCE) {
      return {
        officeId: null,
        method: "none",
        confidence: top?.score ?? 0,
        normalizedAlias,
        aliasMemoryKey: titleMatcherKey,
        shouldPersistAlias: false,
      };
    }

    const second = scored[1];
    if (second && top.score - second.score < MIN_MARGIN) {
      return {
        officeId: null,
        method: "ambiguous",
        confidence: top.score,
        normalizedAlias,
        aliasMemoryKey: titleMatcherKey,
        shouldPersistAlias: false,
      };
    }

    return {
      officeId: top.officeId,
      method: "deterministic_fallback",
      confidence: top.score,
      normalizedAlias,
      aliasMemoryKey: titleMatcherKey,
      shouldPersistAlias: true,
    };
  }
}
