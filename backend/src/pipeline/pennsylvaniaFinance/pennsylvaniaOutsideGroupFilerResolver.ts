import type { PennsylvaniaCampaignFinanceFilerRow } from "./pennsylvaniaCampaignFinanceReader.js";

export type PennsylvaniaOutsideGroupFilerMatch = {
  filerId: string;
  filerName: string;
  filerType: string | null;
  confidence: "exact";
  source: "pa_bulk";
  matchedFilerRowCount: number;
};

export type PennsylvaniaOutsideGroupFilerResolution =
  | ({ status: "matched" } & PennsylvaniaOutsideGroupFilerMatch)
  | {
      status: "unmatched";
      reason: "missing_organization_name" | "no_filer_match";
      organizationNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_filers";
      organizationNameNormalized: string;
      matches: PennsylvaniaOutsideGroupFilerMatch[];
    };

export type PennsylvaniaOutsideGroupFilerAlias = {
  organizationName: string;
  filerName: string;
};

const DEFAULT_OUTSIDE_GROUP_ALIASES: readonly PennsylvaniaOutsideGroupFilerAlias[] = [
  { organizationName: "AFL-CIO", filerName: "AFL CIO" },
  { organizationName: "SEIU PA", filerName: "SEIU PENNSYLVANIA" },
  { organizationName: "PSEA", filerName: "PENNSYLVANIA STATE EDUCATION ASSOCIATION" },
  { organizationName: "PA STATE EDUCATION ASSOCIATION", filerName: "PENNSYLVANIA STATE EDUCATION ASSOCIATION" },
  { organizationName: "LCV", filerName: "LEAGUE OF CONSERVATION VOTERS" },
];

type FilerAccumulator = {
  filerId: string;
  filerName: string;
  filerType: string | null;
  rows: PennsylvaniaCampaignFinanceFilerRow[];
};

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

export function normalizePennsylvaniaOutsideGroupNameKey(value: string | null | undefined): string {
  return normalizeTextKey(value)
    .replace(/\b(THE|COMMITTEE|PAC|POLITICAL ACTION COMMITTEE|INC|INCORPORATED)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function buildAliasMap(
  aliases: readonly PennsylvaniaOutsideGroupFilerAlias[] | undefined
): Map<string, string> {
  const result = new Map<string, string>();
  for (const alias of [...DEFAULT_OUTSIDE_GROUP_ALIASES, ...(aliases ?? [])]) {
    const organizationKey = normalizePennsylvaniaOutsideGroupNameKey(alias.organizationName);
    const filerKey = normalizePennsylvaniaOutsideGroupNameKey(alias.filerName);
    if (organizationKey && filerKey) {
      result.set(organizationKey, filerKey);
    }
  }
  return result;
}

function toFilerMatch(accumulator: FilerAccumulator): PennsylvaniaOutsideGroupFilerMatch {
  return {
    filerId: accumulator.filerId,
    filerName: accumulator.filerName,
    filerType: accumulator.filerType,
    confidence: "exact",
    source: "pa_bulk",
    matchedFilerRowCount: accumulator.rows.length,
  };
}

export function resolvePennsylvaniaOutsideGroupFiler(input: {
  organizationName: string;
  filerRows: readonly PennsylvaniaCampaignFinanceFilerRow[];
  aliases?: readonly PennsylvaniaOutsideGroupFilerAlias[];
}): PennsylvaniaOutsideGroupFilerResolution {
  const organizationNameNormalized = normalizePennsylvaniaOutsideGroupNameKey(input.organizationName);
  if (!organizationNameNormalized) {
    return {
      status: "unmatched",
      reason: "missing_organization_name",
      organizationNameNormalized,
    };
  }

  const aliasMap = buildAliasMap(input.aliases);
  const searchKeys = new Set([organizationNameNormalized]);
  const aliasKey = aliasMap.get(organizationNameNormalized);
  if (aliasKey) {
    searchKeys.add(aliasKey);
  }

  const rowsByFiler = new Map<string, FilerAccumulator>();
  for (const row of input.filerRows) {
    const filerId = row.FILERID.trim().toUpperCase();
    const filerName = row.FILERNAME.trim();
    if (!filerId || !filerName) {
      continue;
    }
    const filerNameKey = normalizePennsylvaniaOutsideGroupNameKey(filerName);
    if (!searchKeys.has(filerNameKey)) {
      continue;
    }
    const accumulator = rowsByFiler.get(filerId) ?? {
      filerId,
      filerName,
      filerType: row.FILERTYPE.trim() || null,
      rows: [],
    };
    accumulator.rows.push(row);
    rowsByFiler.set(filerId, accumulator);
  }

  if (rowsByFiler.size === 0) {
    return {
      status: "unmatched",
      reason: "no_filer_match",
      organizationNameNormalized,
    };
  }

  const matches = [...rowsByFiler.values()]
    .map(toFilerMatch)
    .sort((left, right) => left.filerId.localeCompare(right.filerId));

  if (matches.length === 1) {
    return {
      status: "matched",
      ...matches[0],
    };
  }

  return {
    status: "ambiguous",
    reason: "multiple_matching_filers",
    organizationNameNormalized,
    matches,
  };
}
