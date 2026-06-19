export type CalAccessCampaignCoverRow = Record<string, string | null | undefined>;
export type CalAccessFilerNameRow = Record<string, string | null | undefined>;

export type CaliforniaCandidateCommitteeResolverInput = {
  candidateName: string;
  officeName: string;
  electionYear: number;
  campaignCoverRows: readonly CalAccessCampaignCoverRow[];
  filerNameRows?: readonly CalAccessFilerNameRow[];
  sourceUrl?: string | null;
};

export type CaliforniaCandidateCommitteeMatch = {
  controlledCommitteeId: string;
  controlledCommitteeName: string;
  confidence: "exact";
  source: "cal_access";
  sourceUrl: string | null;
  matchedCoverRowCount: number;
};

export type CaliforniaCandidateCommitteeResolution =
  | ({ status: "matched" } & CaliforniaCandidateCommitteeMatch)
  | {
      status: "unmatched";
      reason: "no_candidate_office_year_match";
      candidateNameNormalized: string;
      officeNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_committees";
      candidateNameNormalized: string;
      officeNameNormalized: string;
      matches: CaliforniaCandidateCommitteeMatch[];
    };

const CAL_ACCESS_OFFICE_CODE_LABELS: Record<string, readonly string[]> = {
  GOV: ["governor"],
  LTG: ["lieutenant governor", "lt governor"],
  SOS: ["secretary of state"],
  ATG: ["attorney general"],
  CON: ["controller", "state controller", "comptroller"],
  TRE: ["treasurer", "state treasurer"],
  INS: ["insurance commissioner"],
  SPI: ["superintendent of public instruction", "superintendent"],
  SEN: ["state senator", "state senate"],
  ASM: ["assembly", "state assembly", "member of the state assembly", "assembly member"],
  BOE: ["board of equalization"],
};

function requireNonEmpty(value: string, label: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${label} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2001 || value > 2100) {
    throw new Error(`Invalid California committee resolver election year: ${value}`);
  }
  return value;
}

function value(row: Record<string, string | null | undefined>, key: string): string {
  return row[key]?.trim() ?? "";
}

function normalizeText(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|OF|FOR)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string): string {
  return normalizeText(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function nameKeysFromDisplayName(input: string): Set<string> {
  const normalized = normalizePersonName(input);
  const keys = new Set<string>();
  if (normalized.length === 0) {
    return keys;
  }
  keys.add(normalized);

  const commaParts = input
    .split(",")
    .map((part) => normalizePersonName(part))
    .filter(Boolean);
  if (commaParts.length === 2) {
    keys.add(`${commaParts[1]} ${commaParts[0]}`.replace(/\s+/g, " ").trim());
    const firstParts = commaParts[1]?.split(" ").filter(Boolean) ?? [];
    const lastParts = commaParts[0]?.split(" ").filter(Boolean) ?? [];
    if (firstParts.length > 0 && lastParts.length > 0) {
      keys.add(`${firstParts[0]} ${lastParts[lastParts.length - 1]}`);
    }
  }

  const parts = normalized.split(" ").filter(Boolean);
  if (parts.length >= 2) {
    keys.add(`${parts[0]} ${parts[parts.length - 1]}`);
  }

  return keys;
}

function candidateNameFromCoverRow(row: CalAccessCampaignCoverRow): string {
  return [value(row, "CAND_NAMF"), value(row, "CAND_NAMT"), value(row, "CAND_NAML")]
    .filter(Boolean)
    .join(" ");
}

function candidateMatches(inputKeys: Set<string>, row: CalAccessCampaignCoverRow): boolean {
  if (inputKeys.size === 0) {
    return false;
  }
  for (const key of nameKeysFromDisplayName(candidateNameFromCoverRow(row))) {
    if (inputKeys.has(key)) {
      return true;
    }
  }
  return false;
}

function officeKeysFromName(input: string): Set<string> {
  const normalized = normalizeText(input);
  const keys = new Set<string>();
  if (normalized.length > 0) {
    keys.add(normalized);
  }
  for (const [officeCode, labels] of Object.entries(CAL_ACCESS_OFFICE_CODE_LABELS)) {
    if (labels.some((label) => normalizeText(label) === normalized)) {
      keys.add(officeCode);
      for (const label of labels) {
        keys.add(normalizeText(label));
      }
    }
  }
  return keys;
}

function officeKeysFromCoverRow(row: CalAccessCampaignCoverRow): Set<string> {
  const keys = new Set<string>();
  const officeDescription = normalizeText(value(row, "OFFIC_DSCR"));
  const officeCode = normalizeText(value(row, "OFFICE_CD"));
  if (officeDescription.length > 0) {
    keys.add(officeDescription);
  }
  if (officeCode.length > 0) {
    keys.add(officeCode);
    for (const label of CAL_ACCESS_OFFICE_CODE_LABELS[officeCode] ?? []) {
      keys.add(normalizeText(label));
    }
  }
  return keys;
}

function officeMatches(inputKeys: Set<string>, row: CalAccessCampaignCoverRow): boolean {
  const rowKeys = officeKeysFromCoverRow(row);
  for (const key of rowKeys) {
    if (inputKeys.has(key)) {
      return true;
    }
  }
  return false;
}

function parseCalAccessDateYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch) {
    return Number(slashMatch[3]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch) {
    return Number(isoMatch[1]);
  }
  return null;
}

function electionYearMatches(row: CalAccessCampaignCoverRow, electionYear: number): boolean {
  return parseCalAccessDateYear(value(row, "ELECT_DATE")) === electionYear;
}

function committeeIdFromCoverRow(row: CalAccessCampaignCoverRow): string {
  return value(row, "FILER_ID") || value(row, "CMTTE_ID");
}

function committeeNameFromFilerRow(row: CalAccessFilerNameRow): string {
  return [value(row, "NAMF"), value(row, "NAMT"), value(row, "NAML")].filter(Boolean).join(" ");
}

function buildFilerNameLookup(rows: readonly CalAccessFilerNameRow[]): Map<string, string> {
  const lookup = new Map<string, string>();
  for (const row of rows) {
    const filerId = value(row, "FILER_ID");
    if (!filerId || lookup.has(filerId)) {
      continue;
    }
    const name = committeeNameFromFilerRow(row);
    if (name) {
      lookup.set(filerId, name);
    }
  }
  return lookup;
}

function committeeNameForMatch(input: {
  committeeId: string;
  rows: readonly CalAccessCampaignCoverRow[];
  filerNames: Map<string, string>;
}): string {
  for (const row of input.rows) {
    const filerName = value(row, "FILER_NAML");
    if (filerName) {
      return filerName;
    }
  }
  return input.filerNames.get(input.committeeId) ?? input.committeeId;
}

function toCommitteeMatch(input: {
  committeeId: string;
  rows: readonly CalAccessCampaignCoverRow[];
  filerNames: Map<string, string>;
  sourceUrl: string | null;
}): CaliforniaCandidateCommitteeMatch {
  return {
    controlledCommitteeId: input.committeeId,
    controlledCommitteeName: committeeNameForMatch(input),
    confidence: "exact",
    source: "cal_access",
    sourceUrl: input.sourceUrl,
    matchedCoverRowCount: input.rows.length,
  };
}

export function resolveCaliforniaCandidateCommittee(
  input: CaliforniaCandidateCommitteeResolverInput
): CaliforniaCandidateCommitteeResolution {
  const candidateName = requireNonEmpty(input.candidateName, "California candidate committee resolver candidateName");
  const officeName = requireNonEmpty(input.officeName, "California candidate committee resolver officeName");
  const electionYear = normalizeElectionYear(input.electionYear);
  const candidateNameKeys = nameKeysFromDisplayName(candidateName);
  const officeKeys = officeKeysFromName(officeName);
  const candidateNameNormalized = [...candidateNameKeys][0] ?? normalizePersonName(candidateName);
  const officeNameNormalized = [...officeKeys][0] ?? normalizeText(officeName);
  const rowsByCommittee = new Map<string, CalAccessCampaignCoverRow[]>();

  for (const row of input.campaignCoverRows) {
    if (!candidateMatches(candidateNameKeys, row)) {
      continue;
    }
    if (!officeMatches(officeKeys, row)) {
      continue;
    }
    if (!electionYearMatches(row, electionYear)) {
      continue;
    }

    const committeeId = committeeIdFromCoverRow(row);
    if (!committeeId) {
      continue;
    }
    const rows = rowsByCommittee.get(committeeId) ?? [];
    rows.push(row);
    rowsByCommittee.set(committeeId, rows);
  }

  if (rowsByCommittee.size === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_office_year_match",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }

  const filerNames = buildFilerNameLookup(input.filerNameRows ?? []);
  const matches = [...rowsByCommittee.entries()]
    .map(([committeeId, rows]) =>
      toCommitteeMatch({
        committeeId,
        rows,
        filerNames,
        sourceUrl: input.sourceUrl ?? null,
      })
    )
    .sort((left, right) => left.controlledCommitteeId.localeCompare(right.controlledCommitteeId));

  if (matches.length === 1) {
    return {
      status: "matched",
      ...matches[0],
    };
  }

  return {
    status: "ambiguous",
    reason: "multiple_matching_committees",
    candidateNameNormalized,
    officeNameNormalized,
    matches,
  };
}
