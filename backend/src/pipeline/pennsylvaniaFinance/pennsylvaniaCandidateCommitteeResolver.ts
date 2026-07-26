import { normalizePennsylvaniaCampaignFinanceExportYear } from "./pennsylvaniaCampaignFinanceArtifactCache.js";
import type { PennsylvaniaCampaignFinanceFilerRow } from "./pennsylvaniaCampaignFinanceReader.js";
import {
  mapPennsylvaniaFinanceOffice,
  mapPennsylvaniaFinanceOfficeCode,
  normalizePennsylvaniaFinanceLegislativeDistrict,
  toPennsylvaniaFinanceOfficeSearchInput,
  type PennsylvaniaFinanceOfficeSearchInput,
} from "./pennsylvaniaFinanceEligibleOffices.js";

export type PennsylvaniaCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  filerRows: readonly PennsylvaniaCampaignFinanceFilerRow[];
  sourceUrl?: string | null;
};

export type PennsylvaniaCandidateCommitteeMatch = {
  filerId: string;
  filerName: string;
  filerType: string | null;
  confidence: "exact";
  source: "pa_bulk";
  sourceUrl: string | null;
  matchedFilerRowCount: number;
};

export type PennsylvaniaCandidateCommitteeResolution =
  | ({ status: "matched" } & PennsylvaniaCandidateCommitteeMatch)
  | {
      status: "unmatched";
      reason:
        | "missing_candidate_name"
        | "unsupported_office"
        | "missing_legislative_district"
        | "no_candidate_filer_match";
      candidateNameNormalized: string;
      officeNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_filers";
      candidateNameNormalized: string;
      officeNameNormalized: string;
      matches: PennsylvaniaCandidateCommitteeMatch[];
    };

type CandidateFilerAccumulator = {
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

function normalizeCommitteeTextKey(value: string | null | undefined): string {
  return normalizeTextKey(value)
    .replace(/\b(THE|OF|FOR|COMMITTEE|FRIENDS|TO|ELECT|CITIZENS|CAMPAIGN|PEOPLE|PENNSYLVANIANS|PA|INC)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string | null | undefined): string {
  return normalizeTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizePennsylvaniaCandidateNameKeys(value: string): Set<string> {
  const trimmed = value.trim();
  const keys = new Set<string>();

  function addName(raw: string): void {
    const hasComma = raw.includes(",");
    const normalized = normalizePersonName(raw);
    if (normalized) {
      keys.add(normalized);
    }

    const parts = normalized.split(" ").filter(Boolean);
    if (!hasComma && parts.length >= 2) {
      keys.add(`${parts[0]} ${parts[parts.length - 1]}`);
    }

    const commaParts = raw
      .split(",")
      .map((part) => normalizePersonName(part))
      .filter(Boolean);
    if (commaParts.length >= 2) {
      const lastName = commaParts[0] ?? "";
      const firstNames = commaParts.slice(1).join(" ").trim();
      const flipped = normalizePersonName(`${firstNames} ${lastName}`);
      if (flipped) {
        keys.add(flipped);
        const flippedParts = flipped.split(" ").filter(Boolean);
        if (flippedParts.length >= 2) {
          keys.add(`${flippedParts[0]} ${flippedParts[flippedParts.length - 1]}`);
        }
      }
    }
  }

  addName(trimmed.replace(/\([^()]+\)/g, " "));
  for (const match of trimmed.matchAll(/\(([^()]+)\)/g)) {
    if (match[1]) {
      addName(match[1]);
    }
  }

  return keys;
}

function candidateNameNormalized(value: string): string {
  return [...normalizePennsylvaniaCandidateNameKeys(value)][0] ?? normalizePersonName(value);
}

export function normalizePennsylvaniaCandidateNameForStorage(value: string): string {
  return candidateNameNormalized(value);
}

function stripFilerWrapper(value: string): string {
  return value
    .replace(/\bC\/O\b.*$/i, " ")
    .replace(/\bCARE OF\b.*$/i, " ")
    .replace(/\bTREASURER\b.*$/i, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function filerNameKeys(row: PennsylvaniaCampaignFinanceFilerRow): Set<string> {
  const keys = new Set<string>();
  const raw = row.FILERNAME.trim();
  const stripped = stripFilerWrapper(raw);
  for (const candidate of [raw, stripped]) {
    for (const key of normalizePennsylvaniaCandidateNameKeys(candidate)) {
      keys.add(key);
    }
    const committeeKey = normalizeCommitteeTextKey(candidate);
    if (committeeKey) {
      keys.add(committeeKey);
    }
  }
  return keys;
}

function tokensContainCandidateName(input: {
  candidateNameKeys: ReadonlySet<string>;
  filerKeys: ReadonlySet<string>;
}): boolean {
  for (const candidateKey of input.candidateNameKeys) {
    const candidateTokens = candidateKey.split(" ").filter(Boolean);
    if (candidateTokens.length < 2) {
      continue;
    }
    for (const filerKey of input.filerKeys) {
      const filerTokens = filerKey.split(" ").filter(Boolean);
      for (let index = 0; index <= filerTokens.length - candidateTokens.length; index += 1) {
        if (candidateTokens.every((token, offset) => filerTokens[index + offset] === token)) {
          return true;
        }
      }
    }
  }
  return false;
}

function rowMatchesCandidateName(input: {
  row: PennsylvaniaCampaignFinanceFilerRow;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const keys = filerNameKeys(input.row);
  for (const key of keys) {
    if (input.candidateNameKeys.has(key)) {
      return true;
    }
  }
  return tokensContainCandidateName({ candidateNameKeys: input.candidateNameKeys, filerKeys: keys });
}

function isLikelyCandidateFiler(row: PennsylvaniaCampaignFinanceFilerRow): boolean {
  if (!row.FILERID.trim() || !row.FILERNAME.trim()) {
    return false;
  }
  const name = normalizeTextKey(row.FILERNAME);
  if (/\b(?:PAC|POLITICAL ACTION|PARTY|CAUCUS|BALLOT|REFERENDUM|SUPER PAC|SUPERPAC)\b/.test(name)) {
    return false;
  }
  return true;
}

function parseYear(raw: string): number | null {
  const normalized = raw.trim();
  if (!/^\d{4}$/.test(normalized)) {
    return null;
  }
  return Number.parseInt(normalized, 10);
}

function rowMatchesElectionYear(row: PennsylvaniaCampaignFinanceFilerRow, electionYear: number): boolean {
  const rowYear = parseYear(row.EYEAR);
  return rowYear === null || rowYear === electionYear;
}

function zip5(value: string): string {
  return value.replace(/[^0-9]/g, "").slice(0, 5);
}

function phoneDigits(value: string): string {
  return value.replace(/[^0-9]/g, "");
}

function rowMatchesOfficeContext(input: {
  row: PennsylvaniaCampaignFinanceFilerRow;
  officeSearchInput: PennsylvaniaFinanceOfficeSearchInput;
}): boolean {
  const mapped = mapPennsylvaniaFinanceOffice({
    office: input.row.OFFICE,
    district: input.row.DISTRICT,
  });
  return (
    mapped !== null &&
    mapped.paOfficeCode === input.officeSearchInput.paOfficeCode &&
    mapped.district === input.officeSearchInput.district
  );
}

function isLegislativeInput(input: { officeScope: string; officeName: string }): boolean {
  return (
    (input.officeScope === "state_upper" && input.officeName.trim() === "State Senator") ||
    (input.officeScope === "state_lower" && input.officeName.trim() === "State Lower Chamber Legislator")
  );
}

function hasValidLegislativeDistrict(input: { officeScope: string; officeName: string; district?: string | null }): boolean {
  if (input.officeScope === "state_upper" && input.officeName.trim() === "State Senator") {
    return normalizePennsylvaniaFinanceLegislativeDistrict(input.district, 50) !== null;
  }
  if (input.officeScope === "state_lower" && input.officeName.trim() === "State Lower Chamber Legislator") {
    return normalizePennsylvaniaFinanceLegislativeDistrict(input.district, 203) !== null;
  }
  return true;
}

function toFilerMatch(input: {
  accumulator: CandidateFilerAccumulator;
  sourceUrl: string | null;
}): PennsylvaniaCandidateCommitteeMatch {
  return {
    filerId: input.accumulator.filerId,
    filerName: input.accumulator.filerName,
    filerType: input.accumulator.filerType,
    confidence: "exact",
    source: "pa_bulk",
    sourceUrl: input.sourceUrl,
    matchedFilerRowCount: input.accumulator.rows.length,
  };
}

export function resolvePennsylvaniaCandidateCommittee(
  input: PennsylvaniaCandidateCommitteeResolverInput
): PennsylvaniaCandidateCommitteeResolution {
  const electionYear = normalizePennsylvaniaCampaignFinanceExportYear(input.electionYear);
  const candidateNameKeys = normalizePennsylvaniaCandidateNameKeys(input.candidateName);
  const candidateNameKey = candidateNameNormalized(input.candidateName);
  const officeSearchInput = toPennsylvaniaFinanceOfficeSearchInput({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
    district: input.district,
  });
  const officeNameNormalized = officeSearchInput?.paOfficeCode ?? normalizeTextKey(input.officeName);

  if (candidateNameKeys.size === 0) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }
  if (!officeSearchInput) {
    return {
      status: "unmatched",
      reason:
        isLegislativeInput(input) && !hasValidLegislativeDistrict(input)
          ? "missing_legislative_district"
          : "unsupported_office",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const rowsByFiler = new Map<string, CandidateFilerAccumulator>();
  for (const row of input.filerRows) {
    const filerId = row.FILERID.trim().toUpperCase();
    const filerName = row.FILERNAME.trim();
    if (!filerId || !filerName) {
      continue;
    }
    if (!rowMatchesElectionYear(row, electionYear)) {
      continue;
    }
    if (!isLikelyCandidateFiler(row)) {
      continue;
    }
    if (!rowMatchesOfficeContext({ row, officeSearchInput })) {
      continue;
    }
    if (!rowMatchesCandidateName({ row, candidateNameKeys })) {
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

  // Most PA committee rows carry no usable office context (3,428 of 4,060
  // in the 2026 export leave OFFICE blank; another 278 name an office but no
  // district), so the office filter above cannot see the funded committee and
  // the candidate resolves to their moneyless FILERTYPE 1 registration row.
  // Recall such a committee ONLY with corroboration: the candidacy is proven
  // by an office-matched registration row from the first pass, the committee
  // name matches the candidate, the row is active this cycle, and the
  // committee shares a ZIP or phone number with that registration row — a
  // same-name stranger's committee shares neither. Name-only committees stay
  // out.
  const registrationRows = [...rowsByFiler.values()]
    .filter((accumulator) => accumulator.filerType === "1")
    .flatMap((accumulator) => accumulator.rows);
  if (registrationRows.length > 0) {
    const registrationZips = new Set(
      registrationRows.map((row) => zip5(row.ZIPCODE)).filter((zip) => zip.length === 5)
    );
    const registrationPhones = new Set(
      registrationRows.map((row) => phoneDigits(row.PHONE)).filter((phone) => phone.length >= 7)
    );

    // The FILER, not the row, is what gets recalled — so every populated
    // OFFICE/DISTRICT across a filer's current-cycle rows must agree with
    // the race: a populated office must denote THIS office, a populated
    // district must normalize to THIS district (statewide races admit none),
    // and junk district values veto too. One conflicting sibling row vetoes
    // the whole filer even when the row under consideration is blank (live:
    // 90 committees carry both blank and populated DISTRICT rows). Rows from
    // other election years do not veto — committees legitimately carried
    // other districts in past cycles.
    const committeeRowsByFilerId = new Map<string, PennsylvaniaCampaignFinanceFilerRow[]>();
    for (const row of input.filerRows) {
      if (row.FILERTYPE.trim() !== "2") {
        continue;
      }
      const filerId = row.FILERID.trim().toUpperCase();
      if (!filerId) {
        continue;
      }
      const rows = committeeRowsByFilerId.get(filerId) ?? [];
      rows.push(row);
      committeeRowsByFilerId.set(filerId, rows);
    }
    const filerContextVerdicts = new Map<string, boolean>();
    const filerContextAgreesWithRace = (filerId: string): boolean => {
      const cached = filerContextVerdicts.get(filerId);
      if (cached !== undefined) {
        return cached;
      }
      let agrees = true;
      for (const sibling of committeeRowsByFilerId.get(filerId) ?? []) {
        if (!rowMatchesElectionYear(sibling, electionYear)) {
          continue;
        }
        const siblingOffice = sibling.OFFICE.trim();
        if (siblingOffice && mapPennsylvaniaFinanceOfficeCode(siblingOffice) !== officeSearchInput.paOfficeCode) {
          agrees = false;
          break;
        }
        const siblingDistrict = sibling.DISTRICT.trim();
        if (
          siblingDistrict &&
          (officeSearchInput.district === null ||
            normalizePennsylvaniaFinanceLegislativeDistrict(siblingDistrict, 203) !== officeSearchInput.district)
        ) {
          agrees = false;
          break;
        }
      }
      filerContextVerdicts.set(filerId, agrees);
      return agrees;
    };

    for (const row of input.filerRows) {
      const filerId = row.FILERID.trim().toUpperCase();
      const filerName = row.FILERNAME.trim();
      if (!filerId || !filerName || rowsByFiler.has(filerId)) {
        continue;
      }
      if (row.FILERTYPE.trim() !== "2") {
        continue;
      }
      if (!filerContextAgreesWithRace(filerId)) {
        continue;
      }
      if (!rowMatchesElectionYear(row, electionYear)) {
        continue;
      }
      if (!isLikelyCandidateFiler(row)) {
        continue;
      }
      if (!rowMatchesCandidateName({ row, candidateNameKeys })) {
        continue;
      }
      const rowZip = zip5(row.ZIPCODE);
      const rowPhone = phoneDigits(row.PHONE);
      const corroborated =
        (rowZip.length === 5 && registrationZips.has(rowZip)) ||
        (rowPhone.length >= 7 && registrationPhones.has(rowPhone));
      if (!corroborated) {
        continue;
      }

      const accumulator = rowsByFiler.get(filerId) ?? {
        filerId,
        filerName,
        filerType: "2",
        rows: [],
      };
      accumulator.rows.push(row);
      rowsByFiler.set(filerId, accumulator);
    }
  }

  if (rowsByFiler.size === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_filer_match",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const sourceUrl = input.sourceUrl?.trim() || null;
  const matches = [...rowsByFiler.values()]
    .map((accumulator) => toFilerMatch({ accumulator, sourceUrl }))
    .sort((left, right) => left.filerId.localeCompare(right.filerId));

  if (matches.length === 1) {
    return {
      status: "matched",
      ...matches[0],
    };
  }

  // PA registers the same candidacy twice: a candidate filer (FILERTYPE 1,
  // the person's own registration) and their political committee (FILERTYPE
  // 2). Contribution reports are filed under the committee's filer id, so
  // when the match set is exactly one committee plus candidate registrations
  // for the same office/district/year, the committee is the funded vehicle.
  // Two committees (or two candidate registrations with no committee) stay
  // ambiguous — there is no evidence which vehicle carries the money.
  const committeeMatches = matches.filter((match) => match.filerType === "2");
  const candidateMatches = matches.filter((match) => match.filerType === "1");
  if (committeeMatches.length === 1 && candidateMatches.length === matches.length - 1) {
    return {
      status: "matched",
      ...committeeMatches[0],
    };
  }

  return {
    status: "ambiguous",
    reason: "multiple_matching_filers",
    candidateNameNormalized: candidateNameKey,
    officeNameNormalized,
    matches,
  };
}
