// Idaho finance office eligibility + Sunshine grid office mapping
// (docs/plans/idaho-finance.md, Phase 1).
//
// Scope is every VoteApp Nov-2026 Idaho office the grid registers under a
// clean, unambiguous name (grid `office` values pinned live 2026-09-01 on the
// 729 election-year-2026 rows). Federal races are the FEC path. Judicial
// offices, prosecuting attorneys, and special districts stay out until the
// roster carries candidates for them.
//
// Idaho's county clerk is the Clerk of the District Court ex officio, so both
// VoteApp spellings map to the grid's single "Clerk" office.

export type IdahoFinanceEligibleOfficeKey = `${string}::${string}`;

export type IdahoSunshineDistrictKind =
  /** Grid districtType "State", cityDistrict "Statewide". */
  | "statewide"
  /** Grid districtType "Legislative", cityDistrict "Legislative District N" (+ seatZone A/B for the House). */
  | "legislative"
  /** Grid districtType "County", jurisdiction = county name. */
  | "county"
  /** As county, plus seatZone = commissioner district number. */
  | "county_commissioner";

export type IdahoSunshineOffice = {
  /** Grid `office` text, compared exactly. */
  gridOffice: string;
  districtKind: IdahoSunshineDistrictKind;
};

const OFFICES: Record<string, IdahoSunshineOffice> = {
  "statewide::Governor": { gridOffice: "Governor", districtKind: "statewide" },
  "statewide::Lieutenant Governor": { gridOffice: "Lieutenant Governor", districtKind: "statewide" },
  "statewide::Secretary of State": { gridOffice: "Secretary of State", districtKind: "statewide" },
  "statewide::Attorney General": { gridOffice: "Attorney General", districtKind: "statewide" },
  "statewide::State Treasurer": { gridOffice: "State Treasurer", districtKind: "statewide" },
  // VoteApp canonical name differs from the grid label.
  "statewide::Comptroller": { gridOffice: "State Controller", districtKind: "statewide" },
  "statewide::Superintendent of Public Instruction": {
    gridOffice: "Superintendent of Public Instruction",
    districtKind: "statewide",
  },
  "state_upper::State Senator": { gridOffice: "State Senator", districtKind: "legislative" },
  "state_lower::State Lower Chamber Legislator": { gridOffice: "State Representative", districtKind: "legislative" },
  "county::County Commissioner": { gridOffice: "County Commissioner", districtKind: "county_commissioner" },
  "county::County Treasurer": { gridOffice: "County Treasurer", districtKind: "county" },
  "county::County Assessor": { gridOffice: "Assessor", districtKind: "county" },
  "county::County Coroner": { gridOffice: "Coroner", districtKind: "county" },
  "county::County Clerk": { gridOffice: "Clerk", districtKind: "county" },
  "county::Clerk of Court": { gridOffice: "Clerk", districtKind: "county" },
  "county::Sheriff": { gridOffice: "Sheriff", districtKind: "county" },
};

export const IDAHO_FINANCE_ELIGIBLE_OFFICE_KEYS: ReadonlySet<string> = new Set(Object.keys(OFFICES));

export function toIdahoFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): IdahoFinanceEligibleOfficeKey | null {
  const scope = input.officeScope?.trim();
  const name = input.officeCanonicalName?.trim();
  if (!scope || !name) return null;
  return `${scope}::${name}`;
}

export function isIdahoFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toIdahoFinanceOfficeKey(input);
  return key !== null && IDAHO_FINANCE_ELIGIBLE_OFFICE_KEYS.has(key);
}

/** Grid office for a VoteApp race; null outside the map (fail closed). */
export function idahoSunshineOfficeForRace(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): IdahoSunshineOffice | null {
  const key = toIdahoFinanceOfficeKey(input);
  return key === null ? null : OFFICES[key] ?? null;
}
