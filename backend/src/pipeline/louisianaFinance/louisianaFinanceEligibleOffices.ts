export type LouisianaFinanceOfficeSearchInput = {
  officeScope: "statewide" | "state_upper" | "state_lower";
  officeName: string;
  district: string | null;
  requiresDistrict: boolean;
};

export const LOUISIANA_FINANCE_ELIGIBLE_OFFICE_KEYS = new Set([
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::State Treasurer",
  // Keys are matched literally against `office.scope || '::' || office.canonical_name`,
  // so they must be the repository's canonical office names (seedOffices.ts), not
  // Louisiana's longer statutory titles.
  "statewide::Commissioner of Agriculture",
  "statewide::Commissioner of Insurance",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
]);

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

export function normalizeLouisianaFinanceDistrict(value: string | null | undefined): string {
  const trimmed = value?.trim() ?? "";
  if (!trimmed) {
    return "";
  }
  const numeric = Number(trimmed);
  if (Number.isInteger(numeric) && numeric >= 0) {
    return String(numeric);
  }
  return normalizeTextKey(trimmed);
}

export function normalizeLouisianaFinanceOfficeName(value: string | null | undefined): string | null {
  switch (normalizeTextKey(value)) {
    case "GOVERNOR":
      return "Governor";
    case "LIEUTENANT GOVERNOR":
    case "LT GOVERNOR":
    case "LT GOV":
      return "Lieutenant Governor";
    case "SECRETARY STATE":
    case "SECRETARY OF STATE":
      return "Secretary of State";
    case "ATTORNEY GENERAL":
      return "Attorney General";
    case "TREASURER":
    case "STATE TREASURER":
      return "State Treasurer";
    case "COMMISSIONER AGRICULTURE FORESTRY":
    case "COMMISSIONER OF AGRICULTURE AND FORESTRY":
    case "COMMISSIONER OF AGRICULTURE":
    case "AGRICULTURE COMMISSIONER":
      // Returned label is an internal join key only: both the repository canonical
      // name and Louisiana's raw OfficeSought values normalize into it before they
      // are compared, and the link row persists the canonical name instead.
      return "Commissioner of Agriculture and Forestry";
    case "COMMISSIONER INSURANCE":
    case "COMMISSIONER OF INSURANCE":
    case "INSURANCE COMMISSIONER":
      return "Commissioner of Insurance";
    case "STATE SENATOR":
    case "STATE SENATE":
    case "SENATOR":
      return "State Senator";
    case "STATE REPRESENTATIVE":
    case "STATE HOUSE":
    case "HOUSE REPRESENTATIVE":
    case "REPRESENTATIVE":
    case "STATE LOWER CHAMBER LEGISLATOR":
      return "State Lower Chamber Legislator";
    default:
      return null;
  }
}

function matchLouisianaFinanceEligibleOffice(input: {
  officeScope: string;
  officeCanonicalName: string;
}): { officeScope: LouisianaFinanceOfficeSearchInput["officeScope"]; officeName: string } | null {
  const officeScope = input.officeScope.trim().toLowerCase();
  const officeName = normalizeLouisianaFinanceOfficeName(input.officeCanonicalName);
  if (officeName === null) {
    return null;
  }

  if (
    officeScope === "statewide" &&
    officeName !== "State Senator" &&
    officeName !== "State Lower Chamber Legislator"
  ) {
    return { officeScope: "statewide", officeName };
  }

  if (officeScope === "state_upper" && officeName === "State Senator") {
    return { officeScope: "state_upper", officeName };
  }

  if (officeScope === "state_lower" && officeName === "State Lower Chamber Legislator") {
    return { officeScope: "state_lower", officeName };
  }

  return null;
}

export function mapLouisianaFinanceOffice(input: {
  officeScope: string;
  officeCanonicalName: string;
  district?: string | null;
}): LouisianaFinanceOfficeSearchInput | null {
  const match = matchLouisianaFinanceEligibleOffice(input);
  if (!match) {
    return null;
  }

  if (match.officeScope === "statewide") {
    return {
      officeScope: "statewide",
      officeName: match.officeName,
      district: null,
      requiresDistrict: false,
    };
  }

  const district = normalizeLouisianaFinanceDistrict(input.district);
  if (!district) {
    return null;
  }
  return {
    officeScope: match.officeScope,
    officeName: match.officeName,
    district,
    requiresDistrict: true,
  };
}

// Eligibility is scope+name only, like the other states' is*FinanceEligibleOffice
// predicates: ballot lookup calls this without a district (its election rows carry
// none), and by then the la_candidate_finance_links table already scopes
// candidate+election, so district disambiguation happened at link time. The
// district requirement lives in mapLouisianaFinanceOffice, which the
// committee-matching sync side uses.
export function isLouisianaFinanceEligibleOffice(input: {
  officeScope: string;
  officeCanonicalName: string;
  district?: string | null;
}): boolean {
  return matchLouisianaFinanceEligibleOffice(input) !== null;
}
