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
  "statewide::Commissioner of Agriculture and Forestry",
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
    case "AGRICULTURE COMMISSIONER":
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

export function mapLouisianaFinanceOffice(input: {
  officeScope: string;
  officeCanonicalName: string;
  district?: string | null;
}): LouisianaFinanceOfficeSearchInput | null {
  const officeScope = input.officeScope.trim().toLowerCase();
  const officeName = normalizeLouisianaFinanceOfficeName(input.officeCanonicalName);
  const district = normalizeLouisianaFinanceDistrict(input.district);

  if (
    officeScope === "statewide" &&
    officeName !== null &&
    officeName !== "State Senator" &&
    officeName !== "State Lower Chamber Legislator"
  ) {
    return {
      officeScope: "statewide",
      officeName,
      district: null,
      requiresDistrict: false,
    };
  }

  if (officeScope === "state_upper" && officeName === "State Senator") {
    if (!district) {
      return null;
    }
    return {
      officeScope: "state_upper",
      officeName,
      district,
      requiresDistrict: true,
    };
  }

  if (officeScope === "state_lower" && officeName === "State Lower Chamber Legislator") {
    if (!district) {
      return null;
    }
    return {
      officeScope: "state_lower",
      officeName,
      district,
      requiresDistrict: true,
    };
  }

  return null;
}

export function isLouisianaFinanceEligibleOffice(input: {
  officeScope: string;
  officeCanonicalName: string;
  district?: string | null;
}): boolean {
  return mapLouisianaFinanceOffice(input) !== null;
}
