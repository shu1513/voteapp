export type MinnesotaFinanceEligibleOfficeScope = "statewide" | "state_upper" | "state_lower";

export type MinnesotaFinanceEligibleOfficeName =
  | "Governor"
  | "Lieutenant Governor"
  | "Secretary of State"
  | "Attorney General"
  | "State Senator"
  | "State Lower Chamber Legislator";

export type MinnesotaFinanceEligibleOffice = {
  officeScope: MinnesotaFinanceEligibleOfficeScope;
  officeName: MinnesotaFinanceEligibleOfficeName;
  requiresDistrict: boolean;
};

export const MINNESOTA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
  "state_lower::State Representative",
] as const;

export type MinnesotaFinanceOfficeInput = {
  officeScope: string | null;
  officeCanonicalName: string | null;
  district?: string | null;
};

export type MinnesotaFinanceOfficeMatch = MinnesotaFinanceEligibleOffice & {
  district: string | null;
};

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|OF|FOR)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeMinnesotaFinanceDistrict(value: string | null | undefined): string {
  const trimmed = value?.trim() ?? "";
  if (!trimmed) {
    return "";
  }

  const districtMatch = /(\d+)\s*([A-Z])?/i.exec(trimmed);
  if (districtMatch) {
    const parsed = Number(districtMatch[1]);
    if (Number.isInteger(parsed)) {
      return `${parsed}${districtMatch[2]?.toUpperCase() ?? ""}`;
    }
  }

  return normalizeTextKey(trimmed);
}

function normalizeOfficeScope(value: string | null | undefined): MinnesotaFinanceEligibleOfficeScope | null {
  const trimmed = value?.trim() ?? "";
  return trimmed === "statewide" || trimmed === "state_upper" || trimmed === "state_lower"
    ? trimmed
    : null;
}

export function normalizeMinnesotaFinanceOfficeName(value: string | null | undefined): MinnesotaFinanceEligibleOfficeName | null {
  switch (normalizeTextKey(value)) {
    case "GOVERNOR":
      return "Governor";
    case "LIEUTENANT GOVERNOR":
      return "Lieutenant Governor";
    case "SECRETARY OF STATE":
      return "Secretary of State";
    case "ATTORNEY GENERAL":
      return "Attorney General";
    case "STATE SENATOR":
      return "State Senator";
    case "STATE REPRESENTATIVE":
    case "STATE LOWER CHAMBER LEGISLATOR":
      return "State Lower Chamber Legislator";
    default:
      return null;
  }
}

function matchMinnesotaFinanceEligibleOffice(
  input: MinnesotaFinanceOfficeInput
): { officeScope: MinnesotaFinanceEligibleOfficeScope; officeName: MinnesotaFinanceEligibleOfficeName } | null {
  const officeScope = normalizeOfficeScope(input.officeScope);
  const officeName = normalizeMinnesotaFinanceOfficeName(input.officeCanonicalName);
  if (!officeScope || !officeName) {
    return null;
  }

  if (
    (officeScope === "statewide" &&
      !["Governor", "Lieutenant Governor", "Secretary of State", "Attorney General"].includes(officeName)) ||
    (officeScope === "state_upper" && officeName !== "State Senator") ||
    (officeScope === "state_lower" && officeName !== "State Lower Chamber Legislator")
  ) {
    return null;
  }

  return { officeScope, officeName };
}

// Eligibility is scope+name only, like the other states' is*FinanceEligibleOffice
// predicates: ballot lookup and the profile enricher call this without a district
// (their election rows carry none), and by then the mn_candidate_finance_links
// table already scopes candidate+election, so district disambiguation happened at
// link time. The district requirement lives in mapMinnesotaFinanceOffice, which
// the committee-matching sync side uses.
export function isMinnesotaFinanceEligibleOffice(input: MinnesotaFinanceOfficeInput): boolean {
  return matchMinnesotaFinanceEligibleOffice(input) !== null;
}

export function mapMinnesotaFinanceOffice(input: MinnesotaFinanceOfficeInput): MinnesotaFinanceOfficeMatch | null {
  const match = matchMinnesotaFinanceEligibleOffice(input);
  if (!match) {
    return null;
  }

  const requiresDistrict = match.officeScope === "state_upper" || match.officeScope === "state_lower";
  const district = normalizeMinnesotaFinanceDistrict(input.district);
  if (requiresDistrict && !district) {
    return null;
  }

  return {
    ...match,
    requiresDistrict,
    district: district || null,
  };
}
