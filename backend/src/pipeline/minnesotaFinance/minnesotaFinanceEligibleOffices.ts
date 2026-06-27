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
  officeScope: string;
  officeName: string;
  district?: string | null;
};

export type MinnesotaFinanceOfficeMatch = MinnesotaFinanceEligibleOffice & {
  district: string | null;
};

function normalizeTextKey(value: string): string {
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

function normalizeOfficeScope(value: string): MinnesotaFinanceEligibleOfficeScope | null {
  const trimmed = value.trim();
  return trimmed === "statewide" || trimmed === "state_upper" || trimmed === "state_lower"
    ? trimmed
    : null;
}

export function normalizeMinnesotaFinanceOfficeName(value: string): MinnesotaFinanceEligibleOfficeName | null {
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

export function isMinnesotaFinanceEligibleOffice(input: MinnesotaFinanceOfficeInput): boolean {
  return mapMinnesotaFinanceOffice(input) !== null;
}

export function mapMinnesotaFinanceOffice(input: MinnesotaFinanceOfficeInput): MinnesotaFinanceOfficeMatch | null {
  const officeScope = normalizeOfficeScope(input.officeScope);
  const officeName = normalizeMinnesotaFinanceOfficeName(input.officeName);
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

  const district = normalizeMinnesotaFinanceDistrict(input.district);
  if ((officeScope === "state_upper" || officeScope === "state_lower") && !district) {
    return null;
  }

  return {
    officeScope,
    officeName,
    requiresDistrict: officeScope === "state_upper" || officeScope === "state_lower",
    district: district || null,
  };
}
