export type ArizonaFinanceEligibleOfficeKey = `${string}::${string}`;

export type ArizonaFinanceOfficeScope = "statewide" | "state_upper" | "state_lower";

export const ARIZONA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::State Treasurer",
  "statewide::Superintendent of Public Instruction",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly ArizonaFinanceEligibleOfficeKey[];

const ARIZONA_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(ARIZONA_FINANCE_ELIGIBLE_OFFICE_KEYS);

function normalizeTextKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function canonicalOfficeNameForInput(officeName: string): string | null {
  switch (normalizeTextKey(officeName)) {
    case "GOVERNOR":
      return "Governor";
    case "SECRETARY OF STATE":
      return "Secretary of State";
    case "ATTORNEY GENERAL":
      return "Attorney General";
    case "STATE TREASURER":
    case "TREASURER":
      return "State Treasurer";
    case "SUPERINTENDENT OF PUBLIC INSTRUCTION":
    case "SCHOOL SUPERINTENDENT":
      return "Superintendent of Public Instruction";
    case "STATE SENATOR":
    case "STATE SENATE":
      return "State Senator";
    case "STATE REPRESENTATIVE":
    case "STATE HOUSE":
    case "STATE LOWER CHAMBER LEGISLATOR":
      return "State Lower Chamber Legislator";
    default:
      return null;
  }
}

export function isArizonaFinanceEligibleOffice(input: {
  officeScope?: string | null;
  officeCanonicalName?: string | null;
}): boolean {
  const scope = input.officeScope?.trim();
  const canonicalName = input.officeCanonicalName?.trim();
  if (!scope || !canonicalName) {
    return false;
  }
  return ARIZONA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(`${scope}::${canonicalName}`);
}

export function normalizeArizonaFinanceOffice(input: {
  officeScope: string;
  officeName: string;
}): { officeScope: ArizonaFinanceOfficeScope; officeCanonicalName: string } | null {
  const scope = input.officeScope.trim();
  if (scope !== "statewide" && scope !== "state_upper" && scope !== "state_lower") {
    return null;
  }
  const officeCanonicalName = canonicalOfficeNameForInput(input.officeName);
  if (!officeCanonicalName) {
    return null;
  }
  return { officeScope: scope, officeCanonicalName };
}
