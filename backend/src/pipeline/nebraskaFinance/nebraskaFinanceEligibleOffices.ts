export type NebraskaFinanceEligibleOfficeKey = `${string}::${string}`;
export type NebraskaFinanceOfficeScope = "statewide" | "state_upper";

export type NebraskaNadcOfficeMapping = {
  officeScope: NebraskaFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: NebraskaFinanceEligibleOfficeKey;
  requiresDistrict: boolean;
  district: string | null;
};

export const NEBRASKA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::State Treasurer",
  "statewide::State Auditor",
  "state_upper::State Senator",
] as const satisfies readonly NebraskaFinanceEligibleOfficeKey[];

const NEBRASKA_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(NEBRASKA_FINANCE_ELIGIBLE_OFFICE_KEYS);

type NadcOfficeDefinition = {
  officeScope: NebraskaFinanceOfficeScope;
  officeCanonicalName: string;
  requiresDistrict: boolean;
};

const NADC_OFFICE_DEFINITIONS = new Map<string, NadcOfficeDefinition>([
  ["GOVERNOR", { officeScope: "statewide", officeCanonicalName: "Governor", requiresDistrict: false }],
  ["SECRETARY OF STATE", { officeScope: "statewide", officeCanonicalName: "Secretary of State", requiresDistrict: false }],
  ["ATTORNEY GENERAL", { officeScope: "statewide", officeCanonicalName: "Attorney General", requiresDistrict: false }],
  ["STATE TREASURER", { officeScope: "statewide", officeCanonicalName: "State Treasurer", requiresDistrict: false }],
  ["AUDITOR OF PUBLIC ACCOUNTS", { officeScope: "statewide", officeCanonicalName: "State Auditor", requiresDistrict: false }],
  ["STATE AUDITOR", { officeScope: "statewide", officeCanonicalName: "State Auditor", requiresDistrict: false }],
  // Nebraska has a unicameral legislature; in our app model this is represented by state_upper/State Senator.
  ["STATE LEGISLATURE", { officeScope: "state_upper", officeCanonicalName: "State Senator", requiresDistrict: true }],
]);

export function toNebraskaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): NebraskaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isNebraskaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toNebraskaFinanceOfficeKey(input);
  return key !== null && NEBRASKA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export function normalizeNebraskaNadcOfficeLabel(value: string | null | undefined): string | null {
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase();
  return normalized ? normalized : null;
}

function normalizeNebraskaNadcDistrict(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  if (!normalized || !/^\d{1,3}$/.test(normalized)) {
    return null;
  }
  return String(Number(normalized));
}

export function mapNebraskaNadcJurisdictionOffice(input: {
  jurisdictionOfficeDistrict: string | null | undefined;
}): NebraskaNadcOfficeMapping | null {
  const normalized = normalizeNebraskaNadcOfficeLabel(input.jurisdictionOfficeDistrict);
  if (!normalized) {
    return null;
  }

  const parts = normalized.split(/\s+-\s+/).map((part) => part.trim()).filter(Boolean);
  if (parts[0] !== "NEBRASKA" || parts.length < 2) {
    return null;
  }

  const officeLabel = parts[1];
  const definition = NADC_OFFICE_DEFINITIONS.get(officeLabel);
  if (!definition) {
    return null;
  }

  const district = definition.requiresDistrict ? normalizeNebraskaNadcDistrict(parts[2]) : null;
  if (definition.requiresDistrict && !district) {
    return null;
  }
  if (!definition.requiresDistrict && parts.length > 2) {
    return null;
  }

  const officeKey = toNebraskaFinanceOfficeKey(definition);
  if (!officeKey || !NEBRASKA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }

  return {
    ...definition,
    officeKey,
    district,
  };
}
