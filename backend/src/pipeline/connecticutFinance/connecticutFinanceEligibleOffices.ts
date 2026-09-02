export type ConnecticutFinanceEligibleOfficeKey = `${string}::${string}`;

export type ConnecticutFinanceOfficeScope = "statewide" | "state_upper" | "state_lower";

export type ConnecticutEcrisOfficeMapping = {
  officeScope: ConnecticutFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: ConnecticutFinanceEligibleOfficeKey;
  requiresDistrict: boolean;
};

export const CONNECTICUT_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::Comptroller",
  "statewide::State Treasurer",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly ConnecticutFinanceEligibleOfficeKey[];

const CONNECTICUT_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  CONNECTICUT_FINANCE_ELIGIBLE_OFFICE_KEYS
);

type EcrisOfficeDefinition = {
  officeScope: ConnecticutFinanceOfficeScope;
  officeCanonicalName: string;
  requiresDistrict: boolean;
};

const ECRIS_OFFICE_DEFINITIONS = new Map<string, EcrisOfficeDefinition>([
  ["GOVERNOR", { officeScope: "statewide", officeCanonicalName: "Governor", requiresDistrict: false }],
  [
    "LIEUTENANT GOVERNOR",
    { officeScope: "statewide", officeCanonicalName: "Lieutenant Governor", requiresDistrict: false },
  ],
  [
    "SECRETARY OF THE STATE",
    { officeScope: "statewide", officeCanonicalName: "Secretary of State", requiresDistrict: false },
  ],
  [
    "SECRETARY OF STATE",
    { officeScope: "statewide", officeCanonicalName: "Secretary of State", requiresDistrict: false },
  ],
  ["ATTORNEY GENERAL", { officeScope: "statewide", officeCanonicalName: "Attorney General", requiresDistrict: false }],
  ["STATE COMPTROLLER", { officeScope: "statewide", officeCanonicalName: "Comptroller", requiresDistrict: false }],
  ["STATE TREASURER", { officeScope: "statewide", officeCanonicalName: "State Treasurer", requiresDistrict: false }],
  ["STATE SENATOR", { officeScope: "state_upper", officeCanonicalName: "State Senator", requiresDistrict: true }],
  [
    "STATE REPRESENTATIVE",
    { officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator", requiresDistrict: true },
  ],
]);

export function toConnecticutFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): ConnecticutFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isConnecticutFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toConnecticutFinanceOfficeKey(input);
  return key !== null && CONNECTICUT_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export function normalizeConnecticutEcrisOfficeLabel(value: string | null | undefined): string | null {
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase();
  return normalized ? normalized : null;
}

/**
 * The app office canonical name for an eCRIS office label alone, e.g.
 * "State Representative" -> "State Lower Chamber Legislator". Unlike
 * mapConnecticutEcrisOffice this does not need a district: the
 * independent-expenditure search names the office but never the district.
 */
export function connecticutEcrisOfficeCanonicalName(officeLabel: string | null | undefined): string | null {
  const normalizedOffice = normalizeConnecticutEcrisOfficeLabel(officeLabel);
  if (!normalizedOffice) {
    return null;
  }
  const definition = ECRIS_OFFICE_DEFINITIONS.get(normalizedOffice);
  if (!definition) {
    return null;
  }
  const officeKey = toConnecticutFinanceOfficeKey(definition);
  return officeKey && CONNECTICUT_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey) ? definition.officeCanonicalName : null;
}

export function mapConnecticutEcrisOffice(input: {
  officeSought: string | null | undefined;
  district?: string | null | undefined;
}): ConnecticutEcrisOfficeMapping | null {
  const normalizedOffice = normalizeConnecticutEcrisOfficeLabel(input.officeSought);
  if (!normalizedOffice) {
    return null;
  }

  const definition = ECRIS_OFFICE_DEFINITIONS.get(normalizedOffice);
  if (!definition) {
    return null;
  }

  if (definition.requiresDistrict && !input.district?.trim()) {
    return null;
  }

  const officeKey = toConnecticutFinanceOfficeKey(definition);
  if (!officeKey || !CONNECTICUT_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }

  return {
    ...definition,
    officeKey,
  };
}
