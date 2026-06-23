export type TexasFinanceEligibleOfficeKey = `${string}::${string}`;

export const TEXAS_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Attorney General",
  "statewide::Comptroller",
  "statewide::Agriculture Commissioner",
  "statewide::Land Commissioner",
  "statewide::Railroad Commissioner",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly TexasFinanceEligibleOfficeKey[];

const TEXAS_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(TEXAS_FINANCE_ELIGIBLE_OFFICE_KEYS);

export type TexasFinanceOfficeScope = "statewide" | "state_upper" | "state_lower";

export type TexasTecOfficeMapping = {
  officeScope: TexasFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: TexasFinanceEligibleOfficeKey;
  requiresDistrict: boolean;
};

type TexasTecOfficeDefinition = {
  officeScope: TexasFinanceOfficeScope;
  officeCanonicalName: string;
  requiresDistrict: boolean;
};

const TEXAS_TEC_OFFICE_DEFINITIONS = new Map<string, TexasTecOfficeDefinition>([
  ["GOVERNOR", { officeScope: "statewide", officeCanonicalName: "Governor", requiresDistrict: false }],
  ["LTGOVERNOR", { officeScope: "statewide", officeCanonicalName: "Lieutenant Governor", requiresDistrict: false }],
  ["ATTYGEN", { officeScope: "statewide", officeCanonicalName: "Attorney General", requiresDistrict: false }],
  ["COMPTROLLER", { officeScope: "statewide", officeCanonicalName: "Comptroller", requiresDistrict: false }],
  ["AGRICULTUR", { officeScope: "statewide", officeCanonicalName: "Agriculture Commissioner", requiresDistrict: false }],
  ["LANDCOMM", { officeScope: "statewide", officeCanonicalName: "Land Commissioner", requiresDistrict: false }],
  ["RRCOMM", { officeScope: "statewide", officeCanonicalName: "Railroad Commissioner", requiresDistrict: false }],
  ["RRCOMM_UNEXPIRED", { officeScope: "statewide", officeCanonicalName: "Railroad Commissioner", requiresDistrict: false }],
  ["STATESEN", { officeScope: "state_upper", officeCanonicalName: "State Senator", requiresDistrict: true }],
  [
    "STATEREP",
    {
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      requiresDistrict: true,
    },
  ],
]);

export function toTexasFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): TexasFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isTexasFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toTexasFinanceOfficeKey(input);
  return key !== null && TEXAS_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export function normalizeTexasTecOfficeCode(value: string | null | undefined): string | null {
  const normalized = value?.trim().replace(/\s+/g, "_").toUpperCase();
  return normalized ? normalized : null;
}

export function mapTexasTecOfficeCode(input: {
  officeCode: string | null | undefined;
}): TexasTecOfficeMapping | null {
  const officeCode = normalizeTexasTecOfficeCode(input.officeCode);
  if (!officeCode) {
    return null;
  }
  const definition = TEXAS_TEC_OFFICE_DEFINITIONS.get(officeCode);
  if (!definition) {
    return null;
  }
  const officeKey = toTexasFinanceOfficeKey(definition);
  if (!officeKey || !TEXAS_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  return {
    ...definition,
    officeKey,
  };
}
