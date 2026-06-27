export type IllinoisFinanceEligibleOfficeKey = `${string}::${string}`;

export type IllinoisFinanceOfficeScope = "statewide" | "state_upper" | "state_lower";

export type IllinoisSbeOfficeMapping = {
  officeScope: IllinoisFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: IllinoisFinanceEligibleOfficeKey;
  sbeOffice: string;
  requiresDistrict: boolean;
  district: string | null;
  maxDistrict: number | null;
};

export const ILLINOIS_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::Treasurer",
  "statewide::Comptroller",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly IllinoisFinanceEligibleOfficeKey[];

const ILLINOIS_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(ILLINOIS_FINANCE_ELIGIBLE_OFFICE_KEYS);

type IllinoisSbeOfficeDefinition = {
  officeScope: IllinoisFinanceOfficeScope;
  officeCanonicalName: string;
  sbeOffice: string;
  requiresDistrict: boolean;
  maxDistrict: number | null;
};

const ILLINOIS_SBE_OFFICE_DEFINITIONS = new Map<string, IllinoisSbeOfficeDefinition>([
  ["GOVERNOR", { officeScope: "statewide", officeCanonicalName: "Governor", sbeOffice: "Governor", requiresDistrict: false, maxDistrict: null }],
  [
    "LIEUTENANT GOVERNOR",
    { officeScope: "statewide", officeCanonicalName: "Lieutenant Governor", sbeOffice: "Lieutenant Governor", requiresDistrict: false, maxDistrict: null },
  ],
  [
    "LT GOVERNOR",
    { officeScope: "statewide", officeCanonicalName: "Lieutenant Governor", sbeOffice: "Lieutenant Governor", requiresDistrict: false, maxDistrict: null },
  ],
  [
    "SECRETARY OF STATE",
    { officeScope: "statewide", officeCanonicalName: "Secretary of State", sbeOffice: "Secretary of State", requiresDistrict: false, maxDistrict: null },
  ],
  [
    "ATTORNEY GENERAL",
    { officeScope: "statewide", officeCanonicalName: "Attorney General", sbeOffice: "Attorney General", requiresDistrict: false, maxDistrict: null },
  ],
  ["TREASURER", { officeScope: "statewide", officeCanonicalName: "Treasurer", sbeOffice: "Treasurer", requiresDistrict: false, maxDistrict: null }],
  ["COMPTROLLER", { officeScope: "statewide", officeCanonicalName: "Comptroller", sbeOffice: "Comptroller", requiresDistrict: false, maxDistrict: null }],
  [
    "STATE SENATE",
    { officeScope: "state_upper", officeCanonicalName: "State Senator", sbeOffice: "State Senate", requiresDistrict: true, maxDistrict: 59 },
  ],
  [
    "STATE SENATOR",
    { officeScope: "state_upper", officeCanonicalName: "State Senator", sbeOffice: "State Senate", requiresDistrict: true, maxDistrict: 59 },
  ],
  [
    "STATE HOUSE",
    { officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator", sbeOffice: "State Representative", requiresDistrict: true, maxDistrict: 118 },
  ],
  [
    "STATE REPRESENTATIVE",
    { officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator", sbeOffice: "State Representative", requiresDistrict: true, maxDistrict: 118 },
  ],
  [
    "HOUSE OF REPRESENTATIVES",
    { officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator", sbeOffice: "State Representative", requiresDistrict: true, maxDistrict: 118 },
  ],
]);

const ILLINOIS_APP_OFFICE_TO_SBE = new Map<string, IllinoisSbeOfficeDefinition>(
  [...ILLINOIS_SBE_OFFICE_DEFINITIONS.values()].map((definition) => [
    `${definition.officeScope}::${definition.officeCanonicalName}`,
    definition,
  ])
);

export function toIllinoisFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): IllinoisFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isIllinoisFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toIllinoisFinanceOfficeKey(input);
  return key !== null && ILLINOIS_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export function normalizeIllinoisSbeOfficeLabel(value: string | null | undefined): string | null {
  const normalized = value
    ?.trim()
    .replace(/[.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
  return normalized ? normalized : null;
}

export function normalizeIllinoisSbeLegislativeDistrict(
  value: string | null | undefined,
  maxDistrict: number
): string | null {
  if (!Number.isInteger(maxDistrict) || maxDistrict <= 0) {
    throw new Error(`Invalid Illinois SBE max district: ${maxDistrict}`);
  }
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase();
  if (!normalized) {
    return null;
  }
  const match = normalized.match(
    /^(?:(?:HD|SD|LD|HOUSE|SEN(?:ATE)?|REP(?:RESENTATIVE)?|LEG(?:ISLATIVE)?)(?:\s+DIST(?:RICT)?)?\s*)?0*([1-9][0-9]{0,2})$/
  );
  if (!match?.[1]) {
    return null;
  }
  const districtNumber = Number.parseInt(match[1], 10);
  if (!Number.isInteger(districtNumber) || districtNumber < 1 || districtNumber > maxDistrict) {
    return null;
  }
  return String(districtNumber);
}

export function mapIllinoisSbeOffice(input: {
  office: string | null | undefined;
  district?: string | null | undefined;
}): IllinoisSbeOfficeMapping | null {
  const normalizedOffice = normalizeIllinoisSbeOfficeLabel(input.office);
  if (!normalizedOffice) {
    return null;
  }
  const definition = ILLINOIS_SBE_OFFICE_DEFINITIONS.get(normalizedOffice);
  if (!definition) {
    return null;
  }
  const district = definition.requiresDistrict
    ? normalizeIllinoisSbeLegislativeDistrict(input.district, definition.maxDistrict ?? 0)
    : null;
  if (definition.requiresDistrict && !district) {
    return null;
  }
  const officeKey = toIllinoisFinanceOfficeKey(definition);
  if (!officeKey || !ILLINOIS_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  return {
    ...definition,
    officeKey,
    district,
  };
}

export function toIllinoisSbeOfficeSearchInput(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  district?: string | null | undefined;
}): { sbeOffice: string; district: string | null } | null {
  const officeKey = toIllinoisFinanceOfficeKey(input);
  if (!officeKey || !ILLINOIS_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  const definition = ILLINOIS_APP_OFFICE_TO_SBE.get(officeKey);
  if (!definition) {
    return null;
  }
  const district = definition.requiresDistrict
    ? normalizeIllinoisSbeLegislativeDistrict(input.district, definition.maxDistrict ?? 0)
    : null;
  if (definition.requiresDistrict && !district) {
    return null;
  }
  return {
    sbeOffice: definition.sbeOffice,
    district,
  };
}
