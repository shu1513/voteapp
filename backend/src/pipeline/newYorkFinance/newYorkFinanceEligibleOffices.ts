export type NewYorkFinanceEligibleOfficeKey = `${string}::${string}`;

export type NewYorkFinanceOfficeScope = "statewide" | "state_upper" | "state_lower";

export type NewYorkBoeOfficeMapping = {
  officeScope: NewYorkFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: NewYorkFinanceEligibleOfficeKey;
  boeOfficeLabels: readonly string[];
  requiresDistrict: boolean;
  district: string | null;
};

export type NewYorkBoeOfficeSearchInput = {
  boeOfficeLabels: readonly string[];
  district: string | null;
};

// State offices only. NYC city offices (Mayor, Council, Public Advocate, NYC
// Comptroller, Borough President) file with the NYC Campaign Finance Board,
// and county/local offices are out of scope (plan-new-york-finance.md).
export const NEW_YORK_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Attorney General",
  "statewide::Comptroller",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly NewYorkFinanceEligibleOfficeKey[];

const NEW_YORK_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(NEW_YORK_FINANCE_ELIGIBLE_OFFICE_KEYS);

type NewYorkBoeOfficeDefinition = {
  officeScope: NewYorkFinanceOfficeScope;
  officeCanonicalName: string;
  // office_desc values as they appear in the NYSBOE filer registry and in
  // Schedule R disclosure rows. Bare "Comptroller" is deliberately absent:
  // in the registry it is the county office (filer_type_desc = 'County').
  boeOfficeLabels: readonly string[];
  requiresDistrict: boolean;
};

const NEW_YORK_BOE_OFFICE_DEFINITIONS: readonly NewYorkBoeOfficeDefinition[] = [
  {
    officeScope: "statewide",
    officeCanonicalName: "Governor",
    boeOfficeLabels: ["Governor"],
    requiresDistrict: false,
  },
  {
    officeScope: "statewide",
    officeCanonicalName: "Lieutenant Governor",
    boeOfficeLabels: ["Lieutenant Governor"],
    requiresDistrict: false,
  },
  {
    officeScope: "statewide",
    officeCanonicalName: "Attorney General",
    boeOfficeLabels: ["Attorney General"],
    requiresDistrict: false,
  },
  {
    officeScope: "statewide",
    officeCanonicalName: "Comptroller",
    boeOfficeLabels: ["State Comptroller"],
    requiresDistrict: false,
  },
  {
    officeScope: "state_upper",
    officeCanonicalName: "State Senator",
    boeOfficeLabels: ["State Senator"],
    requiresDistrict: true,
  },
  {
    officeScope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    boeOfficeLabels: ["Member of Assembly"],
    requiresDistrict: true,
  },
];

const NEW_YORK_APP_OFFICE_TO_BOE = new Map<string, NewYorkBoeOfficeDefinition>(
  NEW_YORK_BOE_OFFICE_DEFINITIONS.map((definition) => [
    `${definition.officeScope}::${definition.officeCanonicalName}`,
    definition,
  ])
);

export function toNewYorkFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): NewYorkFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isNewYorkFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toNewYorkFinanceOfficeKey(input);
  return key !== null && NEW_YORK_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

// NYSBOE stores legislative districts as bare numbers ("43"). Accept common
// prefixes and leading zeros; anything else is not a usable district.
export function normalizeNewYorkDistrict(value: string | null | undefined): string | null {
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase();
  if (!normalized) {
    return null;
  }
  const match = normalized.match(/^(?:AD|SD|DISTRICT)?\s*0*([1-9][0-9]{0,2})$/);
  if (!match?.[1]) {
    return null;
  }
  const districtNumber = Number.parseInt(match[1], 10);
  if (!Number.isInteger(districtNumber) || districtNumber < 1 || districtNumber > 150) {
    return null;
  }
  return String(districtNumber);
}

export function toNewYorkBoeOfficeSearchInput(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  district?: string | null | undefined;
}): NewYorkBoeOfficeSearchInput | null {
  const officeKey = toNewYorkFinanceOfficeKey(input);
  if (!officeKey || !NEW_YORK_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  const definition = NEW_YORK_APP_OFFICE_TO_BOE.get(officeKey);
  if (!definition) {
    return null;
  }
  const district = definition.requiresDistrict ? normalizeNewYorkDistrict(input.district) : null;
  if (definition.requiresDistrict && !district) {
    return null;
  }
  return {
    boeOfficeLabels: definition.boeOfficeLabels,
    district,
  };
}
