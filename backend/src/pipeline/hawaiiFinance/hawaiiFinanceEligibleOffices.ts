export type HawaiiFinanceEligibleOfficeKey = `${string}::${string}`;

export type HawaiiFinanceOfficeScope = "statewide" | "state_upper" | "state_lower";

export type HawaiiCscOfficeMapping = {
  officeScope: HawaiiFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: HawaiiFinanceEligibleOfficeKey;
  cscOffice: string;
  requiresDistrict: boolean;
  district: string | null;
};

export type HawaiiCscOfficeSearchInput = {
  cscOffice: string;
  district: string | null;
};

export const HAWAII_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly HawaiiFinanceEligibleOfficeKey[];

const HAWAII_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(HAWAII_FINANCE_ELIGIBLE_OFFICE_KEYS);

type HawaiiCscOfficeDefinition = {
  officeScope: HawaiiFinanceOfficeScope;
  officeCanonicalName: string;
  cscOffice: string;
  requiresDistrict: boolean;
};

const HAWAII_CSC_OFFICE_DEFINITIONS = new Map<string, HawaiiCscOfficeDefinition>([
  [
    "GOVERNOR",
    { officeScope: "statewide", officeCanonicalName: "Governor", cscOffice: "Governor", requiresDistrict: false },
  ],
  [
    "LT GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      cscOffice: "Lt. Governor",
      requiresDistrict: false,
    },
  ],
  [
    "LIEUTENANT GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      cscOffice: "Lt. Governor",
      requiresDistrict: false,
    },
  ],
  [
    "SENATE",
    { officeScope: "state_upper", officeCanonicalName: "State Senator", cscOffice: "Senate", requiresDistrict: true },
  ],
  [
    "HOUSE",
    {
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      cscOffice: "House",
      requiresDistrict: true,
    },
  ],
]);

const HAWAII_APP_OFFICE_TO_CSC = new Map<string, HawaiiCscOfficeDefinition>(
  [...HAWAII_CSC_OFFICE_DEFINITIONS.values()].map((definition) => [
    `${definition.officeScope}::${definition.officeCanonicalName}`,
    definition,
  ])
);

export function toHawaiiFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): HawaiiFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isHawaiiFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toHawaiiFinanceOfficeKey(input);
  return key !== null && HAWAII_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export function normalizeHawaiiCscOfficeLabel(value: string | null | undefined): string | null {
  const normalized = value
    ?.trim()
    .replace(/[.]/g, "")
    .replace(/\s+/g, " ")
    .toUpperCase();
  return normalized ? normalized : null;
}

export function normalizeHawaiiCscDistrict(value: string | null | undefined): string | null {
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase();
  if (!normalized) {
    return null;
  }
  const match = normalized.match(/^(?:DIST(?:RICT)?\s*)?0*([1-9][0-9]?)$/);
  if (!match?.[1]) {
    return null;
  }
  const districtNumber = Number.parseInt(match[1], 10);
  if (!Number.isInteger(districtNumber) || districtNumber < 1 || districtNumber > 99) {
    return null;
  }
  return String(districtNumber);
}

export function mapHawaiiCscOffice(input: {
  office: string | null | undefined;
  district?: string | null | undefined;
}): HawaiiCscOfficeMapping | null {
  const normalizedOffice = normalizeHawaiiCscOfficeLabel(input.office);
  if (!normalizedOffice) {
    return null;
  }
  const definition = HAWAII_CSC_OFFICE_DEFINITIONS.get(normalizedOffice);
  if (!definition) {
    return null;
  }
  const district = definition.requiresDistrict ? normalizeHawaiiCscDistrict(input.district) : null;
  if (definition.requiresDistrict && !district) {
    return null;
  }
  const officeKey = toHawaiiFinanceOfficeKey(definition);
  if (!officeKey || !HAWAII_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  return {
    ...definition,
    officeKey,
    district,
  };
}

export function toHawaiiCscOfficeSearchInput(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  district?: string | null | undefined;
}): HawaiiCscOfficeSearchInput | null {
  const officeKey = toHawaiiFinanceOfficeKey(input);
  if (!officeKey || !HAWAII_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  const definition = HAWAII_APP_OFFICE_TO_CSC.get(officeKey);
  if (!definition) {
    return null;
  }
  const district = definition.requiresDistrict ? normalizeHawaiiCscDistrict(input.district) : null;
  if (definition.requiresDistrict && !district) {
    return null;
  }
  return {
    cscOffice: definition.cscOffice,
    district,
  };
}
