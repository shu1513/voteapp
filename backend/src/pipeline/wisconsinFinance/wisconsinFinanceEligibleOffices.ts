export type WisconsinFinanceEligibleOfficeKey = `${string}::${string}`;

export type WisconsinFinanceOfficeScope = "statewide" | "state_upper" | "state_lower";

export type WisconsinSunshineOfficeMapping = {
  officeScope: WisconsinFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: WisconsinFinanceEligibleOfficeKey;
  sunshineOffice: string;
  requiresDistrict: boolean;
  district: string | null;
};

export type WisconsinSunshineOfficeSearchInput = {
  sunshineOffice: string;
  district: string | null;
};

export const WISCONSIN_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::State Treasurer",
  "statewide::Superintendent of Public Instruction",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly WisconsinFinanceEligibleOfficeKey[];

const WISCONSIN_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(WISCONSIN_FINANCE_ELIGIBLE_OFFICE_KEYS);

type WisconsinSunshineOfficeDefinition = {
  officeScope: WisconsinFinanceOfficeScope;
  officeCanonicalName: string;
  sunshineOffice: string;
  requiresDistrict: boolean;
};

const WISCONSIN_SUNSHINE_OFFICE_DEFINITIONS = new Map<string, WisconsinSunshineOfficeDefinition>([
  [
    "GOVERNOR",
    { officeScope: "statewide", officeCanonicalName: "Governor", sunshineOffice: "Governor", requiresDistrict: false },
  ],
  [
    "LIEUTENANT GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      sunshineOffice: "Lieutenant Governor",
      requiresDistrict: false,
    },
  ],
  [
    "SECRETARY OF STATE",
    {
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
      sunshineOffice: "Secretary of State",
      requiresDistrict: false,
    },
  ],
  [
    "ATTORNEY GENERAL",
    {
      officeScope: "statewide",
      officeCanonicalName: "Attorney General",
      sunshineOffice: "Attorney General",
      requiresDistrict: false,
    },
  ],
  [
    "STATE TREASURER",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Treasurer",
      sunshineOffice: "State Treasurer",
      requiresDistrict: false,
    },
  ],
  [
    "SUPERINTENDENT OF PUBLIC INSTRUCTION",
    {
      officeScope: "statewide",
      officeCanonicalName: "Superintendent of Public Instruction",
      sunshineOffice: "Superintendent of Public Instruction",
      requiresDistrict: false,
    },
  ],
  [
    "STATE SENATE",
    {
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      sunshineOffice: "State Senate",
      requiresDistrict: true,
    },
  ],
  [
    "STATE ASSEMBLY",
    {
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      sunshineOffice: "State Assembly",
      requiresDistrict: true,
    },
  ],
]);

const WISCONSIN_APP_OFFICE_TO_SUNSHINE = new Map<string, WisconsinSunshineOfficeDefinition>(
  [...WISCONSIN_SUNSHINE_OFFICE_DEFINITIONS.values()].map((definition) => [
    `${definition.officeScope}::${definition.officeCanonicalName}`,
    definition,
  ])
);

export function toWisconsinFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): WisconsinFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isWisconsinFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toWisconsinFinanceOfficeKey(input);
  return key !== null && WISCONSIN_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export function normalizeWisconsinSunshineOfficeLabel(value: string | null | undefined): string | null {
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase();
  return normalized ? normalized : null;
}

export function normalizeWisconsinSunshineLegislativeDistrict(value: string | null | undefined): string | null {
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase();
  if (!normalized) {
    return null;
  }
  const match = normalized.match(
    /^(?:(?:AD|SD|LD|ASSEMBLY|SENATE|LEG(?:ISLATIVE)?)(?:\s+DIST(?:RICT)?)?\s*)?0*([1-9][0-9]?)$/
  );
  if (!match?.[1]) {
    return null;
  }
  const districtNumber = Number.parseInt(match[1], 10);
  if (!Number.isInteger(districtNumber) || districtNumber < 1 || districtNumber > 99) {
    return null;
  }
  return String(districtNumber);
}

export function mapWisconsinSunshineOffice(input: {
  office: string | null | undefined;
  district?: string | null | undefined;
}): WisconsinSunshineOfficeMapping | null {
  const normalizedOffice = normalizeWisconsinSunshineOfficeLabel(input.office);
  if (!normalizedOffice) {
    return null;
  }
  const definition = WISCONSIN_SUNSHINE_OFFICE_DEFINITIONS.get(normalizedOffice);
  if (!definition) {
    return null;
  }
  const district = definition.requiresDistrict ? normalizeWisconsinSunshineLegislativeDistrict(input.district) : null;
  if (definition.requiresDistrict && !district) {
    return null;
  }
  const officeKey = toWisconsinFinanceOfficeKey(definition);
  if (!officeKey || !WISCONSIN_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  return {
    ...definition,
    officeKey,
    district,
  };
}

export function toWisconsinSunshineOfficeSearchInput(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  district?: string | null | undefined;
}): WisconsinSunshineOfficeSearchInput | null {
  const officeKey = toWisconsinFinanceOfficeKey(input);
  if (!officeKey || !WISCONSIN_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  const definition = WISCONSIN_APP_OFFICE_TO_SUNSHINE.get(officeKey);
  if (!definition) {
    return null;
  }
  const district = definition.requiresDistrict ? normalizeWisconsinSunshineLegislativeDistrict(input.district) : null;
  if (definition.requiresDistrict && !district) {
    return null;
  }
  return {
    sunshineOffice: definition.sunshineOffice,
    district,
  };
}
