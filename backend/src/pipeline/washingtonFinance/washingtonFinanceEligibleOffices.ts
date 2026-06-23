export type WashingtonFinanceEligibleOfficeKey = `${string}::${string}`;

export type WashingtonFinanceOfficeScope = "statewide" | "state_upper" | "state_lower";

export type WashingtonPdcOfficeMapping = {
  officeScope: WashingtonFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: WashingtonFinanceEligibleOfficeKey;
  pdcOffice: string;
  requiresLegislativeDistrict: boolean;
  legislativeDistrict: string | null;
};

export type WashingtonPdcOfficeSearchInput = {
  pdcOffice: string;
  legislativeDistrict: string | null;
};

export const WASHINGTON_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::State Treasurer",
  "statewide::State Auditor",
  "statewide::Land Commissioner",
  "statewide::Commissioner of Insurance",
  "statewide::Superintendent of Public Instruction",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly WashingtonFinanceEligibleOfficeKey[];

const WASHINGTON_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  WASHINGTON_FINANCE_ELIGIBLE_OFFICE_KEYS
);

type WashingtonPdcOfficeDefinition = {
  officeScope: WashingtonFinanceOfficeScope;
  officeCanonicalName: string;
  pdcOffice: string;
  requiresLegislativeDistrict: boolean;
};

const WASHINGTON_PDC_OFFICE_DEFINITIONS = new Map<string, WashingtonPdcOfficeDefinition>([
  [
    "GOVERNOR",
    { officeScope: "statewide", officeCanonicalName: "Governor", pdcOffice: "GOVERNOR", requiresLegislativeDistrict: false },
  ],
  [
    "LIEUTENANT GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      pdcOffice: "LIEUTENANT GOVERNOR",
      requiresLegislativeDistrict: false,
    },
  ],
  [
    "SECRETARY OF STATE",
    {
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
      pdcOffice: "SECRETARY OF STATE",
      requiresLegislativeDistrict: false,
    },
  ],
  [
    "ATTORNEY GENERAL",
    {
      officeScope: "statewide",
      officeCanonicalName: "Attorney General",
      pdcOffice: "ATTORNEY GENERAL",
      requiresLegislativeDistrict: false,
    },
  ],
  [
    "STATE TREASURER",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Treasurer",
      pdcOffice: "STATE TREASURER",
      requiresLegislativeDistrict: false,
    },
  ],
  [
    "STATE AUDITOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Auditor",
      pdcOffice: "STATE AUDITOR",
      requiresLegislativeDistrict: false,
    },
  ],
  [
    "PUBLIC LANDS COMMISSIONER",
    {
      officeScope: "statewide",
      officeCanonicalName: "Land Commissioner",
      pdcOffice: "PUBLIC LANDS COMMISSIONER",
      requiresLegislativeDistrict: false,
    },
  ],
  [
    "INSURANCE COMMISSIONER",
    {
      officeScope: "statewide",
      officeCanonicalName: "Commissioner of Insurance",
      pdcOffice: "INSURANCE COMMISSIONER",
      requiresLegislativeDistrict: false,
    },
  ],
  [
    "SUPERINTENDENT OF PUBLIC INSTRUCTION",
    {
      officeScope: "statewide",
      officeCanonicalName: "Superintendent of Public Instruction",
      pdcOffice: "SUPERINTENDENT OF PUBLIC INSTRUCTION",
      requiresLegislativeDistrict: false,
    },
  ],
  [
    "STATE SENATOR",
    {
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      pdcOffice: "STATE SENATOR",
      requiresLegislativeDistrict: true,
    },
  ],
  [
    "STATE REPRESENTATIVE",
    {
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      pdcOffice: "STATE REPRESENTATIVE",
      requiresLegislativeDistrict: true,
    },
  ],
]);

const WASHINGTON_APP_OFFICE_TO_PDC = new Map<string, WashingtonPdcOfficeDefinition>(
  [...WASHINGTON_PDC_OFFICE_DEFINITIONS.values()].map((definition) => [
    `${definition.officeScope}::${definition.officeCanonicalName}`,
    definition,
  ])
);

export function toWashingtonFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): WashingtonFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isWashingtonFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toWashingtonFinanceOfficeKey(input);
  return key !== null && WASHINGTON_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export function normalizeWashingtonPdcOfficeLabel(value: string | null | undefined): string | null {
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase();
  return normalized ? normalized : null;
}

export function normalizeWashingtonPdcLegislativeDistrict(value: string | null | undefined): string | null {
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase();
  if (!normalized) {
    return null;
  }
  const match = normalized.match(/^(?:LD|LEG(?:ISLATIVE)?\s*DISTRICT)?\s*0*([1-9][0-9]?)$/);
  if (!match?.[1]) {
    return null;
  }
  const districtNumber = Number.parseInt(match[1], 10);
  if (!Number.isInteger(districtNumber) || districtNumber < 1 || districtNumber > 99) {
    return null;
  }
  return String(districtNumber).padStart(2, "0");
}

export function mapWashingtonPdcOffice(input: {
  office: string | null | undefined;
  legislativeDistrict?: string | null | undefined;
}): WashingtonPdcOfficeMapping | null {
  const normalizedOffice = normalizeWashingtonPdcOfficeLabel(input.office);
  if (!normalizedOffice) {
    return null;
  }
  const definition = WASHINGTON_PDC_OFFICE_DEFINITIONS.get(normalizedOffice);
  if (!definition) {
    return null;
  }
  const legislativeDistrict = definition.requiresLegislativeDistrict
    ? normalizeWashingtonPdcLegislativeDistrict(input.legislativeDistrict)
    : null;
  if (definition.requiresLegislativeDistrict && !legislativeDistrict) {
    return null;
  }
  const officeKey = toWashingtonFinanceOfficeKey(definition);
  if (!officeKey || !WASHINGTON_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  return {
    ...definition,
    officeKey,
    legislativeDistrict,
  };
}

export function toWashingtonPdcOfficeSearchInput(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  legislativeDistrict?: string | null | undefined;
}): WashingtonPdcOfficeSearchInput | null {
  const officeKey = toWashingtonFinanceOfficeKey(input);
  if (!officeKey || !WASHINGTON_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  const definition = WASHINGTON_APP_OFFICE_TO_PDC.get(officeKey);
  if (!definition) {
    return null;
  }
  const legislativeDistrict = definition.requiresLegislativeDistrict
    ? normalizeWashingtonPdcLegislativeDistrict(input.legislativeDistrict)
    : null;
  if (definition.requiresLegislativeDistrict && !legislativeDistrict) {
    return null;
  }
  return {
    pdcOffice: definition.pdcOffice,
    legislativeDistrict,
  };
}
