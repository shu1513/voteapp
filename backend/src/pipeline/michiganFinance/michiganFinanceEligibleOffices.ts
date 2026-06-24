export type MichiganFinanceEligibleOfficeKey = `${string}::${string}`;

export type MichiganFinanceOfficeScope = "statewide" | "state_upper" | "state_lower";

export type MichiganMitnOfficeMapping = {
  officeScope: MichiganFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: MichiganFinanceEligibleOfficeKey;
  mitnOffice: string;
  requiresDistrict: boolean;
  district: string | null;
};

export type MichiganMitnOfficeSearchInput = {
  mitnOffice: string;
  district: string | null;
};

export const MICHIGAN_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly MichiganFinanceEligibleOfficeKey[];

const MICHIGAN_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(MICHIGAN_FINANCE_ELIGIBLE_OFFICE_KEYS);

type MichiganMitnOfficeDefinition = {
  officeScope: MichiganFinanceOfficeScope;
  officeCanonicalName: string;
  mitnOffice: string;
  requiresDistrict: boolean;
  maxDistrict: number | null;
};

const MICHIGAN_MITN_OFFICE_DEFINITIONS = new Map<string, MichiganMitnOfficeDefinition>([
  [
    "GOVERNOR",
    { officeScope: "statewide", officeCanonicalName: "Governor", mitnOffice: "Governor", requiresDistrict: false, maxDistrict: null },
  ],
  [
    "LIEUTENANT GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      mitnOffice: "Lieutenant Governor",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "LT GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      mitnOffice: "Lieutenant Governor",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "SECRETARY OF STATE",
    {
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
      mitnOffice: "Secretary of State",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "ATTORNEY GENERAL",
    {
      officeScope: "statewide",
      officeCanonicalName: "Attorney General",
      mitnOffice: "Attorney General",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "STATE SENATE",
    {
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      mitnOffice: "State Senate",
      requiresDistrict: true,
      maxDistrict: 38,
    },
  ],
  [
    "STATE SENATOR",
    {
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      mitnOffice: "State Senate",
      requiresDistrict: true,
      maxDistrict: 38,
    },
  ],
  [
    "SENATE",
    {
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      mitnOffice: "State Senate",
      requiresDistrict: true,
      maxDistrict: 38,
    },
  ],
  [
    "STATE HOUSE",
    {
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      mitnOffice: "State House",
      requiresDistrict: true,
      maxDistrict: 110,
    },
  ],
  [
    "STATE REPRESENTATIVE",
    {
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      mitnOffice: "State House",
      requiresDistrict: true,
      maxDistrict: 110,
    },
  ],
  [
    "HOUSE OF REPRESENTATIVES",
    {
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      mitnOffice: "State House",
      requiresDistrict: true,
      maxDistrict: 110,
    },
  ],
]);

const MICHIGAN_APP_OFFICE_TO_MITN = new Map<string, MichiganMitnOfficeDefinition>(
  [...MICHIGAN_MITN_OFFICE_DEFINITIONS.values()].map((definition) => [
    `${definition.officeScope}::${definition.officeCanonicalName}`,
    definition,
  ])
);

export function toMichiganFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): MichiganFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isMichiganFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toMichiganFinanceOfficeKey(input);
  return key !== null && MICHIGAN_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export function normalizeMichiganMitnOfficeLabel(value: string | null | undefined): string | null {
  const normalized = value
    ?.trim()
    .replace(/[.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
  return normalized ? normalized : null;
}

export function normalizeMichiganMitnLegislativeDistrict(
  value: string | null | undefined,
  maxDistrict: number
): string | null {
  if (!Number.isInteger(maxDistrict) || maxDistrict <= 0) {
    throw new Error(`Invalid Michigan MiTN max district: ${maxDistrict}`);
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

export function mapMichiganMitnOffice(input: {
  office: string | null | undefined;
  district?: string | null | undefined;
}): MichiganMitnOfficeMapping | null {
  const normalizedOffice = normalizeMichiganMitnOfficeLabel(input.office);
  if (!normalizedOffice) {
    return null;
  }
  const definition = MICHIGAN_MITN_OFFICE_DEFINITIONS.get(normalizedOffice);
  if (!definition) {
    return null;
  }
  const district = definition.requiresDistrict
    ? normalizeMichiganMitnLegislativeDistrict(input.district, definition.maxDistrict ?? 0)
    : null;
  if (definition.requiresDistrict && !district) {
    return null;
  }
  const officeKey = toMichiganFinanceOfficeKey(definition);
  if (!officeKey || !MICHIGAN_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  return {
    ...definition,
    officeKey,
    district,
  };
}

export function toMichiganMitnOfficeSearchInput(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  district?: string | null | undefined;
}): MichiganMitnOfficeSearchInput | null {
  const officeKey = toMichiganFinanceOfficeKey(input);
  if (!officeKey || !MICHIGAN_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  const definition = MICHIGAN_APP_OFFICE_TO_MITN.get(officeKey);
  if (!definition) {
    return null;
  }
  const district = definition.requiresDistrict
    ? normalizeMichiganMitnLegislativeDistrict(input.district, definition.maxDistrict ?? 0)
    : null;
  if (definition.requiresDistrict && !district) {
    return null;
  }
  return {
    mitnOffice: definition.mitnOffice,
    district,
  };
}
