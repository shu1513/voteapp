export type PennsylvaniaFinanceEligibleOfficeKey = `${string}::${string}`;

export type PennsylvaniaFinanceOfficeScope = "statewide" | "state_upper" | "state_lower";

export type PennsylvaniaFinanceOfficeMapping = {
  officeScope: PennsylvaniaFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: PennsylvaniaFinanceEligibleOfficeKey;
  paOfficeCode: string;
  requiresDistrict: boolean;
  district: string | null;
  maxDistrict: number | null;
};

export type PennsylvaniaFinanceOfficeSearchInput = {
  paOfficeCode: string;
  district: string | null;
};

export const PENNSYLVANIA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Attorney General",
  "statewide::Auditor General",
  "statewide::State Treasurer",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly PennsylvaniaFinanceEligibleOfficeKey[];

const PENNSYLVANIA_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  PENNSYLVANIA_FINANCE_ELIGIBLE_OFFICE_KEYS
);

type PennsylvaniaFinanceOfficeDefinition = {
  officeScope: PennsylvaniaFinanceOfficeScope;
  officeCanonicalName: string;
  paOfficeCode: string;
  requiresDistrict: boolean;
  maxDistrict: number | null;
};

const PENNSYLVANIA_OFFICE_DEFINITIONS = new Map<string, PennsylvaniaFinanceOfficeDefinition>([
  [
    "GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Governor",
      paOfficeCode: "GOV",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "GOV",
    {
      officeScope: "statewide",
      officeCanonicalName: "Governor",
      paOfficeCode: "GOV",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "LIEUTENANT GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      paOfficeCode: "LTG",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "LT GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      paOfficeCode: "LTG",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "LTG",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      paOfficeCode: "LTG",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "ATTORNEY GENERAL",
    {
      officeScope: "statewide",
      officeCanonicalName: "Attorney General",
      paOfficeCode: "ATT",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "ATT",
    {
      officeScope: "statewide",
      officeCanonicalName: "Attorney General",
      paOfficeCode: "ATT",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "AUDITOR GENERAL",
    {
      officeScope: "statewide",
      officeCanonicalName: "Auditor General",
      paOfficeCode: "AUD",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "AUD",
    {
      officeScope: "statewide",
      officeCanonicalName: "Auditor General",
      paOfficeCode: "AUD",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "STATE TREASURER",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Treasurer",
      paOfficeCode: "TRE",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "TREASURER",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Treasurer",
      paOfficeCode: "TRE",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "TRE",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Treasurer",
      paOfficeCode: "TRE",
      requiresDistrict: false,
      maxDistrict: null,
    },
  ],
  [
    "STATE SENATE",
    {
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      paOfficeCode: "STS",
      requiresDistrict: true,
      maxDistrict: 50,
    },
  ],
  [
    "STATE SENATOR",
    {
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      paOfficeCode: "STS",
      requiresDistrict: true,
      maxDistrict: 50,
    },
  ],
  [
    "STS",
    {
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      paOfficeCode: "STS",
      requiresDistrict: true,
      maxDistrict: 50,
    },
  ],
  [
    "STATE HOUSE",
    {
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      paOfficeCode: "STH",
      requiresDistrict: true,
      maxDistrict: 203,
    },
  ],
  [
    "STATE REPRESENTATIVE",
    {
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      paOfficeCode: "STH",
      requiresDistrict: true,
      maxDistrict: 203,
    },
  ],
  [
    "STH",
    {
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      paOfficeCode: "STH",
      requiresDistrict: true,
      maxDistrict: 203,
    },
  ],
]);

const PENNSYLVANIA_APP_OFFICE_TO_SOURCE = new Map<string, PennsylvaniaFinanceOfficeDefinition>(
  [...PENNSYLVANIA_OFFICE_DEFINITIONS.values()].map((definition) => [
    `${definition.officeScope}::${definition.officeCanonicalName}`,
    definition,
  ])
);

/**
 * Office-code-only mapping, ignoring district: what PA office code does this
 * source OFFICE label denote, if any? Used where a row names an office but
 * carries no district of its own.
 */
export function mapPennsylvaniaFinanceOfficeCode(office: string | null | undefined): string | null {
  const normalizedOffice = normalizePennsylvaniaFinanceOfficeLabel(office);
  if (!normalizedOffice) {
    return null;
  }
  return PENNSYLVANIA_OFFICE_DEFINITIONS.get(normalizedOffice)?.paOfficeCode ?? null;
}

export function toPennsylvaniaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): PennsylvaniaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isPennsylvaniaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toPennsylvaniaFinanceOfficeKey(input);
  return key !== null && PENNSYLVANIA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export function normalizePennsylvaniaFinanceOfficeLabel(value: string | null | undefined): string | null {
  const normalized = value
    ?.trim()
    .replace(/[.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
  return normalized ? normalized : null;
}

export function normalizePennsylvaniaFinanceLegislativeDistrict(
  value: string | null | undefined,
  maxDistrict: number
): string | null {
  if (!Number.isInteger(maxDistrict) || maxDistrict <= 0) {
    throw new Error(`Invalid Pennsylvania finance max district: ${maxDistrict}`);
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

export function mapPennsylvaniaFinanceOffice(input: {
  office: string | null | undefined;
  district?: string | null | undefined;
}): PennsylvaniaFinanceOfficeMapping | null {
  const normalizedOffice = normalizePennsylvaniaFinanceOfficeLabel(input.office);
  if (!normalizedOffice) {
    return null;
  }
  const definition = PENNSYLVANIA_OFFICE_DEFINITIONS.get(normalizedOffice);
  if (!definition) {
    return null;
  }
  const district = definition.requiresDistrict
    ? normalizePennsylvaniaFinanceLegislativeDistrict(input.district, definition.maxDistrict ?? 0)
    : null;
  if (definition.requiresDistrict && !district) {
    return null;
  }
  const officeKey = toPennsylvaniaFinanceOfficeKey(definition);
  if (!officeKey || !PENNSYLVANIA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  return {
    ...definition,
    officeKey,
    district,
  };
}

export function toPennsylvaniaFinanceOfficeSearchInput(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  district?: string | null | undefined;
}): PennsylvaniaFinanceOfficeSearchInput | null {
  const officeKey = toPennsylvaniaFinanceOfficeKey(input);
  if (!officeKey || !PENNSYLVANIA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  const definition = PENNSYLVANIA_APP_OFFICE_TO_SOURCE.get(officeKey);
  if (!definition) {
    return null;
  }
  const district = definition.requiresDistrict
    ? normalizePennsylvaniaFinanceLegislativeDistrict(input.district, definition.maxDistrict ?? 0)
    : null;
  if (definition.requiresDistrict && !district) {
    return null;
  }
  return {
    paOfficeCode: definition.paOfficeCode,
    district,
  };
}
