export type WashingtonFinanceEligibleOfficeKey = `${string}::${string}`;

export type WashingtonFinanceOfficeScope = "statewide" | "state_upper" | "state_lower" | "place";

export type WashingtonPdcOfficeMapping = {
  officeScope: WashingtonFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: WashingtonFinanceEligibleOfficeKey;
  pdcOffice: string;
  requiresLegislativeDistrict: boolean;
  legislativeDistrict: string | null;
  requiresJurisdiction: boolean;
  jurisdiction: string | null;
  position: string | null;
};

export type WashingtonPdcOfficeSearchInput = {
  pdcOffice: string;
  legislativeDistrict: string | null;
  requiresJurisdiction: boolean;
  jurisdiction: string | null;
  position: string | null;
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
  "place::Mayor",
  "place::City Council Member",
  "place::Municipal Attorney",
  "place::Place Level Judge",
] as const satisfies readonly WashingtonFinanceEligibleOfficeKey[];

const WASHINGTON_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  WASHINGTON_FINANCE_ELIGIBLE_OFFICE_KEYS
);

type WashingtonPdcOfficeDefinition = {
  officeScope: WashingtonFinanceOfficeScope;
  officeCanonicalName: string;
  pdcOffice: string;
  requiresLegislativeDistrict: boolean;
  // City-scope offices are identified by PDC's jurisdiction column instead of
  // a legislative district ("CITY OF SEATTLE", judges "SEATTLE MUNICIPAL
  // COURT"); both normalize to the bare city name.
  requiresJurisdiction: boolean;
};

const WASHINGTON_PDC_OFFICE_DEFINITIONS = new Map<string, WashingtonPdcOfficeDefinition>([
  [
    "GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Governor",
      pdcOffice: "GOVERNOR",
      requiresLegislativeDistrict: false,
      requiresJurisdiction: false,
    },
  ],
  [
    "LIEUTENANT GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      pdcOffice: "LIEUTENANT GOVERNOR",
      requiresLegislativeDistrict: false,
      requiresJurisdiction: false,
    },
  ],
  [
    "SECRETARY OF STATE",
    {
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
      pdcOffice: "SECRETARY OF STATE",
      requiresLegislativeDistrict: false,
      requiresJurisdiction: false,
    },
  ],
  [
    "ATTORNEY GENERAL",
    {
      officeScope: "statewide",
      officeCanonicalName: "Attorney General",
      pdcOffice: "ATTORNEY GENERAL",
      requiresLegislativeDistrict: false,
      requiresJurisdiction: false,
    },
  ],
  [
    "STATE TREASURER",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Treasurer",
      pdcOffice: "STATE TREASURER",
      requiresLegislativeDistrict: false,
      requiresJurisdiction: false,
    },
  ],
  [
    "STATE AUDITOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Auditor",
      pdcOffice: "STATE AUDITOR",
      requiresLegislativeDistrict: false,
      requiresJurisdiction: false,
    },
  ],
  [
    "PUBLIC LANDS COMMISSIONER",
    {
      officeScope: "statewide",
      officeCanonicalName: "Land Commissioner",
      pdcOffice: "PUBLIC LANDS COMMISSIONER",
      requiresLegislativeDistrict: false,
      requiresJurisdiction: false,
    },
  ],
  [
    "INSURANCE COMMISSIONER",
    {
      officeScope: "statewide",
      officeCanonicalName: "Commissioner of Insurance",
      pdcOffice: "INSURANCE COMMISSIONER",
      requiresLegislativeDistrict: false,
      requiresJurisdiction: false,
    },
  ],
  [
    "SUPERINTENDENT OF PUBLIC INSTRUCTION",
    {
      officeScope: "statewide",
      officeCanonicalName: "Superintendent of Public Instruction",
      pdcOffice: "SUPERINTENDENT OF PUBLIC INSTRUCTION",
      requiresLegislativeDistrict: false,
      requiresJurisdiction: false,
    },
  ],
  [
    "STATE SENATOR",
    {
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      pdcOffice: "STATE SENATOR",
      requiresLegislativeDistrict: true,
      requiresJurisdiction: false,
    },
  ],
  [
    "STATE REPRESENTATIVE",
    {
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      pdcOffice: "STATE REPRESENTATIVE",
      requiresLegislativeDistrict: true,
      requiresJurisdiction: false,
    },
  ],
  [
    "MAYOR",
    {
      officeScope: "place",
      officeCanonicalName: "Mayor",
      pdcOffice: "MAYOR",
      requiresLegislativeDistrict: false,
      requiresJurisdiction: true,
    },
  ],
  [
    "CITY COUNCIL MEMBER",
    {
      officeScope: "place",
      officeCanonicalName: "City Council Member",
      pdcOffice: "CITY COUNCIL MEMBER",
      requiresLegislativeDistrict: false,
      requiresJurisdiction: true,
    },
  ],
  // The app's canonical place office name is "Municipal Attorney"; PDC labels
  // the same office "CITY ATTORNEY" (Seattle 2025 verified).
  [
    "CITY ATTORNEY",
    {
      officeScope: "place",
      officeCanonicalName: "Municipal Attorney",
      pdcOffice: "CITY ATTORNEY",
      requiresLegislativeDistrict: false,
      requiresJurisdiction: true,
    },
  ],
  [
    "MUNICIPAL COURT JUDGE",
    {
      officeScope: "place",
      officeCanonicalName: "Place Level Judge",
      pdcOffice: "MUNICIPAL COURT JUDGE",
      requiresLegislativeDistrict: false,
      requiresJurisdiction: true,
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

// Collapses PDC's jurisdiction labels ("CITY OF SEATTLE", "TOWN OF STEILACOOM",
// "SEATTLE MUNICIPAL COURT", "CITY OF AUBURN *") and VoteApp's place district
// names ("Seattle city, Washington") to one bare city key ("SEATTLE"). Generic
// on purpose: any WA city works the moment its roster exists.
export function normalizeWashingtonPdcJurisdiction(value: string | null | undefined): string | null {
  const normalized = value
    ?.normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) {
    return null;
  }
  const stripped = normalized
    .replace(/\s+(?:WASHINGTON|WA)$/, "")
    .replace(/^(?:CITY|TOWN) OF\s+/, "")
    .replace(/\s+MUNICIPAL COURT$/, "")
    .replace(/\s+(?:CITY|TOWN)$/, "")
    .trim();
  return stripped.length > 0 ? stripped : null;
}

// PDC's position column for council seats and municipal-court judgeships is a
// bare seat number. Free-text positions on other office types (e.g. county
// charter review "District 7, Position 1") normalize to null, which simply
// disables the position-agreement requirement for that row.
export function normalizeWashingtonPdcPosition(value: string | null | undefined): string | null {
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase();
  if (!normalized) {
    return null;
  }
  const match = normalized.match(/^(?:POS(?:ITION)?|DIST(?:RICT)?|SEAT)?\.?\s*(?:NO\.?\s*)?0*([1-9][0-9]{0,2})$/);
  return match?.[1] ?? null;
}

// VoteApp ballot titles carry the seat as "Council District No. 5" or
// "Municipal Court Judge Position No. 5". The last numbered token wins so a
// title with both a district and a position keeps the seat identifier.
export function parseWashingtonPositionFromBallotTitle(value: string | null | undefined): string | null {
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase();
  if (!normalized) {
    return null;
  }
  const matches = [...normalized.matchAll(/\b(?:POSITION|DISTRICT|SEAT|POS|DIST)\.?\s*(?:NO\.?\s*)?0*([1-9][0-9]{0,2})\b/g)];
  return matches[matches.length - 1]?.[1] ?? null;
}

export function mapWashingtonPdcOffice(input: {
  office: string | null | undefined;
  legislativeDistrict?: string | null | undefined;
  jurisdiction?: string | null | undefined;
  position?: string | null | undefined;
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
  const jurisdiction = definition.requiresJurisdiction
    ? normalizeWashingtonPdcJurisdiction(input.jurisdiction)
    : null;
  if (definition.requiresJurisdiction && !jurisdiction) {
    return null;
  }
  const position = definition.requiresJurisdiction ? normalizeWashingtonPdcPosition(input.position) : null;
  const officeKey = toWashingtonFinanceOfficeKey(definition);
  if (!officeKey || !WASHINGTON_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  return {
    ...definition,
    officeKey,
    legislativeDistrict,
    jurisdiction,
    position,
  };
}

export function toWashingtonPdcOfficeSearchInput(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  legislativeDistrict?: string | null | undefined;
  jurisdiction?: string | null | undefined;
  position?: string | null | undefined;
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
  const jurisdiction = definition.requiresJurisdiction
    ? normalizeWashingtonPdcJurisdiction(input.jurisdiction)
    : null;
  if (definition.requiresJurisdiction && !jurisdiction) {
    return null;
  }
  return {
    pdcOffice: definition.pdcOffice,
    legislativeDistrict,
    requiresJurisdiction: definition.requiresJurisdiction,
    jurisdiction,
    position: definition.requiresJurisdiction ? normalizeWashingtonPdcPosition(input.position) : null,
  };
}
