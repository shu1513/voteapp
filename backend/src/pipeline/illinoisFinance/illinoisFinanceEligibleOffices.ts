export type IllinoisFinanceEligibleOfficeKey = `${string}::${string}`;

export type IllinoisFinanceOfficeScope = "statewide" | "state_upper" | "state_lower" | "place";
export type IllinoisSbeLocalDistrictType = "City" | "Village" | "Town";

export type IllinoisSbeOfficeMapping = {
  officeScope: IllinoisFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: IllinoisFinanceEligibleOfficeKey;
  sbeOffice: string;
  requiresDistrict: boolean;
  district: string | null;
  maxDistrict: number | null;
  sbeDistrictType?: IllinoisSbeLocalDistrictType;
  requiresAtLargeEvidence?: boolean;
};

export type IllinoisSbeOfficeSearchInput = {
  sbeOffice: string;
  district: string | null;
  sbeDistrictType?: IllinoisSbeLocalDistrictType;
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
  "place::Mayor",
  "place::City Clerk",
  "place::City Treasurer",
  "place::Municipal Assessor",
  "place::Alderman",
  "place::City Council Member",
  "place::Municipal Trustee",
] as const satisfies readonly IllinoisFinanceEligibleOfficeKey[];

const ILLINOIS_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(ILLINOIS_FINANCE_ELIGIBLE_OFFICE_KEYS);

type IllinoisSbeStateOfficeDefinition = {
  officeScope: Exclude<IllinoisFinanceOfficeScope, "place">;
  officeCanonicalName: string;
  sbeOffice: string;
  requiresDistrict: boolean;
  maxDistrict: number | null;
};

type IllinoisSbeLocalOfficeDefinition = {
  officeCanonicalName: string;
  sbeOffice: string;
  districtTypes: readonly IllinoisSbeLocalDistrictType[];
  requiresAtLargeEvidence: boolean;
};

const ILLINOIS_SBE_STATE_OFFICE_DEFINITIONS = new Map<string, IllinoisSbeStateOfficeDefinition>([
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

const ALL_LOCAL_DISTRICT_TYPES = ["City", "Village", "Town"] as const;
const VILLAGE_TOWN_DISTRICT_TYPES = ["Village", "Town"] as const;

const ILLINOIS_SBE_LOCAL_OFFICE_DEFINITIONS: readonly IllinoisSbeLocalOfficeDefinition[] = [
  { officeCanonicalName: "Mayor", sbeOffice: "Mayor", districtTypes: ALL_LOCAL_DISTRICT_TYPES, requiresAtLargeEvidence: false },
  { officeCanonicalName: "Mayor", sbeOffice: "President", districtTypes: VILLAGE_TOWN_DISTRICT_TYPES, requiresAtLargeEvidence: false },
  { officeCanonicalName: "City Clerk", sbeOffice: "Clerk", districtTypes: ALL_LOCAL_DISTRICT_TYPES, requiresAtLargeEvidence: false },
  { officeCanonicalName: "City Treasurer", sbeOffice: "Treasurer", districtTypes: ALL_LOCAL_DISTRICT_TYPES, requiresAtLargeEvidence: false },
  { officeCanonicalName: "Municipal Assessor", sbeOffice: "Assessor", districtTypes: ALL_LOCAL_DISTRICT_TYPES, requiresAtLargeEvidence: false },
  { officeCanonicalName: "Alderman", sbeOffice: "Alderman", districtTypes: ALL_LOCAL_DISTRICT_TYPES, requiresAtLargeEvidence: true },
  { officeCanonicalName: "Alderman", sbeOffice: "Alderperson", districtTypes: ALL_LOCAL_DISTRICT_TYPES, requiresAtLargeEvidence: true },
  { officeCanonicalName: "City Council Member", sbeOffice: "Councilman", districtTypes: ALL_LOCAL_DISTRICT_TYPES, requiresAtLargeEvidence: true },
  { officeCanonicalName: "City Council Member", sbeOffice: "Councilperson", districtTypes: ALL_LOCAL_DISTRICT_TYPES, requiresAtLargeEvidence: true },
  { officeCanonicalName: "City Council Member", sbeOffice: "City Council", districtTypes: ALL_LOCAL_DISTRICT_TYPES, requiresAtLargeEvidence: true },
  { officeCanonicalName: "Municipal Trustee", sbeOffice: "Trustee", districtTypes: VILLAGE_TOWN_DISTRICT_TYPES, requiresAtLargeEvidence: true },
];

const ILLINOIS_APP_STATE_OFFICE_TO_SBE = new Map<string, IllinoisSbeStateOfficeDefinition>(
  [...ILLINOIS_SBE_STATE_OFFICE_DEFINITIONS.values()].map((definition) => [
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

export function normalizeIllinoisSbeLocalDistrictType(
  value: string | null | undefined
): IllinoisSbeLocalDistrictType | null {
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase();
  if (normalized === "CITY") return "City";
  if (normalized === "VILLAGE") return "Village";
  if (normalized === "TOWN") return "Town";
  return null;
}

export function normalizeIllinoisSbeMunicipality(value: string | null | undefined): string | null {
  const normalized = value?.trim().replace(/\s+/g, " ");
  return normalized ? normalized : null;
}

function normalizeMunicipalityKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/,?\s+(?:ILLINOIS|IL)\s*$/, "")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function illinoisMunicipalityMatches(input: {
  voteAppDistrictName: string | null | undefined;
  sbeDistrictName: string | null | undefined;
  sbeDistrictType: string | null | undefined;
}): boolean {
  const districtType = normalizeIllinoisSbeLocalDistrictType(input.sbeDistrictType);
  const voteAppName = input.voteAppDistrictName?.trim() ?? "";
  const sbeName = input.sbeDistrictName?.trim() ?? "";
  if (!districtType || !voteAppName || !sbeName) {
    return false;
  }
  const voteAppWithoutState = normalizeMunicipalityKey(voteAppName);
  const suffixMatch = voteAppWithoutState.match(/^(.*)\s+(CITY|VILLAGE|TOWN)$/);
  if (suffixMatch?.[2] && suffixMatch[2] !== districtType.toUpperCase()) {
    return false;
  }
  const voteAppCore = suffixMatch?.[1]?.trim() ?? voteAppWithoutState;
  const normalizedSbeName = normalizeMunicipalityKey(sbeName);
  const sbeSuffixMatch = normalizedSbeName.match(/^(.*)\s+(CITY|VILLAGE|TOWN)$/);
  if (sbeSuffixMatch?.[2] && sbeSuffixMatch[2] !== districtType.toUpperCase()) {
    return false;
  }
  const sbeCore = sbeSuffixMatch?.[1]?.trim() ?? normalizedSbeName;
  return voteAppCore.length > 0 && voteAppCore === sbeCore;
}

export function normalizeIllinoisSbeLegislativeDistrict(
  value: string | null | undefined,
  maxDistrict: number,
  officeScope?: IllinoisFinanceOfficeScope | null
): string | null {
  if (!Number.isInteger(maxDistrict) || maxDistrict <= 0) {
    throw new Error(`Invalid Illinois SBE max district: ${maxDistrict}`);
  }
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase();
  if (!normalized) {
    return null;
  }
  const prefixMatch = normalized.match(/^([A-Z]+)(?:\s+DIST(?:RICT)?)?\s+/);
  const prefix = prefixMatch?.[1] ?? null;
  if (officeScope === "state_upper" && prefix && /^(?:HD|HOUSE|REP|REPRESENTATIVE)$/.test(prefix)) {
    return null;
  }
  if (officeScope === "state_lower" && prefix && /^(?:SD|SEN|SENATE)$/.test(prefix)) {
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

function localDefinitionMatches(input: {
  definition: IllinoisSbeLocalOfficeDefinition;
  districtType: IllinoisSbeLocalDistrictType;
  isAtLarge: boolean | null | undefined;
}): boolean {
  return (
    input.definition.districtTypes.includes(input.districtType) &&
    (!input.definition.requiresAtLargeEvidence || input.isAtLarge === true)
  );
}

export function mapIllinoisSbeOffice(input: {
  office: string | null | undefined;
  district?: string | null | undefined;
  districtType?: string | null | undefined;
  isAtLarge?: boolean | null | undefined;
}): IllinoisSbeOfficeMapping | null {
  const normalizedOffice = normalizeIllinoisSbeOfficeLabel(input.office);
  if (!normalizedOffice) {
    return null;
  }

  const districtType = normalizeIllinoisSbeLocalDistrictType(input.districtType);
  const stateDefinition = districtType ? undefined : ILLINOIS_SBE_STATE_OFFICE_DEFINITIONS.get(normalizedOffice);
  if (stateDefinition) {
    const district = stateDefinition.requiresDistrict
      ? normalizeIllinoisSbeLegislativeDistrict(
          input.district,
          stateDefinition.maxDistrict ?? 0,
          stateDefinition.officeScope
        )
      : null;
    if (stateDefinition.requiresDistrict && !district) {
      return null;
    }
    const officeKey = toIllinoisFinanceOfficeKey(stateDefinition);
    if (!officeKey || !ILLINOIS_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
      return null;
    }
    return { ...stateDefinition, officeKey, district };
  }

  const district = normalizeIllinoisSbeMunicipality(input.district);
  if (!districtType || !district) {
    return null;
  }
  const localDefinition = ILLINOIS_SBE_LOCAL_OFFICE_DEFINITIONS.find(
    (definition) =>
      normalizeIllinoisSbeOfficeLabel(definition.sbeOffice) === normalizedOffice &&
      localDefinitionMatches({ definition, districtType, isAtLarge: input.isAtLarge })
  );
  if (!localDefinition) {
    return null;
  }
  const officeKey = toIllinoisFinanceOfficeKey({
    officeScope: "place",
    officeCanonicalName: localDefinition.officeCanonicalName,
  });
  if (!officeKey || !ILLINOIS_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  return {
    officeScope: "place",
    officeCanonicalName: localDefinition.officeCanonicalName,
    officeKey,
    sbeOffice: localDefinition.sbeOffice,
    requiresDistrict: true,
    district,
    maxDistrict: null,
    sbeDistrictType: districtType,
    requiresAtLargeEvidence: localDefinition.requiresAtLargeEvidence,
  };
}

export function toIllinoisSbeOfficeSearchInput(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  district?: string | null | undefined;
  districtType?: string | null | undefined;
  sbeOffice?: string | null | undefined;
  isAtLarge?: boolean | null | undefined;
}): IllinoisSbeOfficeSearchInput | null {
  const officeKey = toIllinoisFinanceOfficeKey(input);
  if (!officeKey || !ILLINOIS_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }

  if (input.officeScope?.trim() !== "place") {
    const definition = ILLINOIS_APP_STATE_OFFICE_TO_SBE.get(officeKey);
    if (!definition) {
      return null;
    }
    const district = definition.requiresDistrict
      ? normalizeIllinoisSbeLegislativeDistrict(input.district, definition.maxDistrict ?? 0, definition.officeScope)
      : null;
    if (definition.requiresDistrict && !district) {
      return null;
    }
    return { sbeOffice: definition.sbeOffice, district };
  }

  const districtType = normalizeIllinoisSbeLocalDistrictType(input.districtType);
  const district = normalizeIllinoisSbeMunicipality(input.district);
  const sourceOffice = normalizeIllinoisSbeOfficeLabel(input.sbeOffice);
  if (!districtType || !district) {
    return null;
  }
  const definition = ILLINOIS_SBE_LOCAL_OFFICE_DEFINITIONS.find(
    (candidate) =>
      candidate.officeCanonicalName === input.officeCanonicalName?.trim() &&
      (!sourceOffice || normalizeIllinoisSbeOfficeLabel(candidate.sbeOffice) === sourceOffice) &&
      localDefinitionMatches({ definition: candidate, districtType, isAtLarge: input.isAtLarge })
  );
  if (!definition) {
    return null;
  }
  return { sbeOffice: definition.sbeOffice, district, sbeDistrictType: districtType };
}
