export type VermontFinanceEligibleOfficeKey = `${string}::${string}`;

export type VermontFinanceOfficeScope = "statewide" | "state_upper" | "state_lower";

export type VermontOfficeSoughtMapping = {
  officeScope: VermontFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: VermontFinanceEligibleOfficeKey;
  officeId: number;
  officeName: string;
};

export type VermontOfficeSearchInput = {
  officeId: number;
  officeName: string;
};

export const VERMONT_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::State Treasurer",
  "statewide::State Auditor",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly VermontFinanceEligibleOfficeKey[];

const VERMONT_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(VERMONT_FINANCE_ELIGIBLE_OFFICE_KEYS);

type VermontOfficeDefinition = {
  officeScope: VermontFinanceOfficeScope;
  officeCanonicalName: string;
  officeId: number;
  officeName: string;
};

const VERMONT_OFFICE_DEFINITIONS = new Map<string, VermontOfficeDefinition>([
  [
    "statewide::Governor",
    { officeScope: "statewide", officeCanonicalName: "Governor", officeId: 19, officeName: "Governor" },
  ],
  [
    "statewide::Lieutenant Governor",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      officeId: 20,
      officeName: "Lieutenant Governor",
    },
  ],
  [
    "statewide::Secretary of State",
    {
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
      officeId: 22,
      officeName: "Secretary of State",
    },
  ],
  [
    "statewide::Attorney General",
    {
      officeScope: "statewide",
      officeCanonicalName: "Attorney General",
      officeId: 24,
      officeName: "Attorney General",
    },
  ],
  [
    "statewide::State Treasurer",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Treasurer",
      officeId: 21,
      officeName: "State Treasurer",
    },
  ],
  [
    "statewide::State Auditor",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Auditor",
      officeId: 23,
      officeName: "Auditor of Accounts",
    },
  ],
  [
    "state_upper::State Senator",
    { officeScope: "state_upper", officeCanonicalName: "State Senator", officeId: 6, officeName: "State Senator" },
  ],
  [
    "state_lower::State Lower Chamber Legislator",
    {
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      officeId: 7,
      officeName: "State Representative",
    },
  ],
]);

export function toVermontFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): VermontFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isVermontFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toVermontFinanceOfficeKey(input);
  return key !== null && VERMONT_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export function mapVermontOfficeSought(input: { officeId: number | null | undefined }): VermontOfficeSoughtMapping | null {
  if (!Number.isInteger(input.officeId)) {
    return null;
  }
  for (const definition of VERMONT_OFFICE_DEFINITIONS.values()) {
    if (definition.officeId !== input.officeId) {
      continue;
    }
    const officeKey = toVermontFinanceOfficeKey(definition);
    if (!officeKey || !VERMONT_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
      return null;
    }
    return {
      ...definition,
      officeKey,
    };
  }
  return null;
}

export function toVermontOfficeSearchInput(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): VermontOfficeSearchInput | null {
  const officeKey = toVermontFinanceOfficeKey(input);
  if (!officeKey || !VERMONT_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  const definition = VERMONT_OFFICE_DEFINITIONS.get(officeKey);
  if (!definition) {
    return null;
  }
  return {
    officeId: definition.officeId,
    officeName: definition.officeName,
  };
}
