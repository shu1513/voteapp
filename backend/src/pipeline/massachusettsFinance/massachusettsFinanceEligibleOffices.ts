export type MassachusettsFinanceEligibleOfficeKey = `${string}::${string}`;

export type MassachusettsFinanceOfficeScope = "statewide" | "state_upper" | "state_lower";

export type MassachusettsOcpfOfficeMapping = {
  officeScope: MassachusettsFinanceOfficeScope;
  officeCanonicalName: string;
  officeKey: MassachusettsFinanceEligibleOfficeKey;
  ocpfOffice: string;
  requiresDistrict: boolean;
  district: string | null;
};

export type MassachusettsOcpfOfficeSearchInput = {
  ocpfOffice: string;
  district: string | null;
};

export const MASSACHUSETTS_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::State Treasurer",
  "statewide::State Auditor",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly MassachusettsFinanceEligibleOfficeKey[];

const MASSACHUSETTS_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  MASSACHUSETTS_FINANCE_ELIGIBLE_OFFICE_KEYS
);

type MassachusettsOcpfOfficeDefinition = {
  officeScope: MassachusettsFinanceOfficeScope;
  officeCanonicalName: string;
  ocpfOffice: string;
  requiresDistrict: boolean;
};

const MASSACHUSETTS_OCPF_STATEWIDE_OFFICE_DEFINITIONS = new Map<string, MassachusettsOcpfOfficeDefinition>([
  [
    "STATEWIDE GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Governor",
      ocpfOffice: "Statewide, Governor",
      requiresDistrict: false,
    },
  ],
  [
    "GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Governor",
      ocpfOffice: "Statewide, Governor",
      requiresDistrict: false,
    },
  ],
  [
    "STATEWIDE LIEUTENANT GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      ocpfOffice: "Statewide, Lt. Governor",
      requiresDistrict: false,
    },
  ],
  [
    "STATEWIDE LT GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      ocpfOffice: "Statewide, Lt. Governor",
      requiresDistrict: false,
    },
  ],
  [
    "LIEUTENANT GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      ocpfOffice: "Statewide, Lt. Governor",
      requiresDistrict: false,
    },
  ],
  [
    "LT GOVERNOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      ocpfOffice: "Statewide, Lt. Governor",
      requiresDistrict: false,
    },
  ],
  [
    "STATEWIDE SECRETARY OF STATE",
    {
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
      ocpfOffice: "Statewide, Secretary of State",
      requiresDistrict: false,
    },
  ],
  [
    "SECRETARY OF STATE",
    {
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
      ocpfOffice: "Statewide, Secretary of State",
      requiresDistrict: false,
    },
  ],
  [
    "STATEWIDE SECRETARY OF COMMONWEALTH",
    {
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
      ocpfOffice: "Statewide, Secretary of State",
      requiresDistrict: false,
    },
  ],
  [
    "SECRETARY OF COMMONWEALTH",
    {
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
      ocpfOffice: "Statewide, Secretary of State",
      requiresDistrict: false,
    },
  ],
  [
    "STATEWIDE ATTORNEY GENERAL",
    {
      officeScope: "statewide",
      officeCanonicalName: "Attorney General",
      ocpfOffice: "Statewide, Attorney General",
      requiresDistrict: false,
    },
  ],
  [
    "ATTORNEY GENERAL",
    {
      officeScope: "statewide",
      officeCanonicalName: "Attorney General",
      ocpfOffice: "Statewide, Attorney General",
      requiresDistrict: false,
    },
  ],
  [
    "STATEWIDE TREASURER",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Treasurer",
      ocpfOffice: "Statewide, Treasurer",
      requiresDistrict: false,
    },
  ],
  [
    "STATE TREASURER",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Treasurer",
      ocpfOffice: "Statewide, Treasurer",
      requiresDistrict: false,
    },
  ],
  [
    "TREASURER",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Treasurer",
      ocpfOffice: "Statewide, Treasurer",
      requiresDistrict: false,
    },
  ],
  [
    "STATEWIDE AUDITOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Auditor",
      ocpfOffice: "Statewide, Auditor",
      requiresDistrict: false,
    },
  ],
  [
    "STATE AUDITOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Auditor",
      ocpfOffice: "Statewide, Auditor",
      requiresDistrict: false,
    },
  ],
  [
    "AUDITOR",
    {
      officeScope: "statewide",
      officeCanonicalName: "State Auditor",
      ocpfOffice: "Statewide, Auditor",
      requiresDistrict: false,
    },
  ],
]);

const MASSACHUSETTS_APP_OFFICE_DEFINITIONS: readonly MassachusettsOcpfOfficeDefinition[] = [
  ...MASSACHUSETTS_OCPF_STATEWIDE_OFFICE_DEFINITIONS.values(),
  {
    officeScope: "state_upper",
    officeCanonicalName: "State Senator",
    ocpfOffice: "Senate",
    requiresDistrict: true,
  },
  {
    officeScope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    ocpfOffice: "House",
    requiresDistrict: true,
  },
];

const MASSACHUSETTS_APP_OFFICE_TO_OCPF = new Map<string, MassachusettsOcpfOfficeDefinition>(
  MASSACHUSETTS_APP_OFFICE_DEFINITIONS.map((definition) => [
    `${definition.officeScope}::${definition.officeCanonicalName}`,
    definition,
  ])
);

export function toMassachusettsFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): MassachusettsFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isMassachusettsFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toMassachusettsFinanceOfficeKey(input);
  return key !== null && MASSACHUSETTS_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export function normalizeMassachusettsOcpfOfficeLabel(value: string | null | undefined): string | null {
  const normalized = value
    ?.trim()
    .replace(/[.,]/g, " ")
    .replace(/\s+/g, " ")
    .toUpperCase();
  return normalized ? normalized : null;
}

export function normalizeMassachusettsOcpfDistrict(value: string | null | undefined): string | null {
  const normalized = value
    ?.trim()
    .replace(/[.,]/g, " ")
    .replace(/\b(?:DISTRICT|DIST)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
  return normalized ? normalized : null;
}

function splitOcpfLegislativeOfficeLabel(value: string | null | undefined): {
  ocpfOffice: "Senate" | "House";
  district: string | null;
} | null {
  const trimmed = value?.trim().replace(/\s+/g, " ");
  if (!trimmed) {
    return null;
  }
  const match = /^(Senate|House)\s*,?\s*(.+)?$/i.exec(trimmed);
  if (!match?.[1]) {
    return null;
  }
  return {
    ocpfOffice: match[1].toLowerCase() === "senate" ? "Senate" : "House",
    district: normalizeMassachusettsOcpfDistrict(match[2]),
  };
}

export function mapMassachusettsOcpfOffice(input: {
  officeSought: string | null | undefined;
}): MassachusettsOcpfOfficeMapping | null {
  const legislative = splitOcpfLegislativeOfficeLabel(input.officeSought);
  if (legislative) {
    const definition: MassachusettsOcpfOfficeDefinition = legislative.ocpfOffice === "Senate"
      ? {
          officeScope: "state_upper",
          officeCanonicalName: "State Senator",
          ocpfOffice: "Senate",
          requiresDistrict: true,
        }
      : {
          officeScope: "state_lower",
          officeCanonicalName: "State Lower Chamber Legislator",
          ocpfOffice: "House",
          requiresDistrict: true,
        };
    const officeKey = toMassachusettsFinanceOfficeKey(definition);
    if (!officeKey || !MASSACHUSETTS_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey) || !legislative.district) {
      return null;
    }
    return {
      ...definition,
      officeKey,
      district: legislative.district,
    };
  }

  const normalizedOffice = normalizeMassachusettsOcpfOfficeLabel(input.officeSought);
  if (!normalizedOffice) {
    return null;
  }
  const definition = MASSACHUSETTS_OCPF_STATEWIDE_OFFICE_DEFINITIONS.get(normalizedOffice);
  if (!definition) {
    return null;
  }
  const officeKey = toMassachusettsFinanceOfficeKey(definition);
  if (!officeKey || !MASSACHUSETTS_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  return {
    ...definition,
    officeKey,
    district: null,
  };
}

export function toMassachusettsOcpfOfficeSearchInput(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  district?: string | null | undefined;
}): MassachusettsOcpfOfficeSearchInput | null {
  const officeKey = toMassachusettsFinanceOfficeKey(input);
  if (!officeKey || !MASSACHUSETTS_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(officeKey)) {
    return null;
  }
  const definition = MASSACHUSETTS_APP_OFFICE_TO_OCPF.get(officeKey);
  if (!definition) {
    return null;
  }
  const district = definition.requiresDistrict ? normalizeMassachusettsOcpfDistrict(input.district) : null;
  if (definition.requiresDistrict && !district) {
    return null;
  }
  return {
    ocpfOffice: definition.ocpfOffice,
    district,
  };
}
