export type MissouriMecOfficeSearchInput = {
  politicalOffice: string;
  requiresSubdivision: boolean;
  politicalDistrict: string | null;
  historySubdivision: string | null;
};

const ELIGIBLE_OFFICE_KEYS = [
  "statewide::State Auditor",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
  "county::Collector of Revenue",
  "county::County Assessor",
  "county::County Auditor",
  "county::County Clerk",
  "county::County Clerk and Recorder",
  "county::County Commissioner",
  "county::County Executive",
  "county::County Level Judge",
  "county::County Recorder",
  "county::County Supervisor",
  "county::County Treasurer",
  "county::District Attorney",
  "county::License Collector",
  "county::Recorder of Deeds",
  "county::Sheriff",
  "place::City Council Member",
  "place::City Treasurer",
  "place::Mayor",
  "place::Municipal Assessor",
  "place::Place Level Judge",
  "school_elementary::School Board Member",
  "school_secondary::School Board Member",
  "school_unified::School Board Member",
] as const;

export const MISSOURI_FINANCE_ELIGIBLE_OFFICE_KEYS: ReadonlySet<string> = new Set(ELIGIBLE_OFFICE_KEYS);

// Direct-finance v1 needs a proven primary -> general boundary. MEC history
// does not expose the candidacy-start boundary required for primary elections
// or general elections without a primary, so municipal and school races stay
// resolver-capable but are not scheduled for direct-finance publication yet.
export const MISSOURI_DIRECT_FINANCE_ELIGIBLE_OFFICE_KEYS: ReadonlySet<string> = new Set(
  ELIGIBLE_OFFICE_KEYS.filter((key) => !key.startsWith("place::") && !key.startsWith("school_"))
);

export function isMissouriFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const scope = input.officeScope?.trim();
  const name = input.officeCanonicalName?.trim();
  return Boolean(scope && name && MISSOURI_FINANCE_ELIGIBLE_OFFICE_KEYS.has(`${scope}::${name}`));
}

export function isMissouriDirectFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const scope = input.officeScope?.trim();
  const name = input.officeCanonicalName?.trim();
  return Boolean(scope && name && MISSOURI_DIRECT_FINANCE_ELIGIBLE_OFFICE_KEYS.has(`${scope}::${name}`));
}

export function normalizeMissouriMecText(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeMissouriMecJurisdiction(value: string | null | undefined): string | null {
  // Census/VoteApp keeps apostrophes (Lee's Summit); MEC subdivision labels
  // omit them (City of Lees Summit). Apostrophes do not distinguish Missouri
  // jurisdictions, so remove them instead of turning them into word breaks.
  let normalized = normalizeMissouriMecText((value ?? "").replace(/['’]/g, ""))
    .replace(/\bSTATE OF MISSOURI\b/g, "MISSOURI")
    .replace(/,? MISSOURI$/g, "")
    .trim();
  const cityOf = /^CITY OF (.+)$/.exec(normalized);
  if (cityOf?.[1]) {
    normalized = `${cityOf[1]} CITY`;
  }
  return normalized || null;
}

export function normalizeMissouriMecPoliticalDistrict(value: string | null | undefined): string | null {
  const normalized = normalizeMissouriMecText(value);
  const match = /\b(DISTRICT|WARD|SEAT)\s+(?:NO\s+)?0*(\d+)\b/.exec(normalized);
  if (match?.[1] && match[2]) {
    return `${match[1]} ${Number.parseInt(match[2], 10)}`;
  }
  return /\bAT LARGE\b/.test(normalized) ? "AT LARGE" : null;
}

function countyPoliticalOffice(input: { officeName: string; ballotTitle: string }): string | null {
  const ballot = normalizeMissouriMecText(input.ballotTitle);
  switch (input.officeName) {
    case "Collector of Revenue":
      return "Collector of Revenue";
    case "County Assessor":
      return "Assessor";
    case "County Auditor":
      return "Auditor";
    case "County Clerk":
      return "County Clerk";
    case "County Clerk and Recorder":
      return "Circuit Clerk & Recorder of Deeds";
    case "County Commissioner":
      return /\bPRESIDING\b/.test(ballot) ? "Presiding Commissioner" : "Associate Commissioner";
    case "County Executive":
      return "County Executive";
    case "County Level Judge":
      return /\bASSOCIATE\b/.test(ballot) ? "Associate Circuit Judge" : "Circuit Judge";
    case "County Recorder":
    case "Recorder of Deeds":
      return "Recorder of Deeds";
    case "County Supervisor":
      if (/\bCOUNTY COUNCIL\b/.test(ballot)) {
        return "County Council";
      }
      if (/\bCOUNTY (?:LEGISLATURE|LEGISLATOR)\b/.test(ballot)) {
        return "County Legislature";
      }
      return null;
    case "County Treasurer":
      return "Treasurer";
    case "District Attorney":
      return "Prosecuting Attorney";
    case "License Collector":
      return "License Collector";
    case "Sheriff":
      return "Sheriff";
    default:
      return null;
  }
}

function placePoliticalOffice(input: { officeName: string; ballotTitle: string }): string | null {
  const ballot = normalizeMissouriMecText(input.ballotTitle);
  switch (input.officeName) {
    case "City Council Member":
      return /\bALDER(?:MAN|PERSON|WOMAN)\b/.test(ballot) ? "Alderperson" : "Council Person";
    case "City Treasurer":
      return "Treasurer";
    case "Mayor":
      return "Mayor";
    case "Municipal Assessor":
      return "Assessor";
    case "Place Level Judge":
      return "Municipal Judge";
    default:
      return null;
  }
}

export function toMissouriMecOfficeSearchInput(input: {
  officeScope: string;
  officeName: string;
  ballotTitle: string;
  legislativeDistrict?: string | null;
}): MissouriMecOfficeSearchInput | null {
  if (input.officeScope === "state_lower" && input.officeName === "State Lower Chamber Legislator") {
    const district = normalizeMissouriMecPoliticalDistrict(`District ${input.legislativeDistrict ?? ""}`);
    return {
      politicalOffice: "State Representative",
      requiresSubdivision: false,
      politicalDistrict: district,
      historySubdivision: "Missouri House of Representatives",
    };
  }
  if (input.officeScope === "state_upper" && input.officeName === "State Senator") {
    const district = normalizeMissouriMecPoliticalDistrict(`District ${input.legislativeDistrict ?? ""}`);
    return {
      politicalOffice: "State Senator",
      requiresSubdivision: false,
      politicalDistrict: district,
      historySubdivision: "Missouri State Senate",
    };
  }
  if (input.officeScope === "statewide" && input.officeName === "State Auditor") {
    return {
      politicalOffice: "State Auditor",
      requiresSubdivision: false,
      politicalDistrict: null,
      historySubdivision: null,
    };
  }
  if (["school_elementary", "school_secondary", "school_unified"].includes(input.officeScope)) {
    if (input.officeName !== "School Board Member") {
      return null;
    }
    return {
      politicalOffice: "Boardmember",
      requiresSubdivision: true,
      politicalDistrict: normalizeMissouriMecPoliticalDistrict(input.ballotTitle),
      historySubdivision: null,
    };
  }

  const politicalOffice =
    input.officeScope === "county"
      ? countyPoliticalOffice(input)
      : input.officeScope === "place"
        ? placePoliticalOffice(input)
        : null;
  if (politicalOffice === null) {
    return null;
  }
  return {
    politicalOffice,
    requiresSubdivision: true,
    politicalDistrict: normalizeMissouriMecPoliticalDistrict(input.ballotTitle),
    historySubdivision: null,
  };
}
