// Kansas finance office eligibility + viewer office mapping
// (plan-kansas-finance.md, Phase 3).
//
// Scope is the plan's v1 list where VoteApp carries the race today: the five
// statewide constitutional offices, State House, and State Senate (2026 has
// special elections; the regular Senate cycle is 2028). Federal races are the
// FEC path. County rows are excluded on purpose: VoteApp's Kansas
// "county::District Attorney" rows are County Attorneys (filed with county
// election officers, not KPDC), so mapping them to the viewer's "District
// Attorney" office would link the wrong filer class. State Board of Education
// is added when the roster carries it.
//
// Viewer facts pinned live 2026-09-01: drpdownOffice option values match
// KANSAS_CFR_OFFICE_CODES; the results grids render the office as
// "State Representative" (reports grid) or "STATE REPRESENTATIVE"
// (appointment/affidavit grids), so office text is compared case-insensitively.

import { KANSAS_CFR_OFFICE_CODES } from "./kansasCfrViewerClient.js";

export type KansasFinanceEligibleOfficeKey = `${string}::${string}`;

export type KansasCfrOffice = {
  /** drpdownOffice option value. */
  code: string;
  /** Dropdown text, compared case-insensitively against grid rows. */
  label: string;
  /** Rows must carry the roster district number; statewide rows ignore it. */
  districted: boolean;
  /**
   * Years before the election year that the K.S.A. 25-4148 cycle window
   * opens: statewide offices and the Senate run four-year cycles
   * (1/1/2023-12/31/2026 for Nov-2026), the House a two-year cycle.
   */
  cycleYearsBefore: number;
};

const OFFICES: Record<string, KansasCfrOffice> = {
  "statewide::Governor": { code: KANSAS_CFR_OFFICE_CODES.governor, label: "Governor", districted: false, cycleYearsBefore: 3 },
  "statewide::Secretary of State": {
    code: KANSAS_CFR_OFFICE_CODES.secretaryOfState,
    label: "Secretary of State",
    districted: false,
    cycleYearsBefore: 3,
  },
  "statewide::Attorney General": {
    code: KANSAS_CFR_OFFICE_CODES.attorneyGeneral,
    label: "Attorney General",
    districted: false,
    cycleYearsBefore: 3,
  },
  // VoteApp canonical name differs from the viewer label.
  "statewide::Commissioner of Insurance": {
    code: KANSAS_CFR_OFFICE_CODES.insuranceCommissioner,
    label: "Insurance Commissioner",
    districted: false,
    cycleYearsBefore: 3,
  },
  "statewide::State Treasurer": {
    code: KANSAS_CFR_OFFICE_CODES.stateTreasurer,
    label: "State Treasurer",
    districted: false,
    cycleYearsBefore: 3,
  },
  "state_upper::State Senator": {
    code: KANSAS_CFR_OFFICE_CODES.stateSenator,
    label: "State Senator",
    districted: true,
    cycleYearsBefore: 3,
  },
  "state_lower::State Lower Chamber Legislator": {
    code: KANSAS_CFR_OFFICE_CODES.stateRepresentative,
    label: "State Representative",
    districted: true,
    cycleYearsBefore: 1,
  },
};

export const KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS: ReadonlySet<string> = new Set(Object.keys(OFFICES));

export function toKansasFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): KansasFinanceEligibleOfficeKey | null {
  const scope = input.officeScope?.trim();
  const name = input.officeCanonicalName?.trim();
  if (!scope || !name) return null;
  return `${scope}::${name}`;
}

export function isKansasFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toKansasFinanceOfficeKey(input);
  return key !== null && KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS.has(key);
}

/** Viewer office for a VoteApp race; null outside the v1 map (fail closed). */
export function kansasCfrOfficeForRace(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): KansasCfrOffice | null {
  const key = toKansasFinanceOfficeKey(input);
  return key === null ? null : OFFICES[key] ?? null;
}

/** MM/DD/YYYY as the viewer's date inputs expect (UTC calendar date). */
export function formatKansasCfrDate(date: Date): string {
  if (Number.isNaN(date.getTime())) throw new Error("Invalid Kansas CFR date");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${month}/${day}/${date.getUTCFullYear()}`;
}

/**
 * Filed-date window for enumerating an office's filers: the cycle window
 * opening (January 1 of the cycle's first year) through today. Filers who
 * last filed before the window are outside the cycle by construction.
 */
export function kansasCfrFiledDateWindow(input: {
  office: KansasCfrOffice;
  electionYear: number;
  now: Date;
}): { startDate: string; endDate: string } {
  if (!Number.isSafeInteger(input.electionYear) || input.electionYear < 2000 || input.electionYear > 2100) {
    throw new Error(`Invalid Kansas election year: ${input.electionYear}`);
  }
  return {
    startDate: `01/01/${input.electionYear - input.office.cycleYearsBefore}`,
    endDate: formatKansasCfrDate(input.now),
  };
}
