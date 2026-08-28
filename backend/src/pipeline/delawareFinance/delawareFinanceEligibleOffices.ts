// Delaware finance office eligibility + CFRS office-code mapping.
//
// The portal's office vocabulary was pinned live 2026-08-28 from the
// committee-search cascade endpoints (POST /Public/BindOffice with
// OfficeType=SO): AGEN, AACC, GOV, INCOM, LGOV, STREP, STSEN, STTRE.
// District options come from POST /Public/GetDistricts and use OPAQUE
// numeric values with "District NN" labels, so district codes are resolved
// live by label at search time, never hardcoded.
//
// County offices stay OUT of v1: the CFRS county vocabulary (Clerk of
// Peace, Levy Court Commissioner, County Council…) does not map cleanly
// onto VoteApp's DE county office keys (Clerk of Court, County Supervisor,
// County Commissioner…) — guessing that mapping risks wrong links, so it
// waits for its own research pass. Statewide + General Assembly are the
// Phase 2 cohort's highest-value races (plan scope note).

export type DelawareCfrsOfficeSearch = {
  /** ddlOffice value: SO = State Office (the only v1 office type). */
  officeType: "SO";
  /** ddlOfficeSought value (e.g. "AGEN"). */
  officeCode: string;
  /** Portal option label (e.g. "Attorney General") — evidence display only. */
  officeLabel: string;
  /**
   * District number the search must filter on (ddljurisdiction), resolved
   * to the portal's numeric option value by matching the "District NN"
   * label live. null = office has no district dimension.
   */
  districtNumber: number | null;
};

const STATEWIDE_OFFICE_CODES: Record<string, { officeCode: string; officeLabel: string }> = {
  "Attorney General": { officeCode: "AGEN", officeLabel: "Attorney General" },
  "State Auditor": { officeCode: "AACC", officeLabel: "Auditor of Accounts" },
  Governor: { officeCode: "GOV", officeLabel: "Governor" },
  "Insurance Commissioner": { officeCode: "INCOM", officeLabel: "Insurance Commissioner" },
  "Lieutenant Governor": { officeCode: "LGOV", officeLabel: "Lieutenant Governor" },
  "State Treasurer": { officeCode: "STTRE", officeLabel: "State Treasurer" },
};

const ELIGIBLE_OFFICE_KEYS = [
  ...Object.keys(STATEWIDE_OFFICE_CODES).map((name) => `statewide::${name}`),
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const;

export const DELAWARE_FINANCE_ELIGIBLE_OFFICE_KEYS: ReadonlySet<string> = new Set(ELIGIBLE_OFFICE_KEYS);

export function isDelawareFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const scope = input.officeScope?.trim();
  const name = input.officeCanonicalName?.trim();
  return Boolean(scope && name && DELAWARE_FINANCE_ELIGIBLE_OFFICE_KEYS.has(`${scope}::${name}`));
}

/** "District 4" / "4" / "04" -> 4; null when no number is present. */
export function parseDelawareDistrictNumber(value: string | null | undefined): number | null {
  const match = /(\d+)/.exec(value ?? "");
  return match === null ? null : Number.parseInt(match[1]!, 10);
}

/**
 * Maps a VoteApp office (+ legislative district) onto the CFRS committee
 * search's office controls. Returns null for offices outside the v1 set —
 * callers must treat null as "not resolvable", never fall back to a
 * name-only search.
 */
export function toDelawareCfrsOfficeSearch(input: {
  officeScope: string;
  officeName: string;
  district?: string | null;
}): DelawareCfrsOfficeSearch | null {
  if (input.officeScope === "statewide") {
    const statewide = STATEWIDE_OFFICE_CODES[input.officeName];
    return statewide === undefined ? null : { officeType: "SO", ...statewide, districtNumber: null };
  }
  if (input.officeScope === "state_upper" && input.officeName === "State Senator") {
    const districtNumber = parseDelawareDistrictNumber(input.district);
    return districtNumber === null
      ? null
      : { officeType: "SO", officeCode: "STSEN", officeLabel: "State Senator", districtNumber };
  }
  if (input.officeScope === "state_lower" && input.officeName === "State Lower Chamber Legislator") {
    const districtNumber = parseDelawareDistrictNumber(input.district);
    return districtNumber === null
      ? null
      : { officeType: "SO", officeCode: "STREP", officeLabel: "State Representative", districtNumber };
  }
  return null;
}
