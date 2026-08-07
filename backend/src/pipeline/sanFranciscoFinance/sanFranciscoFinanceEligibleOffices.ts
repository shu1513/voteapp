// Eligibility and contest-code mapping for San Francisco campaign finance
// (Phase 2 of plan-san-francisco-finance.md). San Francisco is a
// consolidated city-county, so its offices are split across three catalog
// scopes — the pairs below were confirmed against the office catalog, the
// elections validator, and the live SFEC dashboard repo (2026-08-07):
// the validator forces mayor/city-attorney titles into `place` and
// sheriff/district-attorney titles into `county`, and every contest code
// matches a real `elections/<date>/contests/<code>.md` manifest file.
// The Community College Board (`ccb`) is deliberately absent: VoteApp has
// no community-college district type or canonical office, so that contest
// is deferred out of v1.

export const SAN_FRANCISCO_COUNTY_GEOID = "06075";
export const SAN_FRANCISCO_CITY_GEOID = "0667000";
export const SAN_FRANCISCO_UNIFIED_SCHOOL_DISTRICT_GEOID = "0634410";

// scope::canonical-office → SFEC contest code (which doubles as the
// dashboard manifest file locator). Supervisor is handled separately
// because its code carries the district number (bos01 … bos11).
const CONTEST_CODE_BY_OFFICE_KEY: Readonly<Record<string, string>> = {
  "place::Mayor": "myr",
  "place::Municipal Attorney": "cat", // ballot title "City Attorney"
  "place::City Treasurer": "ttx", // Treasurer & Tax Collector
  "county::District Attorney": "dat",
  "county::Sheriff": "shf",
  "county::County Assessor-Recorder": "asr",
  "county::Public Defender": "pdr",
  "school_unified::School Board Member": "usd", // Board of Education, at-large
};

export function parseSanFranciscoSupervisorDistrictNumber(
  officialBallotTitle: string | null | undefined,
): number | null {
  const normalized = officialBallotTitle
    ?.normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return null;
  // Local rows read "Member, Board of Supervisors, District 10".
  const match =
    /^(?:MEMBER )?(?:OF THE )?(?:COUNTY )?(?:BOARD OF SUPERVISORS|SUPERVISOR) DISTRICT (?:NO )?(\d{1,2})$/.exec(
      normalized,
    );
  if (!match) return null;
  const districtNumber = Number(match[1]);
  return isSanFranciscoSupervisorDistrictNumber(districtNumber)
    ? districtNumber
    : null;
}

function isSanFranciscoSupervisorDistrictNumber(
  value: number | null | undefined,
): value is number {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= 1 &&
    value <= 11
  );
}

/**
 * SFEC contest code for an office, or null when the office is not covered.
 * Supervisor needs the district number parsed from the ballot title; every
 * other covered office maps directly.
 */
export function toSanFranciscoContestCode(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  supervisorDistrictNumber?: number | null;
}): string | null {
  const officeScope = input.officeScope?.trim() ?? "";
  const officeName = input.officeCanonicalName?.trim() ?? "";
  if (officeScope === "county" && officeName === "County Supervisor") {
    return isSanFranciscoSupervisorDistrictNumber(
      input.supervisorDistrictNumber,
    )
      ? `bos${String(input.supervisorDistrictNumber).padStart(2, "0")}`
      : null;
  }
  return CONTEST_CODE_BY_OFFICE_KEY[`${officeScope}::${officeName}`] ?? null;
}

export function isSanFranciscoFinanceEligibleElection(input: {
  state: string | null | undefined;
  districtType: string | null | undefined;
  geoidCompact: string | null | undefined;
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  officialBallotTitle?: string | null;
}): boolean {
  if (input.state?.trim().toUpperCase() !== "CA") return false;
  const districtType = input.districtType?.trim();
  const geoid = input.geoidCompact?.trim();
  // The office must live in the district it is scoped to — San Francisco's
  // three district rows are the whole eligible universe.
  const districtMatches =
    (districtType === "county" && geoid === SAN_FRANCISCO_COUNTY_GEOID) ||
    (districtType === "place" && geoid === SAN_FRANCISCO_CITY_GEOID) ||
    (districtType === "school_unified" &&
      geoid === SAN_FRANCISCO_UNIFIED_SCHOOL_DISTRICT_GEOID);
  if (!districtMatches || input.officeScope?.trim() !== districtType)
    return false;
  return (
    toSanFranciscoContestCode({
      ...input,
      supervisorDistrictNumber: parseSanFranciscoSupervisorDistrictNumber(
        input.officialBallotTitle,
      ),
    }) !== null
  );
}
