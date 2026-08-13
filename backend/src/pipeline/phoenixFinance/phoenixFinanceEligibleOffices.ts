// Eligibility for Phoenix city campaign finance (Phase 2 of
// plan-phoenix-finance.md), modeled on sanJoseFinanceEligibleOffices.
// Phoenix city offices only: Mayor (dormant until the next mayoral cycle —
// whitelisted so no code change is needed then) and the eight council
// districts. Sync is additionally gated to roster candidates — eligibility
// here is the structural whitelist, not the per-election gate.

export const PHOENIX_CITY_GEOID = "0455000";

export const PHOENIX_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES = [
  "Mayor",
  "City Council Member",
] as const;

type PhoenixCityFinanceEligibleOfficeName =
  (typeof PHOENIX_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES)[number];

export const PHOENIX_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "place::Mayor",
  "place::City Council Member",
] as const;

export function parsePhoenixCityCouncilDistrictNumber(
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
  // Local rows read "Phoenix City Council, District 4"; the remaining
  // alternations cover the catalog's other council title spellings.
  const match =
    /^(?:PHOENIX CITY COUNCIL|MEMBER OF THE CITY COUNCIL|MEMBER CITY COUNCIL|CITY COUNCIL MEMBER|COUNCIL MEMBER|COUNCILMEMBER|CITY COUNCIL|COUNCIL) DISTRICT (?:NO )?(\d{1,2})$/.exec(
      normalized,
    );
  if (!match) return null;
  const districtNumber = Number(match[1]);
  return isPhoenixCityCouncilDistrictNumber(districtNumber)
    ? districtNumber
    : null;
}

export function isPhoenixCityCouncilDistrictNumber(
  value: number | null | undefined,
): value is number {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= 1 &&
    value <= 8
  );
}

function isPhoenixCityFinanceEligibleOfficeName(
  value: string,
): value is PhoenixCityFinanceEligibleOfficeName {
  return (
    PHOENIX_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES as readonly string[]
  ).includes(value);
}

export function isPhoenixCityFinanceEligibleElection(input: {
  state: string | null | undefined;
  districtType: string | null | undefined;
  geoidCompact: string | null | undefined;
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  officialBallotTitle?: string | null;
}): boolean {
  if (input.state?.trim().toUpperCase() !== "AZ") return false;
  if (
    input.districtType?.trim() !== "place" ||
    input.geoidCompact?.trim() !== PHOENIX_CITY_GEOID ||
    input.officeScope?.trim() !== "place"
  )
    return false;
  const officeName = input.officeCanonicalName?.trim() ?? "";
  if (!isPhoenixCityFinanceEligibleOfficeName(officeName)) return false;
  if (officeName === "City Council Member") {
    return (
      parsePhoenixCityCouncilDistrictNumber(input.officialBallotTitle) !== null
    );
  }
  return true;
}
