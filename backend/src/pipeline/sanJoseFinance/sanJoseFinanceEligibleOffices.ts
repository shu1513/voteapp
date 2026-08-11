// Eligibility for San José campaign finance (Phase 2 of
// plan-san-jose-finance.md), modeled on losAngelesCityFinanceEligibleOffices.
// San José city offices only: Mayor and the ten council districts. Sync is
// additionally gated to roster candidates — eligibility here is the
// structural whitelist, not the per-election gate.

export const SAN_JOSE_CITY_GEOID = "0668000";

export const SAN_JOSE_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES = [
  "Mayor",
  "City Council Member",
] as const;

type SanJoseCityFinanceEligibleOfficeName =
  (typeof SAN_JOSE_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES)[number];

export const SAN_JOSE_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "place::Mayor",
  "place::City Council Member",
] as const;

export function parseSanJoseCityCouncilSeatNumber(
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
  // Local rows read "Member, City Council, District 5"; the remaining
  // alternations cover the catalog's other council title spellings.
  const match =
    /^(?:MEMBER OF THE CITY COUNCIL|MEMBER CITY COUNCIL|CITY COUNCIL MEMBER|COUNCIL MEMBER|COUNCILMEMBER|CITY COUNCIL|COUNCIL) DISTRICT (?:NO )?(\d{1,2})$/.exec(
      normalized,
    );
  if (!match) return null;
  const seatNumber = Number(match[1]);
  return isSanJoseCityCouncilSeatNumber(seatNumber) ? seatNumber : null;
}

export function isSanJoseCityCouncilSeatNumber(
  value: number | null | undefined,
): value is number {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= 1 &&
    value <= 10
  );
}

function isSanJoseCityFinanceEligibleOfficeName(
  value: string,
): value is SanJoseCityFinanceEligibleOfficeName {
  return (
    SAN_JOSE_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES as readonly string[]
  ).includes(value);
}

export function isSanJoseCityFinanceEligibleElection(input: {
  state: string | null | undefined;
  districtType: string | null | undefined;
  geoidCompact: string | null | undefined;
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  officialBallotTitle?: string | null;
}): boolean {
  if (input.state?.trim().toUpperCase() !== "CA") return false;
  if (
    input.districtType?.trim() !== "place" ||
    input.geoidCompact?.trim() !== SAN_JOSE_CITY_GEOID ||
    input.officeScope?.trim() !== "place"
  )
    return false;
  const officeName = input.officeCanonicalName?.trim() ?? "";
  if (!isSanJoseCityFinanceEligibleOfficeName(officeName)) return false;
  if (officeName === "City Council Member") {
    return (
      parseSanJoseCityCouncilSeatNumber(input.officialBallotTitle) !== null
    );
  }
  return true;
}
