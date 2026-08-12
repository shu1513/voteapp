// Eligibility for San Diego city campaign finance (Phase 2 of
// plan-san-diego-finance.md), copy-adapted from sanJoseFinanceEligibleOffices.
// San Diego city offices only: Mayor and the nine council districts, all on
// the place row (GEOID 0666000). Municipal Attorney is a city office too but
// stays out of the whitelist until a cycle actually has the contest — the
// resolver has no live-validated evidence model for it (the committee-name
// office vetoes treat ATTORNEY as foreign). Sync is additionally gated to
// roster candidates — eligibility here is the structural whitelist, not the
// per-election gate.

export const SAN_DIEGO_CITY_GEOID = "0666000";

export const SAN_DIEGO_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES = [
  "Mayor",
  "City Council Member",
] as const;

type SanDiegoCityFinanceEligibleOfficeName =
  (typeof SAN_DIEGO_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES)[number];

export function parseSanDiegoCityCouncilSeatNumber(
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
  // Local rows read "Member of the City Council, District 2"; the remaining
  // alternations cover the catalog's other council title spellings.
  const match =
    /^(?:MEMBER OF THE CITY COUNCIL|MEMBER CITY COUNCIL|CITY COUNCIL MEMBER|COUNCIL MEMBER|COUNCILMEMBER|CITY COUNCIL|COUNCIL) DISTRICT (?:NO )?(\d{1,2})$/.exec(
      normalized,
    );
  if (!match) return null;
  const seatNumber = Number(match[1]);
  return isSanDiegoCityCouncilSeatNumber(seatNumber) ? seatNumber : null;
}

export function isSanDiegoCityCouncilSeatNumber(
  value: number | null | undefined,
): value is number {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= 1 &&
    value <= 9
  );
}

function isSanDiegoCityFinanceEligibleOfficeName(
  value: string,
): value is SanDiegoCityFinanceEligibleOfficeName {
  return (
    SAN_DIEGO_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES as readonly string[]
  ).includes(value);
}

export function isSanDiegoCityFinanceEligibleElection(input: {
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
    input.geoidCompact?.trim() !== SAN_DIEGO_CITY_GEOID ||
    input.officeScope?.trim() !== "place"
  )
    return false;
  const officeName = input.officeCanonicalName?.trim() ?? "";
  if (!isSanDiegoCityFinanceEligibleOfficeName(officeName)) return false;
  if (officeName === "City Council Member") {
    return (
      parseSanDiegoCityCouncilSeatNumber(input.officialBallotTitle) !== null
    );
  }
  return true;
}
