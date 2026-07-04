// Shared display formatters. Election dates arrive as YYYY-MM-DD strings and
// must be treated as calendar dates, not instants: new Date("2026-11-03")
// parses as UTC midnight and renders the previous day in US timezones.

export function formatElectionDate(isoDate: string): string {
  const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(isoDate.trim());
  if (!match) {
    return isoDate;
  }
  const [, year, month, day] = match;
  const date = new Date(Number(year), Number(month) - 1, Number(day));
  return date.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" });
}

export function formatMoney(amount: number | null | undefined): string {
  if (amount === null || amount === undefined || Number.isNaN(amount)) {
    return "—";
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(amount);
}

// Keys are the backend's district_type values (see districts table).
const DISTRICT_TYPE_LABELS: Record<string, string> = {
  statewide: "Statewide",
  county: "County",
  place: "City",
  us_house: "U.S. House district",
  state_upper: "State senate district",
  state_lower: "State house district",
  school_unified: "School district",
  school_elementary: "Elementary school district",
  school_secondary: "Secondary school district",
};

export function formatDistrictType(districtType: string): string {
  return DISTRICT_TYPE_LABELS[districtType] ?? districtType.replaceAll("_", " ");
}

export function formatSourceHost(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return url;
  }
}

const VOTE_POWER_LABELS: Record<string, string> = {
  very_low: "Very low",
  low: "Low",
  medium: "Medium",
  high: "High",
  very_high: "Very high",
  unknown: "Unknown",
};

export function formatVotePowerLabel(label: string): string {
  return VOTE_POWER_LABELS[label] ?? label;
}

/** Election result outcomes arrive snake_case (e.g. "too_close"). */
export function formatOutcome(outcome: string): string {
  const spaced = outcome.replaceAll("_", " ").trim();
  return spaced.length > 0 ? spaced[0].toUpperCase() + spaced.slice(1) : outcome;
}
