// Shared display formatters. Election dates arrive as YYYY-MM-DD strings and
// must be treated as calendar dates, not instants: new Date("2026-11-03")
// parses as UTC midnight and renders the previous day in US timezones.

import type { CandidateRosterStatus } from "./types";

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
  low: "Below average",
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

// Mirrors the backend's FINANCE_INDUSTRY_DISPLAY_NAMES
// (ballotLookupFinanceShared.ts). Finance industry categories arrive as
// slugs; occupation categories arrive as free text and pass through
// unchanged (no underscores).
const FINANCE_CATEGORY_LABELS: Record<string, string> = {
  agriculture_and_food: "Agriculture and food",
  business_associations: "Business associations",
  construction: "Construction",
  defense_aerospace: "Defense and aerospace",
  education: "Education",
  environmental_group: "Environmental groups",
  finance_investment: "Finance and investment",
  healthcare: "Healthcare",
  hospitality: "Hospitality",
  insurance: "Insurance",
  labor_unions: "Labor unions",
  lawyers_and_legal_services: "Lawyers and legal services",
  manufacturing: "Manufacturing",
  oil_gas_energy: "Oil, gas, and energy",
  pharmaceuticals: "Pharmaceuticals",
  real_estate: "Real estate",
  technology: "Technology",
  transportation: "Transportation",
  waste_management: "Waste management",
};

export function formatFinanceCategory(categoryName: string): string {
  const trimmed = categoryName.trim();
  const mapped = FINANCE_CATEGORY_LABELS[trimmed];
  if (mapped) {
    return mapped;
  }
  if (!trimmed.includes("_")) {
    return trimmed.length > 0 ? trimmed : categoryName;
  }
  const spaced = trimmed.replaceAll("_", " ");
  return spaced[0].toUpperCase() + spaced.slice(1).toLowerCase();
}

// Keys are the FinanceSummary.source enum values (backend
// ballotLookupFinanceShared.ts). Raw values like MASSACHUSETTS_OCPF must
// never reach the screen.
const FINANCE_SOURCE_LABELS: Record<string, string> = {
  FEC: "FEC",
  ARIZONA_SOS: "Arizona Secretary of State",
  CALIFORNIA_SOS: "California Secretary of State",
  COLORADO_TRACER: "Colorado TRACER",
  CONNECTICUT_ECRIS: "Connecticut eCRIS",
  INDIANA_CAMPAIGN_FINANCE: "Indiana Campaign Finance",
  NEBRASKA_NADC: "Nebraska NADC",
  NEW_JERSEY_ELEC: "New Jersey ELEC",
  NEW_MEXICO_CFIS: "New Mexico CFIS",
  OKLAHOMA_GUARDIAN: "Oklahoma Guardian",
  TEXAS_TEC: "Texas Ethics Commission",
  HOUSTON_CAMPAIGN_FINANCE: "City of Houston / Texas Ethics Commission",
  FLORIDA_DOS: "Florida Division of Elections",
  UTAH_DISCLOSURES: "Utah Financial Disclosures",
  HAWAII_CSC: "Hawaii Campaign Spending Commission",
  VIRGINIA_CFREPORTS: "Virginia CFReports",
  TENNESSEE_CAMP: "Tennessee Registry of Election Finance",
  WASHINGTON_PDC: "Washington PDC",
  WISCONSIN_SUNSHINE: "Wisconsin Sunshine",
  MASSACHUSETTS_OCPF: "Massachusetts OCPF",
  VERMONT_CFD: "Vermont Campaign Finance",
  LOUISIANA_ETHICS: "Louisiana Ethics Administration",
  KENTUCKY_KREF: "Kentucky KREF",
  MARYLAND_CFS: "Maryland Campaign Reporting",
  MAINE_CFIS: "Maine CFIS",
  MICHIGAN_MITN: "Michigan Campaign Finance",
  ILLINOIS_SBE: "Illinois State Board of Elections",
  MINNESOTA_CFB: "Minnesota CFB",
  ALASKA_APOC: "Alaska APOC",
  ORESTAR: "Oregon ORESTAR",
  PENNSYLVANIA_DOS: "Pennsylvania Department of State",
  DISTRICT_OF_COLUMBIA_OCF: "DC Office of Campaign Finance",
  NEW_YORK_CITY_CFB: "NYC Campaign Finance Board",
};

// Copy for the "why is the candidate list empty" statuses (see
// CandidateRosterStatus in types.ts). `short` fits a ballot-card chip;
// `long` is a sentence for the election page's empty state. Unknown reasons
// (a newer backend) fall back to the generic unavailable copy so the enum
// can grow without breaking older clients.
export function formatRosterStatus(status: CandidateRosterStatus): { short: string; long: string } {
  switch (status.reason) {
    case "awaiting_official_roster":
      return {
        short: "Candidate list not final",
        long:
          "Election officials haven't published a final candidate list for this race." +
          (status.check_after ? ` We'll check again after ${formatElectionDate(status.check_after)}.` : ""),
      };
    case "roster_processing":
      return {
        short: "Candidate details coming soon",
        long: "A candidate list is available — candidate profiles are being prepared.",
      };
    default:
      return {
        short: "Candidate list unavailable",
        long: "Candidate information for this race isn't available yet.",
      };
  }
}

export function financeSourceLabel(source: string): string {
  const mapped = FINANCE_SOURCE_LABELS[source];
  if (mapped) {
    return mapped;
  }
  const spaced = source.replaceAll("_", " ").trim();
  if (spaced.length === 0) {
    return source;
  }
  return spaced
    .split(" ")
    .map((word) => (word.length > 0 ? word[0].toUpperCase() + word.slice(1).toLowerCase() : word))
    .join(" ");
}
