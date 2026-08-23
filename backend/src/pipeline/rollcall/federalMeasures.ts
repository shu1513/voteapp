// Measure identifiers as the two federal roll-call feeds print them. The
// House Clerk writes `H R 1` / `H RES 5` / `H CON RES 14`; the Senate LIS
// writes `H.R. 5371` / `S.J.Res. 3` / `PN12-31`. Both collapse to one shape
// here so the question-class filter, the audit bill_url, and (later) the
// duplicate scan all speak the same key.

export type FederalMeasureType =
  | "hr"
  | "hres"
  | "hjres"
  | "hconres"
  | "s"
  | "sres"
  | "sjres"
  | "sconres"
  // Presidential nomination (Senate only).
  | "pn";

export type FederalMeasure = {
  type: FederalMeasureType;
  // Digits as printed; nominations can carry a part suffix (`12-31`).
  number: string;
};

// Longest prefixes first so `HRES` is not read as `HR` + `ES`.
const MEASURE_PATTERN = /^(HCONRES|HJRES|HRES|HR|SCONRES|SJRES|SRES|S|PN)(\d+(?:-\d+)?)$/;

const TYPE_BY_PREFIX: Record<string, FederalMeasureType> = {
  HR: "hr",
  HRES: "hres",
  HJRES: "hjres",
  HCONRES: "hconres",
  S: "s",
  SRES: "sres",
  SJRES: "sjres",
  SCONRES: "sconres",
  PN: "pn",
};

/**
 * Parses a measure id in either feed's spelling. Returns null for anything
 * that is not a bill, resolution, or nomination (`QUORUM`, treaty documents,
 * an empty string) — those votes carry no measure.
 */
export function parseFederalMeasure(raw: string | null | undefined): FederalMeasure | null {
  if (!raw) {
    return null;
  }
  const compact = raw.toUpperCase().replace(/[\s.]+/g, "");
  const match = MEASURE_PATTERN.exec(compact);
  if (!match) {
    return null;
  }
  return { type: TYPE_BY_PREFIX[match[1]!]!, number: match[2]! };
}

const CONGRESS_GOV_BILL_SLUGS: Record<Exclude<FederalMeasureType, "pn">, string> = {
  hr: "house-bill",
  hres: "house-resolution",
  hjres: "house-joint-resolution",
  hconres: "house-concurrent-resolution",
  s: "senate-bill",
  sres: "senate-resolution",
  sjres: "senate-joint-resolution",
  sconres: "senate-concurrent-resolution",
};

export function ordinalCongress(congress: number): string {
  const lastTwo = congress % 100;
  const lastOne = congress % 10;
  const suffix =
    lastTwo >= 11 && lastTwo <= 13
      ? "th"
      : lastOne === 1
        ? "st"
        : lastOne === 2
          ? "nd"
          : lastOne === 3
            ? "rd"
            : "th";
  return `${congress}${suffix}`;
}

/**
 * The congress.gov bill page, kept on legislative_votes.bill_url for audit
 * only (congress.gov blocks the record validator, so it is never a
 * source_url). Nominations have no bill page.
 */
export function congressGovBillUrl(congress: number, measure: FederalMeasure | null): string | null {
  if (!measure || measure.type === "pn") {
    return null;
  }
  return `https://www.congress.gov/bill/${ordinalCongress(congress)}-congress/${
    CONGRESS_GOV_BILL_SLUGS[measure.type]
  }/${measure.number}`;
}
