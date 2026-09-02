// Cycle totals from filed-report covers (live run 2026-09-01 finding).
//
// The race search's "2026 ELECTION CYCLE" aggregate is NOT cycle-scoped for
// committees older than the cycle: it sums the 2026-tagged reports plus every
// untagged report the committee ever filed (Annual Reports and Major
// Contribution Reports carry no election tag), so a 2014-registered senator
// showed $655,220.53 raised where the 2026-cycle covers sum to $171,102.00.
// New committees (Jones, Tuberville, Robertson) reconcile cent-exact only
// because they have no pre-cycle untagged reports.
//
// The cycle window is therefore taken from the filings themselves: every
// report whose period begins on/after January 1 of the first term year
// (electionYear - termYears + 1) counts, whatever its election tag. That
// window drops the previous general election's reports and its annual report
// naturally, and the balance identity (first window report's beginning
// balance + cash + other - expenditures == latest ending balance) held
// cent-exact on the incumbent it was derived from.
//
// Major Contribution Reports are separate ledger entries, not duplicates: the
// following monthly report's beginning balance already includes them and its
// cash total excludes them (Jones, 08/2026), so all covers sum together.

import {
  AlabamaFcpaClientError,
  getAlabamaCommitteeFilings,
  getAlabamaFilingDetailHtml,
  type AlabamaFcpaClientOptions,
} from "./alabamaFcpaClient.js";
import {
  alabamaCoverCashCents,
  alabamaCoverExpenditureCents,
  alabamaCoverInKindCents,
  alabamaCoverOtherCents,
  parseAlabamaFilingDetailCover,
} from "./alabamaPhaseZero.js";

export type AlabamaCommitteeCycleCovers = {
  /** ISO date (YYYY-MM-DD) the window opens on. */
  windowStart: string;
  /** Every filing the portal lists for the committee. */
  filingCount: number;
  /** Filings whose period begins inside the window (all summed below). */
  windowFilingCount: number;
  cashCents: number;
  inKindCents: number;
  otherCents: number;
  expenditureCents: number;
  /**
   * Beginning balance of the earliest filing in the window, whatever its
   * kind: Major Contribution Reports carry a running balance too, and one
   * filed before the first periodic report is already inside that report's
   * beginning balance (Robertson: $1,065,000 of majors preceded her first
   * monthly), so seeding from the first periodic would double-count them.
   */
  openingBalanceCents: number | null;
  /** Ending balance of the latest periodic report in the window. */
  latestEndingBalanceCents: number | null;
};

export type AlabamaCycleCoversLoader = (
  internalCommitteeId: number,
  windowStart: string
) => Promise<AlabamaCommitteeCycleCovers>;

/** January 1 of the first year of the term ending in electionYear. */
export function alabamaCycleWindowStart(electionYear: number, termYears: number): string {
  if (!Number.isInteger(electionYear) || !Number.isInteger(termYears) || termYears < 1) {
    throw new Error(`invalid cycle window: electionYear=${electionYear} termYears=${termYears}`);
  }
  return `${electionYear - termYears + 1}-01-01`;
}

/** Portal dates are MM/DD/YYYY (optionally followed by a time); null when unparseable. */
export function parseAlabamaFilingDate(raw: string | null | undefined): string | null {
  const match = /^\s*(\d{1,2})\/(\d{1,2})\/(\d{4})/.exec(raw ?? "");
  if (!match) {
    return null;
  }
  const month = Number(match[1]);
  const day = Number(match[2]);
  if (month < 1 || month > 12 || day < 1 || day > 31) {
    return null;
  }
  return `${match[3]}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

export function createAlabamaCycleCoversLoader(input: {
  clientOptions?: AlabamaFcpaClientOptions;
  fetchFilings?: typeof getAlabamaCommitteeFilings;
  fetchCoverHtml?: typeof getAlabamaFilingDetailHtml;
  /** Filing-detail pages intermittently return a System Exception (Phase 0). */
  coverAttempts?: number;
  retryDelayMs?: number;
} = {}): AlabamaCycleCoversLoader {
  const fetchFilings = input.fetchFilings ?? getAlabamaCommitteeFilings;
  const fetchCoverHtml = input.fetchCoverHtml ?? getAlabamaFilingDetailHtml;
  const coverAttempts = input.coverAttempts ?? 3;
  const retryDelayMs = input.retryDelayMs ?? 1500;

  async function coverFor(filingId: number) {
    let lastError: unknown;
    for (let attempt = 1; attempt <= coverAttempts; attempt += 1) {
      try {
        return parseAlabamaFilingDetailCover(await fetchCoverHtml(filingId, input.clientOptions));
      } catch (error) {
        lastError = error;
        if (attempt < coverAttempts) {
          await sleep(retryDelayMs);
        }
      }
    }
    const message = lastError instanceof Error ? lastError.message : String(lastError);
    throw new AlabamaFcpaClientError("bad_response", `filing ${filingId} cover unavailable after ${coverAttempts} attempts: ${message}`);
  }

  async function filingsFor(internalCommitteeId: number) {
    // The paged filings list occasionally comes back one row short
    // ("returned 16 of 17 rows", live 2026-09-01) — the client rejects the
    // partial read, so retry it like a cover.
    let lastError: unknown;
    for (let attempt = 1; attempt <= coverAttempts; attempt += 1) {
      try {
        return await fetchFilings(internalCommitteeId, input.clientOptions);
      } catch (error) {
        lastError = error;
        if (attempt < coverAttempts) {
          await sleep(retryDelayMs);
        }
      }
    }
    const message = lastError instanceof Error ? lastError.message : String(lastError);
    throw new AlabamaFcpaClientError("bad_response", `filings for committee ${internalCommitteeId} unavailable after ${coverAttempts} attempts: ${message}`);
  }

  return async (internalCommitteeId, windowStart) => {
    const filings = await filingsFor(internalCommitteeId);
    const totals: AlabamaCommitteeCycleCovers = {
      windowStart,
      filingCount: filings.length,
      windowFilingCount: 0,
      cashCents: 0,
      inKindCents: 0,
      otherCents: 0,
      expenditureCents: 0,
      openingBalanceCents: null,
      latestEndingBalanceCents: null,
    };
    // Earliest window filing by (period END, filing id). The portal chains
    // balances in filing order, and a Major Contribution Report is filed
    // before the periodic report whose period contains it (Robertson: major
    // dated 06/30 with balance 0, then the 06/01-06/30 monthly opening at
    // $1,065,000) — period end orders those correctly where period begin
    // would not; ids grow with filing time and break same-day ties.
    let earliest: { periodEnd: string; id: number } | null = null;
    let latestPeriodic: string | null = null;
    for (const filing of filings) {
      // Period begin places a report in a cycle; the filed date is only a
      // fallback (a report with neither cannot be placed — fail closed).
      const periodBegin = parseAlabamaFilingDate(filing.PERIODBEGIN) ?? parseAlabamaFilingDate(filing.FILEDDATE);
      if (periodBegin === null) {
        throw new Error(
          `filing ${filing.ID} (${filing.DESCRIPTION.trim()}) for committee ${internalCommitteeId} has no parseable period or filed date`
        );
      }
      if (periodBegin < windowStart) {
        continue;
      }
      const cover = await coverFor(filing.ID);
      totals.windowFilingCount += 1;
      totals.cashCents += alabamaCoverCashCents(cover);
      totals.inKindCents += alabamaCoverInKindCents(cover);
      totals.otherCents += alabamaCoverOtherCents(cover);
      totals.expenditureCents += alabamaCoverExpenditureCents(cover);
      const periodEnd = parseAlabamaFilingDate(filing.PERIODEND) ?? periodBegin;
      if (
        earliest === null ||
        periodEnd < earliest.periodEnd ||
        (periodEnd === earliest.periodEnd && filing.ID < earliest.id)
      ) {
        earliest = { periodEnd, id: filing.ID };
        totals.openingBalanceCents = cover.beginningBalanceCents;
      }
      if (cover.kind === "periodic") {
        if (latestPeriodic === null || periodEnd > latestPeriodic) {
          latestPeriodic = periodEnd;
          totals.latestEndingBalanceCents = cover.endingBalanceCents;
        }
      }
    }
    return totals;
  };
}
