// Statutory South Carolina election-cycle dates, shared by the sync and the
// auto-link (which must not import each other).

function nthWeekdayOfMonthUtc(year: number, monthIndex: number, weekday: number, nth: number): number {
  const firstWeekday = new Date(Date.UTC(year, monthIndex, 1)).getUTCDay();
  return 1 + ((weekday - firstWeekday + 7) % 7) + (nth - 1) * 7;
}

// Both the unpadded form the API serves today ("6/9/2026") and the
// zero-padded form ("06/09/2026"). The aggregator matches accepted dates as
// strings, so covering both keeps a source format drift from silently
// dropping runs instead of failing visibly downstream.
function mdyVariants(year: number, month: number, day: number): string[] {
  const padded = `${String(month).padStart(2, "0")}/${String(day).padStart(2, "0")}/${year}`;
  return [...new Set([`${month}/${day}/${year}`, padded])];
}

// The statutory cycle dates for a South Carolina general-election race:
// primary (second Tuesday in June, S.C. Code § 7-13-40), runoff (two weeks
// after the primary), and the linked general itself. Passing the FULL trio
// matters — omitting the primary date would silently drop the primary run's
// money (the aggregator's contract). A linked election whose date is not the
// statutory general (a special election) gets its own date only: its June
// events do not exist, and any special-primary run is conservatively
// excluded rather than guessed at.
export function southCarolinaAcceptedElectionDates(electionYear: number, electionDateIso: string): string[] {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(electionDateIso)) {
    throw new Error(`Invalid South Carolina election date: ${electionDateIso}`);
  }
  if (Number.parseInt(electionDateIso.slice(0, 4), 10) !== electionYear) {
    throw new Error(`election date ${electionDateIso} does not match election year ${electionYear}`);
  }
  const linkedMonth = Number.parseInt(electionDateIso.slice(5, 7), 10);
  const linkedDay = Number.parseInt(electionDateIso.slice(8, 10), 10);

  // General: the Tuesday after the first Monday in November.
  const statutoryGeneralDay = nthWeekdayOfMonthUtc(electionYear, 10, 1, 1) + 1;
  const isStatutoryGeneral = linkedMonth === 11 && linkedDay === statutoryGeneralDay;
  if (!isStatutoryGeneral) {
    return mdyVariants(electionYear, linkedMonth, linkedDay);
  }

  const primaryDay = nthWeekdayOfMonthUtc(electionYear, 5, 2, 2);
  const primary = new Date(Date.UTC(electionYear, 5, primaryDay));
  const runoff = new Date(Date.UTC(electionYear, 5, primaryDay + 14));
  return [
    ...mdyVariants(primary.getUTCFullYear(), primary.getUTCMonth() + 1, primary.getUTCDate()),
    ...mdyVariants(runoff.getUTCFullYear(), runoff.getUTCMonth() + 1, runoff.getUTCDate()),
    ...mdyVariants(electionYear, 11, statutoryGeneralDay),
  ];
}
