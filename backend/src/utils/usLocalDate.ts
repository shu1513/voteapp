// "Today" for user-facing upcoming/past election checks.
//
// The database runs in UTC (or the host default), so CURRENT_DATE flips to the
// next day while voters in the western United States are still voting — an
// election looked "past" in Hawaii from early afternoon on election day. All
// elections in this app are US contests, so the correct boundary is the last
// US clock to leave a given date: Pacific/Honolulu (UTC-10, no daylight
// saving). An election date counts as past only once the entire United States
// has finished that day.
//
// This is a static SQL fragment, never interpolated with user input.
export const US_LATEST_LOCAL_DATE_SQL = "(now() AT TIME ZONE 'Pacific/Honolulu')::date";

// JS-side equivalent for code that stamps date-only research checkpoints.
// `new Date().toISOString().slice(0, 10)` is the UTC calendar date, which is
// already "tomorrow" for the western US after 5pm Pacific — a records
// checkpoint stamped that way claims a local day that was never researched,
// and delta refreshes starting from the checkpoint skip it forever.
const US_LATEST_LOCAL_DATE_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: "Pacific/Honolulu",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});

export function usLatestLocalDateIso(now: Date = new Date()): string {
  // Assemble YYYY-MM-DD from typed parts instead of trusting en-CA's rendered
  // string: the Intl spec guarantees the part values, not the separator or
  // field order of any locale's short-date pattern.
  const parts = Object.fromEntries(
    US_LATEST_LOCAL_DATE_FORMATTER.formatToParts(now)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value])
  );
  return `${parts.year}-${parts.month}-${parts.day}`;
}
