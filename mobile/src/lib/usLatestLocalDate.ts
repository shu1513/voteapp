// Election dates are YYYY-MM-DD calendar strings; "today" is the last US
// clock still on a given date — Pacific/Honolulu, UTC-10, no DST — mirroring
// the backend's US_LATEST_LOCAL_DATE_SQL and usLatestLocalDateIso
// (usLocalDate.ts): an election counts as past only once the entire United
// States has finished that day.
const US_LATEST_LOCAL_DATE_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: "Pacific/Honolulu",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});

export function usLatestLocalDate(): string {
  // Assemble YYYY-MM-DD from typed parts instead of trusting en-CA's rendered
  // string: the Intl spec guarantees the part values, not the separator or
  // field order of any locale's short-date pattern — and a device without
  // en-CA locale data falls back to another locale entirely. Same
  // implementation as the web's usLatestLocalDate and the backend's
  // usLatestLocalDateIso.
  const parts = Object.fromEntries(
    US_LATEST_LOCAL_DATE_FORMATTER.formatToParts(new Date())
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value])
  );
  return `${parts.year}-${parts.month}-${parts.day}`;
}
