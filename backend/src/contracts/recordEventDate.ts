// Shared event_date parsing for candidate record contracts (discovery and
// source repair). Both contracts must enforce the same rules or the repair
// path becomes an escape hatch for rows the discovery parser rejected.

// Latest event_date accepted: UTC today plus one day. Records are completed
// actions, so a future date is always wrong — but a same-day action reported
// from a timezone ahead of UTC can carry a local date one day past the UTC
// date, so exact "today" comparison would false-reject. YYYY-MM-DD strings
// compare correctly with the lexicographic > operator.
export function maxAllowedEventDate(): string {
  const now = new Date();
  now.setUTCDate(now.getUTCDate() + 1);
  return now.toISOString().slice(0, 10);
}

function isRealCalendarDate(value: string): boolean {
  const [year, month, day] = value.split("-").map((part) => Number.parseInt(part, 10));
  const roundTrip = new Date(Date.UTC(year, month - 1, day));
  return (
    roundTrip.getUTCFullYear() === year &&
    roundTrip.getUTCMonth() === month - 1 &&
    roundTrip.getUTCDate() === day
  );
}

function normalizeEventDate(value: unknown): string | null {
  if (typeof value !== "string" || value.trim().length === 0) {
    return null;
  }
  const trimmed = value.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return trimmed;
  }
  // A timestamp names an instant, not a calendar date; which date the instant
  // falls on depends on timezone. Reading local components here would make
  // the result depend on the *server's* timezone (a UTC-midnight timestamp
  // shifts back a day in timezones behind UTC), so the same payload would
  // produce different event dates — and identity keys — per environment.
  // Take the date the string itself states, i.e. the date in the source's
  // own zone, which is deterministic everywhere.
  const timestamp = trimmed.match(/^(\d{4}-\d{2}-\d{2})[T ]/);
  if (timestamp) {
    return Number.isNaN(new Date(trimmed).getTime()) ? null : timestamp[1];
  }
  // Natural-language fallback intentionally uses local date components:
  // new Date("April 5, 2026") is local midnight, and UTC slicing would shift
  // date-only strings back a day in timezones behind UTC.
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  const year = String(parsed.getFullYear()).padStart(4, "0");
  const month = String(parsed.getMonth() + 1).padStart(2, "0");
  const day = String(parsed.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

// Partial dates ("2025", "2025-04", "2025/04", "April 2025") must be
// rejected, not guessed: records are dated actions, and every parser path
// either invents a day (legacy parsing lands on the 1st) or, for ISO
// year/year-month forms, parses as UTC midnight and shifts a day back in
// timezones behind UTC. Instead of enumerating spellings, detect the absence
// of a day-of-month: a full date always carries year + month + day tokens.
function isMissingDayOfMonth(value: string): boolean {
  if (/^\d{4}-\d{2}-\d{2}/.test(value)) {
    return false;
  }
  const numbers = value.match(/\d+/g) ?? [];
  if (numbers.length === 0) {
    // No digits at all is not a partial date; let the parse-failure path
    // report it as unparseable.
    return false;
  }
  if (/[a-z]/i.test(value)) {
    // Month is spelled out ("April 2025" vs "April 5, 2025"): a day is any
    // standalone 1-2 digit number alongside the year.
    return !numbers.some((token) => token.length <= 2);
  }
  // All-numeric ("2025-4", "2025/04"): a full date needs three fields.
  return numbers.length < 3;
}

// Reasons start with "event_date" so callers can either use them as-is or
// prefix a field path (e.g. `payload.repairs[].${reason}`).
export function parseRecordEventDate(
  value: unknown
): { ok: true; eventDate: string } | { ok: false; reason: string } {
  // The prompt already tells the model to fall back to the source
  // publication date — a full date — or omit the record, so reject partial
  // dates instead of guessing a canonical day.
  if (typeof value === "string" && value.trim().length > 0 && isMissingDayOfMonth(value.trim())) {
    return {
      ok: false,
      reason: `event_date "${value.trim()}" is incomplete; use the full YYYY-MM-DD action date, or the source publication date when the action date is unknown`,
    };
  }
  const eventDate = normalizeEventDate(value);
  if (!eventDate) {
    return { ok: false, reason: "event_date must be parseable date" };
  }
  if (!isRealCalendarDate(eventDate)) {
    return { ok: false, reason: `event_date ${eventDate} is not a real calendar date` };
  }
  if (eventDate > maxAllowedEventDate()) {
    return {
      ok: false,
      reason: `event_date ${eventDate} is in the future; records are completed actions, use the action or publication date`,
    };
  }
  return { ok: true, eventDate };
}
