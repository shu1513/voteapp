import { normalizeHttpUrl } from "./normalizeHttpUrl.js";

export function normalizeCandidateName(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeTwitterHandle(value: string): string | null {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return null;
  }

  let rawHandle = trimmed;
  if (/^https?:\/\//i.test(trimmed)) {
    try {
      const parsed = new URL(trimmed);
      const hostname = parsed.hostname.toLowerCase();
      const allowedHostnames = new Set([
        "x.com",
        "www.x.com",
        "twitter.com",
        "www.twitter.com",
        "mobile.twitter.com",
        "m.twitter.com",
      ]);
      if (!allowedHostnames.has(hostname)) {
        return null;
      }
      const pathToken = parsed.pathname.split("/").filter((token) => token.length > 0)[0];
      if (!pathToken) {
        return null;
      }
      rawHandle = pathToken;
    } catch {
      return null;
    }
  }

  const withoutPrefix = rawHandle.startsWith("@") ? rawHandle.slice(1) : rawHandle;
  const lowered = withoutPrefix.toLowerCase();
  if (!/^[a-z0-9_]{1,15}$/.test(lowered)) {
    return null;
  }
  return lowered;
}

export function normalizeOptionalUrl(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  return normalizeHttpUrl(value);
}

// State candidate rosters mark incumbency with a trailing asterisk ("Jill
// Oberlander *"). Scraped verbatim, the marker rides into display_name and
// last_name and is then served to voters as part of the person's name — 366
// stored candidates across CT and MN carry one. It is never part of a name, it
// is always trailing (no stored value has one mid-name), and
// normalizeCandidateName already ignores it, so stripping changes what is
// DISPLAYED without moving any identity match.
//
// Deliberately only the asterisk, and only at the end: this is a footnote
// stripper, not a general name cleaner. Hyphens, apostrophes, periods and
// parentheses are all legitimate inside names, and internal spacing is left
// exactly as supplied — collapsing it would change values this bug never
// touched.
export function stripNameFootnoteMarkers(value: string): string {
  return value.trim().replace(/[\s*]+$/u, "");
}

export function splitDisplayNameToFirstLast(displayName: string): { firstName: string; lastName: string } {
  const trimmed = displayName.trim();
  if (trimmed.includes(",")) {
    const [lastNamePart, givenNamePart] = trimmed.split(",", 2);
    const givenTokens = (givenNamePart ?? "")
      .trim()
      .split(/\s+/)
      .filter((token) => token.length > 0);
    const lastName = (lastNamePart ?? "").trim();
    if (givenTokens.length > 0 && lastName.length > 0) {
      return {
        firstName: givenTokens[0]!,
        lastName,
      };
    }
  }

  const tokens = trimmed
    .split(/\s+/)
    .filter((token) => token.length > 0);

  if (tokens.length === 0) {
    return { firstName: "Unknown", lastName: "Unknown" };
  }
  if (tokens.length === 1) {
    return { firstName: tokens[0]!, lastName: tokens[0]! };
  }
  return {
    firstName: tokens[0]!,
    lastName: tokens[tokens.length - 1]!,
  };
}

export function hasNormalizedIntersection(left: readonly string[], right: readonly string[]): boolean {
  const set = new Set(left.map((item) => item.trim()).filter((item) => item.length > 0));
  for (const item of right) {
    const normalized = item.trim();
    if (normalized.length === 0) {
      continue;
    }
    if (set.has(normalized)) {
      return true;
    }
  }
  return false;
}
