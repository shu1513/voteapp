import { normalizeHttpUrl } from "./normalizeHttpUrl.js";

export function normalizeCandidateName(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeTwitterHandle(value: string): string {
  const trimmed = value.trim();
  const withoutPrefix = trimmed.startsWith("@") ? trimmed.slice(1) : trimmed;
  return withoutPrefix.toLowerCase();
}

export function normalizeOptionalUrl(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  return normalizeHttpUrl(value);
}

export function splitDisplayNameToFirstLast(displayName: string): { firstName: string; lastName: string } {
  const tokens = displayName
    .trim()
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
