import { normalizeHttpUrl } from "./normalizeHttpUrl.js";

/**
 * Returns true when a field is effectively just a URL instead of plain-language text.
 */
export function isUrlOnlyText(value: string): boolean {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return false;
  }

  if (normalizeHttpUrl(trimmed) !== null) {
    return true;
  }

  return /^www\.[^\s]+$/i.test(trimmed);
}

