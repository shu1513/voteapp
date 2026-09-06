/**
 * Shared environment-variable readers with ONE grammar, replacing per-file
 * copies that disagreed (some accepted "y"/"n", some parsed "10ms" as 10
 * and "1.5" as 1 via parseInt, some checked safe-integer range).
 *
 * Grammar:
 * - Unset, empty or whitespace-only → the fallback.
 * - Boolean: 1/true/yes/y/on → true; 0/false/no/n/off → false
 *   (case-insensitive, trimmed); anything else throws.
 * - Positive integer: the whole trimmed value must be decimal digits with
 *   no leading zero and within Number.MAX_SAFE_INTEGER; "0", "-1", "1.5",
 *   "10ms" and "1e3" throw. A malformed value is a configuration mistake
 *   and must fail at boot, not be silently truncated.
 */

const TRUE_VALUES = new Set(["1", "true", "yes", "y", "on"]);
const FALSE_VALUES = new Set(["0", "false", "no", "n", "off"]);

function rawValue(name: string): string | null {
  const raw = process.env[name];
  if (raw === undefined) {
    return null;
  }
  const trimmed = raw.trim();
  return trimmed.length === 0 ? null : trimmed;
}

export function readBooleanEnv(name: string, fallback: boolean): boolean {
  const raw = rawValue(name);
  if (raw === null) {
    return fallback;
  }
  const normalized = raw.toLowerCase();
  if (TRUE_VALUES.has(normalized)) {
    return true;
  }
  if (FALSE_VALUES.has(normalized)) {
    return false;
  }
  throw new Error(`Invalid boolean env ${name}: ${process.env[name]}`);
}

export function readPositiveIntegerEnv(name: string, fallback: number): number {
  const raw = rawValue(name);
  if (raw === null) {
    return fallback;
  }
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid positive integer env ${name}: ${process.env[name]}`);
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`Invalid positive integer env ${name}: ${process.env[name]}`);
  }
  return parsed;
}
