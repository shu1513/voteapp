/**
 * Reads an integer CLI flag that accepts both `--flag 5` and `--flag=5`
 * forms, requires a strictly positive value, and errors on a flag given
 * without a value instead of silently falling back.
 */
export function readPositiveIntegerFlag(argv: readonly string[], flagName: string, fallback: number): number {
  const flagIndex = argv.indexOf(flagName);
  const inlinePrefix = `${flagName}=`;
  const inline = argv.find((token) => token.startsWith(inlinePrefix));
  const rawValue = flagIndex >= 0 ? argv[flagIndex + 1] : inline ? inline.slice(inlinePrefix.length) : null;
  if (flagIndex >= 0 && rawValue === undefined) {
    throw new Error(`${flagName} requires a value`);
  }
  if (rawValue === null || rawValue === undefined) {
    return fallback;
  }
  if (!/^[1-9]\d*$/.test(rawValue)) {
    throw new Error(`${flagName} must be a positive integer, got: ${rawValue}`);
  }
  // Digit-only values above 2^53 - 1 would be silently rounded by Number().
  const parsed = Number(rawValue);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${flagName} must be a positive integer, got: ${rawValue}`);
  }
  return parsed;
}

/**
 * Strict value reader shared by the finance CLIs. Accepts `--flag value` and
 * `--flag=value`, trims the value, and throws on a flag given without a value
 * (missing, empty, or followed by another `--` token) and on a flag given
 * more than once, instead of silently taking the first occurrence. Returns
 * null when the flag is absent.
 */
export function readStrictFlagValue(argv: readonly string[], flagName: string): string | null {
  const inlinePrefix = `${flagName}=`;
  const values: string[] = [];

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]!;
    if (arg.startsWith(inlinePrefix)) {
      const value = arg.slice(inlinePrefix.length).trim();
      if (value.length === 0) {
        throw new Error(`Missing ${flagName} value`);
      }
      values.push(value);
      continue;
    }
    if (arg === flagName) {
      const next = argv[index + 1];
      if (!next || next.startsWith("--") || next.trim().length === 0) {
        throw new Error(`Missing ${flagName} value`);
      }
      values.push(next.trim());
      index += 1;
    }
  }

  if (values.length > 1) {
    throw new Error(`Provide ${flagName} at most once`);
  }
  return values[0] ?? null;
}

/**
 * Strict positive-integer reader over readStrictFlagValue: digits only with
 * no leading zero, and the parsed value must be a safe integer. Returns
 * undefined when the flag is absent.
 */
export function readStrictPositiveIntegerFlag(argv: readonly string[], flagName: string): number | undefined {
  const raw = readStrictFlagValue(argv, flagName);
  if (raw === null) {
    return undefined;
  }
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${flagName} value: ${raw}`);
  }
  // Digit-only values above 2^53 - 1 would be silently rounded by Number().
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`Invalid ${flagName} value: ${raw}`);
  }
  return parsed;
}

/**
 * readStrictFlagValue for a flag the script cannot run without. Throws
 * `Missing required <flag>` when the flag is absent.
 */
export function readStrictRequiredFlagValue(argv: readonly string[], flagName: string): string {
  const value = readStrictFlagValue(argv, flagName);
  if (value === null) {
    throw new Error(`Missing required ${flagName}`);
  }
  return value;
}

/**
 * readStrictPositiveIntegerFlag for a flag the script cannot run without.
 * Throws `Missing required <flag>` when the flag is absent.
 */
export function readStrictRequiredPositiveIntegerFlag(argv: readonly string[], flagName: string): number {
  const value = readStrictPositiveIntegerFlag(argv, flagName);
  if (value === undefined) {
    throw new Error(`Missing required ${flagName}`);
  }
  return value;
}

/**
 * Strict non-negative decimal reader over readStrictFlagValue: `0`, a
 * positive integer without a leading zero, or either followed by a fraction
 * (`12.5`). Returns undefined when the flag is absent.
 */
export function readStrictNonNegativeNumberFlag(argv: readonly string[], flagName: string): number | undefined {
  const raw = readStrictFlagValue(argv, flagName);
  if (raw === null) {
    return undefined;
  }
  if (!/^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(raw)) {
    throw new Error(`Invalid ${flagName} value: ${raw}`);
  }
  return Number(raw);
}
