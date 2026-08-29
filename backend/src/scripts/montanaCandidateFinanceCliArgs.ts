export type MontanaFinanceAllowedFlags = {
  booleanFlags: readonly string[];
  valueFlags: readonly string[];
};

/**
 * Strict per-script flag validation. Each script passes exactly the flags it
 * consumes — a shared union would let a report-only flag like `--year` pass
 * on the due-sync command while silently doing nothing. Boolean flags reject
 * the `--flag=value` form outright: the parsers read them via
 * `args.includes("--flag")`, so an `=` form would validate and then be
 * silently ignored.
 */
export function assertNoUnknownMontanaFinanceFlags(
  args: readonly string[],
  allowed: MontanaFinanceAllowedFlags
): void {
  const booleanFlags = new Set(allowed.booleanFlags);
  const valueFlags = new Set(allowed.valueFlags);
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (!arg.startsWith("--")) {
      throw new Error(`Unexpected positional argument: ${arg}`);
    }
    const flag = arg.split("=", 1)[0] ?? arg;
    if (booleanFlags.has(flag)) {
      if (arg.includes("=")) {
        throw new Error(`${flag} takes no value`);
      }
      continue;
    }
    if (valueFlags.has(flag)) {
      if (!arg.includes("=")) {
        index += 1;
      }
      continue;
    }
    throw new Error(`Unknown option: ${flag}`);
  }
}

export function parseMontanaFinanceFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const values: string[] = [];

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (arg.startsWith(inlinePrefix)) {
      const value = arg.slice(inlinePrefix.length).trim();
      if (value.length === 0) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(value);
      continue;
    }
    if (arg === name) {
      const next = args[index + 1];
      if (!next || next.startsWith("--") || next.trim().length === 0) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(next.trim());
      index += 1;
    }
  }

  if (values.length > 1) {
    throw new Error(`Provide ${name} at most once`);
  }
  return values[0] ?? null;
}

export function parseMontanaFinancePositiveIntegerFlag(
  args: readonly string[],
  name: string
): number | undefined {
  const raw = parseMontanaFinanceFlagValue(args, name);
  if (raw === null) {
    return undefined;
  }
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  // Digit-only values above 2^53 - 1 would be silently rounded by Number().
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return parsed;
}
