// Shared value-flag parsers for the Idaho finance CLIs (auto-link, due
// sync, manual link). Accept `--name=value` and `--name value`, at most
// once. Unknown-flag and missing-value checks live in financeCliFlagGuard.ts
// and run before these.
export function readIdahoFinanceFlagValue(args: readonly string[], name: string): string | undefined {
  const values: string[] = [];
  const prefix = `${name}=`;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (arg.startsWith(prefix)) {
      values.push(arg.slice(prefix.length).trim());
    } else if (arg === name) {
      values.push(args[index + 1]!.trim());
      index += 1;
    }
  }
  if (values.length > 1) throw new Error(`Provide ${name} at most once`);
  return values[0];
}

/** A required string flag (uuids on the manual-link CLI). */
export function parseIdahoFinanceRequiredStringFlag(args: readonly string[], name: string): string {
  const value = readIdahoFinanceFlagValue(args, name);
  if (value === undefined || value.length === 0) throw new Error(`Missing ${name} value`);
  return value;
}

/** A positive-integer flag with a fallback when absent. */
export function parseIdahoFinancePositiveIntegerFlag<T extends number | null | undefined>(
  args: readonly string[],
  name: string,
  fallback: T
): number | T {
  const value = readIdahoFinanceFlagValue(args, name);
  if (value === undefined) return fallback;
  if (!/^[1-9]\d*$/.test(value) || !Number.isSafeInteger(Number(value))) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return Number(value);
}
