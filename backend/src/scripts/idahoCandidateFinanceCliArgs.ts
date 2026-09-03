// Shared value-flag parser for the Idaho finance CLIs (auto-link and due
// sync). Accepts `--name=value` and `--name value`, at most once; the value
// must be a positive integer. Unknown-flag and missing-value checks live in
// financeCliFlagGuard.ts and run before this.
export function parseIdahoFinancePositiveIntegerFlag<T extends number | null | undefined>(
  args: readonly string[],
  name: string,
  fallback: T
): number | T {
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
  const value = values[0];
  if (value === undefined) return fallback;
  if (!/^[1-9]\d*$/.test(value) || !Number.isSafeInteger(Number(value))) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return Number(value);
}
