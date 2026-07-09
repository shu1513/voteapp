export function parsePositiveIntegerFlag(
  args: readonly string[],
  prefix: string,
  fallback: number
): number {
  const arg = args.find((token) => token.startsWith(`${prefix}=`));
  if (!arg) {
    return fallback;
  }
  const value = Number.parseInt(arg.slice(prefix.length + 1), 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

export function parseOptionalStringFlag(
  args: readonly string[],
  prefix: string
): string | undefined {
  const eqArg = args.find((token) => token.startsWith(`${prefix}=`));
  if (eqArg) {
    const value = eqArg.slice(prefix.length + 1).trim();
    if (!value) {
      throw new Error(`Missing value for ${prefix}`);
    }
    return value;
  }

  const index = args.indexOf(prefix);
  if (index < 0) {
    return undefined;
  }
  const value = args[index + 1]?.trim();
  if (!value || value.startsWith("--")) {
    throw new Error(`Missing value for ${prefix}`);
  }
  return value;
}
