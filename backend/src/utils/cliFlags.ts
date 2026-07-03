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
  return Number(rawValue);
}
