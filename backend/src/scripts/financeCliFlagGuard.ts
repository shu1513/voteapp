// Shared strict flag validation for the finance CLIs (ohio pattern,
// ported fleet-wide after PRs #617/#620/#622 fixed it state by state).
//
// Four silent failure modes this closes, each of which previously ran or
// enqueued a REAL write in place of the intended dry run (or ran an
// untargeted full batch in place of a targeted one):
// - unknown flag (e.g. the typo --dryrun) was ignored
// - bare positional (e.g. "dry-run" after npm's own "--" separator, which
//   npm consumes) was ignored
// - inline value on a boolean flag (--dry-run=true) passed a name-only
//   check yet failed the later args.includes("--dry-run") test
// - value flag without a value (--candidate-id as the last token, or
//   --max-candidates=) silently defaulted in parsers that return null
//   instead of throwing
export function assertKnownCliFlags(
  args: readonly string[],
  label: string,
  booleanFlags: ReadonlySet<string>,
  valueFlags: ReadonlySet<string>
): void {
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (!arg.startsWith("--")) {
      // Space-form value-flag values are consumed below, so any bare
      // token reaching this check is a positional typo.
      throw new Error(`Unexpected positional argument: ${arg}`);
    }
    const eq = arg.indexOf("=");
    const name = eq === -1 ? arg : arg.slice(0, eq);
    if (!booleanFlags.has(name) && !valueFlags.has(name)) {
      throw new Error(`Unknown ${label} flag: ${name}`);
    }
    if (booleanFlags.has(name)) {
      if (eq !== -1) {
        throw new Error(`Boolean flag does not accept a value: ${name}`);
      }
      continue;
    }
    if (eq !== -1) {
      if (arg.slice(eq + 1).trim().length === 0) {
        throw new Error(`Missing ${name} value`);
      }
      continue;
    }
    const next = args[index + 1];
    if (next === undefined || next.startsWith("--") || next.trim().length === 0) {
      throw new Error(`Missing ${name} value`);
    }
    index += 1;
  }
}
