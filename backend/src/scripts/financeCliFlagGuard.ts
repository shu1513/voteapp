// Shared strict flag validation for the finance CLIs (ohio pattern,
// ported fleet-wide after PRs #617/#620/#622 fixed it state by state).
//
// Three silent failure modes this closes, each of which previously ran or
// enqueued a REAL write in place of the intended dry run:
// - unknown flag (e.g. the typo --dryrun) was ignored
// - bare positional (e.g. "dry-run" after npm's own "--" separator, which
//   npm consumes) was ignored
// - inline value on a boolean flag (--dry-run=true) passed a name-only
//   check yet failed the later args.includes("--dry-run") test
export function assertKnownCliFlags(
  args: readonly string[],
  label: string,
  booleanFlags: ReadonlySet<string>,
  valueFlags: ReadonlySet<string>
): void {
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (!arg.startsWith("--")) {
      // A bare token is only legal as the value of the immediately
      // preceding space-form value flag.
      const previous = index > 0 ? args[index - 1]! : undefined;
      if (previous === undefined || !valueFlags.has(previous)) {
        throw new Error(`Unexpected positional argument: ${arg}`);
      }
      continue;
    }
    const name = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (!booleanFlags.has(name) && !valueFlags.has(name)) {
      throw new Error(`Unknown ${label} flag: ${name}`);
    }
    if (booleanFlags.has(name) && arg.includes("=")) {
      throw new Error(`Boolean flag does not accept a value: ${name}`);
    }
  }
}
