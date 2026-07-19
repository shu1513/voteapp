// Manual wrappers historically ignored flags they did not recognize, so a
// typo ("--dry_run") or a flag from a newer checkout ("--evidence-file" on a
// pre-PR-#232 tree) silently did nothing — a live run believed its sweep
// evidence was being enforced while the wrapper never read the flag. Every
// manual CLI entrypoint asserts its known flag set up front so both failure
// modes surface as errors instead of silent no-ops.
//
// Each flag declares how its parser accepts a value, because the parsers
// genuinely differ and a form the parser cannot read is itself a silent
// no-op: readFlag-based writers take only "--file x" (space), the
// elections-worker integer flags take only "--batch-size=50" (equals), the
// presidential readValueFlag takes both, and boolean flags take none.
export type CliFlagValueStyle = "none" | "space" | "equals" | "both";

export type CliFlagSpec = {
  name: string;
  value: CliFlagValueStyle;
};

function describeFlagValueStyle(spec: CliFlagSpec): string {
  switch (spec.value) {
    case "none":
      return spec.name;
    case "space":
      return `${spec.name} <value>`;
    case "equals":
      return `${spec.name}=<value>`;
    case "both":
      return `${spec.name} <value> | ${spec.name}=<value>`;
  }
}

export function assertKnownCliFlags(
  scriptLabel: string,
  argv: readonly string[],
  specs: readonly CliFlagSpec[]
): void {
  // Every manual wrapper asserts flags before loading env or touching
  // Postgres/Redis, so this is the one place a read-only --help can live:
  // wrappers historically rejected --help as an unknown flag, and operators
  // fell back to reading the script source (or a sandbox-blocked probe run)
  // just to discover the flag set — hit live on the deferral and election
  // injector CLIs.
  if (argv.includes("--help")) {
    const lines = [
      `${scriptLabel} flags:`,
      ...specs
        .slice()
        .sort((a, b) => a.name.localeCompare(b.name))
        .map((spec) => `  ${describeFlagValueStyle(spec)}`),
    ];
    console.log(lines.join("\n"));
    process.exit(0);
  }

  const byName = new Map(specs.map((spec) => [spec.name, spec]));
  const problems: string[] = [];

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index]!;
    // Positional words (subcommands) and the flag values consumed below are
    // not flags; the bare "--" separator is npm argument plumbing. Everything
    // else dash-prefixed — including single-dash typos like "-typo" — must
    // resolve to a known spec.
    if (token === "--" || !token.startsWith("-")) {
      continue;
    }
    const equalsIndex = token.indexOf("=");
    const name = equalsIndex >= 0 ? token.slice(0, equalsIndex) : token;
    const spec = byName.get(name);
    if (!spec) {
      problems.push(`unknown flag ${name}`);
      continue;
    }
    if (equalsIndex >= 0 && (spec.value === "none" || spec.value === "space")) {
      problems.push(
        spec.value === "none"
          ? `${name} takes no value (got "${token}")`
          : `${name} takes its value as the next argument ("${name} <value>", not "${token}") — this script's parser silently ignores the "=" form`
      );
      continue;
    }
    if (equalsIndex < 0 && spec.value === "equals") {
      problems.push(
        `${name} only accepts the "${name}=<value>" form — this script's parser silently ignores a space-separated value`
      );
      continue;
    }
    if (equalsIndex < 0 && (spec.value === "space" || spec.value === "both")) {
      // Consume the value token so it is never mistaken for a flag; values
      // may legitimately start with a single dash (free-text notes), which
      // mirrors the parsers (they only reject "--"-prefixed values).
      const next = argv[index + 1];
      if (next !== undefined && !next.startsWith("--")) {
        index += 1;
      }
    }
  }

  if (problems.length > 0) {
    const knownList =
      specs.length > 0 ? specs.map((spec) => spec.name).sort().join(", ") : "(none)";
    throw new Error(
      `${scriptLabel}: ${problems.join("; ")}. Known flags: ${knownList}. ` +
        `If a rejected flag is documented, this checkout predates it — sync to current main before running.`
    );
  }
}
