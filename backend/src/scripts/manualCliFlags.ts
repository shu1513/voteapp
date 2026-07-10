// Manual wrappers historically ignored flags they did not recognize, so a
// typo ("--dry_run") or a flag from a newer checkout ("--evidence-file" on a
// pre-PR-#232 tree) silently did nothing — a live run believed its sweep
// evidence was being enforced while the wrapper never read the flag. Every
// manual CLI entrypoint asserts its known flag set up front so both failure
// modes surface as errors instead of silent no-ops.
export function assertKnownCliFlags(
  scriptLabel: string,
  argv: readonly string[],
  knownFlags: readonly string[]
): void {
  const known = new Set(knownFlags);
  const unknown = argv.filter(
    (token) => token.startsWith("--") && token !== "--" && !known.has(token)
  );
  if (unknown.length > 0) {
    const knownList = knownFlags.length > 0 ? [...knownFlags].sort().join(", ") : "(none)";
    throw new Error(
      `${scriptLabel}: unknown flag(s) ${unknown.join(", ")}. Known flags: ${knownList}. ` +
        `If the flag is documented, this checkout predates it — sync to current main before running.`
    );
  }
}
