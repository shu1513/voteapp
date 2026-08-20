import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const VALUE_FLAGS = new Set(["--cache-dir", "--lookahead-days", "--lookback-days", "--max-candidates", "--stale-after-days"]);

function value(args: readonly string[], name: string): string | undefined {
  const prefix = `${name}=`;
  const inline = args.filter((arg) => arg.startsWith(prefix)).map((arg) => arg.slice(prefix.length));
  const positions = args.flatMap((arg, index) => arg === name ? [index] : []);
  if (inline.length + positions.length > 1) throw new Error(`Provide ${name} at most once`);
  const raw = inline[0] ?? (positions[0] === undefined ? undefined : args[positions[0] + 1]);
  if (raw !== undefined && (!raw.trim() || raw.startsWith("--"))) throw new Error(`Missing ${name} value`);
  return raw?.trim();
}

function positiveInteger(args: readonly string[], name: string): number | undefined {
  const raw = value(args, name);
  if (raw === undefined) return undefined;
  if (!/^[1-9]\d*$/.test(raw) || !Number.isSafeInteger(Number(raw))) throw new Error(`Invalid ${name} value: ${raw}`);
  return Number(raw);
}

export function parseMissouriCandidateFinanceCliArgs(args: readonly string[]) {
  assertKnownCliFlags(args, "Missouri candidate finance", BOOLEAN_FLAGS, VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: positiveInteger(args, "--max-candidates"),
    staleAfterDays: positiveInteger(args, "--stale-after-days"),
    electionLookbackDays: positiveInteger(args, "--lookback-days"),
    electionLookaheadDays: positiveInteger(args, "--lookahead-days"),
    cacheDir: value(args, "--cache-dir"),
  };
}
