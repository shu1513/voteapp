import { writeFile } from "node:fs/promises";

import {
  produceIllinoisSbeNormalizedArtifact,
  type IllinoisSbeBulkDataPaths,
} from "../pipeline/illinoisFinance/illinoisSbeBulkDataProducer.js";

const VALUE_FLAGS = [
  "--candidates",
  "--candidate-elections",
  "--committee-candidate-links",
  "--committees",
  "--filed-documents",
  "--d2-totals",
  "--acquired-at",
  "--output",
] as const;

function parseArgs(args: readonly string[]): Map<string, string> {
  const allowed = new Set<string>(VALUE_FLAGS);
  const parsed = new Map<string, string>();
  for (let index = 0; index < args.length; index += 1) {
    const raw = args[index]!;
    const equals = raw.indexOf("=");
    const flag = equals >= 0 ? raw.slice(0, equals) : raw;
    if (!allowed.has(flag)) throw new Error(`Unknown option: ${flag}`);
    if (parsed.has(flag)) throw new Error(`Provide ${flag} at most once`);
    const value = equals >= 0 ? raw.slice(equals + 1) : args[++index];
    if (!value?.trim() || value.startsWith("--")) throw new Error(`Missing ${flag} value`);
    parsed.set(flag, value.trim());
  }
  return parsed;
}

function required(args: Map<string, string>, flag: string): string {
  const value = args.get(flag);
  if (!value) throw new Error(`Missing required ${flag}`);
  return value;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const paths: IllinoisSbeBulkDataPaths = {
    candidates: required(args, "--candidates"),
    candidateElections: required(args, "--candidate-elections"),
    committeeCandidateLinks: required(args, "--committee-candidate-links"),
    committees: required(args, "--committees"),
    filedDocuments: required(args, "--filed-documents"),
    d2Totals: required(args, "--d2-totals"),
  };
  const { artifact, stats } = await produceIllinoisSbeNormalizedArtifact({
    paths,
    acquiredAt: required(args, "--acquired-at"),
  });
  const json = `${JSON.stringify(artifact, null, 2)}\n`;
  const output = args.get("--output");
  if (output) await writeFile(output, json, "utf8");
  else process.stdout.write(json);
  process.stderr.write(`${JSON.stringify(stats)}\n`);
}

main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
