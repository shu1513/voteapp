import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

// Postgres infers an untyped parameter's type from its casts. When one query
// casts the same parameter to BOTH ::date and ::timestamptz, the parameter
// resolves to date and the timestamptz comparison silently collapses to
// midnight. In the finance due queries this froze the staleness threshold at
// midnight-of-the-run-day, so a row synced earlier the same day could never
// become due again, at any --stale-after-days value including 0.
//
// The fix is to root every date use in the timestamp: ($N::timestamptz)::date.
// This scan fails if any template literal reintroduces the ambiguous pair.

function templateLiterals(source: string): string[] {
  const parts = source.split("`");
  const literals: string[] = [];
  for (let index = 1; index < parts.length; index += 2) {
    literals.push(parts[index]!);
  }
  return literals;
}

async function listTypeScriptFiles(dir: string): Promise<string[]> {
  const entries = await readdir(dir, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await listTypeScriptFiles(path)));
    } else if (entry.name.endsWith(".ts")) {
      files.push(path);
    }
  }
  return files;
}

describe("SQL parameter type ambiguity", () => {
  it("no query casts the same parameter to both ::date and ::timestamptz", async () => {
    const files = await listTypeScriptFiles("src");
    const offenders: string[] = [];
    for (const file of files.sort()) {
      const source = await readFile(file, "utf8");
      for (const literal of templateLiterals(source)) {
        const bareDateParams = new Set(
          [...literal.matchAll(/(?<![\w)])\$(\d+)::date\b/g)].map((match) => match[1]!)
        );
        const timestamptzParams = new Set(
          [...literal.matchAll(/\$(\d+)::timestamptz\b/g)].map((match) => match[1]!)
        );
        for (const param of bareDateParams) {
          if (timestamptzParams.has(param)) {
            offenders.push(`${file}: $${param} cast to both ::date and ::timestamptz in one query`);
          }
        }
      }
    }
    expect(offenders).toEqual([]);
  });
});
