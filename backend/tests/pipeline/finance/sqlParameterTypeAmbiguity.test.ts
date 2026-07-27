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
// The fix roots every date use in the timestamp:
// ($N::timestamptz AT TIME ZONE 'UTC')::date. This scan fails if any template
// literal reintroduces the ambiguous pair.

function templateLiterals(source: string): string[] {
  const parts = source.split("`");
  const literals: string[] = [];
  for (let index = 1; index < parts.length; index += 2) {
    literals.push(parts[index]!);
  }
  return literals;
}

/**
 * Parameters cast to date while the parameter itself is still untyped: bare
 * `$N::date`, and the equivalent parenthesized or spaced forms `($N)::date`
 * and `$N ::date`. A parameter whose first cast is another type — as in
 * `($N::timestamptz AT TIME ZONE 'UTC')::date` — is already resolved, so it
 * does not count.
 */
export function bareDateCastParams(literal: string): Set<string> {
  return new Set([...literal.matchAll(/\$(\d+)[\s)]*::date\b/g)].map((match) => match[1]!));
}

export function ambiguousDateTimestamptzParams(literal: string): Set<string> {
  const dateParams = bareDateCastParams(literal);
  const timestamptzParams = new Set(
    [...literal.matchAll(/\$(\d+)::timestamptz\b/g)].map((match) => match[1]!)
  );
  return new Set([...dateParams].filter((param) => timestamptzParams.has(param)));
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
  it("detects every bare-date cast spelling and none of the resolved ones", () => {
    // Equivalent spellings that all leave the parameter's own type as date.
    expect(bareDateCastParams("WHERE d >= $1::date AND t < $1::timestamptz")).toEqual(new Set(["1"]));
    expect(bareDateCastParams("WHERE d >= ($1)::date")).toEqual(new Set(["1"]));
    expect(bareDateCastParams("WHERE d >= (($1))::date")).toEqual(new Set(["1"]));
    expect(bareDateCastParams("WHERE d >= $1 ::date")).toEqual(new Set(["1"]));
    // The parameter's first cast is timestamptz, so it is already resolved.
    expect(bareDateCastParams("WHERE d >= ($1::timestamptz)::date")).toEqual(new Set());
    expect(bareDateCastParams("WHERE d >= ($1::timestamptz AT TIME ZONE 'UTC')::date")).toEqual(new Set());

    expect(ambiguousDateTimestamptzParams("WHERE d >= ($1)::date AND t < $1::timestamptz")).toEqual(
      new Set(["1"])
    );
    expect(
      ambiguousDateTimestamptzParams(
        "WHERE d >= ($1::timestamptz AT TIME ZONE 'UTC')::date AND t < $1::timestamptz"
      )
    ).toEqual(new Set());
    // date-only queries are fine; the ambiguity needs both casts on one param.
    expect(ambiguousDateTimestamptzParams("WHERE d >= $1::date AND t < $2::timestamptz")).toEqual(new Set());
  });

  it("no query casts the same parameter to both ::date and ::timestamptz", async () => {
    const files = await listTypeScriptFiles("src");
    const offenders: string[] = [];
    for (const file of files.sort()) {
      const source = await readFile(file, "utf8");
      for (const literal of templateLiterals(source)) {
        for (const param of ambiguousDateTimestamptzParams(literal)) {
          offenders.push(`${file}: $${param} cast to both ::date and ::timestamptz in one query`);
        }
      }
    }
    expect(offenders).toEqual([]);
  });
});
