import { describe, expect, it } from "vitest";

import {
  assertConfirmedTarget,
  assertPromotionEndpoints,
  chunk,
  describeEndpoint,
  diffMigrationSets,
  isLocalHost,
  parseEndpoint,
  planRows,
  sameScalar,
  sameStringArray,
  TAG_PROJECTION_SQL,
  UPSERT_LABELS_SQL,
  UPSERT_RECORDS_SQL,
  UPSERT_TAGS_SQL,
  upsertBatched,
} from "../../src/scripts/promoteResearchData.js";

const LOCAL = "postgresql://localhost:5432/voteapp";
const REMOTE = "postgresql://voteapp_api:secret@db.example.render.com:5432/voteapp_prod";

describe("parseEndpoint", () => {
  it("reads host, port, database and user", () => {
    expect(parseEndpoint("target", REMOTE)).toEqual({
      host: "db.example.render.com",
      port: 5432,
      database: "voteapp_prod",
      user: "voteapp_api",
    });
  });

  it("defaults the port when the URL omits it", () => {
    expect(parseEndpoint("source", "postgresql://localhost/voteapp").port).toBe(5432);
  });

  it("honours the libpq host parameter, which overrides the URL authority", () => {
    // The same reason localDatabaseGuard parses with pg-connection-string: a
    // URL that looks local can point somewhere else entirely.
    const endpoint = parseEndpoint("target", "postgresql://localhost:5432/voteapp?host=/tmp/pgsock");
    expect(endpoint.host).toBe("/tmp/pgsock");
  });

  it("rejects blank, malformed and non-postgres URLs", () => {
    expect(() => parseEndpoint("target", "  ")).toThrow(/required/);
    expect(() => parseEndpoint("target", "not a url")).toThrow(/postgres/);
    expect(() => parseEndpoint("target", "mysql://localhost/db")).toThrow(/unsupported protocol/);
  });

  it("rejects a URL with no database name", () => {
    expect(() => parseEndpoint("target", "postgresql://localhost:5432")).toThrow(/does not name a database/);
  });
});

describe("describeEndpoint", () => {
  it("never includes the password", () => {
    const described = describeEndpoint(parseEndpoint("target", REMOTE));
    expect(described).toBe("voteapp_api@db.example.render.com:5432/voteapp_prod");
    expect(described).not.toContain("secret");
  });
});

describe("isLocalHost", () => {
  it("accepts loopback names and unix sockets, rejects remote hosts", () => {
    expect(isLocalHost("localhost")).toBe(true);
    expect(isLocalHost("127.0.0.1")).toBe(true);
    expect(isLocalHost("/tmp/pgsock")).toBe(true);
    expect(isLocalHost("db.example.render.com")).toBe(false);
  });
});

describe("assertPromotionEndpoints", () => {
  it("accepts a local source with a remote target", () => {
    const endpoints = assertPromotionEndpoints({ sourceUrl: LOCAL, targetUrl: REMOTE, env: {} });
    expect(endpoints.source.host).toBe("localhost");
    expect(endpoints.target.host).toBe("db.example.render.com");
  });

  it("allows a local target so a promotion can be rehearsed against a scratch database", () => {
    expect(() =>
      assertPromotionEndpoints({
        sourceUrl: LOCAL,
        targetUrl: "postgresql://localhost:5432/voteapp_promotion_target",
        env: {},
      })
    ).not.toThrow();
  });

  it("fails closed when ALLOW_REMOTE_DB_WRITES is set", () => {
    // That variable relaxes the manual-writer localhost guard. A shell holding
    // it must not silently loosen the tool that writes to production.
    expect(() =>
      assertPromotionEndpoints({
        sourceUrl: LOCAL,
        targetUrl: REMOTE,
        env: { ALLOW_REMOTE_DB_WRITES: "1" },
      })
    ).toThrow(/ALLOW_REMOTE_DB_WRITES/);
  });

  it("rejects a non-local source", () => {
    expect(() =>
      assertPromotionEndpoints({ sourceUrl: REMOTE, targetUrl: LOCAL, env: {} })
    ).toThrow(/non-local source/);
  });

  it("rejects promoting a database into itself", () => {
    expect(() =>
      assertPromotionEndpoints({ sourceUrl: LOCAL, targetUrl: LOCAL, env: {} })
    ).toThrow(/into itself/);
  });

  it("treats a different database on the same host as a distinct target", () => {
    expect(() =>
      assertPromotionEndpoints({
        sourceUrl: LOCAL,
        targetUrl: "postgresql://localhost:5432/other",
        env: {},
      })
    ).not.toThrow();
  });
});

describe("planRows", () => {
  type Row = { key: string; value: string | null };
  const keyOf = (row: Row) => row.key;
  const isEqual = (a: Row, b: Row) => sameScalar(a.value, b.value);

  it("classifies inserts, updates, unchanged and target-only rows", () => {
    const plan = planRows({
      sourceRows: [
        { key: "new", value: "a" },
        { key: "changed", value: "b2" },
        { key: "same", value: "c" },
      ],
      targetRows: [
        { key: "changed", value: "b1" },
        { key: "same", value: "c" },
        { key: "target-only", value: "d" },
      ],
      keyOf,
      isEqual,
    });

    expect(plan.inserts.map(keyOf)).toEqual(["new"]);
    expect(plan.updates.map(keyOf)).toEqual(["changed"]);
    expect(plan.unchangedCount).toBe(1);
    // Never deleted — production is allowed to be a superset.
    expect(plan.targetOnlyCount).toBe(1);
  });

  it("is a pure no-op when source and target already agree", () => {
    const rows = [
      { key: "a", value: "1" },
      { key: "b", value: null },
    ];
    const plan = planRows({ sourceRows: rows, targetRows: rows, keyOf, isEqual });

    expect(plan.inserts).toHaveLength(0);
    expect(plan.updates).toHaveLength(0);
    expect(plan.unchangedCount).toBe(2);
    expect(plan.targetOnlyCount).toBe(0);
  });

  it("treats null and a value as distinct, like IS DISTINCT FROM", () => {
    const plan = planRows({
      sourceRows: [{ key: "a", value: null }],
      targetRows: [{ key: "a", value: "set" }],
      keyOf,
      isEqual,
    });
    expect(plan.updates).toHaveLength(1);
  });

  it("handles an empty target as all inserts", () => {
    const plan = planRows({
      sourceRows: [{ key: "a", value: "1" }],
      targetRows: [],
      keyOf,
      isEqual,
    });
    expect(plan.inserts).toHaveLength(1);
    expect(plan.targetOnlyCount).toBe(0);
  });
});

describe("sameScalar / sameStringArray", () => {
  it("treats null and undefined as equal absence", () => {
    expect(sameScalar(null, undefined)).toBe(true);
    expect(sameScalar("", null)).toBe(false);
  });

  it("compares string arrays by order and content", () => {
    expect(sameStringArray(["a", "b"], ["a", "b"])).toBe(true);
    expect(sameStringArray(["a", "b"], ["b", "a"])).toBe(false);
    expect(sameStringArray([], null)).toBe(true);
    expect(sameStringArray(["a"], ["a", "b"])).toBe(false);
  });
});

describe("diffMigrationSets", () => {
  const known = ["001_init.sql", "002_next.sql"];

  it("passes when both sides have every on-disk migration at the same checksum", () => {
    const both = new Map([
      ["001_init.sql", "a"],
      ["002_next.sql", "b"],
    ]);
    expect(diffMigrationSets({ source: both, target: both, knownFilenames: known })).toEqual({
      missingOnSource: [],
      missingOnTarget: [],
      checksumMismatch: [],
    });
  });

  it("ignores applied rows whose migration file no longer exists", () => {
    // The local database carries 9 such rows from renamed migrations (e.g.
    // 196_add_county_council_chairman_alias.sql). Comparing raw sets would
    // report a permanent mismatch and block every promotion.
    const source = new Map([
      ["001_init.sql", "a"],
      ["002_next.sql", "b"],
      ["196_add_county_council_chairman_alias.sql", "ghost"],
    ]);
    const target = new Map([
      ["001_init.sql", "a"],
      ["002_next.sql", "b"],
    ]);
    const diff = diffMigrationSets({ source, target, knownFilenames: known });
    expect(diff.missingOnTarget).toEqual([]);
    expect(diff.checksumMismatch).toEqual([]);
  });

  it("reports a target that is behind, and a checksum disagreement", () => {
    const source = new Map([
      ["001_init.sql", "a"],
      ["002_next.sql", "b"],
    ]);
    const target = new Map([["001_init.sql", "different"]]);
    const diff = diffMigrationSets({ source, target, knownFilenames: known });
    expect(diff.missingOnTarget).toEqual(["002_next.sql"]);
    expect(diff.checksumMismatch).toEqual(["001_init.sql"]);
  });
});

describe("chunk", () => {
  it("splits rows into batches and keeps every row", () => {
    expect(chunk([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]]);
    expect(chunk([], 2)).toEqual([]);
    expect(chunk([1, 2], 10)).toEqual([[1, 2]]);
  });

  it("rejects a non-positive size rather than looping forever", () => {
    expect(() => chunk([1], 0)).toThrow(/positive/);
  });
});

describe("upsert statements", () => {
  const statements = [UPSERT_RECORDS_SQL, UPSERT_TAGS_SQL, UPSERT_LABELS_SQL];

  it("never delete, truncate, or issue DDL — the property that makes this safe on production", () => {
    for (const sql of statements) {
      expect(sql).not.toMatch(/\bDELETE\b/i);
      expect(sql).not.toMatch(/\bTRUNCATE\b/i);
      expect(sql).not.toMatch(/\bDROP\b/i);
      expect(sql).not.toMatch(/\bALTER\b/i);
    }
  });

  it("guard every update with a distinctness check so unchanged rows never fire updated_at", () => {
    for (const sql of statements) {
      expect(sql).toMatch(/ON CONFLICT/i);
      expect(sql).toMatch(/IS DISTINCT FROM/i);
    }
  });

  it("touch only the three allowlisted tables", () => {
    const written = statements.map((sql) => /INSERT INTO (public\.\w+)/i.exec(sql)?.[1]);
    expect(written).toEqual([
      "public.candidate_records",
      "public.candidate_record_area_tags",
      "public.finance_committee_labels",
    ]);
  });

  it("resolves tag parents through the target's own rows, never a transported id", () => {
    expect(UPSERT_TAGS_SQL).toMatch(/JOIN public\.candidate_records/i);
    expect(UPSERT_TAGS_SQL).toMatch(/r\.record_identity_key/i);
    expect(UPSERT_TAGS_SQL).toMatch(/JOIN public\.research_areas AS a ON a\.slug/i);
    // candidate_record_id may appear in the projection's JOIN (that is how the
    // source denormalises), but must never be one of the transported columns —
    // carrying it across is exactly the bug that attaches tags to the wrong
    // record when the target's record id differs.
    const selectedColumns = /SELECT([\s\S]*?)FROM/i.exec(TAG_PROJECTION_SQL)?.[1] ?? "";
    expect(selectedColumns).not.toMatch(/candidate_record_id/i);
    expect(selectedColumns).toMatch(/record_identity_key/i);
    expect(selectedColumns).toMatch(/research_area_slug/i);
  });

  it("copies researched_at rather than stamping now(), so history is not falsified", () => {
    expect(UPSERT_LABELS_SQL).toMatch(/researched_at_utc/);
    expect(UPSERT_LABELS_SQL).not.toMatch(/now\(\)/i);
  });
});

describe("upsertBatched", () => {
  it("sends one statement per batch and sums the rows written", async () => {
    const calls: unknown[][] = [];
    const client = {
      query: async (_text: string, values?: readonly unknown[]) => {
        calls.push(values as unknown[]);
        return { rows: [], rowCount: 2 };
      },
    };
    const written = await upsertBatched(client, "SQL", [1, 2, 3], 2);

    expect(calls).toHaveLength(2);
    expect(JSON.parse(calls[0]![0] as string)).toEqual([1, 2]);
    expect(JSON.parse(calls[1]![0] as string)).toEqual([3]);
    expect(written).toBe(4);
  });

  it("issues no statement at all when there is nothing to write", async () => {
    let called = false;
    const client = {
      query: async () => {
        called = true;
        return { rows: [], rowCount: 0 };
      },
    };
    expect(await upsertBatched(client, "SQL", [])).toBe(0);
    expect(called).toBe(false);
  });
});

describe("assertConfirmedTarget", () => {
  const target = parseEndpoint("target", REMOTE);

  it("accepts the matching host, case-insensitively", () => {
    expect(() => assertConfirmedTarget(target, "DB.example.render.com")).not.toThrow();
  });

  it("names the required flag value when confirmation is missing", () => {
    expect(() => assertConfirmedTarget(target, "")).toThrow(/--confirm-target db\.example\.render\.com/);
  });

  it("rejects a mismatched confirmation", () => {
    expect(() => assertConfirmedTarget(target, "localhost")).toThrow(/does not match/);
  });
});
