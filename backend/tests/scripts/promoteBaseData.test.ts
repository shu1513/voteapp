import { describe, expect, it } from "vitest";

import {
  BASE_TABLES,
  baseInsertSql,
  findPresentIds,
  idProjectionSql,
  insertPlannedRows,
  planTableInserts,
  rowProjectionSql,
  toBaseIdRow,
  type BaseIdRow,
  type BaseTableSpec,
} from "../../src/scripts/promoteBaseData.js";

describe("BASE_TABLES", () => {
  it("covers exactly the four base tables, in FK order", () => {
    expect(BASE_TABLES.map((spec) => spec.table)).toEqual([
      "elections",
      "candidates",
      "candidate_elections",
      "ballot_measures",
    ]);
  });

  it("every promoted FK points at this table or an earlier one — the order inserts run in", () => {
    // A promoted parent later in the list would be planned AFTER its child,
    // so the closure check would see an empty planned set and skip rows that
    // were actually going to be satisfied.
    const seen = new Set<string>();
    for (const spec of BASE_TABLES) {
      for (const fk of spec.fks) {
        if (fk.kind === "promoted") {
          expect(
            seen.has(fk.parentTable) || fk.parentTable === spec.table,
            `${spec.table}.${fk.column} -> ${fk.parentTable}`
          ).toBe(true);
        }
      }
      seen.add(spec.table);
    }
  });

  it("external FKs point outside the promoted set — this tool never invents those parents", () => {
    const promoted = new Set(BASE_TABLES.map((spec) => spec.table));
    for (const spec of BASE_TABLES) {
      for (const fk of spec.fks) {
        if (fk.kind === "external") {
          expect(promoted.has(fk.parentTable), `${spec.table}.${fk.column}`).toBe(false);
        }
      }
    }
  });
});

describe("insert statements", () => {
  const statements = BASE_TABLES.map((spec) => baseInsertSql(spec.table));

  it("are INSERT-only — no delete, truncate, update or DDL, the property that makes this safe on production", () => {
    for (const sql of statements) {
      expect(sql).not.toMatch(/\bDELETE\b/i);
      expect(sql).not.toMatch(/\bTRUNCATE\b/i);
      expect(sql).not.toMatch(/\bDROP\b/i);
      expect(sql).not.toMatch(/\bALTER\b/i);
      expect(sql).not.toMatch(/\bUPDATE\b/i);
    }
  });

  it("conflict only on id, and DO NOTHING — an existing row is never touched", () => {
    for (const sql of statements) {
      expect(sql).toMatch(/ON CONFLICT \(id\) DO NOTHING/);
      expect(sql).not.toMatch(/DO UPDATE/i);
    }
  });

  it("populate rows by NAME via jsonb_populate_record, never positionally", () => {
    // Positional transport would let a historical column-order difference
    // between the two databases shift values into the wrong columns.
    for (const [index, spec] of BASE_TABLES.entries()) {
      expect(statements[index]).toContain(`jsonb_populate_record(NULL::public.${spec.table}`);
      expect(statements[index]).toContain(`INSERT INTO public.${spec.table}`);
    }
  });
});

describe("projections", () => {
  it("the id projection carries the id and every FK column as text", () => {
    const elections = BASE_TABLES[0]!;
    const sql = idProjectionSql(elections);
    expect(sql).toContain("id::text AS id");
    expect(sql).toContain("district_id::text AS district_id");
    expect(sql).toContain("office_id::text AS office_id");
    expect(sql).toContain("FROM public.elections");
  });

  it("the row projection ships to_jsonb rows for exactly the requested ids", () => {
    const sql = rowProjectionSql("candidates");
    expect(sql).toContain("to_jsonb(t) AS row");
    expect(sql).toContain("t.id = ANY($1::uuid[])");
  });

  it("toBaseIdRow maps absent and null FK values to null", () => {
    const spec: BaseTableSpec = {
      table: "elections",
      fks: [
        { column: "district_id", parentTable: "districts", kind: "external" },
        { column: "office_id", parentTable: "offices", kind: "external" },
      ],
    };
    expect(toBaseIdRow(spec, { id: "e1", district_id: "d1", office_id: null })).toEqual({
      id: "e1",
      fks: { district_id: "d1", office_id: null },
    });
  });
});

describe("planTableInserts", () => {
  const row = (id: string, fks: Record<string, string | null> = {}): BaseIdRow => ({ id, fks });

  const electionsSpec: BaseTableSpec = {
    table: "elections",
    fks: [
      { column: "district_id", parentTable: "districts", kind: "external" },
      { column: "office_id", parentTable: "offices", kind: "external" },
    ],
  };

  const candidateElectionsSpec: BaseTableSpec = {
    table: "candidate_elections",
    fks: [
      { column: "candidate_id", parentTable: "candidates", kind: "promoted" },
      { column: "election_id", parentTable: "elections", kind: "promoted" },
      { column: "running_mate_candidate_id", parentTable: "candidates", kind: "promoted" },
    ],
  };

  const candidatesSpec: BaseTableSpec = {
    table: "candidates",
    fks: [{ column: "merged_into_candidate_id", parentTable: "candidates", kind: "promoted" }],
  };

  it("classifies missing, already-on-target and target-only rows", () => {
    const plan = planTableInserts({
      spec: electionsSpec,
      sourceRows: [row("shared", { district_id: "d1" }), row("new", { district_id: "d1" })],
      targetIds: new Set(["shared", "target-only"]),
      targetParentIds: new Map([["districts", new Set(["d1"])]]),
      plannedParentIds: new Map(),
    });
    expect(plan.insertIds).toEqual(["new"]);
    expect(plan.alreadyOnTargetCount).toBe(1);
    // Never deleted — production is allowed to be a superset.
    expect(plan.targetOnlyCount).toBe(1);
    expect(plan.skipped).toEqual([]);
  });

  it("skips a row whose external parent is absent on the target, naming the FK and parent", () => {
    const plan = planTableInserts({
      spec: electionsSpec,
      sourceRows: [row("e1", { district_id: "d-missing" }), row("e2", { district_id: "d1" })],
      targetIds: new Set(),
      targetParentIds: new Map([["districts", new Set(["d1"])]]),
      plannedParentIds: new Map(),
    });
    expect(plan.insertIds).toEqual(["e2"]);
    expect(plan.skipped).toEqual([{ id: "e1", column: "district_id", parentId: "d-missing" }]);
  });

  it("a null FK always passes — office_id is nullable", () => {
    const plan = planTableInserts({
      spec: electionsSpec,
      sourceRows: [row("e1", { district_id: "d1", office_id: null })],
      targetIds: new Set(),
      targetParentIds: new Map([["districts", new Set(["d1"])], ["offices", new Set()]]),
      plannedParentIds: new Map(),
    });
    expect(plan.insertIds).toEqual(["e1"]);
  });

  it("a promoted parent is satisfied by the target OR by an earlier table's planned inserts", () => {
    const plan = planTableInserts({
      spec: candidateElectionsSpec,
      sourceRows: [
        row("ce1", { candidate_id: "c-on-target", election_id: "e-planned", running_mate_candidate_id: null }),
        row("ce2", { candidate_id: "c-absent", election_id: "e-planned", running_mate_candidate_id: null }),
      ],
      targetIds: new Set(),
      targetParentIds: new Map([
        ["candidates", new Set(["c-on-target"])],
        ["elections", new Set()],
      ]),
      plannedParentIds: new Map([
        ["candidates", new Set()],
        ["elections", new Set(["e-planned"])],
      ]),
    });
    expect(plan.insertIds).toEqual(["ce1"]);
    // The cascade: an election skipped upstream is not in the planned set,
    // so its children skip here rather than failing the FK at insert time.
    expect(plan.skipped).toEqual([{ id: "ce2", column: "candidate_id", parentId: "c-absent" }]);
  });

  it("admits a self-FK merge chain in dependency order, survivor first", () => {
    // c-mergee -> c-survivor -> null, handed in the wrong order on purpose.
    const plan = planTableInserts({
      spec: candidatesSpec,
      sourceRows: [
        row("c-mergee", { merged_into_candidate_id: "c-survivor" }),
        row("c-survivor", { merged_into_candidate_id: null }),
      ],
      targetIds: new Set(),
      targetParentIds: new Map([["candidates", new Set()]]),
      plannedParentIds: new Map(),
    });
    // Order matters across statement batches: the survivor's INSERT must run
    // before the row that references it.
    expect(plan.insertIds).toEqual(["c-survivor", "c-mergee"]);
    expect(plan.skipped).toEqual([]);
  });

  it("a self-FK parent already on the target satisfies the mergee directly", () => {
    const plan = planTableInserts({
      spec: candidatesSpec,
      sourceRows: [row("c-mergee", { merged_into_candidate_id: "c-survivor" })],
      targetIds: new Set(["c-survivor"]),
      targetParentIds: new Map([["candidates", new Set(["c-survivor"])]]),
      plannedParentIds: new Map(),
    });
    expect(plan.insertIds).toEqual(["c-mergee"]);
  });

  it("skips a self-FK row whose merge target is absent everywhere, and a cycle, instead of looping", () => {
    const plan = planTableInserts({
      spec: candidatesSpec,
      sourceRows: [
        row("c-orphan", { merged_into_candidate_id: "c-nowhere" }),
        // A hand-corrupted mutual merge: the database's own FK ordering could
        // not accept these either.
        row("c-a", { merged_into_candidate_id: "c-b" }),
        row("c-b", { merged_into_candidate_id: "c-a" }),
        row("c-fine", { merged_into_candidate_id: null }),
      ],
      targetIds: new Set(),
      targetParentIds: new Map([["candidates", new Set()]]),
      plannedParentIds: new Map(),
    });
    expect(plan.insertIds).toEqual(["c-fine"]);
    expect(plan.skipped.map((skip) => skip.id).sort()).toEqual(["c-a", "c-b", "c-orphan"]);
  });
});

describe("insertPlannedRows", () => {
  function fakeSource(rowsById: Record<string, Record<string, unknown>>) {
    const fetches: unknown[][] = [];
    return {
      fetches,
      client: {
        query: async (_text: string, values?: readonly unknown[]) => {
          const ids = values![0] as string[];
          fetches.push(ids);
          return {
            rows: ids.filter((id) => id in rowsById).map((id) => ({ row: rowsById[id]! })),
            rowCount: null,
          };
        },
      },
    };
  }

  function fakeTarget(rowCountPerBatch: (batch: unknown[]) => number) {
    const inserts: unknown[][] = [];
    return {
      inserts,
      client: {
        query: async (_text: string, values?: readonly unknown[]) => {
          const batch = JSON.parse(values![0] as string) as unknown[];
          inserts.push(batch);
          return { rows: [], rowCount: rowCountPerBatch(batch) };
        },
      },
    };
  }

  it("fetches full rows for exactly the planned ids and inserts them in plan order", async () => {
    const source = fakeSource({
      a: { id: "a", name: "row a" },
      b: { id: "b", name: "row b" },
    });
    const target = fakeTarget((batch) => batch.length);
    const written = await insertPlannedRows({
      source: source.client,
      target: target.client,
      table: "candidates",
      insertIds: ["b", "a"],
    });
    expect(written).toBe(2);
    // Plan order preserved — the self-FK order must hold across the wire.
    expect(target.inserts).toEqual([[{ id: "b", name: "row b" }, { id: "a", name: "row a" }]]);
  });

  it("throws when a planned row vanished from the source, rather than silently writing fewer", async () => {
    const source = fakeSource({ a: { id: "a" } });
    const target = fakeTarget((batch) => batch.length);
    await expect(
      insertPlannedRows({
        source: source.client,
        target: target.client,
        table: "elections",
        insertIds: ["a", "gone"],
      })
    ).rejects.toThrow(/vanished from the source/);
    expect(target.inserts).toHaveLength(0);
  });

  it("issues no statements at all when the plan is empty", async () => {
    const source = fakeSource({});
    const target = fakeTarget(() => 0);
    expect(
      await insertPlannedRows({
        source: source.client,
        target: target.client,
        table: "ballot_measures",
        insertIds: [],
      })
    ).toBe(0);
    expect(source.fetches).toHaveLength(0);
    expect(target.inserts).toHaveLength(0);
  });
});

describe("findPresentIds", () => {
  it("returns the subset of referenced ids the target actually has, across batches", async () => {
    const queried: unknown[][] = [];
    const client = {
      query: async (_text: string, values?: readonly unknown[]) => {
        const ids = values![0] as string[];
        queried.push(ids);
        return { rows: ids.filter((id) => id.startsWith("present")).map((id) => ({ id })), rowCount: null };
      },
    };
    const ids = Array.from({ length: 501 }, (_, i) => (i === 0 ? "present-0" : `absent-${i}`));
    const present = await findPresentIds(client, "districts", ids);
    expect(present).toEqual(new Set(["present-0"]));
    // 501 ids crosses the 500-row batch boundary, exercising accumulation.
    expect(queried).toHaveLength(2);
  });

  it("issues no query for an empty reference set", async () => {
    let called = false;
    const client = {
      query: async () => {
        called = true;
        return { rows: [], rowCount: null };
      },
    };
    expect(await findPresentIds(client, "offices", [])).toEqual(new Set());
    expect(called).toBe(false);
  });
});
