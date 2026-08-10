import { describe, expect, it } from "vitest";

import {
  BASE_TABLES,
  baseInsertSql,
  CONSOLE_SKIPPED_ROWS_LIMIT,
  consoleReportView,
  findPresentIds,
  idProjectionSql,
  insertPlannedRows,
  planTableInserts,
  rowProjectionSql,
  targetIdProjectionSql,
  toBaseIdRow,
  type BaseIdRow,
  type BasePromotionReport,
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

  it("candidates carry the usability predicate — a retired candidate must not adopt new children", () => {
    // promoteResearchData.findUnresolvableCandidates enforces the same rule
    // (absent OR soft-deleted OR merged away) for record parents; dropping
    // either clause here would silently attach candidate_elections to an
    // identity the target retired.
    const candidates = BASE_TABLES.find((spec) => spec.table === "candidates")!;
    expect(candidates.targetUnusableWhenSql).toContain("deleted_at IS NOT NULL");
    expect(candidates.targetUnusableWhenSql).toContain("merged_into_candidate_id IS NOT NULL");
    for (const spec of BASE_TABLES) {
      if (spec.table !== "candidates") {
        expect(spec.targetUnusableWhenSql, spec.table).toBeUndefined();
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

  it("the target id projection carries the usability flag only where a predicate exists", () => {
    const candidates = BASE_TABLES.find((spec) => spec.table === "candidates")!;
    const elections = BASE_TABLES.find((spec) => spec.table === "elections")!;
    expect(targetIdProjectionSql(candidates)).toContain(
      "(deleted_at IS NOT NULL OR merged_into_candidate_id IS NOT NULL) AS unusable"
    );
    expect(targetIdProjectionSql(elections)).toBe("SELECT id::text AS id FROM public.elections");
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
    expect(plan.skipped).toEqual([
      { id: "e1", column: "district_id", parentId: "d-missing", reason: "absent_on_target" },
    ]);
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
    expect(plan.skipped).toEqual([
      { id: "ce2", column: "candidate_id", parentId: "c-absent", reason: "absent_on_target" },
    ]);
  });

  it("a soft-deleted or merged-away target parent does not adopt children, and the skip says why", () => {
    // The parent's id EXISTS on the target — a bare id-presence check would
    // wave this through and file new election links under a retired identity.
    // The caller keeps retired ids out of targetParentIds and hands them over
    // as unusableParentIds so the skip reason distinguishes the repair:
    // divergence to resolve by hand, not a parent to promote first.
    const plan = planTableInserts({
      spec: candidateElectionsSpec,
      sourceRows: [
        row("ce1", { candidate_id: "c-retired", election_id: "e-on-target", running_mate_candidate_id: null }),
      ],
      targetIds: new Set(),
      targetParentIds: new Map([
        ["candidates", new Set()],
        ["elections", new Set(["e-on-target"])],
      ]),
      plannedParentIds: new Map([["candidates", new Set()], ["elections", new Set()]]),
      unusableParentIds: new Map([["candidates", new Set(["c-retired"])]]),
    });
    expect(plan.insertIds).toEqual([]);
    expect(plan.skipped).toEqual([
      { id: "ce1", column: "candidate_id", parentId: "c-retired", reason: "deleted_or_merged_on_target" },
    ]);
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

describe("consoleReportView", () => {
  const skippedRow = (index: number) => ({
    id: `row-${index}`,
    column: "district_id",
    parentId: `d-${index}`,
    reason: "absent_on_target" as const,
  });

  const reportWith = (skipCount: number): BasePromotionReport => ({
    mode: "dry_run",
    source: "src",
    target: "tgt",
    tables: {
      elections: {
        inserts: 0,
        alreadyOnTarget: 0,
        targetOnly: 0,
        skipped: skipCount,
        skippedRows: Array.from({ length: skipCount }, (_, i) => skippedRow(i)),
      },
    },
  });

  it("truncates long skip lists and says exactly how many rows it withheld", () => {
    const report = reportWith(CONSOLE_SKIPPED_ROWS_LIMIT + 5);
    const view = consoleReportView(report);
    expect(view.tables.elections!.skippedRows).toHaveLength(CONSOLE_SKIPPED_ROWS_LIMIT);
    expect(view.tables.elections!.skippedRowsOmitted).toBe(5);
    // The count survives truncation — the operator always sees the true total.
    expect(view.tables.elections!.skipped).toBe(CONSOLE_SKIPPED_ROWS_LIMIT + 5);
  });

  it("never mutates the report — the full version is what lands in --report-file", () => {
    const report = reportWith(CONSOLE_SKIPPED_ROWS_LIMIT + 5);
    consoleReportView(report);
    expect(report.tables.elections!.skippedRows).toHaveLength(CONSOLE_SKIPPED_ROWS_LIMIT + 5);
    expect(report.tables.elections!.skippedRowsOmitted).toBeUndefined();
  });

  it("leaves short skip lists untouched, with no omitted marker", () => {
    const view = consoleReportView(reportWith(2));
    expect(view.tables.elections!.skippedRows).toHaveLength(2);
    expect(view.tables.elections!.skippedRowsOmitted).toBeUndefined();
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

  it("reports the target's actual row counts, so a conflict shortfall reaches the caller", async () => {
    const source = fakeSource({ a: { id: "a" }, b: { id: "b" } });
    // The target already gained "b" after planning: ON CONFLICT (id) DO
    // NOTHING absorbs it, and the shortfall is what main() compares against
    // the plan length to refuse the commit.
    const target = fakeTarget((batch) => batch.length - 1);
    const written = await insertPlannedRows({
      source: source.client,
      target: target.client,
      table: "candidates",
      insertIds: ["a", "b"],
    });
    expect(written).toBe(1);
  });

  it("preserves plan order across the 500-row batch boundary", async () => {
    // The self-FK ordering guarantee (TablePlan.insertIds) must survive the
    // split into statements, not just hold within one batch.
    const ids = Array.from({ length: 501 }, (_, i) => `c-${500 - i}`);
    const source = fakeSource(Object.fromEntries(ids.map((id) => [id, { id }])));
    const target = fakeTarget((batch) => batch.length);
    expect(
      await insertPlannedRows({
        source: source.client,
        target: target.client,
        table: "candidates",
        insertIds: ids,
      })
    ).toBe(501);
    expect(target.inserts).toHaveLength(2);
    expect(target.inserts.flat().map((row) => (row as { id: string }).id)).toEqual(ids);
  });

  it("refuses a table outside the promoted catalog rather than interpolating it into SQL", async () => {
    const source = fakeSource({});
    const target = fakeTarget(() => 0);
    await expect(
      insertPlannedRows({
        source: source.client,
        target: target.client,
        table: "users",
        insertIds: ["a"],
      })
    ).rejects.toThrow(/unknown table "users"/);
    expect(source.fetches).toHaveLength(0);
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

  it("refuses tables outside the external-parent catalog — even promoted ones", async () => {
    // Precision matters: findPresentIds exists for external parents only;
    // promoted parents resolve through the id sets captured during planning.
    const client = {
      query: async () => ({ rows: [], rowCount: null }),
    };
    await expect(findPresentIds(client, "candidates", ["x"])).rejects.toThrow(/unknown table/);
    await expect(findPresentIds(client, "users; DROP TABLE users", ["x"])).rejects.toThrow(
      /unknown table/
    );
  });
});
