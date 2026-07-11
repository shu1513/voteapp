import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildNewYorkSodaUrl,
  parseProbeNewYorkSodaConnectivityArgs,
  runProbeNewYorkSodaConnectivity,
  soqlStringLiteral,
  NEW_YORK_SODA_DISCLOSURES_DATASET,
  NEW_YORK_SODA_FILERS_DATASET,
} from "../../src/scripts/probeNewYorkSodaConnectivity.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

function jsonResponse(payload: unknown, init?: { status?: number; headers?: Record<string, string> }): Response {
  return new Response(JSON.stringify(payload), {
    status: init?.status ?? 200,
    headers: { "content-type": "application/json", ...init?.headers },
  });
}

const FILER_ROW = {
  filer_id: "590891",
  filer_name: "Citizens for Affordable Rates PAC",
  committee_type_desc: "Independent Expenditure Committee",
};

const SCHEDULE_R_ROW = {
  filer_id: "590891",
  filing_trans_id: "trans-1",
  filing_sched_abbrev: "R",
  r_support_oppose: "S",
  trans_mapping: "mapping-1",
  election_year_r: "2026",
};

const PARENT_F_ROW = {
  filer_id: "590891",
  trans_number: "mapping-1",
  filing_sched_abbrev: "F",
};

const FUNDER_ROW = {
  flng_ent_name: "Uber Technologies Inc.",
  cntrbr_type_desc: "",
  org_amt: "41200",
  election_year: "2026",
};

// URL discriminators survive URLSearchParams encoding: the filer dataset id,
// the funders $select field list, the parent lookup's trans_number filter,
// and the filer-scoped Schedule R trans_mapping filter.
function routeUrl(url: string): "filers" | "funders" | "parent" | "filer_schedule_r" | "global_schedule_r" {
  if (url.includes(NEW_YORK_SODA_FILERS_DATASET)) {
    return "filers";
  }
  if (url.includes("flng_ent_name")) {
    return "funders";
  }
  if (url.includes("trans_number")) {
    return "parent";
  }
  if (url.includes("trans_mapping")) {
    return "filer_schedule_r";
  }
  return "global_schedule_r";
}

function happyPathFetch(overrides?: Partial<Record<ReturnType<typeof routeUrl>, () => Response>>): typeof fetch {
  return vi.fn(async (input: RequestInfo | URL) => {
    const route = routeUrl(String(input));
    const override = overrides?.[route];
    if (override) {
      return override();
    }
    switch (route) {
      case "filers":
        return jsonResponse([FILER_ROW], { headers: { etag: '"filer-etag"' } });
      case "funders":
        return jsonResponse([FUNDER_ROW]);
      case "parent":
        return jsonResponse([PARENT_F_ROW]);
      case "filer_schedule_r":
        return jsonResponse([SCHEDULE_R_ROW]);
      case "global_schedule_r":
        return jsonResponse([SCHEDULE_R_ROW], { headers: { "last-modified": "Sat, 11 Jul 2026 12:00:00 GMT" } });
    }
  }) as unknown as typeof fetch;
}

describe("probeNewYorkSodaConnectivity script", () => {
  it("parses args with defaults and keeps the app token out of probe output", async () => {
    vi.stubEnv("NEW_YORK_SODA_APP_TOKEN", "env-token");

    const args = parseProbeNewYorkSodaConnectivityArgs(["--year=2026", "--page-limit", "50"]);
    expect(args).toEqual({
      electionYear: 2026,
      pageLimit: 50,
      maxPages: 3,
      timeoutMs: 30_000,
      maxAttempts: 3,
      knownIeFilerId: "590891",
      appToken: "env-token",
    });

    const fetchImpl = happyPathFetch();
    const output = await runProbeNewYorkSodaConnectivity({
      args,
      fetchImpl,
      now: new Date("2026-07-11T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "new_york_soda_connectivity_probe",
      ts: "2026-07-11T12:00:00.000Z",
      ok: true,
      args: { electionYear: 2026, appTokenProvided: true },
    });
    expect(output.args).not.toHaveProperty("appToken");
    expect(output.checks.map((check) => check.name)).toEqual([
      "filer_lookup",
      "schedule_r_paging",
      "parent_expenditure_mapping",
      "ie_group_funders",
    ]);
    expect(output.checks.every((check) => check.ok)).toBe(true);

    const firstCall = (fetchImpl as unknown as ReturnType<typeof vi.fn>).mock.calls[0];
    const headers = firstCall[1]?.headers as Headers;
    expect(headers.get("X-App-Token")).toBe("env-token");
  });

  it("treats a blank app token env as absent", () => {
    vi.stubEnv("NEW_YORK_SODA_APP_TOKEN", "   ");
    expect(parseProbeNewYorkSodaConnectivityArgs([]).appToken).toBeNull();
  });

  it("rejects malformed, unknown, and out-of-range flags", () => {
    expect(() => parseProbeNewYorkSodaConnectivityArgs(["--year=0"])).toThrow("Invalid --year value: 0");
    expect(() => parseProbeNewYorkSodaConnectivityArgs(["--yeer=2024"])).toThrow("Unknown flag: --yeer");
    expect(() => parseProbeNewYorkSodaConnectivityArgs(["2024"])).toThrow("Unexpected positional argument: 2024");
    expect(() => parseProbeNewYorkSodaConnectivityArgs(["--year=2200"])).toThrow(
      "Out-of-range --year value: 2200 (expected 2000-2100)"
    );
    expect(() => parseProbeNewYorkSodaConnectivityArgs(["--max-attempts=99999999999999999999"])).toThrow(
      "Out-of-range --max-attempts value"
    );
    expect(() => parseProbeNewYorkSodaConnectivityArgs(["--known-ie-filer-id=abc"])).toThrow(
      "Invalid --known-ie-filer-id value: abc"
    );
    expect(() => buildNewYorkSodaUrl("not-a-dataset-id", {})).toThrow("Invalid New York SODA dataset ID");
  });

  it("escapes single quotes when building SoQL string literals", () => {
    expect(soqlStringLiteral("O'Brien")).toBe("'O''Brien'");
    expect(soqlStringLiteral("590891")).toBe("'590891'");
  });

  it("retries 429 responses before succeeding and records attempts", async () => {
    let filerCalls = 0;
    const fetchImpl = happyPathFetch({
      filers: () => {
        filerCalls += 1;
        if (filerCalls === 1) {
          return jsonResponse({ message: "slow down" }, { status: 429 });
        }
        return jsonResponse([FILER_ROW]);
      },
    });
    const sleep = vi.fn(async () => {});

    const output = await runProbeNewYorkSodaConnectivity({
      args: parseProbeNewYorkSodaConnectivityArgs([]),
      fetchImpl,
      sleep,
    });

    expect(output.ok).toBe(true);
    const filerCheck = output.checks.find((check) => check.name === "filer_lookup");
    expect(filerCheck).toMatchObject({ ok: true, attempts: 2, status: 200 });
    expect(sleep).toHaveBeenCalledWith(500);
  });

  it("keeps status and attempt telemetry on checks that exhaust their retries", async () => {
    const fetchImpl = happyPathFetch({
      filers: () => jsonResponse({ message: "unavailable" }, { status: 503 }),
    });

    const output = await runProbeNewYorkSodaConnectivity({
      args: parseProbeNewYorkSodaConnectivityArgs(["--max-attempts=2"]),
      fetchImpl,
      sleep: async () => {},
    });

    expect(output.ok).toBe(false);
    const filerCheck = output.checks.find((check) => check.name === "filer_lookup");
    expect(filerCheck).toMatchObject({ ok: false, status: 503, attempts: 2 });
    expect(filerCheck?.detail).toContain("failed after 2 attempts (HTTP 503)");
    expect(output.checks.filter((check) => check.ok)).toHaveLength(3);
  });

  it("pages Schedule R rows with a stable order and flags duplicate ids across pages", async () => {
    const pageRows = [
      [
        { ...SCHEDULE_R_ROW, filing_trans_id: "dup" },
        { ...SCHEDULE_R_ROW, filing_trans_id: "unique-1" },
      ],
      [{ ...SCHEDULE_R_ROW, filing_trans_id: "dup" }],
    ];
    let disclosurePage = 0;
    const fetchImpl = happyPathFetch({
      global_schedule_r: () => {
        const rows = pageRows[disclosurePage] ?? [];
        disclosurePage += 1;
        return jsonResponse(rows);
      },
    });

    const output = await runProbeNewYorkSodaConnectivity({
      args: parseProbeNewYorkSodaConnectivityArgs(["--page-limit=2", "--max-pages=2"]),
      fetchImpl,
    });

    const pagingCheck = output.checks.find((check) => check.name === "schedule_r_paging");
    expect(pagingCheck).toMatchObject({ ok: false, row_count: 3 });
    expect(pagingCheck?.detail).toContain("duplicate filing_trans_id dup");
    expect(pagingCheck?.url).toContain(NEW_YORK_SODA_DISCLOSURES_DATASET);
    expect(pagingCheck?.url).toContain("%24order=filing_trans_id");
  });

  it("proves the parent expenditure chain for the known IE filer, not an arbitrary filer", async () => {
    const fetchImpl = happyPathFetch({
      // Global pages only contain some other filer; the mapping check must not use it.
      global_schedule_r: () => jsonResponse([{ ...SCHEDULE_R_ROW, filer_id: "111111", trans_mapping: "other" }]),
    });

    const output = await runProbeNewYorkSodaConnectivity({
      args: parseProbeNewYorkSodaConnectivityArgs([]),
      fetchImpl,
    });

    expect(output.ok).toBe(true);
    const mappingCheck = output.checks.find((check) => check.name === "parent_expenditure_mapping");
    expect(mappingCheck?.ok).toBe(true);
    expect(mappingCheck?.detail).toContain(
      "1 of 1 sampled trans_mapping value(s) resolved to exactly one same-filer Schedule F expenditure for filer 590891"
    );
    expect(mappingCheck?.url).toContain("filer_id%3D%27590891%27");

    const fetchMock = fetchImpl as unknown as ReturnType<typeof vi.fn>;
    const filerScheduleRCall = fetchMock.mock.calls.find(
      (call) => routeUrl(String(call[0])) === "filer_schedule_r"
    );
    expect(String(filerScheduleRCall?.[0])).toContain("filer_id%3D%27590891%27");
  });

  it("fails the mapping check when the known IE filer has no Schedule R rows with trans_mapping", async () => {
    const fetchImpl = happyPathFetch({
      filer_schedule_r: () => jsonResponse([]),
    });

    const output = await runProbeNewYorkSodaConnectivity({
      args: parseProbeNewYorkSodaConnectivityArgs([]),
      fetchImpl,
    });

    expect(output.ok).toBe(false);
    const mappingCheck = output.checks.find((check) => check.name === "parent_expenditure_mapping");
    expect(mappingCheck).toMatchObject({ ok: false, row_count: 0 });
    expect(mappingCheck?.detail).toContain("no Schedule R row with trans_mapping");
  });

  it("fails the mapping check when no sampled mapping resolves to exactly one Schedule F row", async () => {
    const fetchImpl = happyPathFetch({
      parent: () => jsonResponse([PARENT_F_ROW, { ...PARENT_F_ROW, filing_sched_abbrev: "R" }]),
    });

    const output = await runProbeNewYorkSodaConnectivity({
      args: parseProbeNewYorkSodaConnectivityArgs([]),
      fetchImpl,
    });

    expect(output.ok).toBe(false);
    const mappingCheck = output.checks.find((check) => check.name === "parent_expenditure_mapping");
    expect(mappingCheck).toMatchObject({ ok: false, row_count: 0 });
    expect(mappingCheck?.detail).toContain("0 of 1 sampled trans_mapping value(s) resolved");
    expect(mappingCheck?.detail).toContain("unresolved: mapping-1 -> 2 row(s)");
  });

  it("passes the mapping check when at least one sampled mapping resolves, reporting the ratio", async () => {
    let parentCalls = 0;
    const fetchImpl = happyPathFetch({
      filer_schedule_r: () =>
        jsonResponse([
          { ...SCHEDULE_R_ROW, trans_mapping: "stale-mapping" },
          { ...SCHEDULE_R_ROW, filing_trans_id: "trans-2", trans_mapping: "mapping-1" },
        ]),
      parent: () => {
        parentCalls += 1;
        // First sampled mapping points at an amended/absent expenditure.
        return parentCalls === 1 ? jsonResponse([]) : jsonResponse([PARENT_F_ROW]);
      },
    });

    const output = await runProbeNewYorkSodaConnectivity({
      args: parseProbeNewYorkSodaConnectivityArgs([]),
      fetchImpl,
    });

    const mappingCheck = output.checks.find((check) => check.name === "parent_expenditure_mapping");
    expect(mappingCheck).toMatchObject({ ok: true, row_count: 1 });
    expect(mappingCheck?.detail).toContain("1 of 2 sampled trans_mapping value(s) resolved");
    expect(mappingCheck?.detail).toContain("unresolved: stale-mapping -> 0 row(s)");
  });

  it("scopes the funders check to the probed election year and requires backtrace fields", async () => {
    const fetchImpl = happyPathFetch({
      funders: () => jsonResponse([]),
    });

    const output = await runProbeNewYorkSodaConnectivity({
      args: parseProbeNewYorkSodaConnectivityArgs(["--year=2024"]),
      fetchImpl,
    });

    expect(output.ok).toBe(false);
    const fundersCheck = output.checks.find((check) => check.name === "ie_group_funders");
    expect(fundersCheck).toMatchObject({ ok: false, row_count: 0 });
    expect(fundersCheck?.detail).toContain("no itemized schedule A-D receipts for election year 2024");
    expect(fundersCheck?.url).toContain("election_year%3D%272024%27");

    const missingFieldsFetch = happyPathFetch({
      funders: () => jsonResponse([{ ...FUNDER_ROW, flng_ent_name: "" }]),
    });
    return runProbeNewYorkSodaConnectivity({
      args: parseProbeNewYorkSodaConnectivityArgs([]),
      fetchImpl: missingFieldsFetch,
    }).then((secondOutput) => {
      const secondCheck = secondOutput.checks.find((check) => check.name === "ie_group_funders");
      expect(secondCheck?.ok).toBe(false);
      expect(secondCheck?.detail).toContain("missing them");
    });
  });
});
