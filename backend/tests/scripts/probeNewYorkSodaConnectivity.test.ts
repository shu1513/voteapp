import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildNewYorkSodaUrl,
  parseProbeNewYorkSodaConnectivityArgs,
  runProbeNewYorkSodaConnectivity,
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

function happyPathFetch(): typeof fetch {
  return vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes(NEW_YORK_SODA_FILERS_DATASET)) {
      return jsonResponse([FILER_ROW], { headers: { etag: '"filer-etag"' } });
    }
    if (url.includes("receipt_count")) {
      return jsonResponse([{ receipt_count: "28" }]);
    }
    if (url.includes("trans_number")) {
      return jsonResponse([PARENT_F_ROW]);
    }
    return jsonResponse([SCHEDULE_R_ROW], { headers: { "last-modified": "Sat, 11 Jul 2026 12:00:00 GMT" } });
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

  it("rejects malformed flags", () => {
    expect(() => parseProbeNewYorkSodaConnectivityArgs(["--year=0"])).toThrow("Invalid --year value: 0");
    expect(() => parseProbeNewYorkSodaConnectivityArgs(["--known-ie-filer-id=abc"])).toThrow(
      "Invalid --known-ie-filer-id value: abc"
    );
    expect(() => buildNewYorkSodaUrl("not-a-dataset-id", {})).toThrow("Invalid New York SODA dataset ID");
  });

  it("retries 429 responses before succeeding and records attempts", async () => {
    let filerCalls = 0;
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes(NEW_YORK_SODA_FILERS_DATASET)) {
        filerCalls += 1;
        if (filerCalls === 1) {
          return jsonResponse({ message: "slow down" }, { status: 429 });
        }
        return jsonResponse([FILER_ROW]);
      }
      if (url.includes("receipt_count")) {
        return jsonResponse([{ receipt_count: "28" }]);
      }
      if (url.includes("trans_number")) {
        return jsonResponse([PARENT_F_ROW]);
      }
      return jsonResponse([SCHEDULE_R_ROW]);
    }) as unknown as typeof fetch;
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

  it("fails a check after exhausting retries without failing the whole run structure", async () => {
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes(NEW_YORK_SODA_FILERS_DATASET)) {
        return jsonResponse({ message: "unavailable" }, { status: 503 });
      }
      if (url.includes("receipt_count")) {
        return jsonResponse([{ receipt_count: "28" }]);
      }
      if (url.includes("trans_number")) {
        return jsonResponse([PARENT_F_ROW]);
      }
      return jsonResponse([SCHEDULE_R_ROW]);
    }) as unknown as typeof fetch;

    const output = await runProbeNewYorkSodaConnectivity({
      args: parseProbeNewYorkSodaConnectivityArgs(["--max-attempts=2"]),
      fetchImpl,
      sleep: async () => {},
    });

    expect(output.ok).toBe(false);
    const filerCheck = output.checks.find((check) => check.name === "filer_lookup");
    expect(filerCheck?.ok).toBe(false);
    expect(filerCheck?.detail).toContain("failed after 2 attempts (HTTP 503)");
    expect(output.checks.filter((check) => check.ok)).toHaveLength(3);
  });

  it("pages Schedule R rows with a stable order and flags duplicate ids across pages", async () => {
    const pageRows = [
      [
        { ...SCHEDULE_R_ROW, filing_trans_id: "dup" },
        { ...SCHEDULE_R_ROW, filing_trans_id: "unique-1" },
      ],
      [
        { ...SCHEDULE_R_ROW, filing_trans_id: "dup" },
      ],
    ];
    let disclosurePage = 0;
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes(NEW_YORK_SODA_FILERS_DATASET)) {
        return jsonResponse([FILER_ROW]);
      }
      if (url.includes("receipt_count")) {
        return jsonResponse([{ receipt_count: "28" }]);
      }
      if (url.includes("trans_number")) {
        return jsonResponse([PARENT_F_ROW]);
      }
      const rows = pageRows[disclosurePage] ?? [];
      disclosurePage += 1;
      return jsonResponse(rows);
    }) as unknown as typeof fetch;

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

  it("fails the parent mapping check when the mapped transaction is not exactly one Schedule F row", async () => {
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes(NEW_YORK_SODA_FILERS_DATASET)) {
        return jsonResponse([FILER_ROW]);
      }
      if (url.includes("receipt_count")) {
        return jsonResponse([{ receipt_count: "28" }]);
      }
      if (url.includes("trans_number")) {
        return jsonResponse([PARENT_F_ROW, { ...PARENT_F_ROW, filing_sched_abbrev: "R" }]);
      }
      return jsonResponse([SCHEDULE_R_ROW]);
    }) as unknown as typeof fetch;

    const output = await runProbeNewYorkSodaConnectivity({
      args: parseProbeNewYorkSodaConnectivityArgs([]),
      fetchImpl,
    });

    expect(output.ok).toBe(false);
    const mappingCheck = output.checks.find((check) => check.name === "parent_expenditure_mapping");
    expect(mappingCheck?.ok).toBe(false);
    expect(mappingCheck?.detail).toContain("expected exactly one Schedule F row");
  });
});
