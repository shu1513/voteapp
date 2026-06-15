import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildDataverseGuestbookResponse,
  fetchHistoricalContestCsv,
  parseHistoricalContestMarginImportArgs,
} from "../../src/scripts/importHistoricalContestMarginsCli.js";

describe("importHistoricalContestMarginsCli", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("parses local file imports", () => {
    const args = parseHistoricalContestMarginImportArgs([
      "--file=./tmp/2024-house.csv",
      "--source=MIT_2024",
      "--source-url=https://github.com/MEDSL/2024-elections-official",
      "--dry-run",
      "--stale-after-redistricting",
    ]);

    expect(args).toMatchObject({
      inputKind: "file",
      source: "MIT_2024",
      sourceUrl: "https://github.com/MEDSL/2024-elections-official",
      format: "medsl_aggregate_csv",
      dryRun: true,
      staleAfterRedistricting: true,
    });
    expect(args.input.endsWith("/tmp/2024-house.csv")).toBe(true);
  });

  it("parses URL imports and defaults sourceUrl to the URL", () => {
    expect(
      parseHistoricalContestMarginImportArgs([
        "--url=https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/2024-senate-state.csv",
        "--source=MIT_2024",
      ])
    ).toEqual({
      inputKind: "url",
      input: "https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/2024-senate-state.csv",
      preset: null,
      source: "MIT_2024",
      sourceUrl: "https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/2024-senate-state.csv",
      format: "medsl_aggregate_csv",
      dryRun: false,
      staleAfterRedistricting: false,
    });
  });

  it("parses precinct CSV imports", () => {
    const args = parseHistoricalContestMarginImportArgs([
      "--file=./tmp/ca24.csv",
      "--source=MIT_2024",
      "--format=medsl_precinct_csv",
      "--dry-run",
    ]);

    expect(args).toMatchObject({
      inputKind: "file",
      source: "MIT_2024",
      format: "medsl_precinct_csv",
      dryRun: true,
    });
  });

  it("parses known MEDSL presets without requiring an explicit source", () => {
    expect(parseHistoricalContestMarginImportArgs(["--preset=medsl-2024-president-state", "--dry-run"])).toEqual({
      inputKind: "preset",
      input: "https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/2024-president-state.csv",
      preset: "medsl-2024-president-state",
      source: "MIT_2024",
      sourceUrl: "https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/2024-president-state.csv",
      format: "medsl_aggregate_csv",
      dryRun: true,
      staleAfterRedistricting: false,
    });
  });

  it("parses known MEDSL precinct presets with their catalog format", () => {
    expect(parseHistoricalContestMarginImportArgs(["--preset=medsl-2024-house-precinct", "--dry-run"])).toEqual({
      inputKind: "preset",
      input: "https://dataverse.harvard.edu/api/access/datafile/13731101",
      preset: "medsl-2024-house-precinct",
      source: "MIT_2024",
      sourceUrl: "https://dataverse.harvard.edu/api/access/datafile/13731101",
      format: "medsl_precinct_csv",
      dryRun: true,
      staleAfterRedistricting: false,
    });
  });

  it("rejects multi-file presets for the single-source import script", () => {
    expect(() => parseHistoricalContestMarginImportArgs(["--preset=medsl-2024-state-precinct"])).toThrow(
      "Historical contest import preset medsl-2024-state-precinct has multiple source files; use the verified import script."
    );
    expect(() => parseHistoricalContestMarginImportArgs(["--preset=medsl-2022-precinct"])).toThrow(
      "Historical contest import preset medsl-2022-precinct has multiple source files; use the verified import script."
    );
    expect(() => parseHistoricalContestMarginImportArgs(["--preset=medsl-2020-precinct-by-state"])).toThrow(
      "Historical contest import preset medsl-2020-precinct-by-state has multiple source files; use the verified import script."
    );
  });

  it("allows preset source overrides", () => {
    expect(
      parseHistoricalContestMarginImportArgs([
        "--preset=medsl-2024-senate-state",
        "--source=MEDSL_2024_OFFICIAL",
        "--source-url=https://github.com/MEDSL/2024-elections-official",
      ])
    ).toMatchObject({
      inputKind: "preset",
      preset: "medsl-2024-senate-state",
      source: "MEDSL_2024_OFFICIAL",
      sourceUrl: "https://github.com/MEDSL/2024-elections-official",
    });
  });

  it("requires exactly one input flag", () => {
    expect(() => parseHistoricalContestMarginImportArgs(["--source=MIT_2024"])).toThrow(
      "Provide exactly one input flag"
    );
    expect(() =>
      parseHistoricalContestMarginImportArgs([
        "--file=./local.csv",
        "--url=https://example.test/local.csv",
        "--source=MIT_2024",
      ])
    ).toThrow("Provide exactly one input flag");
    expect(() =>
      parseHistoricalContestMarginImportArgs([
        "--file=./local.csv",
        "--preset=medsl-2024-senate-state",
        "--source=MIT_2024",
      ])
    ).toThrow("Provide exactly one input flag");
  });

  it("rejects non-https URL imports", () => {
    expect(() =>
      parseHistoricalContestMarginImportArgs(["--url=http://example.test/local.csv", "--source=MIT_2024"])
    ).toThrow("Invalid --url URL protocol: http:. Only https is allowed.");

    expect(() =>
      parseHistoricalContestMarginImportArgs(["--url=file:///tmp/local.csv", "--source=MIT_2024"])
    ).toThrow("Invalid --url URL protocol: file:. Only https is allowed.");
  });

  it("rejects unknown import formats", () => {
    expect(() =>
      parseHistoricalContestMarginImportArgs(["--file=./local.csv", "--source=MIT_2024", "--format=zip"])
    ).toThrow("Unknown historical contest import format: zip");
  });

  it("rejects preset format overrides that do not match the catalog", () => {
    expect(() =>
      parseHistoricalContestMarginImportArgs([
        "--preset=medsl-2024-president-state",
        "--format=medsl_precinct_csv",
      ])
    ).toThrow("Preset medsl-2024-president-state uses format medsl_aggregate_csv");
  });

  it("rejects unknown presets", () => {
    expect(() => parseHistoricalContestMarginImportArgs(["--preset=medsl-2024-governor-state"])).toThrow(
      "Known presets: medsl-2024-president-state, medsl-2024-senate-state, medsl-2024-house-precinct, medsl-2022-precinct, medsl-2020-precinct-by-state, medsl-2018-precinct-by-state, medsl-2024-state-precinct"
    );
  });

  it("rejects inherited object property names as presets", () => {
    expect(() => parseHistoricalContestMarginImportArgs(["--preset=constructor"])).toThrow(
      "Unknown historical contest import preset: constructor"
    );
  });

  it("fetches remote CSV text", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        text: vi.fn().mockResolvedValue("year,state_po\n2024,CA\n"),
      })
    );

    await expect(fetchHistoricalContestCsv("https://example.test/contest.csv")).resolves.toBe(
      "year,state_po\n2024,CA\n"
    );
    expect(fetch).toHaveBeenCalledWith(
      "https://example.test/contest.csv",
      expect.objectContaining({
        headers: expect.any(Headers),
        signal: expect.any(AbortSignal),
      })
    );
  });

  it("builds Dataverse guestbook responses from explicit values", () => {
    expect(
      buildDataverseGuestbookResponse({
        name: "Importer",
        email: "importer@example.test",
        institution: "VoteApp Test",
        position: "Test runner",
      })
    ).toEqual({
      name: "Importer",
      email: "importer@example.test",
      institution: "VoteApp Test",
      position: "Test runner",
      answers: [],
    });
  });

  it("fetches guestbook-gated Dataverse CSV text through a signed URL", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        ok: true,
        text: vi.fn().mockResolvedValue(
          JSON.stringify({
            status: "OK",
            data: {
              signedUrl: "https://dataverse.harvard.edu/api/v1/access/datafile/123?token=signed",
            },
          })
        ),
      })
      .mockResolvedValueOnce({
        ok: true,
        text: vi.fn().mockResolvedValue("year,state_po\n2020,CA\n"),
      });
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      fetchHistoricalContestCsv("https://dataverse.harvard.edu/api/access/datafile/123", {
        downloadMode: "dataverse_guestbook",
        dataverseGuestbookResponse: {
          name: "Importer",
          email: "importer@example.test",
          institution: "VoteApp Test",
          position: "Test runner",
        },
      })
    ).resolves.toBe("year,state_po\n2020,CA\n");

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "https://dataverse.harvard.edu/api/access/datafile/123?signed=true",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          name: "Importer",
          email: "importer@example.test",
          institution: "VoteApp Test",
          position: "Test runner",
          answers: [],
        }),
        headers: expect.any(Headers),
      })
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "https://dataverse.harvard.edu/api/v1/access/datafile/123?token=signed",
      expect.objectContaining({
        headers: expect.any(Headers),
      })
    );
  });

  it("throws a clear error when remote CSV fetches time out", async () => {
    const abortError = new Error("aborted");
    abortError.name = "AbortError";
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(abortError));

    await expect(fetchHistoricalContestCsv("https://example.test/slow.csv")).rejects.toThrow(
      "Failed to fetch historical contest CSV: request timed out after 30000ms for https://example.test/slow.csv"
    );
  });

  it("throws on failed remote CSV fetch", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 404,
        statusText: "Not Found",
      })
    );

    await expect(fetchHistoricalContestCsv("https://example.test/missing.csv")).rejects.toThrow(
      "Failed to fetch historical contest CSV: 404 Not Found"
    );
  });
});
