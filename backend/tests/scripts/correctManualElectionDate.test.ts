import { describe, expect, it } from "vitest";

import {
  appendElectionSource,
  assertIsoDate,
  runElectionDateCorrection,
  type ElectionDateCorrectionClient,
} from "../../src/scripts/correctManualElectionDate.js";

const ELECTION_ID = "8b94eb16-4f9a-4c5c-931a-f1752b05f57e";
const DISTRICT_ID = "00000000-0000-4000-8000-000000000001";
const SOURCE_URL = "https://apps.arizona.vote/electioninfo/Election/68";

type FakeRow = {
  id: string;
  district_id: string;
  official_ballot_title: string;
  official_ballot_title_key: string;
  election_date: string;
  sources: unknown;
};

function electionRow(overrides: Partial<FakeRow> = {}): FakeRow {
  return {
    id: ELECTION_ID,
    district_id: DISTRICT_ID,
    official_ballot_title: "U.S. Representative, Arizona's 9th Congressional District",
    official_ballot_title_key: "us representative arizonas 9th congressional district",
    election_date: "2026-08-04",
    sources: ["https://ballotpedia.org/example"],
    ...overrides,
  };
}

// Answers the lock SELECT with `row` (or nothing), the collision SELECT with
// `collision` (or nothing), the persisted-results SELECT with `resultRows`,
// and records every statement so assertions can pin the exact transaction
// shape.
function fakeClient(input: {
  row?: FakeRow;
  collision?: { id: string };
  resultRows?: boolean;
}): {
  client: ElectionDateCorrectionClient;
  statements: { text: string; values?: unknown[] }[];
} {
  const statements: { text: string; values?: unknown[] }[] = [];
  const client: ElectionDateCorrectionClient = {
    async query<T>(text: string, values?: unknown[]): Promise<{ rows: T[] }> {
      statements.push({ text, values });
      if (text.includes("FOR UPDATE")) {
        return { rows: (input.row ? [input.row] : []) as T[] };
      }
      if (text.includes("id <>")) {
        return { rows: (input.collision ? [input.collision] : []) as T[] };
      }
      if (text.includes("FROM public.election_results")) {
        return { rows: (input.resultRows ? [{ source: "election_results" }] : []) as T[] };
      }
      return { rows: [] as T[] };
    },
  };
  return { client, statements };
}

function options(overrides: Partial<Parameters<typeof runElectionDateCorrection>[1]> = {}) {
  return {
    electionId: ELECTION_ID,
    expectedDate: "2026-08-04",
    correctedDate: "2026-07-21",
    sourceUrl: SOURCE_URL,
    dryRun: false,
    ...overrides,
  };
}

function dateUpdate(statements: { text: string; values?: unknown[] }[]) {
  return statements.find((statement) => statement.text.includes("election_date = $2::date"));
}

describe("runElectionDateCorrection", () => {
  it("updates the date, sources, and result-tracking reset inside one committed transaction, preserving the election id", async () => {
    const { client, statements } = fakeClient({ row: electionRow() });

    const result = await runElectionDateCorrection(client, options());

    expect(result).toEqual({
      alreadyCorrected: false,
      dryRun: false,
      electionId: ELECTION_ID,
      districtId: DISTRICT_ID,
      officialBallotTitle: "U.S. Representative, Arizona's 9th Congressional District",
      expectedDate: "2026-08-04",
      correctedDate: "2026-07-21",
      sources: ["https://ballotpedia.org/example", SOURCE_URL],
    });

    const update = dateUpdate(statements);
    expect(update?.values).toEqual([
      ELECTION_ID,
      "2026-07-21",
      JSON.stringify(["https://ballotpedia.org/example", SOURCE_URL]),
    ]);
    // Result-polling markers set under the wrong date would make the
    // scheduler skip the corrected date; the same UPDATE must clear them.
    for (const column of [
      "election_night_results_checked_at = NULL",
      "election_night_results_attempt_count = 0",
      "election_night_results_last_attempted_at = NULL",
      "certified_results_checked_at = NULL",
      "certified_results_attempt_count = 0",
      "certified_results_last_attempted_at = NULL",
    ]) {
      expect(update?.text).toContain(column);
    }
    expect(statements.map((statement) => statement.text.trim().split(/\s/)[0])).toEqual([
      "BEGIN",
      "SELECT",
      "SELECT",
      "SELECT",
      "UPDATE",
      "COMMIT",
    ]);
  });

  it("rolls back instead of committing on --dry-run", async () => {
    const { client, statements } = fakeClient({ row: electionRow() });

    const result = await runElectionDateCorrection(client, options({ dryRun: true }));

    expect(result).toMatchObject({ alreadyCorrected: false, dryRun: true });
    expect(dateUpdate(statements)).toBeUndefined();
    expect(statements.at(-1)?.text).toBe("ROLLBACK");
  });

  it("already-corrected row with the source present rolls back and reports no append", async () => {
    const { client, statements } = fakeClient({
      row: electionRow({
        election_date: "2026-07-21",
        sources: ["https://ballotpedia.org/example", SOURCE_URL],
      }),
    });

    const result = await runElectionDateCorrection(client, options());

    expect(result).toEqual({
      alreadyCorrected: true,
      electionId: ELECTION_ID,
      correctedDate: "2026-07-21",
      sourceAppended: false,
      dryRun: false,
    });
    expect(statements.some((statement) => statement.text.includes("UPDATE public.elections"))).toBe(false);
    expect(statements.at(-1)?.text).toBe("ROLLBACK");
  });

  it("already-corrected row missing the official source appends it transactionally", async () => {
    const { client, statements } = fakeClient({
      row: electionRow({ election_date: "2026-07-21" }),
    });

    const result = await runElectionDateCorrection(client, options());

    expect(result).toEqual({
      alreadyCorrected: true,
      electionId: ELECTION_ID,
      correctedDate: "2026-07-21",
      sourceAppended: true,
      dryRun: false,
    });
    const update = statements.find((statement) =>
      statement.text.includes("SET sources = $2::jsonb")
    );
    expect(update?.values).toEqual([
      ELECTION_ID,
      JSON.stringify(["https://ballotpedia.org/example", SOURCE_URL]),
    ]);
    expect(update?.text).not.toContain("election_date");
    expect(statements.at(-1)?.text).toBe("COMMIT");
  });

  it("already-corrected dry-run reports the pending source append without writing", async () => {
    const { client, statements } = fakeClient({
      row: electionRow({ election_date: "2026-07-21" }),
    });

    const result = await runElectionDateCorrection(client, options({ dryRun: true }));

    expect(result).toMatchObject({ alreadyCorrected: true, sourceAppended: true, dryRun: true });
    expect(statements.some((statement) => statement.text.includes("UPDATE public.elections"))).toBe(false);
    expect(statements.at(-1)?.text).toBe("ROLLBACK");
  });

  it("refuses when the live date does not match --expected-date", async () => {
    const { client } = fakeClient({ row: electionRow({ election_date: "2026-09-01" }) });

    await expect(runElectionDateCorrection(client, options())).rejects.toThrow(
      /Expected election .* date 2026-08-04, found 2026-09-01; refusing correction/
    );
  });

  it("refuses when the corrected date collides with another election on the identity key", async () => {
    const { client, statements } = fakeClient({
      row: electionRow(),
      collision: { id: "11111111-1111-4111-8111-111111111111" },
    });

    await expect(runElectionDateCorrection(client, options())).rejects.toThrow(
      /would collide with election 11111111-1111-4111-8111-111111111111; merge required/
    );
    expect(dateUpdate(statements)).toBeUndefined();
    expect(statements.at(-1)?.text).toBe("ROLLBACK");
  });

  it("refuses when persisted result rows exist for the election", async () => {
    const { client, statements } = fakeClient({ row: electionRow(), resultRows: true });

    await expect(runElectionDateCorrection(client, options())).rejects.toThrow(
      /persisted election_results rows collected under the stored date/
    );
    expect(dateUpdate(statements)).toBeUndefined();
    expect(statements.at(-1)?.text).toBe("ROLLBACK");
  });

  it("refuses cross-calendar-year corrections before opening a transaction", async () => {
    const { client, statements } = fakeClient({ row: electionRow() });

    await expect(
      runElectionDateCorrection(
        client,
        options({ expectedDate: "2026-12-31", correctedDate: "2027-01-01" })
      )
    ).rejects.toThrow(/Cross-calendar-year correction .* is not supported/);
    expect(statements).toEqual([]);
  });

  it("refuses a non-HTTPS source URL even when called directly", async () => {
    const { client, statements } = fakeClient({ row: electionRow() });

    await expect(
      runElectionDateCorrection(client, options({ sourceUrl: "http://example.org/official" }))
    ).rejects.toThrow(/--source-url must use HTTPS/);
    expect(statements).toEqual([]);
  });

  it("refuses when the election does not exist", async () => {
    const { client } = fakeClient({});

    await expect(runElectionDateCorrection(client, options())).rejects.toThrow(
      `Election not found: ${ELECTION_ID}`
    );
  });
});

describe("appendElectionSource", () => {
  it("appends without duplicating and drops non-string entries", () => {
    expect(appendElectionSource(["https://a.example", 5, "  "], "https://b.example")).toEqual([
      "https://a.example",
      "https://b.example",
    ]);
    expect(appendElectionSource(["https://b.example"], "https://b.example")).toEqual([
      "https://b.example",
    ]);
    expect(appendElectionSource(null, "https://b.example")).toEqual(["https://b.example"]);
  });

  it("trims entries so whitespace variants dedupe instead of surviving as duplicates", () => {
    expect(appendElectionSource([" https://b.example "], "https://b.example")).toEqual([
      "https://b.example",
    ]);
    expect(appendElectionSource(["https://a.example"], " https://b.example ")).toEqual([
      "https://a.example",
      "https://b.example",
    ]);
  });
});

describe("assertIsoDate", () => {
  it("accepts real calendar dates and rejects malformed or impossible ones", () => {
    expect(() => assertIsoDate("--corrected-date", "2026-07-21")).not.toThrow();
    expect(() => assertIsoDate("--corrected-date", "2026-02-30")).toThrow(/valid YYYY-MM-DD/);
    expect(() => assertIsoDate("--corrected-date", "07/21/2026")).toThrow(/valid YYYY-MM-DD/);
  });
});
