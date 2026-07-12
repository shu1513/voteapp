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
// `collision` (or nothing), and records every statement so assertions can pin
// the exact transaction shape.
function fakeClient(input: { row?: FakeRow; collision?: { id: string } }): {
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

describe("runElectionDateCorrection", () => {
  it("updates only the date and sources inside one committed transaction, preserving the election id", async () => {
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

    const update = statements.find((statement) => statement.text.includes("UPDATE public.elections"));
    expect(update?.values).toEqual([
      ELECTION_ID,
      "2026-07-21",
      JSON.stringify(["https://ballotpedia.org/example", SOURCE_URL]),
    ]);
    expect(statements.map((statement) => statement.text.trim().split(/\s/)[0])).toEqual([
      "BEGIN",
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
    expect(statements.some((statement) => statement.text.includes("UPDATE public.elections"))).toBe(false);
    expect(statements.at(-1)?.text).toBe("ROLLBACK");
  });

  it("is idempotent: a row already at the corrected date rolls back and reports alreadyCorrected", async () => {
    const { client, statements } = fakeClient({
      row: electionRow({ election_date: "2026-07-21" }),
    });

    const result = await runElectionDateCorrection(client, options());

    expect(result).toEqual({
      alreadyCorrected: true,
      electionId: ELECTION_ID,
      correctedDate: "2026-07-21",
    });
    expect(statements.some((statement) => statement.text.includes("UPDATE public.elections"))).toBe(false);
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
    expect(statements.some((statement) => statement.text.includes("UPDATE public.elections"))).toBe(false);
    expect(statements.at(-1)?.text).toBe("ROLLBACK");
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
});

describe("assertIsoDate", () => {
  it("accepts real calendar dates and rejects malformed or impossible ones", () => {
    expect(() => assertIsoDate("--corrected-date", "2026-07-21")).not.toThrow();
    expect(() => assertIsoDate("--corrected-date", "2026-02-30")).toThrow(/valid YYYY-MM-DD/);
    expect(() => assertIsoDate("--corrected-date", "07/21/2026")).toThrow(/valid YYYY-MM-DD/);
  });
});
