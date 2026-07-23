import { describe, expect, it, vi } from "vitest";

import {
  parseOfficeContestFamily,
  runElectionContestFamilyCorrection,
  type ElectionContestFamilyCorrectionClient,
} from "../../src/scripts/correctManualElectionContestFamily.js";

const ELECTION_ID = "10000000-0000-4000-8000-000000000001";
const JUDGE_OFFICE_ID = "20000000-0000-4000-8000-000000000001";
const OTHER_OFFICE_ID = "20000000-0000-4000-8000-000000000002";
const SOURCE_URL = "https://nmcourts.gov/courts-by-county/";

type FakeElectionRow = {
  id: string;
  official_ballot_title: string;
  race_type: string;
  discovery_contest_family: string | null;
  office_id: string | null;
  sources: unknown;
  district_name: string;
  state: string;
  district_type: "county";
};

function electionRow(overrides: Partial<FakeElectionRow> = {}): FakeElectionRow {
  return {
    id: ELECTION_ID,
    official_ballot_title: "Bernalillo County Probate Judge",
    race_type: "office",
    discovery_contest_family: "non_judicial_office",
    office_id: null,
    sources: ["https://candidateportal.servis.sos.state.nm.us/"],
    district_name: "Bernalillo County, New Mexico",
    state: "NM",
    district_type: "county",
    ...overrides,
  };
}

function fakeClient(
  row?: FakeElectionRow,
  input: { aliasOfficeId?: string } = {}
) {
  const statements: { text: string; values?: unknown[] }[] = [];
  const query = vi.fn(async (text: string, values?: unknown[]) => {
    statements.push({ text, values });
    if (text.includes("FOR UPDATE OF e")) return { rows: row ? [row] : [] };
    if (text.includes("FROM public.office_title_aliases")) {
      return {
        rows: input.aliasOfficeId
          ? [
              {
                office_id: input.aliasOfficeId,
                normalized_alias: "bernalillo county probate judge",
              },
            ]
          : [],
      };
    }
    if (text.includes("WHERE id = $1::uuid") && text.includes("FROM public.offices")) {
      const officeId = values?.[0];
      const offices = [
        { id: JUDGE_OFFICE_ID, canonical_name: "County Level Judge" },
        { id: OTHER_OFFICE_ID, canonical_name: "County Recorder" },
      ];
      return { rows: offices.filter((office) => office.id === officeId) };
    }
    if (text.includes("FROM public.offices")) {
      return {
        rows: [
          { id: JUDGE_OFFICE_ID, canonical_name: "County Level Judge" },
          { id: OTHER_OFFICE_ID, canonical_name: "County Recorder" },
        ],
      };
    }
    return { rows: [] };
  });
  return {
    client: { query } as unknown as ElectionContestFamilyCorrectionClient,
    statements,
  };
}

function options(
  overrides: Partial<Parameters<typeof runElectionContestFamilyCorrection>[1]> = {}
) {
  return {
    electionId: ELECTION_ID,
    expectedFamily: "non_judicial_office" as const,
    correctedFamily: "judicial_office" as const,
    sourceUrl: SOURCE_URL,
    dryRun: false,
    ...overrides,
  };
}

function updateStatement(statements: { text: string; values?: unknown[] }[]) {
  return statements.find((statement) =>
    statement.text.includes("SET discovery_contest_family = $2")
  );
}

describe("runElectionContestFamilyCorrection", () => {
  it("corrects the family, backfills the resolved office, appends provenance, and commits", async () => {
    const { client, statements } = fakeClient(electionRow());

    const result = await runElectionContestFamilyCorrection(client, options());

    expect(result).toEqual({
      electionId: ELECTION_ID,
      officialBallotTitle: "Bernalillo County Probate Judge",
      expectedFamily: "non_judicial_office",
      correctedFamily: "judicial_office",
      alreadyCorrected: false,
      officeId: JUDGE_OFFICE_ID,
      officeBackfilled: true,
      sourceAppended: true,
      matchMethod: "deterministic_fallback",
      dryRun: false,
    });
    expect(updateStatement(statements)?.values).toEqual([
      ELECTION_ID,
      "judicial_office",
      JUDGE_OFFICE_ID,
      JSON.stringify([
        "https://candidateportal.servis.sos.state.nm.us/",
        SOURCE_URL,
      ]),
    ]);
    expect(statements.at(-1)?.text).toBe("COMMIT");
  });

  it("rolls back without updating on dry-run", async () => {
    const { client, statements } = fakeClient(electionRow());

    const result = await runElectionContestFamilyCorrection(client, options({ dryRun: true }));

    expect(result).toMatchObject({ dryRun: true, officeBackfilled: true });
    expect(updateStatement(statements)).toBeUndefined();
    expect(statements.at(-1)?.text).toBe("ROLLBACK");
  });

  it("refuses a stale expected family", async () => {
    const { client, statements } = fakeClient(
      electionRow({ discovery_contest_family: "ballot_measure" })
    );

    await expect(runElectionContestFamilyCorrection(client, options())).rejects.toThrow(
      /Expected election .* family non_judicial_office, found ballot_measure/
    );
    expect(updateStatement(statements)).toBeUndefined();
    expect(statements.at(-1)?.text).toBe("ROLLBACK");
  });

  it("allows a past election because exact-ID repair is not future-scoped", async () => {
    const { client, statements } = fakeClient(electionRow());

    await runElectionContestFamilyCorrection(client, options({ dryRun: true }));

    const lock = statements.find((statement) => statement.text.includes("FOR UPDATE OF e"));
    expect(lock?.text).not.toContain("election_date");
  });

  it("converges an already-corrected row by appending a missing source", async () => {
    const { client, statements } = fakeClient(
      electionRow({
        discovery_contest_family: "judicial_office",
        office_id: JUDGE_OFFICE_ID,
      })
    );

    const result = await runElectionContestFamilyCorrection(client, options());

    expect(result).toMatchObject({
      alreadyCorrected: true,
      officeBackfilled: false,
      sourceAppended: true,
    });
    expect(updateStatement(statements)).toBeDefined();
    expect(statements.at(-1)?.text).toBe("COMMIT");
  });

  it("is a no-op when the corrected family, office, and source already converge", async () => {
    const { client, statements } = fakeClient(
      electionRow({
        discovery_contest_family: "judicial_office",
        office_id: JUDGE_OFFICE_ID,
        sources: [SOURCE_URL],
      })
    );

    const result = await runElectionContestFamilyCorrection(client, options());

    expect(result).toMatchObject({ alreadyCorrected: true, sourceAppended: false });
    expect(updateStatement(statements)).toBeUndefined();
    expect(statements.at(-1)?.text).toBe("ROLLBACK");
  });

  it("refuses to overwrite a conflicting non-null office", async () => {
    const { client, statements } = fakeClient(electionRow({ office_id: OTHER_OFFICE_ID }));

    await expect(runElectionContestFamilyCorrection(client, options())).rejects.toThrow(
      /already references office .* but corrected family resolves/
    );
    expect(updateStatement(statements)).toBeUndefined();
    expect(statements.at(-1)?.text).toBe("ROLLBACK");
  });

  it("refuses a non-judge office resolved under the judicial family", async () => {
    const { client, statements } = fakeClient(electionRow(), {
      aliasOfficeId: OTHER_OFFICE_ID,
    });

    await expect(runElectionContestFamilyCorrection(client, options())).rejects.toThrow(
      /judicial_office resolved incompatible office County Recorder/
    );
    expect(updateStatement(statements)).toBeUndefined();
    expect(statements.at(-1)?.text).toBe("ROLLBACK");
  });

  it("validates HTTPS provenance before opening a transaction", async () => {
    const { client, statements } = fakeClient(electionRow());

    await expect(
      runElectionContestFamilyCorrection(
        client,
        options({ sourceUrl: "http://example.org/not-https" })
      )
    ).rejects.toThrow(/must use HTTPS/);
    expect(statements).toEqual([]);
  });
});

describe("parseOfficeContestFamily", () => {
  it("accepts only office contest families", () => {
    expect(parseOfficeContestFamily("--expected-family", "judicial_office")).toBe(
      "judicial_office"
    );
    expect(() => parseOfficeContestFamily("--expected-family", "ballot_measure")).toThrow(
      /must be one of non_judicial_office, judicial_office/
    );
  });
});
