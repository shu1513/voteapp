import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  parseBlockerKey,
  parseFlags,
  runCommand,
  type DeferralClient,
} from "../../src/scripts/manualResearchDeferrals.js";

const DISTRICT_ID = "00000000-0000-4000-8000-000000000001";
const ELECTION_ID = "00000000-0000-4000-8000-000000000002";
const EXISTING_ID = "00000000-0000-4000-8000-0000000000ff";
const NEW_ID = "00000000-0000-4000-8000-0000000000aa";

type Statement = { text: string; values?: unknown[] };

// Answers the district lookup, the election lookup, and the open-row probe
// (with `existing` or nothing), and records every statement so assertions can
// pin which write path ran.
function fakeClient(input: { existing?: { reason: string; blocked_until: string } } = {}): {
  client: DeferralClient;
  statements: Statement[];
} {
  const statements: Statement[] = [];
  const client: DeferralClient = {
    async query<T>(text: string, values?: unknown[]): Promise<{ rows: T[] }> {
      statements.push({ text, values });
      if (text.includes("FROM public.districts")) {
        return { rows: [{ name: "Testville, Ohio" }] as T[] };
      }
      if (text.includes("FROM public.elections")) {
        return { rows: [{ id: ELECTION_ID }] as T[] };
      }
      if (text.includes("SELECT id, reason, blocked_until")) {
        return {
          rows: (input.existing
            ? [{ id: EXISTING_ID, ...input.existing }]
            : []) as T[],
        };
      }
      if (text.includes("UPDATE public.manual_research_deferrals")) {
        return { rows: [{ id: EXISTING_ID }] as T[] };
      }
      if (text.includes("INSERT INTO public.manual_research_deferrals")) {
        return { rows: [{ id: NEW_ID }] as T[] };
      }
      return { rows: [] as T[] };
    },
  };
  return { client, statements };
}

function recordFlags(extra: string[] = []): Map<string, string> {
  return parseFlags([
    "--district-id",
    DISTRICT_ID,
    "--stage",
    "elections",
    "--reason",
    "ballot questions not yet certified",
    "--blocked-until",
    "2026-09-19",
    ...extra,
  ]);
}

describe("parseBlockerKey", () => {
  it("accepts a lowercase slug", () => {
    expect(parseBlockerKey("ballot_measure_family")).toBe("ballot_measure_family");
    expect(parseBlockerKey("office-matcher")).toBe("office-matcher");
    expect(parseBlockerKey(null)).toBeNull();
  });

  // Free text as the discriminator would mint a new row on every retry
  // instead of updating the one already recorded.
  it("rejects free text, capitals, and over-long values", () => {
    expect(() => parseBlockerKey("waiting on the clerk")).toThrow(/short lowercase slug/);
    expect(() => parseBlockerKey("BallotMeasure")).toThrow(/short lowercase slug/);
    expect(() => parseBlockerKey("_leading")).toThrow(/short lowercase slug/);
    expect(() => parseBlockerKey("a".repeat(41))).toThrow(/short lowercase slug/);
  });
});

describe("manual:deferral record", () => {
  beforeEach(() => {
    vi.spyOn(console, "log").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("inserts when no open row exists, carrying the blocker key", async () => {
    const { client, statements } = fakeClient();
    await runCommand(client, "record", recordFlags(["--blocker-key", "ballot_measure_family"]));

    const insert = statements.find((s) => s.text.includes("INSERT INTO"));
    expect(insert).toBeDefined();
    expect(insert?.values).toContain("ballot_measure_family");
  });

  // The regression this whole change exists for: a second district-wide
  // blocker on the same stage used to silently replace the first.
  it("refuses to overwrite an existing open row and names the escape hatches", async () => {
    const { client, statements } = fakeClient({
      existing: { reason: "judicial retention slate unresolved", blocked_until: "2026-10-05" },
    });

    await expect(runCommand(client, "record", recordFlags())).rejects.toThrow(
      /already exists/
    );
    await expect(runCommand(client, "record", recordFlags())).rejects.toThrow(
      /--blocker-key/
    );

    expect(statements.some((s) => s.text.includes("UPDATE public.manual_research_deferrals"))).toBe(
      false
    );
    expect(statements.some((s) => s.text.includes("INSERT INTO"))).toBe(false);
  });

  it("names the conflicting row's id, date, and reason so the caller can act", async () => {
    const { client } = fakeClient({
      existing: { reason: "judicial retention slate unresolved", blocked_until: "2026-10-05" },
    });
    await expect(runCommand(client, "record", recordFlags())).rejects.toThrow(
      /judicial retention slate unresolved/
    );
    await expect(runCommand(client, "record", recordFlags())).rejects.toThrow(/2026-10-05/);
    await expect(runCommand(client, "record", recordFlags())).rejects.toThrow(new RegExp(EXISTING_ID));
  });

  it("updates in place when --replace is given", async () => {
    const { client, statements } = fakeClient({
      existing: { reason: "old reason", blocked_until: "2026-10-05" },
    });
    await runCommand(client, "record", recordFlags(["--replace"]));

    const update = statements.find((s) => s.text.includes("UPDATE public.manual_research_deferrals"));
    expect(update).toBeDefined();
    expect(update?.values).toContain(EXISTING_ID);
    expect(statements.some((s) => s.text.includes("INSERT INTO"))).toBe(false);
  });

  // A distinct blocker key is a distinct row, so the probe must not match the
  // NULL-keyed row and the write must be an INSERT.
  it("scopes the open-row probe by blocker key and election id", async () => {
    const { client, statements } = fakeClient();
    await runCommand(
      client,
      "record",
      recordFlags(["--election-id", ELECTION_ID, "--blocker-key", "office-matcher"])
    );

    const probe = statements.find((s) => s.text.includes("SELECT id, reason, blocked_until"));
    expect(probe?.text).toContain("blocker_key IS NOT DISTINCT FROM");
    expect(probe?.values).toEqual([DISTRICT_ID, "elections", ELECTION_ID, "office-matcher"]);
    expect(statements.some((s) => s.text.includes("INSERT INTO"))).toBe(true);
  });

  it("rejects an invalid blocker key before touching the ledger", async () => {
    const { client, statements } = fakeClient();
    await expect(
      runCommand(client, "record", recordFlags(["--blocker-key", "Not A Slug"]))
    ).rejects.toThrow(/Invalid --blocker-key/);
    expect(statements.some((s) => s.text.includes("INSERT INTO"))).toBe(false);
  });
});

describe("manual:deferral due", () => {
  beforeEach(() => {
    vi.spyOn(console, "log").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // Without this the due list cannot tell two same-stage blockers apart.
  it("selects blocker_key so the worklist can itemize blockers", async () => {
    const { client, statements } = fakeClient();
    await runCommand(client, "due", parseFlags([]));
    const select = statements.find((s) => s.text.includes("FROM public.manual_research_deferrals"));
    expect(select?.text).toContain("blocker_key");
  });
});
