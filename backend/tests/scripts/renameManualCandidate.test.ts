import { describe, expect, it, vi } from "vitest";

import { normalizeName, runRenameCandidate } from "../../src/scripts/renameManualCandidate.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const PEER_ID = "22222222-2222-4222-8222-222222222222";
const ELECTION_ID = "33333333-3333-4333-8333-333333333333";
const AUDIT_ID = "44444444-4444-4444-8444-444444444444";
const SOURCE_URL = "https://sos.idaho.gov/elections/candidates";

function candidateRow(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: CANDIDATE_ID,
    display_name: "Marvin Richardson",
    first_name: "Marvin",
    last_name: "Richardson",
    deleted_at: null,
    merged_into_candidate_id: null,
    profile_sources: ["https://example.com/old-profile"],
    ...overrides,
  };
}

// Query order: BEGIN, lock candidate, (idempotent source-append OR collision
// guard, candidates update, audit insert), COMMIT/ROLLBACK.
function buildClient(responses: Record<string, { rows: unknown[] }[]>) {
  const calls: { text: string; values: unknown[] }[] = [];
  const queue = { ...responses };
  const query = vi.fn(async (text: string, values?: unknown[]) => {
    calls.push({ text, values: values ?? [] });
    for (const key of Object.keys(queue)) {
      if (text.includes(key)) {
        const result = queue[key]!.shift();
        if (result !== undefined) return result;
      }
    }
    return { rows: [] };
  });
  return { query, calls };
}

function happyResponses(overrides: Partial<Record<string, { rows: unknown[] }[]>> = {}) {
  return {
    "FROM public.candidates\n        WHERE id": [{ rows: [candidateRow()] }],
    "FROM public.candidate_elections own_link": [{ rows: [] }],
    "INSERT INTO public.candidate_rename_audit": [{ rows: [{ id: AUDIT_ID }] }],
    ...overrides,
  };
}

function options(overrides: Partial<Parameters<typeof runRenameCandidate>[1]> = {}) {
  return {
    candidateId: CANDIDATE_ID,
    displayName: "Pro-Life",
    firstName: null,
    lastName: null,
    sourceUrl: SOURCE_URL,
    reason: "Idaho SOS lists the legal ballot name Pro-Life since 2008",
    dryRun: false,
    ...overrides,
  };
}

describe("runRenameCandidate", () => {
  it("updates display_name, appends the source, and records an audit row", async () => {
    const { query, calls } = buildClient(happyResponses());

    const result = await runRenameCandidate({ query }, options());

    expect(result).toMatchObject({
      alreadyRenamed: false,
      dryRun: false,
      candidateId: CANDIDATE_ID,
      displayName: { old: "Marvin Richardson", new: "Pro-Life" },
      firstName: null,
      lastName: null,
      sourceAppended: true,
      auditRowId: AUDIT_ID,
    });
    const update = calls.find((call) => call.text.includes("UPDATE public.candidates SET"));
    expect(update?.text).toContain("display_name = $2");
    expect(update?.text).toContain("profile_sources = $3::jsonb");
    expect(update?.text).not.toContain("first_name");
    expect(update?.text).not.toContain("last_name");
    expect(update?.values).toEqual([
      CANDIDATE_ID,
      "Pro-Life",
      JSON.stringify(["https://example.com/old-profile", SOURCE_URL]),
    ]);
    const audit = calls.find((call) => call.text.includes("candidate_rename_audit"));
    expect(audit?.values).toEqual([
      CANDIDATE_ID,
      "Marvin Richardson",
      "Pro-Life",
      "Marvin",
      null,
      "Richardson",
      null,
      SOURCE_URL,
      "Idaho SOS lists the legal ballot name Pro-Life since 2008",
    ]);
    // Records and election links are out of scope for a rename.
    expect(calls.some((call) => call.text.includes("UPDATE public.candidate_elections"))).toBe(false);
    expect(calls.some((call) => call.text.includes("candidate_records"))).toBe(false);
    expect(calls.at(-1)?.text).toBe("COMMIT");
  });

  it("updates first/last name when provided and stores them in the audit row", async () => {
    const { query, calls } = buildClient(happyResponses());

    const result = await runRenameCandidate(
      { query },
      options({ firstName: "Pro-Life", lastName: "Richardson" })
    );

    expect(result).toMatchObject({
      alreadyRenamed: false,
      firstName: { old: "Marvin", new: "Pro-Life" },
      // Unchanged even though provided: the audit row records no new value.
      lastName: null,
    });
    const update = calls.find((call) => call.text.includes("UPDATE public.candidates SET"));
    expect(update?.text).toContain("first_name = $3");
    expect(update?.text).not.toContain("last_name");
    const audit = calls.find((call) => call.text.includes("candidate_rename_audit"));
    expect(audit?.values?.[4]).toBe("Pro-Life");
    expect(audit?.values?.[6]).toBeNull();
  });

  it("dry run executes the full transaction and rolls back", async () => {
    const { query, calls } = buildClient(happyResponses());

    const result = await runRenameCandidate({ query }, options({ dryRun: true }));

    expect(result).toMatchObject({ alreadyRenamed: false, dryRun: true, auditRowId: AUDIT_ID });
    expect(calls.some((call) => call.text.includes("UPDATE public.candidates SET"))).toBe(true);
    expect(calls.at(-1)?.text).toBe("ROLLBACK");
  });

  it("refuses when another candidate in a shared election already carries the name", async () => {
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.candidate_elections own_link": [
          { rows: [{ id: PEER_ID, election_id: ELECTION_ID }] },
        ],
      })
    );

    await expect(runRenameCandidate({ query }, options())).rejects.toThrow(
      /already\s+carries the name "Pro-Life"/
    );
    expect(calls.some((call) => call.text.includes("UPDATE public.candidates SET"))).toBe(false);
    expect(calls.some((call) => call.text.includes("candidate_rename_audit"))).toBe(false);
    expect(calls.at(-1)?.text).toBe("ROLLBACK");
  });

  it("collision guard ignores merged and soft-deleted peers in SQL", async () => {
    const { query, calls } = buildClient(happyResponses());

    await runRenameCandidate({ query }, options());

    const guard = calls.find((call) => call.text.includes("FROM public.candidate_elections own_link"));
    expect(guard?.text).toContain("peer.merged_into_candidate_id IS NULL");
    expect(guard?.text).toContain("peer.deleted_at IS NULL");
    expect(guard?.values).toEqual([CANDIDATE_ID, "Pro-Life"]);
  });

  it("is idempotent: an already-renamed row only converges provenance, without a second audit row", async () => {
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.candidates\n        WHERE id": [
          { rows: [candidateRow({ display_name: "Pro-Life", profile_sources: [] })] },
        ],
      })
    );

    const result = await runRenameCandidate({ query }, options());

    expect(result).toMatchObject({
      alreadyRenamed: true,
      displayName: "Pro-Life",
      sourceAppended: true,
    });
    const update = calls.find((call) => call.text.includes("UPDATE public.candidates"));
    expect(update?.text).toContain("profile_sources");
    expect(update?.text).not.toContain("display_name");
    expect(calls.some((call) => call.text.includes("candidate_rename_audit"))).toBe(false);
    expect(calls.at(-1)?.text).toBe("COMMIT");
  });

  it("already-renamed with the source already stored touches nothing", async () => {
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.candidates\n        WHERE id": [
          { rows: [candidateRow({ display_name: "Pro-Life", profile_sources: [SOURCE_URL] })] },
        ],
      })
    );

    const result = await runRenameCandidate({ query }, options());

    expect(result).toMatchObject({ alreadyRenamed: true, sourceAppended: false });
    expect(calls.some((call) => call.text.includes("UPDATE public.candidates"))).toBe(false);
    expect(calls.at(-1)?.text).toBe("ROLLBACK");
  });

  it("refuses when the candidate does not exist", async () => {
    const { query } = buildClient(
      happyResponses({ "FROM public.candidates\n        WHERE id": [{ rows: [] }] })
    );

    await expect(runRenameCandidate({ query }, options())).rejects.toThrow(/Candidate not found/);
  });

  it("refuses a merged candidate and points at the survivor", async () => {
    const { query } = buildClient(
      happyResponses({
        "FROM public.candidates\n        WHERE id": [
          { rows: [candidateRow({ merged_into_candidate_id: PEER_ID, deleted_at: "2026-01-01" })] },
        ],
      })
    );

    await expect(runRenameCandidate({ query }, options())).rejects.toThrow(
      /is merged into 22222222-2222-4222-8222-222222222222/
    );
  });

  it("refuses a soft-deleted candidate", async () => {
    const { query } = buildClient(
      happyResponses({
        "FROM public.candidates\n        WHERE id": [{ rows: [candidateRow({ deleted_at: "2026-01-01" })] }],
      })
    );

    await expect(runRenameCandidate({ query }, options())).rejects.toThrow(/soft-deleted/);
  });

  it("refuses a non-HTTPS source before touching the database", async () => {
    const { query, calls } = buildClient(happyResponses());

    await expect(
      runRenameCandidate({ query }, options({ sourceUrl: "http://sos.idaho.gov/x" }))
    ).rejects.toThrow(/--source-url must use HTTPS/);
    expect(calls.length).toBe(0);
  });

  it("refuses a blank display name", async () => {
    const { query } = buildClient(happyResponses());

    await expect(runRenameCandidate({ query }, options({ displayName: "   " }))).rejects.toThrow(
      /--display-name must not be blank/
    );
  });

  it("treats whitespace-only differences as already renamed", async () => {
    const { query } = buildClient(
      happyResponses({
        "FROM public.candidates\n        WHERE id": [
          { rows: [candidateRow({ display_name: "  Pro-Life ", profile_sources: [SOURCE_URL] })] },
        ],
      })
    );

    const result = await runRenameCandidate({ query }, options({ displayName: "Pro-Life" }));
    expect(result).toMatchObject({ alreadyRenamed: true });
  });

  it("accepts uppercase UUID input by normalizing before row matching", async () => {
    const { query, calls } = buildClient(happyResponses());

    await runRenameCandidate({ query }, options({ candidateId: CANDIDATE_ID.toUpperCase() }));

    const lock = calls.find((call) => call.text.includes("FROM public.candidates\n        WHERE id"));
    expect(lock?.values).toEqual([CANDIDATE_ID]);
  });
});

describe("normalizeName", () => {
  it("trims and collapses internal whitespace like migration 044", () => {
    expect(normalizeName("  Pro-Life   (formerly  Marvin) ")).toBe("Pro-Life (formerly Marvin)");
  });
});
