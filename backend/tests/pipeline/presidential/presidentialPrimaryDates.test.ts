import { describe, expect, it, vi } from "vitest";

import {
  ensurePresidentialStatePrimaryDateRows,
  PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS,
  PresidentialPrimaryDateBootstrapError,
} from "../../../src/pipeline/presidential/presidentialPrimaryDates.js";

const DEMOCRATIC_CYCLE_ID = "11111111-1111-4111-8111-111111111111";
const REPUBLICAN_CYCLE_ID = "22222222-2222-4222-8222-222222222222";
const GENERAL_CYCLE_ID = "33333333-3333-4333-8333-333333333333";
const UNKNOWN_CYCLE_ID = "44444444-4444-4444-8444-444444444444";

describe("ensurePresidentialStatePrimaryDateRows", () => {
  it("returns an empty result without querying for an empty cycle list", async () => {
    const query = vi.fn();

    await expect(ensurePresidentialStatePrimaryDateRows({ query }, [])).resolves.toEqual({
      requestedCycleCount: 0,
      stateCount: PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS.length,
      requestedRowCount: 0,
      insertedRowCount: 0,
      existingRowCount: 0,
    });

    expect(query).not.toHaveBeenCalled();
  });

  it("rejects invalid cycle IDs before issuing SQL", async () => {
    const query = vi.fn();

    await expect(
      ensurePresidentialStatePrimaryDateRows({ query }, [DEMOCRATIC_CYCLE_ID, "not-a-uuid", " "])
    ).rejects.toMatchObject({
      code: "invalid_cycle_ids",
      details: {
        invalidCycleIds: ["not-a-uuid", " "],
      },
    } satisfies Partial<PresidentialPrimaryDateBootstrapError>);

    expect(query).not.toHaveBeenCalled();
  });

  it("rejects unknown presidential cycle IDs before inserting", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [{ id: DEMOCRATIC_CYCLE_ID, stage: "primary" }],
    });

    await expect(
      ensurePresidentialStatePrimaryDateRows({ query }, [DEMOCRATIC_CYCLE_ID, UNKNOWN_CYCLE_ID])
    ).rejects.toMatchObject({
      code: "unknown_cycle_ids",
      details: {
        unknownCycleIds: [UNKNOWN_CYCLE_ID],
      },
    } satisfies Partial<PresidentialPrimaryDateBootstrapError>);

    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("FROM public.presidential_cycles");
  });

  it("rejects non-primary presidential cycles before inserting", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        { id: DEMOCRATIC_CYCLE_ID, stage: "primary" },
        { id: GENERAL_CYCLE_ID, stage: "general" },
      ],
    });

    await expect(
      ensurePresidentialStatePrimaryDateRows({ query }, [DEMOCRATIC_CYCLE_ID, GENERAL_CYCLE_ID])
    ).rejects.toMatchObject({
      code: "non_primary_cycles",
      details: {
        nonPrimaryCycleIds: [GENERAL_CYCLE_ID],
      },
    } satisfies Partial<PresidentialPrimaryDateBootstrapError>);

    expect(query).toHaveBeenCalledTimes(1);
  });

  it("inserts one placeholder row per state for each primary cycle", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          { id: DEMOCRATIC_CYCLE_ID, stage: "primary" },
          { id: REPUBLICAN_CYCLE_ID, stage: "primary" },
        ],
      })
      .mockResolvedValueOnce({
        rows: [{ inserted_count: "102" }],
      });

    await expect(
      ensurePresidentialStatePrimaryDateRows({ query }, [DEMOCRATIC_CYCLE_ID, REPUBLICAN_CYCLE_ID])
    ).resolves.toEqual({
      requestedCycleCount: 2,
      stateCount: 51,
      requestedRowCount: 102,
      insertedRowCount: 102,
      existingRowCount: 0,
    });

    expect(PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS).toContain("06");
    expect(PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS).toContain("11");
    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[1]?.[0]).toContain("INSERT INTO public.presidential_state_primary_dates");
    expect(query.mock.calls[1]?.[0]).toContain("ON CONFLICT (cycle_id, state_fips) DO NOTHING");
    expect(query.mock.calls[1]?.[1]).toEqual([
      [DEMOCRATIC_CYCLE_ID, REPUBLICAN_CYCLE_ID],
      PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS,
    ]);
  });

  it("dedupes cycle IDs and reports existing rows", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [{ id: DEMOCRATIC_CYCLE_ID, stage: "primary" }],
      })
      .mockResolvedValueOnce({
        rows: [{ inserted_count: 12 }],
      });

    await expect(
      ensurePresidentialStatePrimaryDateRows({ query }, [DEMOCRATIC_CYCLE_ID, DEMOCRATIC_CYCLE_ID.toUpperCase()])
    ).resolves.toEqual({
      requestedCycleCount: 1,
      stateCount: 51,
      requestedRowCount: 51,
      insertedRowCount: 12,
      existingRowCount: 39,
    });

    expect(query.mock.calls[0]?.[1]).toEqual([[DEMOCRATIC_CYCLE_ID]]);
    expect(query.mock.calls[1]?.[1]).toEqual([[DEMOCRATIC_CYCLE_ID], PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS]);
  });
});
