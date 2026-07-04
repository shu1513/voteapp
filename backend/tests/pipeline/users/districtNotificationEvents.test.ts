import { describe, expect, it, vi } from "vitest";

import {
  createDistrictNewElectionNotificationEvents,
  DistrictNotificationEventsError,
} from "../../../src/pipeline/users/districtNotificationEvents.js";

const ELECTION_A = "11111111-1111-4111-8111-111111111111";
const ELECTION_B = "22222222-2222-4222-8222-222222222222";

describe("createDistrictNewElectionNotificationEvents", () => {
  it("returns zero without querying for an empty batch", async () => {
    const query = vi.fn();

    await expect(createDistrictNewElectionNotificationEvents({ query } as never, [])).resolves.toEqual({
      createdCount: 0,
    });
    expect(query).not.toHaveBeenCalled();
  });

  it("rejects non-UUID election ids without querying", async () => {
    const query = vi.fn();

    await expect(
      createDistrictNewElectionNotificationEvents({ query } as never, [ELECTION_A, "nope"])
    ).rejects.toBeInstanceOf(DistrictNotificationEventsError);
    expect(query).not.toHaveBeenCalled();
  });

  it("fans out via user_districts to alert-enabled live users, dedupes, and re-checks the date", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [{ id: "e1" }, { id: "e2" }], rowCount: 2 });

    await expect(
      createDistrictNewElectionNotificationEvents({ query } as never, [ELECTION_A, ` ${ELECTION_B} `])
    ).resolves.toEqual({ createdCount: 2 });

    const sql = String(query.mock.calls[0][0]);
    expect(sql).toContain("JOIN public.user_districts");
    expect(sql).toContain("email_new_election_alerts = true");
    expect(sql).toContain("deleted_at IS NULL");
    // Only future elections notify, even if the caller passes stale ids.
    expect(sql).toContain("election.election_date >=");
    // Writer re-runs and prune re-arms rely on the unique pair + DO NOTHING.
    expect(sql).toContain("ON CONFLICT DO NOTHING");
    expect(query.mock.calls[0][1]).toEqual([[ELECTION_A, ELECTION_B]]);
  });
});
