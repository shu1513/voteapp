import { describe, expect, it, vi } from "vitest";

import {
  disableUserEmailDigest,
  disableUserEmailNewElectionAlerts,
  getUserEmailPreferences,
  setUserEmailPreferences,
  UserEmailPreferencesError,
} from "../../../src/pipeline/users/userEmailPreferences.js";

const USER_ID = "11111111-1111-4111-8111-111111111111";
const PREFS = { email_digest: true, email_election_reminders: false, email_new_election_alerts: true };

describe("userEmailPreferences", () => {
  it("getUserEmailPreferences returns the stored booleans", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [PREFS], rowCount: 1 });

    await expect(getUserEmailPreferences({ query } as never, USER_ID)).resolves.toEqual(PREFS);
    expect(String(query.mock.calls[0][0])).toContain("deleted_at IS NULL");
    expect(query.mock.calls[0][1]).toEqual([USER_ID]);
  });

  it("get throws user_not_found for unknown or deleted users", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(getUserEmailPreferences({ query } as never, USER_ID)).rejects.toMatchObject({
      code: "user_not_found",
    });
  });

  it("get rejects a non-UUID userId without querying", async () => {
    const query = vi.fn();

    await expect(getUserEmailPreferences({ query } as never, "bob")).rejects.toBeInstanceOf(
      UserEmailPreferencesError
    );
    expect(query).not.toHaveBeenCalled();
  });

  it("setUserEmailPreferences updates all three flags and returns the row", async () => {
    const updated = { email_digest: false, email_election_reminders: true, email_new_election_alerts: false };
    const query = vi.fn().mockResolvedValue({ rows: [updated], rowCount: 1 });

    await expect(setUserEmailPreferences({ query } as never, USER_ID, updated)).resolves.toEqual(updated);
    expect(query.mock.calls[0][1]).toEqual([USER_ID, false, true, false]);
  });

  it("disableUserEmailDigest flips only the digest flag and is idempotent-friendly", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 1 });

    await expect(disableUserEmailDigest({ query } as never, USER_ID)).resolves.toBeUndefined();
    const sql = String(query.mock.calls[0][0]);
    expect(sql).toContain("email_digest = false");
    expect(sql).not.toContain("email_election_reminders");
  });

  it("disableUserEmailNewElectionAlerts flips only the alerts flag", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 1 });

    await expect(disableUserEmailNewElectionAlerts({ query } as never, USER_ID)).resolves.toBeUndefined();
    const sql = String(query.mock.calls[0][0]);
    expect(sql).toContain("email_new_election_alerts = false");
    expect(sql).not.toContain("email_digest");
  });

  it("disableUserEmailNewElectionAlerts throws user_not_found when no row matched", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(disableUserEmailNewElectionAlerts({ query } as never, USER_ID)).rejects.toMatchObject({
      code: "user_not_found",
    });
  });

  it("disableUserEmailDigest throws user_not_found when no row matched", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(disableUserEmailDigest({ query } as never, USER_ID)).rejects.toMatchObject({
      code: "user_not_found",
    });
  });
});
