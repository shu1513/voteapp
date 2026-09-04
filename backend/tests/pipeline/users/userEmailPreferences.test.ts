import { describe, expect, it, vi } from "vitest";

import {
  disableUserEmailDigest,
  disableUserEmailElectionReminders,
  disableUserEmailIssueUpdates,
  disableUserEmailNewElectionAlerts,
  disableUserEmailPreferences,
  getUserEmailPreferences,
  setUserEmailPreferences,
  UserEmailPreferencesError,
} from "../../../src/pipeline/users/userEmailPreferences.js";

const USER_ID = "11111111-1111-4111-8111-111111111111";
const PREFS = {
  email_digest: true,
  email_election_reminders: false,
  email_new_election_alerts: true,
  email_issue_updates: true,
  email_member_newsletter: true,
};

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

  it("setUserEmailPreferences updates all four flags and returns the row", async () => {
    const updated = {
      email_digest: false,
      email_election_reminders: true,
      email_new_election_alerts: false,
      email_issue_updates: false,
      email_member_newsletter: true,
    };
    const query = vi.fn().mockResolvedValue({ rows: [updated], rowCount: 1 });

    await expect(setUserEmailPreferences({ query } as never, USER_ID, updated)).resolves.toEqual(updated);
    expect(query.mock.calls[0][1]).toEqual([USER_ID, false, true, false, false, true]);
  });

  it("disableUserEmailPreferences flips only the named columns in one statement", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 1 });

    await expect(
      disableUserEmailPreferences({ query } as never, USER_ID, ["email_member_newsletter", "email_digest"])
    ).resolves.toBeUndefined();
    expect(query).toHaveBeenCalledTimes(1);
    const sql = String(query.mock.calls[0][0]);
    expect(sql).toContain("email_digest = false, email_member_newsletter = false");
    expect(sql).not.toContain("email_election_reminders");
    expect(sql).toContain("deleted_at IS NULL");
    expect(query.mock.calls[0][1]).toEqual([USER_ID]);
  });

  it("disableUserEmailPreferences ignores unknown columns and refuses an empty list", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 1 });

    await expect(
      disableUserEmailPreferences({ query } as never, USER_ID, ["password_hash" as never])
    ).rejects.toThrow("at least one known column");
    expect(query).not.toHaveBeenCalled();
  });

  it("disableUserEmailPreferences throws user_not_found when no row matched", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(disableUserEmailPreferences({ query } as never, USER_ID, ["email_digest"])).rejects.toMatchObject({
      code: "user_not_found",
    });
  });

  it("disableUserEmailDigest flips only the digest flag and is idempotent-friendly", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 1 });

    await expect(disableUserEmailDigest({ query } as never, USER_ID)).resolves.toBeUndefined();
    const sql = String(query.mock.calls[0][0]);
    expect(sql).toContain("email_digest = false");
    expect(sql).not.toContain("email_election_reminders");
  });

  it("disableUserEmailIssueUpdates flips only the issue-updates flag", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 1 });

    await expect(disableUserEmailIssueUpdates({ query } as never, USER_ID)).resolves.toBeUndefined();
    const sql = String(query.mock.calls[0][0]);
    expect(sql).toContain("email_issue_updates = false");
    expect(sql).not.toContain("email_digest");
    expect(sql).not.toContain("email_election_reminders");
    expect(sql).not.toContain("email_new_election_alerts");
  });

  it("disableUserEmailIssueUpdates throws user_not_found when no row matched", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(disableUserEmailIssueUpdates({ query } as never, USER_ID)).rejects.toMatchObject({
      code: "user_not_found",
    });
  });

  it("disableUserEmailElectionReminders flips only the reminders flag", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 1 });

    await expect(disableUserEmailElectionReminders({ query } as never, USER_ID)).resolves.toBeUndefined();
    const sql = String(query.mock.calls[0][0]);
    expect(sql).toContain("email_election_reminders = false");
    expect(sql).not.toContain("email_digest");
    expect(sql).not.toContain("email_new_election_alerts");
  });

  it("disableUserEmailElectionReminders throws user_not_found when no row matched", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(disableUserEmailElectionReminders({ query } as never, USER_ID)).rejects.toMatchObject({
      code: "user_not_found",
    });
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
