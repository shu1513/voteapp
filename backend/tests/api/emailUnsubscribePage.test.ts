import { describe, expect, it } from "vitest";

import {
  buildEmailSettingsUrl,
  describeUnsubscribedPreferences,
  renderEmailUnsubscribeConfirmPage,
  renderEmailUnsubscribeDonePage,
} from "../../src/api/emailUnsubscribePage.js";
import {
  EMAIL_UNSUBSCRIBE_PREFERENCE_COLUMNS,
  EMAIL_UNSUBSCRIBE_PREFERENCES,
  parseEmailUnsubscribeFormBody,
  parseEmailUnsubscribePreferences,
} from "../../src/api/apiValidation.js";

describe("parseEmailUnsubscribePreferences", () => {
  it("returns [] for nothing usable and null for unknown values", () => {
    expect(parseEmailUnsubscribePreferences([])).toEqual([]);
    expect(parseEmailUnsubscribePreferences(["", "  "])).toEqual([]);
    expect(parseEmailUnsubscribePreferences(["digest", "everything"])).toBeNull();
    expect(parseEmailUnsubscribePreferences(["DIGEST"])).toBeNull();
  });

  it("dedupes, trims, keeps canonical order, and expands all", () => {
    expect(parseEmailUnsubscribePreferences([" member_newsletter ", "digest", "digest"])).toEqual([
      "digest",
      "member_newsletter",
    ]);
    expect(parseEmailUnsubscribePreferences(["all"])).toEqual([...EMAIL_UNSUBSCRIBE_PREFERENCES]);
  });

  it("maps every preference to a users column", () => {
    for (const preference of EMAIL_UNSUBSCRIBE_PREFERENCES) {
      expect(EMAIL_UNSUBSCRIBE_PREFERENCE_COLUMNS[preference]).toMatch(/^email_/);
    }
  });
});

describe("parseEmailUnsubscribeFormBody", () => {
  it("recognizes the form marker and collects single or repeated pref fields", () => {
    expect(parseEmailUnsubscribeFormBody(undefined)).toEqual({ isForm: false, preferenceValues: [] });
    expect(parseEmailUnsubscribeFormBody({ "List-Unsubscribe": "One-Click" })).toEqual({
      isForm: false,
      preferenceValues: [],
    });
    expect(parseEmailUnsubscribeFormBody({ form: "1" })).toEqual({ isForm: true, preferenceValues: [] });
    expect(parseEmailUnsubscribeFormBody({ form: "1", pref: "digest" })).toEqual({
      isForm: true,
      preferenceValues: ["digest"],
    });
    expect(parseEmailUnsubscribeFormBody({ form: "1", pref: ["digest", 7, "all"] })).toEqual({
      isForm: true,
      preferenceValues: ["digest", "all"],
    });
  });
});

describe("emailUnsubscribePage", () => {
  it("describes one, two, several, or all preferences", () => {
    expect(describeUnsubscribedPreferences(["digest"])).toBe("candidate update digest emails");
    expect(describeUnsubscribedPreferences(["member_newsletter", "digest"])).toBe(
      "candidate update digest emails and member newsletter emails"
    );
    expect(describeUnsubscribedPreferences(["digest", "election_reminders", "issue_updates"])).toBe(
      "candidate update digest emails, election reminder emails, and emails about your saved issues"
    );
    expect(describeUnsubscribedPreferences([...EMAIL_UNSUBSCRIBE_PREFERENCES])).toBe(
      "all Elections Simplified notification emails"
    );
  });

  it("builds the settings link only from an http(s) origin", () => {
    expect(buildEmailSettingsUrl("https://example.com/")).toBe("https://example.com/me/settings");
    expect(buildEmailSettingsUrl("example.com")).toBeUndefined();
    expect(buildEmailSettingsUrl(undefined)).toBeUndefined();
  });

  it("renders a no-script form with the marker, every opt-in, and escaped values", () => {
    const html = renderEmailUnsubscribeConfirmPage({
      formAction: '/api/email/unsubscribe?token=a"b',
      selected: ["issue_updates"],
      settingsUrl: "https://example.com/me/settings",
      notice: "<pick one>",
    });
    expect(html).not.toContain("<script");
    expect(html).toContain('action="/api/email/unsubscribe?token=a&quot;b"');
    expect(html).toContain('name="form" value="1"');
    expect(html).toContain("&lt;pick one&gt;");
    for (const preference of EMAIL_UNSUBSCRIBE_PREFERENCES) {
      expect(html).toContain(`value="${preference}"`);
    }
    expect(html).toContain('value="issue_updates" checked');
    expect((html.match(/ checked/g) ?? []).length).toBe(1);
    expect(html).toContain('href="https://example.com/me/settings"');
  });

  it("renders the done page with the account-email reassurance", () => {
    const html = renderEmailUnsubscribeDonePage({ preferences: ["election_reminders"] });
    expect(html).toContain("You have been unsubscribed from election reminder emails.");
    expect(html).toContain("sign-in links");
    expect(html).toContain("account settings");
  });
});
