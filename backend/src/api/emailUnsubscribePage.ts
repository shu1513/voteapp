import { APP_NAME } from "../constants/brand.js";
import { EMAIL_UNSUBSCRIBE_PREFERENCES, type EmailUnsubscribePreference } from "./apiValidation.js";

// Standalone HTML pages behind GET/POST /api/email/unsubscribe. They are
// served by the API host (not the SPA), so they carry their own minimal
// styling, run without JavaScript, and never read account state: the link
// holder only ever turns opt-ins OFF, so the page needs no current values.

export const EMAIL_UNSUBSCRIBE_FORM_MARKER_FIELD = "form";
export const EMAIL_UNSUBSCRIBE_FORM_MARKER_VALUE = "1";
export const EMAIL_UNSUBSCRIBE_ALL_VALUE = "all";

// The pages inline their stylesheet and submit a same-origin form; nothing
// else (no scripts, images, or third-party requests) is ever needed.
export const EMAIL_UNSUBSCRIBE_PAGE_CSP =
  "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'";

export type EmailUnsubscribePreferenceCopy = {
  /** Checkbox label on the confirmation page. */
  title: string;
  /** One-line explanation under the checkbox. */
  description: string;
  /** Noun phrase for "You have been unsubscribed from <doneLabel>." */
  doneLabel: string;
};

// Second-person versions of the settings-page labels (EmailPreferenceToggles)
// so the email link and the in-app toggles describe the same thing.
export const EMAIL_UNSUBSCRIBE_PREFERENCE_COPY: Record<EmailUnsubscribePreference, EmailUnsubscribePreferenceCopy> = {
  digest: {
    title: "Updates about your candidates and election results",
    description:
      "Occasional emails when candidates you follow take new actions, and when elections in your districts have results.",
    doneLabel: "candidate update digest emails",
  },
  new_election_alerts: {
    title: "New elections in your districts",
    description: "A heads-up when a new election is coming up where you vote.",
    doneLabel: "new election alert emails",
  },
  election_reminders: {
    title: "Election-day reminder",
    description: "One reminder to vote the day before election day.",
    doneLabel: "election reminder emails",
  },
  issue_updates: {
    title: "Updates about the issues you saved",
    description: "Occasional emails when there is something important about the issues that matter most to you.",
    doneLabel: "emails about your saved issues",
  },
  member_newsletter: {
    title: "Member newsletter",
    description: "Member-only analysis and newsletters. Turning this off does not affect your membership.",
    doneLabel: "member newsletter emails",
  },
};

export function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

/** Absolute settings URL when the public site origin is configured. */
export function buildEmailSettingsUrl(publicSiteOrigin: string | undefined): string | undefined {
  const origin = publicSiteOrigin?.trim().replace(/\/+$/, "");
  if (!origin || !/^https?:\/\//.test(origin)) {
    return undefined;
  }
  return `${origin}/me/settings`;
}

/** "a", "a and b", "a, b, and c" — or the all-emails phrase for a full set. */
export function describeUnsubscribedPreferences(preferences: readonly EmailUnsubscribePreference[]): string {
  const ordered = EMAIL_UNSUBSCRIBE_PREFERENCES.filter((preference) => preferences.includes(preference));
  if (ordered.length === EMAIL_UNSUBSCRIBE_PREFERENCES.length) {
    return `all ${APP_NAME} notification emails`;
  }
  const labels = ordered.map((preference) => EMAIL_UNSUBSCRIBE_PREFERENCE_COPY[preference].doneLabel);
  if (labels.length <= 1) {
    return labels[0] ?? "";
  }
  if (labels.length === 2) {
    return `${labels[0]} and ${labels[1]}`;
  }
  return `${labels.slice(0, -1).join(", ")}, and ${labels[labels.length - 1]}`;
}

const PAGE_STYLE = `
  :root { color-scheme: light dark; }
  * { box-sizing: border-box; }
  body {
    margin: 0; min-height: 100vh; display: flex; align-items: flex-start; justify-content: center;
    padding: 40px 16px; background: #f5f4f0; color: #1f1f1f;
    font: 16px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  }
  main { width: 100%; max-width: 520px; background: #fff; border: 1px solid #e3e1da; border-radius: 12px; padding: 28px 28px 24px; }
  .brand { font-size: 14px; font-weight: 600; letter-spacing: 0.02em; color: #6b6a64; margin: 0 0 12px; }
  h1 { font-size: 22px; line-height: 1.25; margin: 0 0 8px; }
  p { margin: 0 0 12px; }
  .muted { color: #6b6a64; font-size: 14px; }
  .notice { background: #fff4e5; border: 1px solid #f0c27a; color: #6d3f00; border-radius: 8px; padding: 10px 12px; font-size: 14px; }
  fieldset { border: 0; margin: 16px 0 8px; padding: 0; }
  legend { font-weight: 600; margin-bottom: 8px; }
  .option { display: flex; gap: 12px; align-items: flex-start; padding: 10px 0; border-top: 1px solid #ecebe5; cursor: pointer; }
  .option:last-of-type { border-bottom: 1px solid #ecebe5; }
  .option input { width: 20px; height: 20px; margin: 3px 0 0; flex: none; accent-color: #1f5f8b; }
  .option .title { display: block; font-weight: 500; }
  .option .desc { display: block; font-size: 14px; color: #6b6a64; }
  .option.all { margin-top: 8px; border: 0; }
  .option.all .title { font-weight: 600; }
  button { appearance: none; border: 0; border-radius: 8px; background: #1f5f8b; color: #fff; font: inherit; font-weight: 600;
    padding: 12px 18px; margin-top: 16px; cursor: pointer; width: 100%; }
  button:hover { background: #184b6e; }
  a { color: #1f5f8b; }
  .footer { margin-top: 20px; font-size: 14px; color: #6b6a64; }
  @media (prefers-color-scheme: dark) {
    body { background: #161616; color: #ececec; }
    main { background: #1f1f1f; border-color: #333; }
    .brand, .muted, .footer, .option .desc { color: #a5a49e; }
    .option, .option:last-of-type { border-color: #333; }
    .notice { background: #3a2a10; border-color: #8a5a1a; color: #ffd9a3; }
    a, .option input { color: #7fb6df; accent-color: #7fb6df; }
    button { background: #2b76ab; }
    button:hover { background: #3688c2; }
  }
`;

function renderPage(title: string, body: string): string {
  return (
    "<!doctype html><html lang=\"en\"><head><meta charset=\"UTF-8\">" +
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">" +
    "<meta name=\"robots\" content=\"noindex\">" +
    `<title>${escapeHtml(title)}</title><style>${PAGE_STYLE}</style></head><body><main>` +
    `<p class="brand">${escapeHtml(APP_NAME)}</p>` +
    body +
    "</main></body></html>"
  );
}

function renderSettingsFooter(settingsUrl: string | undefined, lead: string): string {
  return settingsUrl
    ? `<p class="footer">${escapeHtml(lead)} <a href="${escapeHtml(settingsUrl)}">Manage all email settings</a>.</p>`
    : `<p class="footer">${escapeHtml(lead)} You can manage every email setting in your account settings.</p>`;
}

export function renderEmailUnsubscribeConfirmPage(input: {
  formAction: string;
  /** Opt-ins the link advertised; rendered pre-checked. */
  selected: readonly EmailUnsubscribePreference[];
  settingsUrl?: string;
  /** Shown above the form, e.g. after an empty submit. */
  notice?: string;
}): string {
  const options = EMAIL_UNSUBSCRIBE_PREFERENCES.map((preference) => {
    const copy = EMAIL_UNSUBSCRIBE_PREFERENCE_COPY[preference];
    const checked = input.selected.includes(preference) ? " checked" : "";
    return (
      `<label class="option"><input type="checkbox" name="pref" value="${preference}"${checked}>` +
      `<span><span class="title">${escapeHtml(copy.title)}</span>` +
      `<span class="desc">${escapeHtml(copy.description)}</span></span></label>`
    );
  }).join("");
  const body =
    "<h1>Unsubscribe from emails</h1>" +
    "<p class=\"muted\">Choose which emails to stop. Nothing changes until you press the button.</p>" +
    (input.notice ? `<p class="notice">${escapeHtml(input.notice)}</p>` : "") +
    `<form method="post" action="${escapeHtml(input.formAction)}">` +
    `<input type="hidden" name="${EMAIL_UNSUBSCRIBE_FORM_MARKER_FIELD}" value="${EMAIL_UNSUBSCRIBE_FORM_MARKER_VALUE}">` +
    "<fieldset><legend>Stop sending me</legend>" +
    options +
    `<label class="option all"><input type="checkbox" name="pref" value="${EMAIL_UNSUBSCRIBE_ALL_VALUE}">` +
    `<span><span class="title">All ${escapeHtml(APP_NAME)} notification emails</span>` +
    "<span class=\"desc\">Account emails such as sign-in links and receipts still arrive.</span></span></label>" +
    "</fieldset>" +
    "<button type=\"submit\">Unsubscribe</button></form>" +
    renderSettingsFooter(input.settingsUrl, "Changed your mind later?");
  return renderPage("Unsubscribe", body);
}

export function renderEmailUnsubscribeDonePage(input: {
  preferences: readonly EmailUnsubscribePreference[];
  settingsUrl?: string;
}): string {
  const body =
    "<h1>Unsubscribed</h1>" +
    `<p>You have been unsubscribed from ${escapeHtml(describeUnsubscribedPreferences(input.preferences))}.</p>` +
    "<p class=\"muted\">Account emails such as sign-in links and receipts are not affected.</p>" +
    renderSettingsFooter(input.settingsUrl, "You can turn them back on any time.");
  return renderPage("Unsubscribed", body);
}

export function renderEmailUnsubscribeInvalidPage(input: { settingsUrl?: string }): string {
  const body =
    "<h1>This link is invalid</h1>" +
    "<p>This unsubscribe link is invalid or incomplete. It may have been cut off when the email was forwarded or copied.</p>" +
    renderSettingsFooter(input.settingsUrl, "Nothing was changed.");
  return renderPage("Invalid link", body);
}
