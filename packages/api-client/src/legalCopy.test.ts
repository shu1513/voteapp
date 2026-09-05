import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { describe, expect, it } from "vitest";

import {
  ADDRESS_FIELD_PRIVACY_NOTE,
  PRE_SEARCH_AGREEMENT_PARAGRAPHS,
  PRE_SEARCH_CHECKBOX_LABEL,
  RENEWAL_CHECKBOX_LABEL,
  SIGNUP_CHECKBOX_LABEL,
  TERMS_VERSION,
  VERIFY_WITH_OFFICIALS_NOTE,
} from "./legalCopy";

// docs/legal/checkbox-copy.md says the frontend must copy these strings
// verbatim, and nothing checked it. Splitting the pre-search clickwrap into a
// short label plus dialog paragraphs turned one string into five, so drift is
// now easier — and the whole evidentiary value of the gate rests on the
// shipped wording matching the archived wording.
// Walked up from the cwd rather than resolved from import.meta.url: the jsdom
// environment hands this module a non-file URL, and the suite runs from either
// the package or the repo root.
function findLegalDoc(filename: string): string {
  let directory = process.cwd();
  for (;;) {
    const candidate = resolve(directory, "docs/legal", filename);
    if (existsSync(candidate)) {
      return candidate;
    }
    const parent = dirname(directory);
    if (parent === directory) {
      throw new Error(`Could not locate docs/legal/${filename} above the working directory`);
    }
    directory = parent;
  }
}

function findCheckboxCopyDoc(): string {
  return findLegalDoc("checkbox-copy.md");
}

// Only the published copy counts. The file opens with an HTML comment of
// implementation notes that quotes fragments of these strings; matching
// against it would let a stale published string pass on the strength of a
// note.
const CHECKBOX_COPY_DOC = readFileSync(findCheckboxCopyDoc(), "utf8").replace(/<!--[\s\S]*?-->/g, "");

/** The doc is prose: markdown link brackets, blockquote markers, wrapped lines. */
function normalize(value: string): string {
  return value.replace(/[[\]]/g, "").replace(/^\s*>\s?/gm, "").replace(/\s+/g, " ").trim();
}

const normalizedDoc = normalize(CHECKBOX_COPY_DOC);

describe("legal copy matches docs/legal/checkbox-copy.md", () => {
  it.each([
    ["pre-search label", PRE_SEARCH_CHECKBOX_LABEL],
    ["signup label", SIGNUP_CHECKBOX_LABEL],
    ["renewal label", RENEWAL_CHECKBOX_LABEL],
    ["short privacy note", ADDRESS_FIELD_PRIVACY_NOTE],
    ["results verification line", VERIFY_WITH_OFFICIALS_NOTE],
    ...PRE_SEARCH_AGREEMENT_PARAGRAPHS.map(
      (paragraph, index) => [`full-agreement paragraph ${index + 1}`, paragraph] as const
    ),
  ])("archives the %s", (_name, copy) => {
    expect(normalizedDoc).toContain(normalize(copy));
  });

  it("carries the version the doc is headed with", () => {
    expect(normalizedDoc).toContain(`Checkbox and notice copy — Version ${TERMS_VERSION}`);
  });

  // No checkbox restates Section 12: arbitration binds through the linked
  // Terms of Use, and repeating it beside every box was scare copy on screens
  // people came to for something else (2026-08-30 trimmed the pre-search gate,
  // 2026-08-31 the signup and renewal labels). Pinned so the restatement does
  // not creep back one label at a time. The reasoning is in legalCopy.ts above
  // PRE_SEARCH_CHECKBOX_LABEL.
  it.each([
    ["pre-search label", PRE_SEARCH_CHECKBOX_LABEL],
    ["signup label", SIGNUP_CHECKBOX_LABEL],
    ["renewal label", RENEWAL_CHECKBOX_LABEL],
    ...PRE_SEARCH_AGREEMENT_PARAGRAPHS.map(
      (paragraph, index) => [`full-agreement paragraph ${index + 1}`, paragraph] as const
    ),
  ])("leaves arbitration out of the %s", (_name, copy) => {
    expect(copy).not.toContain("arbitration");
    expect(copy).not.toContain("class-action");
  });

  // Dropping the arbitration callout leaves the linked documents carrying the
  // whole of the notice, so every label must still name all three — the
  // dialogs hard-code that same list as links beside it.
  it.each([
    ["pre-search", PRE_SEARCH_CHECKBOX_LABEL],
    ["signup", SIGNUP_CHECKBOX_LABEL],
    ["renewal", RENEWAL_CHECKBOX_LABEL],
  ])("names all three documents in the %s label", (_name, copy) => {
    for (const title of [
      "Terms of Use",
      "Privacy Policy",
      "AI Research and Election Information Disclaimer",
    ]) {
      expect(copy).toContain(title);
    }
  });
});

// The acceptance ledger records a version string, and the enforcement paths
// compare against one. Neither notices if the TEXT behind a version changes:
// an edit to any of these files ships silently, and every acceptance already
// recorded against that version now points at wording nobody agreed to.
//
// Pinning the bytes makes that impossible to do by accident. A deliberate
// change fails here, and clearing the failure means deciding, on purpose,
// whether the edit is a clarification that keeps the version or a change of
// substance that needs a version bump and re-acceptance.
//
// This deliberately does not pin checkbox-copy.md: the assertions above
// already tie every string it publishes to the shipped constants, and that
// file also carries implementation notes that change for non-legal reasons.
const PINNED_DOCUMENTS = [
  {
    filename: "terms-of-use.md",
    // 1.1 → 1.2 (2026-08-21): Section 14 support payments and memberships
    // (docs/plans/membership-contributions.md); Contact renumbered to 15.
    // Substance change — bundle bumps to 1.2 (CURRENT_TERMS_VERSION,
    // TERMS_VERSION, disclaimer.md) and signed-in users re-accept once.
    // Required BEFORE STRIPE_SECRET_KEY in prod.
    // 1.2 → 1.3 (2026-08-28): Section 14.5 member communications (member-only
    // newsletters/analysis reports); 14.1 carve-out excepts 14.5 while the
    // no-influence shield stays absolute. Substance change — bundle bumps to
    // 1.3, GRACE_TERMS_VERSIONS ships ["1.2"], signed-in users re-accept
    // once. Required BEFORE the first member-only communication is sent.
    // 1.3 → 1.4 (2026-08-29): operator/contracting party is now Elections
    // Simplified Inc., a Delaware corporation (previously impactperdollar).
    // Party-identity change is material — bundle bumps to 1.4,
    // GRACE_TERMS_VERSIONS ships ["1.3", "1.2"] (the 1.3 rollout was only a
    // day old), signed-in users re-accept once.
    // 1.4 wording fix (2026-09-04, docs/plans/membership-manage-page.md):
    // 14.3 cancellation path "(via the Stripe billing portal)" → "under
    // Manage membership" — navigation wording only, no change in rights, so
    // the hash is re-pinned WITHOUT a version bump or re-acceptance.
    version: "1.4",
    sha256: "5a2132a0be1fc34abf442d8ac714cb29179a35b147275518e06520f2168a93f7",
  },
  {
    filename: "privacy-policy.md",
    // 1.0 → 1.1 (2026-08-12): Ask question-log disclosure + OpenAI processor
    // entry, required BEFORE CHATBOT_LLM_ENABLED in prod (chatbot Phase 2).
    // 1.1 → 1.2 (2026-08-12): Sign in with Google disclosure (ID-token data,
    // Google processor entry, password-less account handling), required
    // BEFORE GOOGLE_OAUTH_CLIENT_ID in prod. Clarifying addition for an
    // optional feature — TERMS_VERSION stays 1.1, no re-acceptance.
    // 1.2 → 1.3 (2026-08-21): support-payments disclosure (Stripe processor
    // entry, payment data in Section 1, payment-record retention after
    // account deletion in Section 4). Ships with the Terms 1.2 bump.
    // Required BEFORE STRIPE_SECRET_KEY in prod.
    // 1.3 → 1.4 (2026-08-29): operator named as Elections Simplified Inc., a
    // Delaware corporation. Ships with the Terms 1.4 bump.
    version: "1.4",
    sha256: "bf9f7f47b87d676282d19f65c525443dd39900e8ac0e0890ed20a2ac186f2098",
  },
  {
    filename: "disclaimer.md",
    // 1.1 → 1.2 (2026-08-21): no content change; version string tracks
    // CURRENT_TERMS_VERSION, which moved for the Terms 1.2 payments section.
    // 1.2 → 1.3 (2026-08-28): no content change; tracks the Terms 1.3 bump
    // (Section 14.5 member communications).
    // 1.3 → 1.4 (2026-08-29): operator named as Elections Simplified Inc., a
    // Delaware corporation; tracks the Terms 1.4 bump.
    version: "1.4",
    sha256: "0af5f6adf2e3192d2a94fc3bb2de2e26e4418bae066c60c4d533e34253a9324c",
  },
] as const;

describe("legal documents are pinned to their versions", () => {
  it.each(PINNED_DOCUMENTS)("$filename still declares version $version", ({ filename, version }) => {
    const text = readFileSync(findLegalDoc(filename), "utf8");
    expect(text).toContain(`**Version:** ${version}`);
  });

  it.each(PINNED_DOCUMENTS)("$filename content is unchanged", ({ filename, sha256 }) => {
    const text = readFileSync(findLegalDoc(filename), "utf8");
    expect(createHash("sha256").update(text, "utf8").digest("hex")).toBe(sha256);
  });
});
