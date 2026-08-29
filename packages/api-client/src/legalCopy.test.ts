import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { describe, expect, it } from "vitest";

import {
  ADDRESS_FIELD_PRIVACY_NOTE,
  PRE_SEARCH_AGREEMENT_PARAGRAPHS,
  PRE_SEARCH_CHECKBOX_LABEL,
  PRIVACY_NOTICE,
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
    ["privacy notice", PRIVACY_NOTICE],
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

  it("keeps the full privacy disclosure separate from the short address-field note", () => {
    expect(PRIVACY_NOTICE).not.toBe(ADDRESS_FIELD_PRIVACY_NOTE);
    expect(PRIVACY_NOTICE).toContain("account information");
    expect(PRIVACY_NOTICE).toContain("device and usage information");
    expect(PRIVACY_NOTICE).toContain("Privacy Policy");
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
    version: "1.3",
    sha256: "98ffe184c3eeb470908a0f67f0efda4ca3597141268e59ce1f860f43776f5097",
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
    version: "1.3",
    sha256: "e49c8c1f273500f2a55a03ea3794f16a9888595d3c597c5fc5dd2409b7519e0d",
  },
  {
    filename: "disclaimer.md",
    // 1.1 → 1.2 (2026-08-21): no content change; version string tracks
    // CURRENT_TERMS_VERSION, which moved for the Terms 1.2 payments section.
    // 1.2 → 1.3 (2026-08-28): no content change; tracks the Terms 1.3 bump
    // (Section 14.5 member communications).
    version: "1.3",
    sha256: "630bbf2e6e55f86ac47228a9167b00323a71b23fbaf25882eb5e9ba852575d14",
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
