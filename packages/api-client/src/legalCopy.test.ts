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
    version: "1.1",
    sha256: "7068b1f66de681e0a5fb09b22652bfa1fdd1463993c30987bf6ecb3659e329a3",
  },
  {
    filename: "privacy-policy.md",
    // 1.0 → 1.1 (2026-08-12): Ask question-log disclosure + OpenAI processor
    // entry, required BEFORE CHATBOT_LLM_ENABLED in prod (chatbot Phase 2).
    // 1.1 → 1.2 (2026-08-12): Sign in with Google disclosure (ID-token data,
    // Google processor entry, password-less account handling), required
    // BEFORE GOOGLE_OAUTH_CLIENT_ID in prod. Clarifying addition for an
    // optional feature — TERMS_VERSION stays 1.1, no re-acceptance.
    version: "1.2",
    sha256: "8c9760e960c5bf4135488b6378920dbdf519bfc00133df3afdc141b5f3f99e8f",
  },
  {
    filename: "disclaimer.md",
    version: "1.1",
    sha256: "7a4ad2f12d23c537712b743e86f6c2caa74a311e7aba86c99d6f4028ff51a542",
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
