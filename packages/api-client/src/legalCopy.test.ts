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
function findCheckboxCopyDoc(): string {
  let directory = process.cwd();
  for (;;) {
    const candidate = resolve(directory, "docs/legal/checkbox-copy.md");
    if (existsSync(candidate)) {
      return candidate;
    }
    const parent = dirname(directory);
    if (parent === directory) {
      throw new Error("Could not locate docs/legal/checkbox-copy.md above the working directory");
    }
    directory = parent;
  }
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
});
