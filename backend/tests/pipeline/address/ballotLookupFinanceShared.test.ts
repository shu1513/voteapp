import { describe, expect, it } from "vitest";

import {
  FINANCE_INDUSTRY_DISPLAY_NAMES,
  buildOutsideIndustrySupportExplanation,
  type BallotLookupFinanceOutsideIndustrySupportEvidence,
} from "../../../src/pipeline/address/ballotLookupFinanceShared.js";
import { FINANCE_INDUSTRY_SLUGS } from "../../../src/pipeline/finance/financeLabelClassifier.js";

describe("FINANCE_INDUSTRY_DISPLAY_NAMES", () => {
  it("has a display name for every industry slug", () => {
    // The api-client's FINANCE_CATEGORY_LABELS mirrors this map by hand (the
    // package cannot import backend code); keeping this map complete is what
    // makes that mirror auditable.
    for (const slug of FINANCE_INDUSTRY_SLUGS) {
      expect(FINANCE_INDUSTRY_DISPLAY_NAMES[slug], `missing display name for ${slug}`).toBeTruthy();
    }
  });
});

function evidenceRow(
  overrides: Partial<BallotLookupFinanceOutsideIndustrySupportEvidence>
): BallotLookupFinanceOutsideIndustrySupportEvidence {
  return {
    organization_name: "Org",
    organization_type: "donor",
    amount: 1000,
    contributor_count: null,
    committee_id: "C1",
    committee_name: "Committee",
    source_url: null,
    ...overrides,
  };
}

describe("buildOutsideIndustrySupportExplanation", () => {
  it("keeps the generic sentence when there is no evidence", () => {
    expect(buildOutsideIndustrySupportExplanation("technology", [])).toBe(
      "The Technology category is a top outside-spending support industry because organizations classified in this industry contributed to outside groups that reported independent spending supporting this candidate."
    );
  });

  it("phrases donor evidence as the organization's own money", () => {
    expect(
      buildOutsideIndustrySupportExplanation("technology", [
        evidenceRow({ organization_name: "Acme Corp", committee_name: "Future PAC" }),
      ])
    ).toBe(
      "The Technology category is a top outside-spending support industry because Acme Corp contributed to Future PAC, which reported independent spending supporting this candidate."
    );
  });

  it("never phrases employer evidence as the company itself donating", () => {
    expect(
      buildOutsideIndustrySupportExplanation("technology", [
        evidenceRow({
          organization_name: "Google LLC",
          organization_type: "employer",
          committee_name: "Support Candidate PAC",
        }),
      ])
    ).toBe(
      "The Technology category is a top outside-spending support industry because contributors employed by Google LLC contributed to Support Candidate PAC, which reported independent spending supporting this candidate."
    );
  });

  it("combines donor and employer evidence for the same committee", () => {
    expect(
      buildOutsideIndustrySupportExplanation("technology", [
        evidenceRow({ organization_name: "Acme Corp", committee_name: "Future PAC" }),
        evidenceRow({
          organization_name: "Google LLC",
          organization_type: "employer",
          committee_name: "Future PAC",
        }),
      ])
    ).toBe(
      "The Technology category is a top outside-spending support industry because Acme Corp, and contributors employed by Google LLC contributed to Future PAC, which reported independent spending supporting this candidate."
    );
  });

  it("keeps each organization paired with the committee it funded", () => {
    expect(
      buildOutsideIndustrySupportExplanation("technology", [
        evidenceRow({ organization_name: "Acme Corp", committee_name: "Future PAC" }),
        evidenceRow({
          organization_name: "Google LLC",
          organization_type: "employer",
          committee_name: "Other PAC",
        }),
      ])
    ).toBe(
      "The Technology category is a top outside-spending support industry because Acme Corp contributed to Future PAC; contributors employed by Google LLC contributed to Other PAC; all of these groups reported independent spending supporting this candidate."
    );
  });

  it("threads a custom support action through", () => {
    expect(
      buildOutsideIndustrySupportExplanation(
        "labor_unions",
        [evidenceRow({ organization_name: "Local 99", committee_name: "Workers PAC" })],
        "PAC contributions supporting this candidate"
      )
    ).toBe(
      "The Labor unions category is a top outside-spending support industry because Local 99 contributed to Workers PAC, which reported PAC contributions supporting this candidate."
    );
  });

  it("falls back when the committee name is blank", () => {
    expect(
      buildOutsideIndustrySupportExplanation("technology", [
        evidenceRow({ organization_name: "Acme Corp", committee_name: "  " }),
      ])
    ).toBe(
      "The Technology category is a top outside-spending support industry because Acme Corp contributed to an outside group, which reported independent spending supporting this candidate."
    );
  });
});
