import { describe, expect, it } from "vitest";

import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
} from "../../../src/pipeline/finance/financeLabelClassifier.js";

describe("financeLabelClassifier", () => {
  it("normalizes labels for stable classification keys", () => {
    expect(normalizeFinanceLabel(" Google, Inc. ", "employer")).toBe("GOOGLE");
    expect(normalizeFinanceLabel("Energy Transfer LP", "donor")).toBe("ENERGY TRANSFER");
    expect(normalizeFinanceLabel("Oil & Gas", "occupation")).toBe("OIL AND GAS");
  });

  it("classifies known technology employers conservatively", () => {
    expect(classifyFinanceLabel({ rawLabel: "Google LLC", labelType: "employer" })).toMatchObject({
      normalizedLabel: "GOOGLE",
      industrySlug: "technology",
      confidence: "high",
      classificationSource: "rule",
      matchedRule: "organization_exact_google",
    });

    expect(classifyFinanceLabel({ rawLabel: "Acme Cloud Technologies", labelType: "employer" })).toMatchObject({
      normalizedLabel: "ACME CLOUD TECHNOLOGIES",
      industrySlug: "technology",
      confidence: "medium",
      classificationSource: "rule",
      matchedRule: "organization_pattern_technology",
    });
  });

  it("classifies known energy and finance organizations", () => {
    expect(classifyFinanceLabel({ rawLabel: "Energy Transfer LP", labelType: "donor" })).toMatchObject({
      normalizedLabel: "ENERGY TRANSFER",
      industrySlug: "oil_gas_energy",
      confidence: "high",
      matchedRule: "organization_exact_energy_transfer",
    });

    expect(classifyFinanceLabel({ rawLabel: "Cantor Fitzgerald", labelType: "employer" })).toMatchObject({
      normalizedLabel: "CANTOR FITZGERALD",
      industrySlug: "finance_investment",
      confidence: "high",
      matchedRule: "organization_exact_cantor",
    });
  });

  it("classifies clear environmental organizations", () => {
    expect(classifyFinanceLabel({ rawLabel: "Sierra Club", labelType: "employer" })).toMatchObject({
      normalizedLabel: "SIERRA CLUB",
      industrySlug: "environmental_group",
      confidence: "high",
      matchedRule: "organization_exact_sierra_club",
    });

    expect(classifyFinanceLabel({ rawLabel: "Clean Climate Conservation Fund", labelType: "donor" })).toMatchObject({
      normalizedLabel: "CLEAN CLIMATE CONSERVATION FUND",
      industrySlug: "environmental_group",
      confidence: "medium",
      matchedRule: "organization_pattern_environmental_group",
    });
  });

  it("classifies occupations when the label is specific enough", () => {
    expect(classifyFinanceLabel({ rawLabel: "Attorney", labelType: "occupation" })).toMatchObject({
      industrySlug: "legal",
      confidence: "high",
      matchedRule: "occupation_exact_attorney",
    });

    expect(classifyFinanceLabel({ rawLabel: "Software Engineer", labelType: "occupation" })).toMatchObject({
      industrySlug: "technology",
      confidence: "high",
      matchedRule: "occupation_exact_software_engineer",
    });

    expect(classifyFinanceLabel({ rawLabel: "Oil & Gas Producer", labelType: "occupation" })).toMatchObject({
      normalizedLabel: "OIL AND GAS PRODUCER",
      industrySlug: "oil_gas_energy",
      confidence: "medium",
      matchedRule: "occupation_pattern_energy",
    });
  });

  it("recognizes non-industry occupation buckets without forcing them into an industry", () => {
    expect(classifyFinanceLabel({ rawLabel: "Retired", labelType: "occupation" })).toMatchObject({
      normalizedLabel: "RETIRED",
      industrySlug: null,
      confidence: "high",
      classificationSource: "rule",
      matchedRule: "occupation_known_non_industry",
    });

    expect(classifyFinanceLabel({ rawLabel: "Not Employed", labelType: "occupation" })).toMatchObject({
      normalizedLabel: "NOT EMPLOYED",
      industrySlug: null,
      confidence: "high",
      classificationSource: "rule",
    });
  });

  it("does not treat outside political committees as industries", () => {
    expect(classifyFinanceLabel({ rawLabel: "Make America Great Again Inc.", labelType: "committee" })).toMatchObject({
      normalizedLabel: "MAKE AMERICA GREAT AGAIN",
      industrySlug: null,
      confidence: "unknown",
      classificationSource: "unknown",
      matchedRule: null,
    });

    expect(classifyFinanceLabel({ rawLabel: "America PAC", labelType: "committee" })).toMatchObject({
      normalizedLabel: "AMERICA PAC",
      industrySlug: null,
      confidence: "unknown",
      classificationSource: "unknown",
    });
  });

  it("leaves ambiguous broad labels unknown", () => {
    expect(classifyFinanceLabel({ rawLabel: "CEO", labelType: "occupation" })).toMatchObject({
      normalizedLabel: "CEO",
      industrySlug: null,
      confidence: "unknown",
      classificationSource: "unknown",
    });

    expect(classifyFinanceLabel({ rawLabel: "Human Resources", labelType: "employer" })).toMatchObject({
      normalizedLabel: "HUMAN RESOURCES",
      industrySlug: null,
      confidence: "unknown",
      classificationSource: "unknown",
    });
  });
});
