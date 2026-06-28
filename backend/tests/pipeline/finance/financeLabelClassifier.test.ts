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

    expect(classifyFinanceLabel({ rawLabel: "Asana", labelType: "employer" })).toMatchObject({
      normalizedLabel: "ASANA",
      industrySlug: "technology",
      confidence: "high",
      matchedRule: "organization_exact_asana",
    });

    expect(classifyFinanceLabel({ rawLabel: "Oracle Corporation", labelType: "employer" })).toMatchObject({
      normalizedLabel: "ORACLE",
      industrySlug: "technology",
      confidence: "high",
      matchedRule: "organization_exact_oracle",
    });

    expect(classifyFinanceLabel({ rawLabel: "Ripple", labelType: "employer" })).toMatchObject({
      normalizedLabel: "RIPPLE",
      industrySlug: "technology",
      confidence: "medium",
      matchedRule: "organization_exact_ripple",
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

    expect(classifyFinanceLabel({ rawLabel: "Alliance Resource Partners LP", labelType: "donor" })).toMatchObject({
      normalizedLabel: "ALLIANCE RESOURCE PARTNERS",
      industrySlug: "oil_gas_energy",
      confidence: "high",
      matchedRule: "organization_exact_alliance_resource_partners",
    });

    expect(classifyFinanceLabel({ rawLabel: "Andreessen Horowitz", labelType: "employer" })).toMatchObject({
      normalizedLabel: "ANDREESSEN HOROWITZ",
      industrySlug: "finance_investment",
      confidence: "high",
      matchedRule: "organization_exact_andreessen_horowitz",
    });

    expect(classifyFinanceLabel({ rawLabel: "Valor Equity Partners", labelType: "employer" })).toMatchObject({
      normalizedLabel: "VALOR EQUITY PARTNERS",
      industrySlug: "finance_investment",
      confidence: "high",
      matchedRule: "organization_exact_valor_equity_partners",
    });

    expect(classifyFinanceLabel({ rawLabel: "Elliott Management Corp", labelType: "employer" })).toMatchObject({
      normalizedLabel: "ELLIOTT MANAGEMENT",
      industrySlug: "finance_investment",
      confidence: "high",
      matchedRule: "organization_exact_elliott_management",
    });

    expect(classifyFinanceLabel({ rawLabel: "Bloomberg Inc.", labelType: "employer" })).toMatchObject({
      normalizedLabel: "BLOOMBERG",
      industrySlug: "finance_investment",
      confidence: "medium",
      matchedRule: "organization_exact_bloomberg",
    });

    expect(classifyFinanceLabel({ rawLabel: "Walton Enterprises Inc", labelType: "employer" })).toMatchObject({
      normalizedLabel: "WALTON ENTERPRISES",
      industrySlug: "finance_investment",
      confidence: "high",
      matchedRule: "organization_exact_walton_enterprises",
    });

    expect(classifyFinanceLabel({ rawLabel: "America First Credit Union", labelType: "donor" })).toMatchObject({
      normalizedLabel: "AMERICA FIRST CREDIT UNION",
      industrySlug: "finance_investment",
      confidence: "medium",
      matchedRule: "organization_pattern_credit_union",
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

  it("classifies pharmaceuticals and labor unions as separate voter-facing categories", () => {
    expect(classifyFinanceLabel({ rawLabel: "Pfizer Inc.", labelType: "employer" })).toMatchObject({
      normalizedLabel: "PFIZER",
      industrySlug: "pharmaceuticals",
      confidence: "high",
      matchedRule: "organization_exact_pfizer",
    });

    expect(classifyFinanceLabel({ rawLabel: "Johnson & Johnson Innovation", labelType: "employer" })).toMatchObject({
      normalizedLabel: "JOHNSON AND JOHNSON INNOVATION",
      industrySlug: "pharmaceuticals",
      confidence: "medium",
      matchedRule: "organization_pattern_pharmaceuticals",
    });

    expect(classifyFinanceLabel({ rawLabel: "SEIU Committee on Political Education", labelType: "donor" })).toMatchObject({
      normalizedLabel: "SEIU COMMITTEE ON POLITICAL EDUCATION",
      industrySlug: "labor_unions",
      confidence: "medium",
      matchedRule: "organization_pattern_labor_unions",
    });

    expect(classifyFinanceLabel({ rawLabel: "AFL-CIO", labelType: "donor" })).toMatchObject({
      normalizedLabel: "AFL CIO",
      industrySlug: "labor_unions",
      confidence: "high",
      matchedRule: "organization_exact_afl_cio",
    });
  });

  it("classifies clear transportation, manufacturing, and agriculture organizations", () => {
    expect(classifyFinanceLabel({ rawLabel: "American Airlines", labelType: "employer" })).toMatchObject({
      normalizedLabel: "AMERICAN AIRLINES",
      industrySlug: "transportation",
      confidence: "high",
      matchedRule: "organization_exact_american_airlines",
    });

    expect(classifyFinanceLabel({ rawLabel: "Haworth Inc.", labelType: "employer" })).toMatchObject({
      normalizedLabel: "HAWORTH",
      industrySlug: "manufacturing",
      confidence: "high",
      matchedRule: "organization_exact_haworth",
    });

    expect(classifyFinanceLabel({ rawLabel: "Mountaire Farms", labelType: "employer" })).toMatchObject({
      normalizedLabel: "MOUNTAIRE FARMS",
      industrySlug: "agriculture_and_food",
      confidence: "high",
      matchedRule: "organization_exact_mountaire_farms",
    });

    expect(classifyFinanceLabel({ rawLabel: "Hendricks Holding Co Inc", labelType: "employer" })).toMatchObject({
      normalizedLabel: "HENDRICKS HOLDING",
      industrySlug: "construction",
      confidence: "medium",
      matchedRule: "organization_exact_hendricks_holding",
    });

    expect(classifyFinanceLabel({ rawLabel: "Hawaii Carpenters Market Recovery Program Fund", labelType: "donor" })).toMatchObject({
      normalizedLabel: "HAWAII CARPENTERS MARKET RECOVERY PROGRAM FUND",
      industrySlug: "construction",
      confidence: "medium",
      matchedRule: "organization_pattern_construction",
    });
  });

  it("classifies chamber-style business associations without treating generic PACs as industries", () => {
    expect(classifyFinanceLabel({ rawLabel: "KY Chamber Advocacy Committee", labelType: "donor" })).toMatchObject({
      normalizedLabel: "KY CHAMBER ADVOCACY COMMITTEE",
      industrySlug: "business_associations",
      confidence: "high",
      matchedRule: "organization_exact_ky_chamber_advocacy",
    });

    expect(classifyFinanceLabel({ rawLabel: "Local Chamber of Commerce PAC", labelType: "donor" })).toMatchObject({
      normalizedLabel: "LOCAL CHAMBER OF COMMERCE PAC",
      industrySlug: "business_associations",
      confidence: "medium",
      matchedRule: "organization_pattern_business_associations",
    });

    expect(classifyFinanceLabel({ rawLabel: "American Conservative Fund", labelType: "donor" })).toMatchObject({
      normalizedLabel: "AMERICAN CONSERVATIVE FUND",
      industrySlug: null,
      confidence: "unknown",
      classificationSource: "unknown",
    });
  });

  it("classifies waste management organizations separately from environmental advocacy groups", () => {
    expect(classifyFinanceLabel({ rawLabel: "Southern Waste Systems", labelType: "employer" })).toMatchObject({
      normalizedLabel: "SOUTHERN WASTE SYSTEMS",
      industrySlug: "waste_management",
      confidence: "medium",
      matchedRule: "organization_exact_southern_waste_systems",
    });

    expect(classifyFinanceLabel({ rawLabel: "Acme Waste Management", labelType: "employer" })).toMatchObject({
      normalizedLabel: "ACME WASTE MANAGEMENT",
      industrySlug: "waste_management",
      confidence: "medium",
      matchedRule: "organization_pattern_waste_management",
    });
  });

  it("classifies occupations when the label is specific enough", () => {
    expect(classifyFinanceLabel({ rawLabel: "Attorney", labelType: "occupation" })).toMatchObject({
      industrySlug: "lawyers_and_legal_services",
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

    expect(classifyFinanceLabel({ rawLabel: "Farmer", labelType: "occupation" })).toMatchObject({
      industrySlug: "agriculture_and_food",
      confidence: "high",
      matchedRule: "occupation_exact_farmer",
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

    expect(classifyFinanceLabel({ rawLabel: "Self-employed", labelType: "employer" })).toMatchObject({
      normalizedLabel: "SELF EMPLOYED",
      industrySlug: null,
      confidence: "high",
      classificationSource: "rule",
      matchedRule: "organization_known_non_industry",
    });

    expect(classifyFinanceLabel({ rawLabel: "Information Requested", labelType: "employer" })).toMatchObject({
      normalizedLabel: "INFORMATION REQUESTED",
      industrySlug: null,
      confidence: "high",
      classificationSource: "rule",
      matchedRule: "organization_known_non_industry",
    });

    expect(classifyFinanceLabel({ rawLabel: "USPS", labelType: "employer" })).toMatchObject({
      normalizedLabel: "USPS",
      industrySlug: null,
      confidence: "high",
      classificationSource: "rule",
      matchedRule: "organization_known_non_industry",
    });

    expect(classifyFinanceLabel({ rawLabel: "America First Policy Institute", labelType: "employer" })).toMatchObject({
      normalizedLabel: "AMERICA FIRST POLICY INSTITUTE",
      industrySlug: null,
      confidence: "high",
      classificationSource: "rule",
      matchedRule: "organization_known_non_industry",
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
