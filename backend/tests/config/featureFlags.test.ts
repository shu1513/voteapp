import { afterEach, describe, expect, it } from "vitest";

import {
  isCaliforniaCampaignFinanceEnabled,
  isCaliforniaCampaignFinanceSyncEnabled,
  isPresidentialElectionsEnabled,
  isPresidentialFeatureEnabled,
} from "../../src/config/featureFlags.js";

const ORIGINAL_VALUE = process.env.PRESIDENTIAL_ELECTIONS_ENABLED;
const ORIGINAL_ROSTER_VALUE = process.env.PRESIDENTIAL_ROSTER_RESEARCH_ENABLED;
const ORIGINAL_CALIFORNIA_FINANCE_VALUE = process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_CALIFORNIA_FINANCE_SYNC_VALUE = process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED;

describe("featureFlags", () => {
  afterEach(() => {
    if (ORIGINAL_VALUE === undefined) {
      delete process.env.PRESIDENTIAL_ELECTIONS_ENABLED;
    } else {
      process.env.PRESIDENTIAL_ELECTIONS_ENABLED = ORIGINAL_VALUE;
    }
    if (ORIGINAL_ROSTER_VALUE === undefined) {
      delete process.env.PRESIDENTIAL_ROSTER_RESEARCH_ENABLED;
    } else {
      process.env.PRESIDENTIAL_ROSTER_RESEARCH_ENABLED = ORIGINAL_ROSTER_VALUE;
    }
    if (ORIGINAL_CALIFORNIA_FINANCE_VALUE === undefined) {
      delete process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_CALIFORNIA_FINANCE_VALUE;
    }
    if (ORIGINAL_CALIFORNIA_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_CALIFORNIA_FINANCE_SYNC_VALUE;
    }
  });

  it("enables presidential elections by default", () => {
    delete process.env.PRESIDENTIAL_ELECTIONS_ENABLED;

    expect(isPresidentialElectionsEnabled()).toBe(true);
  });

  it("reads enabled boolean values", () => {
    for (const value of ["true", "1", "yes", "y", "on", " TRUE "]) {
      process.env.PRESIDENTIAL_ELECTIONS_ENABLED = value;

      expect(isPresidentialElectionsEnabled()).toBe(true);
    }
  });

  it("reads disabled boolean values", () => {
    for (const value of ["false", "0", "no", "n", "off", " FALSE "]) {
      process.env.PRESIDENTIAL_ELECTIONS_ENABLED = value;

      expect(isPresidentialElectionsEnabled()).toBe(false);
    }
  });

  it("treats blank values as the default", () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "   ";

    expect(isPresidentialElectionsEnabled()).toBe(true);
  });

  it("rejects invalid boolean values", () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "maybe";

    expect(() => isPresidentialElectionsEnabled()).toThrow(
      "Invalid boolean env PRESIDENTIAL_ELECTIONS_ENABLED: maybe"
    );
  });

  it("requires the master presidential flag before a specific feature can run", () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "false";
    process.env.PRESIDENTIAL_ROSTER_RESEARCH_ENABLED = "true";

    expect(isPresidentialFeatureEnabled("PRESIDENTIAL_ROSTER_RESEARCH_ENABLED")).toBe(false);
    expect(isPresidentialFeatureEnabled("PRESIDENTIAL_ROSTER_RESEARCH_ENABLED", true)).toBe(false);
  });

  it("allows force to bypass only the specific feature flag", () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "true";
    process.env.PRESIDENTIAL_ROSTER_RESEARCH_ENABLED = "false";

    expect(isPresidentialFeatureEnabled("PRESIDENTIAL_ROSTER_RESEARCH_ENABLED")).toBe(false);
    expect(isPresidentialFeatureEnabled("PRESIDENTIAL_ROSTER_RESEARCH_ENABLED", true)).toBe(true);
  });

  it("disables California campaign finance by default", () => {
    delete process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED;

    expect(isCaliforniaCampaignFinanceEnabled()).toBe(false);
    expect(isCaliforniaCampaignFinanceSyncEnabled()).toBe(false);
  });

  it("requires the California campaign finance master flag before sync can run", () => {
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isCaliforniaCampaignFinanceEnabled()).toBe(false);
    expect(isCaliforniaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isCaliforniaCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the California campaign finance sync flag", () => {
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isCaliforniaCampaignFinanceEnabled()).toBe(true);
    expect(isCaliforniaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isCaliforniaCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables California campaign finance sync when both flags are enabled", () => {
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isCaliforniaCampaignFinanceEnabled()).toBe(true);
    expect(isCaliforniaCampaignFinanceSyncEnabled()).toBe(true);
  });
});
