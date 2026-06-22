import { afterEach, describe, expect, it } from "vitest";

import {
  isCaliforniaCampaignFinanceEnabled,
  isCaliforniaCampaignFinanceRawDataRefreshEnabled,
  isCaliforniaCampaignFinanceSyncEnabled,
  isColoradoCampaignFinanceEnabled,
  isColoradoCampaignFinanceSyncEnabled,
  isColoradoTracerRawDataRefreshEnabled,
  isConnecticutCampaignFinanceEnabled,
  isConnecticutCampaignFinanceSyncEnabled,
  isConnecticutEcrisRawDataRefreshEnabled,
  isNewMexicoCampaignFinanceEnabled,
  isNewMexicoCampaignFinanceSyncEnabled,
  isNewMexicoCfisRawDataRefreshEnabled,
  isPresidentialElectionsEnabled,
  isPresidentialFeatureEnabled,
} from "../../src/config/featureFlags.js";

const ORIGINAL_VALUE = process.env.PRESIDENTIAL_ELECTIONS_ENABLED;
const ORIGINAL_ROSTER_VALUE = process.env.PRESIDENTIAL_ROSTER_RESEARCH_ENABLED;
const ORIGINAL_CALIFORNIA_FINANCE_VALUE = process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_CALIFORNIA_FINANCE_SYNC_VALUE = process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_CALIFORNIA_RAW_REFRESH_VALUE = process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED;
const ORIGINAL_COLORADO_FINANCE_VALUE = process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_COLORADO_FINANCE_SYNC_VALUE = process.env.COLORADO_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_COLORADO_RAW_REFRESH_VALUE = process.env.COLORADO_TRACER_RAW_DATA_REFRESH_ENABLED;
const ORIGINAL_CONNECTICUT_FINANCE_VALUE = process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_CONNECTICUT_FINANCE_SYNC_VALUE = process.env.CONNECTICUT_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_CONNECTICUT_RAW_REFRESH_VALUE = process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED;
const ORIGINAL_NEW_MEXICO_FINANCE_VALUE = process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_NEW_MEXICO_FINANCE_SYNC_VALUE = process.env.NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_NEW_MEXICO_RAW_REFRESH_VALUE = process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_ENABLED;

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
    if (ORIGINAL_CALIFORNIA_RAW_REFRESH_VALUE === undefined) {
      delete process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED;
    } else {
      process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = ORIGINAL_CALIFORNIA_RAW_REFRESH_VALUE;
    }
    if (ORIGINAL_COLORADO_FINANCE_VALUE === undefined) {
      delete process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_COLORADO_FINANCE_VALUE;
    }
    if (ORIGINAL_COLORADO_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.COLORADO_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.COLORADO_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_COLORADO_FINANCE_SYNC_VALUE;
    }
    if (ORIGINAL_COLORADO_RAW_REFRESH_VALUE === undefined) {
      delete process.env.COLORADO_TRACER_RAW_DATA_REFRESH_ENABLED;
    } else {
      process.env.COLORADO_TRACER_RAW_DATA_REFRESH_ENABLED = ORIGINAL_COLORADO_RAW_REFRESH_VALUE;
    }
    if (ORIGINAL_CONNECTICUT_FINANCE_VALUE === undefined) {
      delete process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_CONNECTICUT_FINANCE_VALUE;
    }
    if (ORIGINAL_CONNECTICUT_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.CONNECTICUT_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.CONNECTICUT_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_CONNECTICUT_FINANCE_SYNC_VALUE;
    }
    if (ORIGINAL_CONNECTICUT_RAW_REFRESH_VALUE === undefined) {
      delete process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED;
    } else {
      process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED = ORIGINAL_CONNECTICUT_RAW_REFRESH_VALUE;
    }
    if (ORIGINAL_NEW_MEXICO_FINANCE_VALUE === undefined) {
      delete process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_NEW_MEXICO_FINANCE_VALUE;
    }
    if (ORIGINAL_NEW_MEXICO_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_NEW_MEXICO_FINANCE_SYNC_VALUE;
    }
    if (ORIGINAL_NEW_MEXICO_RAW_REFRESH_VALUE === undefined) {
      delete process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_ENABLED;
    } else {
      process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_ENABLED = ORIGINAL_NEW_MEXICO_RAW_REFRESH_VALUE;
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

  it("requires the California campaign finance master flag before raw data refresh can run", () => {
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = "true";

    expect(isCaliforniaCampaignFinanceRawDataRefreshEnabled()).toBe(false);
    expect(isCaliforniaCampaignFinanceRawDataRefreshEnabled(true)).toBe(false);
  });

  it("disables Colorado campaign finance by default", () => {
    delete process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.COLORADO_CAMPAIGN_FINANCE_SYNC_ENABLED;
    delete process.env.COLORADO_TRACER_RAW_DATA_REFRESH_ENABLED;

    expect(isColoradoCampaignFinanceEnabled()).toBe(false);
    expect(isColoradoCampaignFinanceSyncEnabled()).toBe(false);
    expect(isColoradoTracerRawDataRefreshEnabled()).toBe(false);
  });

  it("requires the Colorado campaign finance master flag before sync can run", () => {
    process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.COLORADO_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isColoradoCampaignFinanceEnabled()).toBe(false);
    expect(isColoradoCampaignFinanceSyncEnabled()).toBe(false);
    expect(isColoradoCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Colorado campaign finance sync flag", () => {
    process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.COLORADO_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isColoradoCampaignFinanceEnabled()).toBe(true);
    expect(isColoradoCampaignFinanceSyncEnabled()).toBe(false);
    expect(isColoradoCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables Colorado campaign finance sync when both flags are enabled", () => {
    process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.COLORADO_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isColoradoCampaignFinanceEnabled()).toBe(true);
    expect(isColoradoCampaignFinanceSyncEnabled()).toBe(true);
  });

  it("requires the Colorado campaign finance master flag before TRACER raw data refresh can run", () => {
    process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.COLORADO_TRACER_RAW_DATA_REFRESH_ENABLED = "true";

    expect(isColoradoTracerRawDataRefreshEnabled()).toBe(false);
    expect(isColoradoTracerRawDataRefreshEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Colorado TRACER raw data refresh flag", () => {
    process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.COLORADO_TRACER_RAW_DATA_REFRESH_ENABLED = "false";

    expect(isColoradoTracerRawDataRefreshEnabled()).toBe(false);
    expect(isColoradoTracerRawDataRefreshEnabled(true)).toBe(true);
  });

  it("disables Connecticut campaign finance by default", () => {
    delete process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.CONNECTICUT_CAMPAIGN_FINANCE_SYNC_ENABLED;
    delete process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED;

    expect(isConnecticutCampaignFinanceEnabled()).toBe(false);
    expect(isConnecticutCampaignFinanceSyncEnabled()).toBe(false);
    expect(isConnecticutEcrisRawDataRefreshEnabled()).toBe(false);
  });

  it("requires the Connecticut campaign finance master flag before sync can run", () => {
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isConnecticutCampaignFinanceEnabled()).toBe(false);
    expect(isConnecticutCampaignFinanceSyncEnabled()).toBe(false);
    expect(isConnecticutCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Connecticut campaign finance sync flag", () => {
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isConnecticutCampaignFinanceEnabled()).toBe(true);
    expect(isConnecticutCampaignFinanceSyncEnabled()).toBe(false);
    expect(isConnecticutCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables Connecticut campaign finance sync when both flags are enabled", () => {
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isConnecticutCampaignFinanceEnabled()).toBe(true);
    expect(isConnecticutCampaignFinanceSyncEnabled()).toBe(true);
  });

  it("requires the Connecticut campaign finance master flag before eCRIS raw data refresh can run", () => {
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED = "true";

    expect(isConnecticutEcrisRawDataRefreshEnabled()).toBe(false);
    expect(isConnecticutEcrisRawDataRefreshEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Connecticut eCRIS raw data refresh flag", () => {
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED = "false";

    expect(isConnecticutEcrisRawDataRefreshEnabled()).toBe(false);
    expect(isConnecticutEcrisRawDataRefreshEnabled(true)).toBe(true);
  });

  it("disables New Mexico campaign finance by default", () => {
    delete process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED;
    delete process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_ENABLED;

    expect(isNewMexicoCampaignFinanceEnabled()).toBe(false);
    expect(isNewMexicoCampaignFinanceSyncEnabled()).toBe(false);
    expect(isNewMexicoCfisRawDataRefreshEnabled()).toBe(false);
  });

  it("requires the New Mexico campaign finance master flag before sync can run", () => {
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isNewMexicoCampaignFinanceEnabled()).toBe(false);
    expect(isNewMexicoCampaignFinanceSyncEnabled()).toBe(false);
    expect(isNewMexicoCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the New Mexico campaign finance sync flag", () => {
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isNewMexicoCampaignFinanceEnabled()).toBe(true);
    expect(isNewMexicoCampaignFinanceSyncEnabled()).toBe(false);
    expect(isNewMexicoCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables New Mexico campaign finance sync when both flags are enabled", () => {
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isNewMexicoCampaignFinanceEnabled()).toBe(true);
    expect(isNewMexicoCampaignFinanceSyncEnabled()).toBe(true);
  });

  it("requires the New Mexico campaign finance master flag before CFIS raw data refresh can run", () => {
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_ENABLED = "true";

    expect(isNewMexicoCfisRawDataRefreshEnabled()).toBe(false);
    expect(isNewMexicoCfisRawDataRefreshEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the New Mexico CFIS raw data refresh flag", () => {
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_ENABLED = "false";

    expect(isNewMexicoCfisRawDataRefreshEnabled()).toBe(false);
    expect(isNewMexicoCfisRawDataRefreshEnabled(true)).toBe(true);
  });
});
