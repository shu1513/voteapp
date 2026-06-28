import { afterEach, describe, expect, it } from "vitest";

import {
  isAlaskaCampaignFinanceEnabled,
  isAlaskaCampaignFinanceSyncEnabled,
  isArizonaCampaignFinanceEnabled,
  isArizonaCampaignFinanceSyncEnabled,
  isCaliforniaCampaignFinanceEnabled,
  isCaliforniaCampaignFinanceRawDataRefreshEnabled,
  isCaliforniaCampaignFinanceSyncEnabled,
  isColoradoCampaignFinanceEnabled,
  isColoradoCampaignFinanceSyncEnabled,
  isColoradoTracerRawDataRefreshEnabled,
  isConnecticutCampaignFinanceEnabled,
  isConnecticutCampaignFinanceSyncEnabled,
  isConnecticutEcrisRawDataRefreshEnabled,
  isDistrictOfColumbiaCampaignFinanceEnabled,
  isDistrictOfColumbiaCampaignFinanceSyncEnabled,
  isFloridaCampaignFinanceBrowserExportEnabled,
  isFloridaCampaignFinanceEnabled,
  isFloridaCampaignFinanceSyncEnabled,
  isIllinoisCampaignFinanceEnabled,
  isIllinoisCampaignFinanceSyncEnabled,
  isIndianaCampaignFinanceEnabled,
  isIndianaCampaignFinanceRawDataRefreshEnabled,
  isIndianaCampaignFinanceSyncEnabled,
  isKentuckyCampaignFinanceEnabled,
  isKentuckyCampaignFinanceSyncEnabled,
  isMaineCampaignFinanceEnabled,
  isMaineCampaignFinanceSyncEnabled,
  isMaineCfisRawDataRefreshEnabled,
  isNewMexicoCampaignFinanceEnabled,
  isNewMexicoCampaignFinanceSyncEnabled,
  isNewMexicoCfisRawDataRefreshEnabled,
  isOklahomaCampaignFinanceEnabled,
  isOklahomaCampaignFinanceSyncEnabled,
  isOklahomaGuardianRawDataRefreshEnabled,
  isPresidentialElectionsEnabled,
  isPresidentialFeatureEnabled,
  isTennesseeCampaignFinanceEnabled,
  isTennesseeCampaignFinanceSyncEnabled,
  isTexasCampaignFinanceEnabled,
  isTexasCampaignFinanceSyncEnabled,
  isTexasTecRawDataRefreshEnabled,
  isWashingtonCampaignFinanceEnabled,
  isWashingtonCampaignFinanceSyncEnabled,
} from "../../src/config/featureFlags.js";

const ORIGINAL_VALUE = process.env.PRESIDENTIAL_ELECTIONS_ENABLED;
const ORIGINAL_ROSTER_VALUE = process.env.PRESIDENTIAL_ROSTER_RESEARCH_ENABLED;
const ORIGINAL_ARIZONA_FINANCE_VALUE = process.env.ARIZONA_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_ARIZONA_FINANCE_SYNC_VALUE = process.env.ARIZONA_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_CALIFORNIA_FINANCE_VALUE = process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_CALIFORNIA_FINANCE_SYNC_VALUE = process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_CALIFORNIA_RAW_REFRESH_VALUE = process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED;
const ORIGINAL_ALASKA_FINANCE_VALUE = process.env.ALASKA_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_ALASKA_FINANCE_SYNC_VALUE = process.env.ALASKA_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_COLORADO_FINANCE_VALUE = process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_COLORADO_FINANCE_SYNC_VALUE = process.env.COLORADO_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_COLORADO_RAW_REFRESH_VALUE = process.env.COLORADO_TRACER_RAW_DATA_REFRESH_ENABLED;
const ORIGINAL_CONNECTICUT_FINANCE_VALUE = process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_CONNECTICUT_FINANCE_SYNC_VALUE = process.env.CONNECTICUT_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_CONNECTICUT_RAW_REFRESH_VALUE = process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED;
const ORIGINAL_INDIANA_FINANCE_VALUE = process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_INDIANA_FINANCE_SYNC_VALUE = process.env.INDIANA_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_INDIANA_RAW_REFRESH_VALUE = process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED;
const ORIGINAL_NEW_MEXICO_FINANCE_VALUE = process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_NEW_MEXICO_FINANCE_SYNC_VALUE = process.env.NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_NEW_MEXICO_RAW_REFRESH_VALUE = process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_ENABLED;
const ORIGINAL_MAINE_FINANCE_VALUE = process.env.MAINE_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_MAINE_FINANCE_SYNC_VALUE = process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_MAINE_RAW_REFRESH_VALUE = process.env.MAINE_CFIS_RAW_DATA_REFRESH_ENABLED;
const ORIGINAL_OKLAHOMA_FINANCE_VALUE = process.env.OKLAHOMA_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_OKLAHOMA_FINANCE_SYNC_VALUE = process.env.OKLAHOMA_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_OKLAHOMA_RAW_REFRESH_VALUE = process.env.OKLAHOMA_GUARDIAN_RAW_DATA_REFRESH_ENABLED;
const ORIGINAL_TENNESSEE_FINANCE_VALUE = process.env.TENNESSEE_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_TENNESSEE_FINANCE_SYNC_VALUE = process.env.TENNESSEE_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_TEXAS_FINANCE_VALUE = process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_TEXAS_FINANCE_SYNC_VALUE = process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_TEXAS_RAW_REFRESH_VALUE = process.env.TEXAS_TEC_RAW_DATA_REFRESH_ENABLED;
const ORIGINAL_WASHINGTON_FINANCE_VALUE = process.env.WASHINGTON_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_WASHINGTON_FINANCE_SYNC_VALUE = process.env.WASHINGTON_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_DC_FINANCE_VALUE = process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_DC_FINANCE_SYNC_VALUE = process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_ILLINOIS_FINANCE_VALUE = process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_ILLINOIS_FINANCE_SYNC_VALUE = process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_KENTUCKY_FINANCE_VALUE = process.env.KENTUCKY_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_KENTUCKY_FINANCE_SYNC_VALUE = process.env.KENTUCKY_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_FLORIDA_FINANCE_VALUE = process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_FLORIDA_FINANCE_SYNC_VALUE = process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED;
const ORIGINAL_FLORIDA_BROWSER_EXPORT_VALUE = process.env.FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED;

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
    if (ORIGINAL_ARIZONA_FINANCE_VALUE === undefined) {
      delete process.env.ARIZONA_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.ARIZONA_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_ARIZONA_FINANCE_VALUE;
    }
    if (ORIGINAL_ARIZONA_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.ARIZONA_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.ARIZONA_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_ARIZONA_FINANCE_SYNC_VALUE;
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
    if (ORIGINAL_ALASKA_FINANCE_VALUE === undefined) {
      delete process.env.ALASKA_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.ALASKA_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_ALASKA_FINANCE_VALUE;
    }
    if (ORIGINAL_ALASKA_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.ALASKA_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.ALASKA_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_ALASKA_FINANCE_SYNC_VALUE;
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
    if (ORIGINAL_INDIANA_FINANCE_VALUE === undefined) {
      delete process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_INDIANA_FINANCE_VALUE;
    }
    if (ORIGINAL_INDIANA_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.INDIANA_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.INDIANA_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_INDIANA_FINANCE_SYNC_VALUE;
    }
    if (ORIGINAL_INDIANA_RAW_REFRESH_VALUE === undefined) {
      delete process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED;
    } else {
      process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = ORIGINAL_INDIANA_RAW_REFRESH_VALUE;
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
    if (ORIGINAL_MAINE_FINANCE_VALUE === undefined) {
      delete process.env.MAINE_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_MAINE_FINANCE_VALUE;
    }
    if (ORIGINAL_MAINE_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_MAINE_FINANCE_SYNC_VALUE;
    }
    if (ORIGINAL_MAINE_RAW_REFRESH_VALUE === undefined) {
      delete process.env.MAINE_CFIS_RAW_DATA_REFRESH_ENABLED;
    } else {
      process.env.MAINE_CFIS_RAW_DATA_REFRESH_ENABLED = ORIGINAL_MAINE_RAW_REFRESH_VALUE;
    }
    if (ORIGINAL_OKLAHOMA_FINANCE_VALUE === undefined) {
      delete process.env.OKLAHOMA_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.OKLAHOMA_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_OKLAHOMA_FINANCE_VALUE;
    }
    if (ORIGINAL_OKLAHOMA_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.OKLAHOMA_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.OKLAHOMA_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_OKLAHOMA_FINANCE_SYNC_VALUE;
    }
    if (ORIGINAL_OKLAHOMA_RAW_REFRESH_VALUE === undefined) {
      delete process.env.OKLAHOMA_GUARDIAN_RAW_DATA_REFRESH_ENABLED;
    } else {
      process.env.OKLAHOMA_GUARDIAN_RAW_DATA_REFRESH_ENABLED = ORIGINAL_OKLAHOMA_RAW_REFRESH_VALUE;
    }
    if (ORIGINAL_TENNESSEE_FINANCE_VALUE === undefined) {
      delete process.env.TENNESSEE_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.TENNESSEE_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_TENNESSEE_FINANCE_VALUE;
    }
    if (ORIGINAL_TENNESSEE_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.TENNESSEE_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.TENNESSEE_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_TENNESSEE_FINANCE_SYNC_VALUE;
    }
    if (ORIGINAL_TEXAS_FINANCE_VALUE === undefined) {
      delete process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_TEXAS_FINANCE_VALUE;
    }
    if (ORIGINAL_TEXAS_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_TEXAS_FINANCE_SYNC_VALUE;
    }
    if (ORIGINAL_TEXAS_RAW_REFRESH_VALUE === undefined) {
      delete process.env.TEXAS_TEC_RAW_DATA_REFRESH_ENABLED;
    } else {
      process.env.TEXAS_TEC_RAW_DATA_REFRESH_ENABLED = ORIGINAL_TEXAS_RAW_REFRESH_VALUE;
    }
    if (ORIGINAL_WASHINGTON_FINANCE_VALUE === undefined) {
      delete process.env.WASHINGTON_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.WASHINGTON_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_WASHINGTON_FINANCE_VALUE;
    }
    if (ORIGINAL_WASHINGTON_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.WASHINGTON_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.WASHINGTON_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_WASHINGTON_FINANCE_SYNC_VALUE;
    }
    if (ORIGINAL_DC_FINANCE_VALUE === undefined) {
      delete process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_DC_FINANCE_VALUE;
    }
    if (ORIGINAL_DC_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_DC_FINANCE_SYNC_VALUE;
    }
    if (ORIGINAL_ILLINOIS_FINANCE_VALUE === undefined) {
      delete process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_ILLINOIS_FINANCE_VALUE;
    }
    if (ORIGINAL_ILLINOIS_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_ILLINOIS_FINANCE_SYNC_VALUE;
    }
    if (ORIGINAL_KENTUCKY_FINANCE_VALUE === undefined) {
      delete process.env.KENTUCKY_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.KENTUCKY_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_KENTUCKY_FINANCE_VALUE;
    }
    if (ORIGINAL_KENTUCKY_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.KENTUCKY_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.KENTUCKY_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_KENTUCKY_FINANCE_SYNC_VALUE;
    }
    if (ORIGINAL_FLORIDA_FINANCE_VALUE === undefined) {
      delete process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_FLORIDA_FINANCE_VALUE;
    }
    if (ORIGINAL_FLORIDA_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_FLORIDA_FINANCE_SYNC_VALUE;
    }
    if (ORIGINAL_FLORIDA_BROWSER_EXPORT_VALUE === undefined) {
      delete process.env.FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED;
    } else {
      process.env.FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED = ORIGINAL_FLORIDA_BROWSER_EXPORT_VALUE;
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

  it("disables Arizona campaign finance by default", () => {
    delete process.env.ARIZONA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.ARIZONA_CAMPAIGN_FINANCE_SYNC_ENABLED;

    expect(isArizonaCampaignFinanceEnabled()).toBe(false);
    expect(isArizonaCampaignFinanceSyncEnabled()).toBe(false);
  });

  it("requires the Arizona campaign finance master flag before sync can run", () => {
    process.env.ARIZONA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.ARIZONA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isArizonaCampaignFinanceEnabled()).toBe(false);
    expect(isArizonaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isArizonaCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Arizona campaign finance sync flag", () => {
    process.env.ARIZONA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ARIZONA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isArizonaCampaignFinanceEnabled()).toBe(true);
    expect(isArizonaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isArizonaCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables Arizona campaign finance sync when both flags are enabled", () => {
    process.env.ARIZONA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ARIZONA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isArizonaCampaignFinanceEnabled()).toBe(true);
    expect(isArizonaCampaignFinanceSyncEnabled()).toBe(true);
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

  it("gates Alaska campaign finance sync behind the Alaska master flag", () => {
    delete process.env.ALASKA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.ALASKA_CAMPAIGN_FINANCE_SYNC_ENABLED;

    expect(isAlaskaCampaignFinanceEnabled()).toBe(false);
    expect(isAlaskaCampaignFinanceSyncEnabled()).toBe(false);

    process.env.ALASKA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.ALASKA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    expect(isAlaskaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isAlaskaCampaignFinanceSyncEnabled(true)).toBe(false);

    process.env.ALASKA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ALASKA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    expect(isAlaskaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isAlaskaCampaignFinanceSyncEnabled(true)).toBe(true);

    process.env.ALASKA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    expect(isAlaskaCampaignFinanceSyncEnabled()).toBe(true);
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

  it("disables Indiana campaign finance by default", () => {
    delete process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.INDIANA_CAMPAIGN_FINANCE_SYNC_ENABLED;
    delete process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED;

    expect(isIndianaCampaignFinanceEnabled()).toBe(false);
    expect(isIndianaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isIndianaCampaignFinanceRawDataRefreshEnabled()).toBe(false);
  });

  it("requires the Indiana campaign finance master flag before sync can run", () => {
    process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.INDIANA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isIndianaCampaignFinanceEnabled()).toBe(false);
    expect(isIndianaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isIndianaCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Indiana campaign finance sync flag", () => {
    process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.INDIANA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isIndianaCampaignFinanceEnabled()).toBe(true);
    expect(isIndianaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isIndianaCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables Indiana campaign finance sync when both flags are enabled", () => {
    process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.INDIANA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isIndianaCampaignFinanceEnabled()).toBe(true);
    expect(isIndianaCampaignFinanceSyncEnabled()).toBe(true);
  });

  it("requires the Indiana campaign finance master flag before raw data refresh can run", () => {
    process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = "true";

    expect(isIndianaCampaignFinanceRawDataRefreshEnabled()).toBe(false);
    expect(isIndianaCampaignFinanceRawDataRefreshEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Indiana campaign finance raw data refresh flag", () => {
    process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = "false";

    expect(isIndianaCampaignFinanceRawDataRefreshEnabled()).toBe(false);
    expect(isIndianaCampaignFinanceRawDataRefreshEnabled(true)).toBe(true);
  });

  it("disables Maine campaign finance by default", () => {
    delete process.env.MAINE_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED;
    delete process.env.MAINE_CFIS_RAW_DATA_REFRESH_ENABLED;

    expect(isMaineCampaignFinanceEnabled()).toBe(false);
    expect(isMaineCampaignFinanceSyncEnabled()).toBe(false);
    expect(isMaineCfisRawDataRefreshEnabled()).toBe(false);
  });

  it("requires the Maine campaign finance master flag before sync can run", () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isMaineCampaignFinanceEnabled()).toBe(false);
    expect(isMaineCampaignFinanceSyncEnabled()).toBe(false);
    expect(isMaineCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Maine campaign finance sync flag", () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isMaineCampaignFinanceEnabled()).toBe(true);
    expect(isMaineCampaignFinanceSyncEnabled()).toBe(false);
    expect(isMaineCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables Maine campaign finance sync when both flags are enabled", () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isMaineCampaignFinanceEnabled()).toBe(true);
    expect(isMaineCampaignFinanceSyncEnabled()).toBe(true);
  });

  it("requires the Maine campaign finance master flag before CFIS raw data refresh can run", () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.MAINE_CFIS_RAW_DATA_REFRESH_ENABLED = "true";

    expect(isMaineCfisRawDataRefreshEnabled()).toBe(false);
    expect(isMaineCfisRawDataRefreshEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Maine CFIS raw data refresh flag", () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CFIS_RAW_DATA_REFRESH_ENABLED = "false";

    expect(isMaineCfisRawDataRefreshEnabled()).toBe(false);
    expect(isMaineCfisRawDataRefreshEnabled(true)).toBe(true);
  });

  it("enables Maine CFIS raw data refresh when both flags are enabled", () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CFIS_RAW_DATA_REFRESH_ENABLED = "true";

    expect(isMaineCfisRawDataRefreshEnabled()).toBe(true);
  });

  it("disables Oklahoma campaign finance by default", () => {
    delete process.env.OKLAHOMA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.OKLAHOMA_CAMPAIGN_FINANCE_SYNC_ENABLED;
    delete process.env.OKLAHOMA_GUARDIAN_RAW_DATA_REFRESH_ENABLED;

    expect(isOklahomaCampaignFinanceEnabled()).toBe(false);
    expect(isOklahomaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isOklahomaGuardianRawDataRefreshEnabled()).toBe(false);
  });

  it("requires the Oklahoma campaign finance master flag before sync can run", () => {
    process.env.OKLAHOMA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.OKLAHOMA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isOklahomaCampaignFinanceEnabled()).toBe(false);
    expect(isOklahomaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isOklahomaCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Oklahoma campaign finance sync flag", () => {
    process.env.OKLAHOMA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.OKLAHOMA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isOklahomaCampaignFinanceEnabled()).toBe(true);
    expect(isOklahomaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isOklahomaCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables Oklahoma campaign finance sync when both flags are enabled", () => {
    process.env.OKLAHOMA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.OKLAHOMA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isOklahomaCampaignFinanceEnabled()).toBe(true);
    expect(isOklahomaCampaignFinanceSyncEnabled()).toBe(true);
  });

  it("requires the Oklahoma campaign finance master flag before Guardian raw data refresh can run", () => {
    process.env.OKLAHOMA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.OKLAHOMA_GUARDIAN_RAW_DATA_REFRESH_ENABLED = "true";

    expect(isOklahomaGuardianRawDataRefreshEnabled()).toBe(false);
    expect(isOklahomaGuardianRawDataRefreshEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Oklahoma Guardian raw data refresh flag", () => {
    process.env.OKLAHOMA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.OKLAHOMA_GUARDIAN_RAW_DATA_REFRESH_ENABLED = "false";

    expect(isOklahomaGuardianRawDataRefreshEnabled()).toBe(false);
    expect(isOklahomaGuardianRawDataRefreshEnabled(true)).toBe(true);
  });

  it("disables Tennessee campaign finance by default", () => {
    delete process.env.TENNESSEE_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.TENNESSEE_CAMPAIGN_FINANCE_SYNC_ENABLED;

    expect(isTennesseeCampaignFinanceEnabled()).toBe(false);
    expect(isTennesseeCampaignFinanceSyncEnabled()).toBe(false);
  });

  it("requires the Tennessee campaign finance master flag before sync can run", () => {
    process.env.TENNESSEE_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.TENNESSEE_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isTennesseeCampaignFinanceEnabled()).toBe(false);
    expect(isTennesseeCampaignFinanceSyncEnabled()).toBe(false);
    expect(isTennesseeCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Tennessee campaign finance sync flag", () => {
    process.env.TENNESSEE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.TENNESSEE_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isTennesseeCampaignFinanceEnabled()).toBe(true);
    expect(isTennesseeCampaignFinanceSyncEnabled()).toBe(false);
    expect(isTennesseeCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables Tennessee campaign finance sync when both flags are enabled", () => {
    process.env.TENNESSEE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.TENNESSEE_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isTennesseeCampaignFinanceEnabled()).toBe(true);
    expect(isTennesseeCampaignFinanceSyncEnabled()).toBe(true);
  });

  it("disables Texas campaign finance by default", () => {
    delete process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED;
    delete process.env.TEXAS_TEC_RAW_DATA_REFRESH_ENABLED;

    expect(isTexasCampaignFinanceEnabled()).toBe(false);
    expect(isTexasCampaignFinanceSyncEnabled()).toBe(false);
    expect(isTexasTecRawDataRefreshEnabled()).toBe(false);
  });

  it("requires the Texas campaign finance master flag before sync can run", () => {
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isTexasCampaignFinanceEnabled()).toBe(false);
    expect(isTexasCampaignFinanceSyncEnabled()).toBe(false);
    expect(isTexasCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Texas campaign finance sync flag", () => {
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isTexasCampaignFinanceEnabled()).toBe(true);
    expect(isTexasCampaignFinanceSyncEnabled()).toBe(false);
    expect(isTexasCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables Texas campaign finance sync when both flags are enabled", () => {
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isTexasCampaignFinanceEnabled()).toBe(true);
    expect(isTexasCampaignFinanceSyncEnabled()).toBe(true);
  });

  it("requires the Texas campaign finance master flag before TEC raw data refresh can run", () => {
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.TEXAS_TEC_RAW_DATA_REFRESH_ENABLED = "true";

    expect(isTexasTecRawDataRefreshEnabled()).toBe(false);
    expect(isTexasTecRawDataRefreshEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Texas TEC raw data refresh flag", () => {
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.TEXAS_TEC_RAW_DATA_REFRESH_ENABLED = "false";

    expect(isTexasTecRawDataRefreshEnabled()).toBe(false);
    expect(isTexasTecRawDataRefreshEnabled(true)).toBe(true);
  });

  it("disables Florida campaign finance by default", () => {
    delete process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED;
    delete process.env.FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED;

    expect(isFloridaCampaignFinanceEnabled()).toBe(false);
    expect(isFloridaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isFloridaCampaignFinanceBrowserExportEnabled()).toBe(false);
  });

  it("requires the Florida campaign finance master flag before sync or browser export can run", () => {
    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED = "true";

    expect(isFloridaCampaignFinanceEnabled()).toBe(false);
    expect(isFloridaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isFloridaCampaignFinanceSyncEnabled(true)).toBe(false);
    expect(isFloridaCampaignFinanceBrowserExportEnabled()).toBe(false);
    expect(isFloridaCampaignFinanceBrowserExportEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Florida browser export flag", () => {
    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED = "false";

    expect(isFloridaCampaignFinanceBrowserExportEnabled()).toBe(false);
    expect(isFloridaCampaignFinanceBrowserExportEnabled(true)).toBe(true);
  });

  it("enables Florida campaign finance sync and browser export when their flags are enabled", () => {
    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED = "true";

    expect(isFloridaCampaignFinanceEnabled()).toBe(true);
    expect(isFloridaCampaignFinanceSyncEnabled()).toBe(true);
    expect(isFloridaCampaignFinanceBrowserExportEnabled()).toBe(true);
  });

  it("disables Washington campaign finance by default", () => {
    delete process.env.WASHINGTON_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.WASHINGTON_CAMPAIGN_FINANCE_SYNC_ENABLED;

    expect(isWashingtonCampaignFinanceEnabled()).toBe(false);
    expect(isWashingtonCampaignFinanceSyncEnabled()).toBe(false);
  });

  it("requires the Washington campaign finance master flag before sync can run", () => {
    process.env.WASHINGTON_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.WASHINGTON_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isWashingtonCampaignFinanceEnabled()).toBe(false);
    expect(isWashingtonCampaignFinanceSyncEnabled()).toBe(false);
    expect(isWashingtonCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Washington campaign finance sync flag", () => {
    process.env.WASHINGTON_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.WASHINGTON_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isWashingtonCampaignFinanceEnabled()).toBe(true);
    expect(isWashingtonCampaignFinanceSyncEnabled()).toBe(false);
    expect(isWashingtonCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables Washington campaign finance sync when both flags are enabled", () => {
    process.env.WASHINGTON_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.WASHINGTON_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isWashingtonCampaignFinanceEnabled()).toBe(true);
    expect(isWashingtonCampaignFinanceSyncEnabled()).toBe(true);
  });

  it("disables District of Columbia campaign finance by default", () => {
    delete process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_SYNC_ENABLED;

    expect(isDistrictOfColumbiaCampaignFinanceEnabled()).toBe(false);
    expect(isDistrictOfColumbiaCampaignFinanceSyncEnabled()).toBe(false);
  });

  it("requires the District of Columbia campaign finance master flag before sync can run", () => {
    process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isDistrictOfColumbiaCampaignFinanceEnabled()).toBe(false);
    expect(isDistrictOfColumbiaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isDistrictOfColumbiaCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the District of Columbia campaign finance sync flag", () => {
    process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isDistrictOfColumbiaCampaignFinanceEnabled()).toBe(true);
    expect(isDistrictOfColumbiaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isDistrictOfColumbiaCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables District of Columbia campaign finance sync when both flags are enabled", () => {
    process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isDistrictOfColumbiaCampaignFinanceEnabled()).toBe(true);
    expect(isDistrictOfColumbiaCampaignFinanceSyncEnabled()).toBe(true);
  });

  it("disables Illinois campaign finance by default", () => {
    delete process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED;

    expect(isIllinoisCampaignFinanceEnabled()).toBe(false);
    expect(isIllinoisCampaignFinanceSyncEnabled()).toBe(false);
  });

  it("requires the Illinois campaign finance master flag before sync can run", () => {
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isIllinoisCampaignFinanceEnabled()).toBe(false);
    expect(isIllinoisCampaignFinanceSyncEnabled()).toBe(false);
    expect(isIllinoisCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Illinois campaign finance sync flag", () => {
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isIllinoisCampaignFinanceEnabled()).toBe(true);
    expect(isIllinoisCampaignFinanceSyncEnabled()).toBe(false);
    expect(isIllinoisCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables Illinois campaign finance sync when both flags are enabled", () => {
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isIllinoisCampaignFinanceEnabled()).toBe(true);
    expect(isIllinoisCampaignFinanceSyncEnabled()).toBe(true);
  });

  it("disables Kentucky campaign finance by default", () => {
    delete process.env.KENTUCKY_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.KENTUCKY_CAMPAIGN_FINANCE_SYNC_ENABLED;

    expect(isKentuckyCampaignFinanceEnabled()).toBe(false);
    expect(isKentuckyCampaignFinanceSyncEnabled()).toBe(false);
  });

  it("requires the Kentucky campaign finance master flag before sync can run", () => {
    process.env.KENTUCKY_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.KENTUCKY_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isKentuckyCampaignFinanceEnabled()).toBe(false);
    expect(isKentuckyCampaignFinanceSyncEnabled()).toBe(false);
    expect(isKentuckyCampaignFinanceSyncEnabled(true)).toBe(false);
  });

  it("allows force to bypass only the Kentucky campaign finance sync flag", () => {
    process.env.KENTUCKY_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.KENTUCKY_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isKentuckyCampaignFinanceEnabled()).toBe(true);
    expect(isKentuckyCampaignFinanceSyncEnabled()).toBe(false);
    expect(isKentuckyCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("enables Kentucky campaign finance sync when both flags are enabled", () => {
    process.env.KENTUCKY_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.KENTUCKY_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isKentuckyCampaignFinanceEnabled()).toBe(true);
    expect(isKentuckyCampaignFinanceSyncEnabled()).toBe(true);
  });
});
