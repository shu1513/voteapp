import { afterEach, describe, expect, it, vi } from "vitest";

import {
  isNorthDakotaCampaignFinanceEnabled,
  isNorthDakotaCfrsRawDataRefreshEnabled,
} from "../../src/config/featureFlags.js";

afterEach(() => vi.unstubAllEnvs());

describe("North Dakota campaign-finance feature flags", () => {
  it("defaults both gates off", () => {
    vi.stubEnv("NORTH_DAKOTA_CAMPAIGN_FINANCE_ENABLED", "");
    vi.stubEnv("NORTH_DAKOTA_CFRS_RAW_DATA_REFRESH_ENABLED", "");
    expect(isNorthDakotaCampaignFinanceEnabled()).toBe(false);
    expect(isNorthDakotaCfrsRawDataRefreshEnabled()).toBe(false);
  });

  it("requires the master gate and lets force bypass only the raw-refresh gate", () => {
    vi.stubEnv("NORTH_DAKOTA_CAMPAIGN_FINANCE_ENABLED", "false");
    vi.stubEnv("NORTH_DAKOTA_CFRS_RAW_DATA_REFRESH_ENABLED", "true");
    expect(isNorthDakotaCfrsRawDataRefreshEnabled(true)).toBe(false);

    vi.stubEnv("NORTH_DAKOTA_CAMPAIGN_FINANCE_ENABLED", "true");
    vi.stubEnv("NORTH_DAKOTA_CFRS_RAW_DATA_REFRESH_ENABLED", "false");
    expect(isNorthDakotaCfrsRawDataRefreshEnabled()).toBe(false);
    expect(isNorthDakotaCfrsRawDataRefreshEnabled(true)).toBe(true);
  });
});
