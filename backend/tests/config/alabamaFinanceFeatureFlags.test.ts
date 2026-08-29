import { afterEach, describe, expect, it, vi } from "vitest";

import {
  isAlabamaCampaignFinanceEnabled,
  isAlabamaCampaignFinanceSyncEnabled,
  isAlabamaFcpaRawDataRefreshEnabled,
} from "../../src/config/featureFlags.js";

afterEach(() => vi.unstubAllEnvs());

describe("Alabama campaign-finance feature flags", () => {
  it("defaults every gate off", () => {
    vi.stubEnv("ALABAMA_CAMPAIGN_FINANCE_ENABLED", "");
    vi.stubEnv("ALABAMA_CAMPAIGN_FINANCE_SYNC_ENABLED", "");
    vi.stubEnv("ALABAMA_FCPA_RAW_DATA_REFRESH_ENABLED", "");
    expect(isAlabamaCampaignFinanceEnabled()).toBe(false);
    expect(isAlabamaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isAlabamaFcpaRawDataRefreshEnabled()).toBe(false);
  });

  it("requires the master gate and lets force bypass only the sub-gates", () => {
    vi.stubEnv("ALABAMA_CAMPAIGN_FINANCE_ENABLED", "false");
    vi.stubEnv("ALABAMA_CAMPAIGN_FINANCE_SYNC_ENABLED", "true");
    vi.stubEnv("ALABAMA_FCPA_RAW_DATA_REFRESH_ENABLED", "true");
    expect(isAlabamaCampaignFinanceSyncEnabled(true)).toBe(false);
    expect(isAlabamaFcpaRawDataRefreshEnabled(true)).toBe(false);

    vi.stubEnv("ALABAMA_CAMPAIGN_FINANCE_ENABLED", "true");
    vi.stubEnv("ALABAMA_CAMPAIGN_FINANCE_SYNC_ENABLED", "false");
    vi.stubEnv("ALABAMA_FCPA_RAW_DATA_REFRESH_ENABLED", "false");
    expect(isAlabamaCampaignFinanceSyncEnabled()).toBe(false);
    expect(isAlabamaCampaignFinanceSyncEnabled(true)).toBe(true);
    expect(isAlabamaFcpaRawDataRefreshEnabled()).toBe(false);
    expect(isAlabamaFcpaRawDataRefreshEnabled(true)).toBe(true);
  });
});
