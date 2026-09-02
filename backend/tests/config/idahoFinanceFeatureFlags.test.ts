import { afterEach, describe, expect, it, vi } from "vitest";

import {
  isIdahoCampaignFinanceEnabled,
  isIdahoCampaignFinanceRawDataRefreshEnabled,
  isIdahoCampaignFinanceSyncEnabled,
} from "../../src/config/featureFlags.js";

afterEach(() => vi.unstubAllEnvs());

describe("Idaho campaign-finance feature flags", () => {
  it("defaults every gate off", () => {
    vi.stubEnv("IDAHO_CAMPAIGN_FINANCE_ENABLED", "");
    vi.stubEnv("IDAHO_CAMPAIGN_FINANCE_SYNC_ENABLED", "");
    vi.stubEnv("IDAHO_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED", "");
    expect(isIdahoCampaignFinanceEnabled()).toBe(false);
    expect(isIdahoCampaignFinanceSyncEnabled()).toBe(false);
    expect(isIdahoCampaignFinanceRawDataRefreshEnabled()).toBe(false);
  });

  it("requires the master gate and lets force bypass only the sub-gates", () => {
    vi.stubEnv("IDAHO_CAMPAIGN_FINANCE_ENABLED", "false");
    vi.stubEnv("IDAHO_CAMPAIGN_FINANCE_SYNC_ENABLED", "true");
    vi.stubEnv("IDAHO_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED", "true");
    expect(isIdahoCampaignFinanceSyncEnabled(true)).toBe(false);
    expect(isIdahoCampaignFinanceRawDataRefreshEnabled(true)).toBe(false);

    vi.stubEnv("IDAHO_CAMPAIGN_FINANCE_ENABLED", "true");
    vi.stubEnv("IDAHO_CAMPAIGN_FINANCE_SYNC_ENABLED", "false");
    vi.stubEnv("IDAHO_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED", "false");
    expect(isIdahoCampaignFinanceSyncEnabled()).toBe(false);
    expect(isIdahoCampaignFinanceSyncEnabled(true)).toBe(true);
    expect(isIdahoCampaignFinanceRawDataRefreshEnabled()).toBe(false);
    expect(isIdahoCampaignFinanceRawDataRefreshEnabled(true)).toBe(true);
  });
});
