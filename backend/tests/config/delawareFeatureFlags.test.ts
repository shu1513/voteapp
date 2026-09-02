import { afterEach, describe, expect, it, vi } from "vitest";

import {
  isDelawareCampaignFinanceEnabled,
  isDelawareCampaignFinanceRawDataRefreshEnabled,
  isDelawareCampaignFinanceSyncEnabled,
} from "../../src/config/featureFlags.js";

afterEach(() => vi.unstubAllEnvs());

describe("Delaware campaign finance flags", () => {
  it("defaults every flag to false", () => {
    expect(isDelawareCampaignFinanceEnabled()).toBe(false);
    expect(isDelawareCampaignFinanceSyncEnabled()).toBe(false);
    expect(isDelawareCampaignFinanceRawDataRefreshEnabled()).toBe(false);
  });

  it("gates sync and raw refresh on the base flag", () => {
    vi.stubEnv("DELAWARE_CAMPAIGN_FINANCE_SYNC_ENABLED", "true");
    vi.stubEnv("DELAWARE_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED", "true");
    expect(isDelawareCampaignFinanceSyncEnabled()).toBe(false);
    expect(isDelawareCampaignFinanceRawDataRefreshEnabled()).toBe(false);

    vi.stubEnv("DELAWARE_CAMPAIGN_FINANCE_ENABLED", "true");
    expect(isDelawareCampaignFinanceSyncEnabled()).toBe(true);
    expect(isDelawareCampaignFinanceRawDataRefreshEnabled()).toBe(true);
  });

  it("honors force only when the base flag is on", () => {
    expect(isDelawareCampaignFinanceSyncEnabled(true)).toBe(false);
    vi.stubEnv("DELAWARE_CAMPAIGN_FINANCE_ENABLED", "true");
    expect(isDelawareCampaignFinanceSyncEnabled(true)).toBe(true);
    expect(isDelawareCampaignFinanceRawDataRefreshEnabled(true)).toBe(true);
  });
});
