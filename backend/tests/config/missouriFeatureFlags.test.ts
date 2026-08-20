import { afterEach, describe, expect, it, vi } from "vitest";

import {
  isMissouriCampaignFinanceEnabled,
  isMissouriCampaignFinanceRawDataRefreshEnabled,
  isMissouriCampaignFinanceSyncEnabled,
} from "../../src/config/featureFlags.js";

afterEach(() => vi.unstubAllEnvs());

describe("Missouri campaign-finance flags", () => {
  it("keeps all paths off by default", () => {
    vi.stubEnv("MISSOURI_CAMPAIGN_FINANCE_ENABLED", "false");
    expect(isMissouriCampaignFinanceEnabled()).toBe(false);
    expect(isMissouriCampaignFinanceSyncEnabled(true)).toBe(false);
    expect(isMissouriCampaignFinanceRawDataRefreshEnabled(true)).toBe(false);
  });

  it("gates sync and raw refresh independently under the master flag", () => {
    vi.stubEnv("MISSOURI_CAMPAIGN_FINANCE_ENABLED", "true");
    vi.stubEnv("MISSOURI_CAMPAIGN_FINANCE_SYNC_ENABLED", "true");
    vi.stubEnv("MISSOURI_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED", "false");
    expect(isMissouriCampaignFinanceSyncEnabled()).toBe(true);
    expect(isMissouriCampaignFinanceRawDataRefreshEnabled()).toBe(false);
    expect(isMissouriCampaignFinanceRawDataRefreshEnabled(true)).toBe(true);
  });
});
