import { afterEach, describe, expect, it, vi } from "vitest";

import { isNevadaCampaignFinanceEnabled } from "../../src/config/featureFlags.js";

afterEach(() => vi.unstubAllEnvs());

describe("Nevada campaign-finance feature flag", () => {
  it("defaults off and honors the env gate", () => {
    vi.stubEnv("NEVADA_CAMPAIGN_FINANCE_ENABLED", "");
    expect(isNevadaCampaignFinanceEnabled()).toBe(false);
    vi.stubEnv("NEVADA_CAMPAIGN_FINANCE_ENABLED", "true");
    expect(isNevadaCampaignFinanceEnabled()).toBe(true);
    vi.stubEnv("NEVADA_CAMPAIGN_FINANCE_ENABLED", "false");
    expect(isNevadaCampaignFinanceEnabled()).toBe(false);
  });
});
