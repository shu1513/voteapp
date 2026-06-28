import { afterEach, describe, expect, it } from "vitest";

import {
  isIllinoisCampaignFinanceEnabled,
  isIllinoisCampaignFinanceSyncEnabled,
} from "../../src/config/featureFlags.js";

const ORIGINAL_ILLINOIS_FINANCE_VALUE = process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_ILLINOIS_FINANCE_SYNC_VALUE = process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED;

describe("Illinois campaign finance feature flags", () => {
  afterEach(() => {
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
  });

  it("keeps Illinois finance disabled by default", () => {
    delete process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED;

    expect(isIllinoisCampaignFinanceEnabled()).toBe(false);
    expect(isIllinoisCampaignFinanceSyncEnabled()).toBe(false);
  });

  it("requires both the master Illinois flag and sync flag unless forced", () => {
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    expect(isIllinoisCampaignFinanceSyncEnabled()).toBe(false);
    expect(isIllinoisCampaignFinanceSyncEnabled(true)).toBe(true);
  });

  it("does not let force bypass the master Illinois finance flag", () => {
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    expect(isIllinoisCampaignFinanceSyncEnabled(true)).toBe(false);
  });
});
