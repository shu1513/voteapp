import { afterEach, describe, expect, it, vi } from "vitest";

import { defineStateFinanceFlagPair } from "../../src/config/featureFlags.js";

afterEach(() => vi.unstubAllEnvs());

const PREFIX = "STATE_FINANCE_FLAG_PAIR_TEST";
const pair = defineStateFinanceFlagPair(PREFIX);
const isRawRefreshEnabled = pair.gate(`${PREFIX}_RAW_REFRESH_ENABLED`);

describe("defineStateFinanceFlagPair", () => {
  it("defaults every gate off when the env keys are unset or blank", () => {
    vi.stubEnv(`${PREFIX}_ENABLED`, "");
    vi.stubEnv(`${PREFIX}_SYNC_ENABLED`, " ");
    expect(pair.isEnabled()).toBe(false);
    expect(pair.isSyncEnabled()).toBe(false);
    expect(isRawRefreshEnabled()).toBe(false);
  });

  it("reads the env at call time instead of caching the first value", () => {
    vi.stubEnv(`${PREFIX}_ENABLED`, "true");
    vi.stubEnv(`${PREFIX}_SYNC_ENABLED`, "yes");
    expect(pair.isEnabled()).toBe(true);
    expect(pair.isSyncEnabled()).toBe(true);

    vi.stubEnv(`${PREFIX}_ENABLED`, "off");
    expect(pair.isEnabled()).toBe(false);
    expect(pair.isSyncEnabled()).toBe(false);
  });

  it("never lets force bypass the master flag", () => {
    vi.stubEnv(`${PREFIX}_ENABLED`, "false");
    vi.stubEnv(`${PREFIX}_SYNC_ENABLED`, "true");
    vi.stubEnv(`${PREFIX}_RAW_REFRESH_ENABLED`, "true");
    expect(pair.isSyncEnabled(true)).toBe(false);
    expect(isRawRefreshEnabled(true)).toBe(false);
  });

  it("lets force bypass only the sub-gate once the master flag is on", () => {
    vi.stubEnv(`${PREFIX}_ENABLED`, "1");
    vi.stubEnv(`${PREFIX}_SYNC_ENABLED`, "0");
    vi.stubEnv(`${PREFIX}_RAW_REFRESH_ENABLED`, "no");
    expect(pair.isSyncEnabled()).toBe(false);
    expect(pair.isSyncEnabled(true)).toBe(true);
    expect(isRawRefreshEnabled()).toBe(false);
    expect(isRawRefreshEnabled(true)).toBe(true);
  });

  it("short-circuits on the master flag before reading a sub-gate key", () => {
    vi.stubEnv(`${PREFIX}_ENABLED`, "false");
    vi.stubEnv(`${PREFIX}_SYNC_ENABLED`, "maybe");
    vi.stubEnv(`${PREFIX}_RAW_REFRESH_ENABLED`, "maybe");
    expect(pair.isSyncEnabled()).toBe(false);
    expect(isRawRefreshEnabled()).toBe(false);
  });

  it("still throws on invalid boolean values", () => {
    vi.stubEnv(`${PREFIX}_ENABLED`, "maybe");
    expect(() => pair.isEnabled()).toThrow(`Invalid boolean env ${PREFIX}_ENABLED: maybe`);
    expect(() => pair.isSyncEnabled(true)).toThrow(`Invalid boolean env ${PREFIX}_ENABLED: maybe`);

    vi.stubEnv(`${PREFIX}_ENABLED`, "true");
    vi.stubEnv(`${PREFIX}_SYNC_ENABLED`, "maybe");
    vi.stubEnv(`${PREFIX}_RAW_REFRESH_ENABLED`, "sometimes");
    expect(() => pair.isSyncEnabled()).toThrow(`Invalid boolean env ${PREFIX}_SYNC_ENABLED: maybe`);
    expect(() => isRawRefreshEnabled()).toThrow(`Invalid boolean env ${PREFIX}_RAW_REFRESH_ENABLED: sometimes`);
  });

  it("keys each gate off the explicit env name it was given", () => {
    vi.stubEnv(`${PREFIX}_ENABLED`, "true");
    vi.stubEnv(`${PREFIX}_SYNC_ENABLED`, "false");
    vi.stubEnv(`${PREFIX}_RAW_REFRESH_ENABLED`, "true");
    expect(pair.isSyncEnabled()).toBe(false);
    expect(isRawRefreshEnabled()).toBe(true);
  });
});
