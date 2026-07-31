import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TERMS_VERSION } from "@voteapp/api-client";
import {
  hasCurrentTermsAcceptance,
  rememberTermsAcceptance,
  TERMS_ACCEPTANCE_TTL_MS,
} from "./termsAcceptance";

const STORAGE_KEY = "voteapp_terms_acceptance";
const NOW = 1_800_000_000_000;

beforeEach(() => {
  localStorage.clear();
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("hasCurrentTermsAcceptance", () => {
  it("accepts a current-version record inside the window", () => {
    rememberTermsAcceptance(NOW);
    expect(hasCurrentTermsAcceptance(NOW)).toBe(true);
    expect(hasCurrentTermsAcceptance(NOW + TERMS_ACCEPTANCE_TTL_MS)).toBe(true);
  });

  it.each([
    ["nothing stored", null],
    ["unparseable json", "{not json"],
    ["a json primitive", '"nope"'],
    ["a missing version", JSON.stringify({ acceptedAt: NOW })],
    ["a non-numeric timestamp", JSON.stringify({ version: TERMS_VERSION, acceptedAt: "today" })],
    ["superseded terms", JSON.stringify({ version: "0.9", acceptedAt: NOW })],
  ])("re-prompts on %s", (_name, raw) => {
    if (raw !== null) {
      localStorage.setItem(STORAGE_KEY, raw);
    }
    expect(hasCurrentTermsAcceptance(NOW)).toBe(false);
  });

  it("re-prompts one millisecond past the window", () => {
    rememberTermsAcceptance(NOW);
    expect(hasCurrentTermsAcceptance(NOW + TERMS_ACCEPTANCE_TTL_MS + 1)).toBe(false);
  });

  it("re-prompts on a future-dated record rather than trusting it forever", () => {
    // A clock change or a hand-edited value; an acceptance that never ages is
    // the one outcome this must not produce.
    rememberTermsAcceptance(NOW + 60_000);
    expect(hasCurrentTermsAcceptance(NOW)).toBe(false);
  });

  it("re-prompts when storage cannot be read", () => {
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("storage disabled");
    });
    expect(hasCurrentTermsAcceptance(NOW)).toBe(false);
  });
});

describe("rememberTermsAcceptance", () => {
  it("stores only the version and the timestamp", () => {
    rememberTermsAcceptance(NOW);
    // No identifier of any kind: nothing here identifies a person, so there is
    // nothing to disclose or hand over.
    expect(JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}")).toEqual({
      version: TERMS_VERSION,
      acceptedAt: NOW,
    });
  });

  it("stays silent when storage cannot be written", () => {
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("storage full");
    });
    // Never block the search on being able to remember it.
    expect(() => rememberTermsAcceptance(NOW)).not.toThrow();
  });
});
