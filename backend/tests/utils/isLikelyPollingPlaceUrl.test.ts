import { describe, expect, it } from "vitest";
import { isLikelyPollingPlaceUrl } from "../../src/utils/isLikelyPollingPlaceUrl.ts";

describe("isLikelyPollingPlaceUrl", () => {
  it("returns true for polling locator URLs", () => {
    expect(isLikelyPollingPlaceUrl("https://www.vote.org/polling-place-locator/")).toBe(true);
    expect(isLikelyPollingPlaceUrl("https://www.sos.ca.gov/elections/polling-place")).toBe(true);
  });

  it("returns false for registration or mail URLs", () => {
    expect(isLikelyPollingPlaceUrl("https://www.usa.gov/register-to-vote")).toBe(false);
    expect(isLikelyPollingPlaceUrl("https://www.vote.org/absentee-ballot/")).toBe(false);
    expect(isLikelyPollingPlaceUrl("https://www.usvotefoundation.org/voter-id-laws")).toBe(false);
  });
});

