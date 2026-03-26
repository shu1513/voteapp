import { describe, expect, it } from "vitest";
import { isLikelyPollingPlaceUrl } from "../../src/utils/isLikelyPollingPlaceUrl.ts";

describe("isLikelyPollingPlaceUrl", () => {
  it("returns true for polling locator URLs", () => {
    expect(isLikelyPollingPlaceUrl("https://www.vote.org/polling-place-locator/")).toBe(true);
    expect(isLikelyPollingPlaceUrl("https://www.sos.ca.gov/elections/polling-place")).toBe(true);
    expect(isLikelyPollingPlaceUrl("https://subdomain.vote.org/FIND-YOUR-POLLING-PLACE?ref=1")).toBe(true);
    expect(isLikelyPollingPlaceUrl("https://elections.state.gov/find-location")).toBe(true);
  });

  it("returns false for registration or mail URLs", () => {
    expect(isLikelyPollingPlaceUrl("https://www.usa.gov/register-to-vote")).toBe(false);
    expect(isLikelyPollingPlaceUrl("https://www.vote.org/absentee-ballot/")).toBe(false);
    expect(isLikelyPollingPlaceUrl("https://www.usvotefoundation.org/voter-id-laws")).toBe(false);
  });

  it("prefers polling signal when both polling and non-polling terms appear", () => {
    expect(isLikelyPollingPlaceUrl("https://example.gov/register?next=polling-place-locator")).toBe(true);
  });

  it("handles invalid or edge URLs", () => {
    expect(isLikelyPollingPlaceUrl("")).toBe(false);
    expect(isLikelyPollingPlaceUrl("not-a-url")).toBe(false);
    expect(isLikelyPollingPlaceUrl("ftp://example.org/polling-place-locator")).toBe(false);
    expect(isLikelyPollingPlaceUrl("https://example.com/locator")).toBe(false);
  });
});
