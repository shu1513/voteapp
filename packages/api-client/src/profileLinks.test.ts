import { describe, expect, it } from "vitest";
import { candidateProfileLinks } from "./profileLinks";

const NONE = { official_website_url: null, twitter_handle: null, linkedin_url: null };

describe("candidateProfileLinks", () => {
  it("returns nothing when no link fields are set", () => {
    expect(candidateProfileLinks(NONE)).toEqual([]);
  });

  it("lists website, X, then LinkedIn in that order", () => {
    expect(
      candidateProfileLinks({
        official_website_url: "https://jordan.example",
        twitter_handle: "jordan_voter",
        linkedin_url: "https://www.linkedin.com/in/jordan-voter",
      })
    ).toEqual([
      { href: "https://jordan.example", label: "Official website" },
      { href: "https://x.com/jordan_voter", label: "X (Twitter)" },
      { href: "https://www.linkedin.com/in/jordan-voter", label: "LinkedIn" },
    ]);
  });

  it("skips only the missing fields", () => {
    expect(candidateProfileLinks({ ...NONE, twitter_handle: "jordan_voter" })).toEqual([
      { href: "https://x.com/jordan_voter", label: "X (Twitter)" },
    ]);
  });

  it("drops a malformed handle rather than building an href from it", () => {
    expect(candidateProfileLinks({ ...NONE, twitter_handle: "@jordan" })).toEqual([]);
    expect(candidateProfileLinks({ ...NONE, twitter_handle: "https://x.com/jordan" })).toEqual([]);
    expect(candidateProfileLinks({ ...NONE, twitter_handle: "" })).toEqual([]);
  });
});
