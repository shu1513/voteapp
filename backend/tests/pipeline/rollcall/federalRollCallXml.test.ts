import { readFileSync } from "node:fs";

import { describe, expect, it, vi } from "vitest";

import {
  decodeXmlEntities,
  fetchFederalRollCallXml,
  firstTagText,
  houseRollCallUrls,
  houseRollCallYear,
  parseHouseActionDate,
  parseHouseRollCallXml,
  parseSenateRollCallXml,
  parseSenateVoteDate,
  senateRollCallUrls,
} from "../../../src/pipeline/rollcall/federalRollCallXml.js";

function fixture(name: string): string {
  return readFileSync(new URL(`../../fixtures/rollcall/${name}`, import.meta.url), "utf8");
}

describe("roll-call URLs", () => {
  it("files House votes by calendar year with a three-digit roll", () => {
    expect(houseRollCallYear(119, 1)).toBe(2025);
    expect(houseRollCallYear(119, 2)).toBe(2026);
    expect(houseRollCallYear(118, 2)).toBe(2024);
    expect(houseRollCallUrls(119, 1, 14)).toEqual({
      displayUrl: "https://clerk.house.gov/Votes/202514",
      machineUrl: "https://clerk.house.gov/evs/2025/roll014.xml",
    });
    expect(houseRollCallUrls(119, 1, 1234).machineUrl).toBe("https://clerk.house.gov/evs/2025/roll1234.xml");
  });

  it("files Senate votes by congress + session with a five-digit roll", () => {
    expect(senateRollCallUrls(119, 1, 618)).toEqual({
      displayUrl: "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1191/vote_119_1_00618.htm",
      machineUrl: "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1191/vote_119_1_00618.xml",
    });
  });
});

describe("XML helpers", () => {
  it("decodes entities and collapses whitespace", () => {
    expect(decodeXmlEntities("Hershel &#8220;Woody&#8221; Williams &amp; Co &lt;3 &#x41;")).toBe(
      "Hershel “Woody” Williams & Co <3 A"
    );
    expect(firstTagText("<a>\n  one\n   two </a>", "a")).toBe("one two");
    expect(firstTagText("<a/><b></b>", "a")).toBeNull();
    expect(firstTagText("<a></a>", "a")).toBeNull();
    expect(firstTagText("<vote_date>May 14, 2025</vote_date>", "vote")).toBeNull();
  });

  it("reads both chambers' date spellings", () => {
    expect(parseHouseActionDate("3-Jan-2025")).toBe("2025-01-03");
    expect(parseHouseActionDate("22-May-2025")).toBe("2025-05-22");
    expect(parseSenateVoteDate("May 14, 2025,  02:47 PM")).toBe("2025-05-14");
    expect(parseSenateVoteDate("November 10, 2025,  08:58 PM")).toBe("2025-11-10");
    expect(() => parseHouseActionDate("2025-01-03")).toThrow(/unreadable date/);
    expect(() => parseSenateVoteDate("14 May 2025")).toThrow(/unreadable date/);
  });
});

describe("parseHouseRollCallXml", () => {
  it("reads a passage vote", () => {
    expect(parseHouseRollCallXml(fixture("house-119-1-roll145.xml"))).toEqual({
      chamber: "house",
      congress: 119,
      session: 1,
      rollNumber: 145,
      voteDate: "2025-05-22",
      measureId: "H R 1",
      question: "On Passage",
      result: "Passed",
      yeas: 215,
      nays: 214,
      title: "One Big Beautiful Act",
      memberVoteCount: 2,
    });
  });

  it("reads a quorum call as a 0-0 vote on the QUORUM pseudo-measure", () => {
    const parsed = parseHouseRollCallXml(fixture("house-119-1-roll001.xml"));
    expect(parsed).toMatchObject({
      rollNumber: 1,
      voteDate: "2025-01-03",
      measureId: "QUORUM",
      question: "Call by States",
      result: "Passed",
      yeas: 0,
      nays: 0,
      title: null,
    });
  });

  it("refuses the Speaker election, which tallies by candidate instead of yea/nay", () => {
    expect(() => parseHouseRollCallXml(fixture("house-119-1-roll002.xml"))).toThrow(/Speaker election/);
  });

  it("takes the chamber-wide tally, not the first party's", () => {
    const xml = fixture("house-119-1-roll145.xml");
    // Republican yea-total appears before totals-by-vote in the feed.
    expect(xml.indexOf("<yea-total>")).toBeLessThan(xml.indexOf("<totals-by-vote>"));
    expect(parseHouseRollCallXml(xml).yeas).toBe(215);
  });

  it("rejects a non-vote document", () => {
    expect(() => parseHouseRollCallXml("<html>Not Found</html>")).toThrow(/no <rollcall-vote> root/);
  });
});

describe("parseSenateRollCallXml", () => {
  it("reads a passage vote", () => {
    expect(parseSenateRollCallXml(fixture("senate-119-1-vote00618.xml"))).toEqual({
      chamber: "senate",
      congress: 119,
      session: 1,
      rollNumber: 618,
      voteDate: "2025-11-10",
      measureId: "H.R. 5371",
      question: "On Passage of the Bill",
      result: "Bill Passed",
      yeas: 60,
      nays: 40,
      title: "H.R. 5371, As Amended",
      memberVoteCount: 2,
    });
  });

  it("reads a cloture vote on a nomination", () => {
    const parsed = parseSenateRollCallXml(fixture("senate-119-1-vote00253.xml"));
    expect(parsed.rollNumber).toBe(253);
    expect(parsed.measureId).toBe("PN12-31");
    expect(parsed.question).toBe("On the Cloture Motion");
    expect(parsed.result).toBe("Cloture Motion Agreed to");
    expect(parsed.yeas).toBe(53);
    expect(parsed.nays).toBe(43);
  });

  it("rejects a non-vote document", () => {
    expect(() => parseSenateRollCallXml("<html>Moved</html>")).toThrow(/no <roll_call_vote> root/);
  });
});

describe("fetchFederalRollCallXml", () => {
  it("returns the body on 200 and does not follow redirects", async () => {
    const fetchFn = vi.fn(async () => new Response("<roll_call_vote/>", { status: 200 }));
    await expect(fetchFederalRollCallXml("https://example.test/v.xml", { fetchFn })).resolves.toEqual({
      status: "ok",
      body: "<roll_call_vote/>",
    });
    expect(fetchFn.mock.calls[0]?.[1]).toMatchObject({ redirect: "manual", method: "GET" });
  });

  it("treats 404 and 3xx as a roll call that does not exist", async () => {
    for (const status of [404, 301, 302]) {
      const fetchFn = vi.fn(async () => new Response("", { status }));
      await expect(fetchFederalRollCallXml("https://example.test/v.xml", { fetchFn })).resolves.toEqual({
        status: "missing",
      });
    }
  });

  it("throws on other failures so the caller can retry", async () => {
    const fetchFn = vi.fn(async () => new Response("", { status: 503 }));
    await expect(fetchFederalRollCallXml("https://example.test/v.xml", { fetchFn })).rejects.toThrow(
      /HTTP 503/
    );
  });
});
