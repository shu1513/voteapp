import { describe, expect, it } from "vitest";

import { descriptionMentionsMeasure, rollCallUrlKey } from "../../../src/pipeline/rollcall/rollCallRecordUrls.js";

describe("rollCallUrlKey", () => {
  it("folds the Clerk XML file and the Clerk vote page to one House key", () => {
    const xml = rollCallUrlKey("https://clerk.house.gov/evs/2025/roll145.xml");
    expect(xml).toEqual({ chamber: "house", key: "house:2025:145" });
    expect(rollCallUrlKey("https://clerk.house.gov/Votes/2025145")).toEqual(xml);
    expect(rollCallUrlKey("https://clerk.house.gov/Votes/2025145?Title=Laken")).toEqual(xml);
    expect(rollCallUrlKey("https://clerk.house.gov/evs/2025/roll023.xml")?.key).toBe("house:2025:23");
    expect(rollCallUrlKey("https://clerk.house.gov/Votes/202523")?.key).toBe("house:2025:23");
    expect(rollCallUrlKey("https://clerk.house.gov/evs/2007/roll1186.xml")?.key).toBe("house:2007:1186");
  });

  it("folds the Senate .htm and .xml pages to one key", () => {
    const htm = rollCallUrlKey("https://www.senate.gov/legislative/LIS/roll_call_votes/vote1191/vote_119_1_00618.htm");
    expect(htm).toEqual({ chamber: "senate", key: "senate:119-1:618" });
    expect(rollCallUrlKey("https://www.senate.gov/legislative/LIS/roll_call_votes/vote1191/vote_119_1_00618.xml")).toEqual(
      htm
    );
    expect(rollCallUrlKey("http://senate.gov/legislative/LIS/roll_call_votes/vote1172/vote_117_2_00134.htm")?.key).toBe(
      "senate:117-2:134"
    );
  });

  it("returns null for anything that is not a roll-call page", () => {
    for (const url of [
      "https://clerk.house.gov/members/A000370",
      "https://clerk.house.gov/Votes/MemberVotes?BillNum=H.R.1&RollCallNum=145&Session=1st",
      "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_119_1.xml",
      "https://www.congress.gov/bill/119th-congress/house-bill/1",
      "https://hinson.house.gov/media/press-releases/x",
      "",
    ]) {
      expect(rollCallUrlKey(url), url).toBeNull();
    }
  });
});

describe("descriptionMentionsMeasure", () => {
  const hr29 = { type: "hr" as const, number: "29" };

  it("reads the spellings the hand-written records use", () => {
    for (const text of [
      "Voted for H.R. 29, the Laken Riley Act.",
      "Voted to pass H. R. 29 on final passage.",
      "Voted yes on HR 29.",
      "Voted for H.R.29 (Laken Riley Act).",
    ]) {
      expect(descriptionMentionsMeasure(text, hr29), text).toBe(true);
    }
    expect(descriptionMentionsMeasure("Voted for S. 5, the Laken Riley Act.", { type: "s", number: "5" })).toBe(true);
    expect(descriptionMentionsMeasure("Voted for S.J.Res. 3.", { type: "sjres", number: "3" })).toBe(true);
    expect(descriptionMentionsMeasure("Voted for H. Con. Res. 86 to end hostilities.", { type: "hconres", number: "86" })).toBe(
      true
    );
    expect(descriptionMentionsMeasure("Voted for H.Res. 863.", { type: "hres", number: "863" })).toBe(true);
  });

  it("does not confuse a different number, a different type, or U.S. with S.", () => {
    expect(descriptionMentionsMeasure("Voted for H.R. 290.", hr29)).toBe(false);
    expect(descriptionMentionsMeasure("Voted for H.Res. 29.", hr29)).toBe(false);
    expect(descriptionMentionsMeasure("Voted for S. 29.", hr29)).toBe(false);
    expect(descriptionMentionsMeasure("Voted for U.S. 5 funding.", { type: "s", number: "5" })).toBe(false);
    expect(descriptionMentionsMeasure("Voted for the Laken Riley Act.", hr29)).toBe(false);
  });
});
