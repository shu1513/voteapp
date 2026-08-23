import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import {
  parseFederalMemberVotes,
  parseHouseMemberVotes,
  parseSenateMemberVotes,
} from "../../../src/pipeline/rollcall/federalRollCallMembers.js";

function fixture(name: string): string {
  return readFileSync(new URL(`../../fixtures/rollcall/${name}`, import.meta.url), "utf8");
}

describe("parseHouseMemberVotes", () => {
  it("reads bioguide id, state, party, and the vote as printed", () => {
    expect(parseHouseMemberVotes(fixture("house-119-1-roll145.xml"))).toEqual([
      { chamber: "house", memberId: "A000370", name: "Adams", state: "NC", party: "D", vote: "Nay" },
      { chamber: "house", memberId: "A000055", name: "Aderholt", state: "AL", party: "R", vote: "Yea" },
    ]);
  });

  it("keeps Present / Not Voting and the Speaker's row, and decodes entities", () => {
    const xml = [
      "<rollcall-vote><vote-data>",
      '<recorded-vote><legislator name-id="J000299" sort-field="Johnson (LA)" unaccented-name="Johnson (LA)" party="R" state="LA" role="speaker">Johnson (LA)</legislator><vote>Yea</vote></recorded-vote>',
      '<recorded-vote><legislator name-id="V000081" sort-field="Velazquez" unaccented-name="Velazquez" party="D" state="NY" role="legislator">Vel&#225;zquez</legislator><vote>Not Voting</vote></recorded-vote>',
      '<recorded-vote><legislator name-id="A000370" party="D" state="NC" role="legislator">Adams</legislator><vote>Present</vote></recorded-vote>',
      "</vote-data></rollcall-vote>",
    ].join("\n");
    expect(parseHouseMemberVotes(xml).map((row) => [row.memberId, row.name, row.vote])).toEqual([
      ["J000299", "Johnson (LA)", "Yea"],
      ["V000081", "Velázquez", "Not Voting"],
      ["A000370", "Adams", "Present"],
    ]);
  });

  it("returns no rows for a file without member rows", () => {
    expect(parseHouseMemberVotes("<rollcall-vote><vote-metadata/></rollcall-vote>")).toEqual([]);
  });

  it("refuses a row without a bioguide id rather than inventing one", () => {
    const xml =
      '<recorded-vote><legislator party="D" state="NC" role="legislator">Adams</legislator><vote>Yea</vote></recorded-vote>';
    expect(() => parseHouseMemberVotes(xml)).toThrow(/House name-id is missing/);
  });
});

describe("parseSenateMemberVotes", () => {
  it("reads LIS id, state, party, and the vote as printed", () => {
    expect(parseSenateMemberVotes(fixture("senate-119-1-vote00618.xml"))).toEqual([
      { chamber: "senate", memberId: "S428", name: "Alsobrooks (D-MD)", state: "MD", party: "D", vote: "Nay" },
      { chamber: "senate", memberId: "S354", name: "Baldwin (D-WI)", state: "WI", party: "D", vote: "Nay" },
    ]);
  });

  it("does not confuse the <members> wrapper or the header <state> with a member row", () => {
    const xml = [
      "<roll_call_vote><congress>119</congress><state>XX</state><members>",
      "<member><member_full>Baldwin (D-WI)</member_full><last_name>Baldwin</last_name><first_name>Tammy</first_name>",
      "<party>D</party><state>WI</state><vote_cast>Yea</vote_cast><lis_member_id>S354</lis_member_id></member>",
      "</members></roll_call_vote>",
    ].join("\n");
    expect(parseSenateMemberVotes(xml)).toEqual([
      { chamber: "senate", memberId: "S354", name: "Baldwin (D-WI)", state: "WI", party: "D", vote: "Yea" },
    ]);
  });

  it("refuses a row without a LIS id", () => {
    const xml = "<member><member_full>Baldwin (D-WI)</member_full><state>WI</state><vote_cast>Yea</vote_cast></member>";
    expect(() => parseSenateMemberVotes(xml)).toThrow(/Senate lis_member_id is missing/);
  });
});

describe("parseFederalMemberVotes", () => {
  it("dispatches on chamber", () => {
    expect(parseFederalMemberVotes("house", fixture("house-119-1-roll001.xml"))).toHaveLength(2);
    expect(parseFederalMemberVotes("senate", fixture("senate-119-1-vote00253.xml"))).toHaveLength(2);
  });
});
