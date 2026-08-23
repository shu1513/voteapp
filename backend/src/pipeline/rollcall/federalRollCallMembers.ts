import { decodeXmlEntities, firstTagText } from "./federalRollCallXml.js";
import type { LegislativeVoteChamber } from "./legislativeVotes.js";

// Member rows of the two federal roll-call feeds. The House identifies a
// member by bioguide id (`<legislator name-id="A000370" state="NC">`), the
// Senate by LIS id (`<lis_member_id>S428</lis_member_id>`); the crosswalk
// (congressLegislators.ts) joins both to FEC ids. The vote text is kept as
// the feed prints it (`Yea`, `Nay`, `Aye`, `No`, `Present`, `Not Voting`):
// identity resolution does not depend on it, and the fan-out decides later
// which values produce a record.

export type FederalMemberVote = {
  chamber: LegislativeVoteChamber;
  // Bioguide id (House) or LIS id (Senate).
  memberId: string;
  // Display name as printed; never used for matching.
  name: string;
  state: string;
  party: string | null;
  vote: string;
};

function attribute(tag: string, name: string): string | null {
  const match = new RegExp(`\\s${name}="([^"]*)"`).exec(tag);
  if (!match) {
    return null;
  }
  const value = decodeXmlEntities(match[1]!).trim();
  return value.length > 0 ? value : null;
}

function required(value: string | null, what: string, row: string): string {
  if (value === null) {
    throw new Error(`${what} is missing in member row: ${row.replace(/\s+/g, " ").trim().slice(0, 160)}`);
  }
  return value;
}

/** Every `<recorded-vote>` of a House Clerk file, in file order. */
export function parseHouseMemberVotes(xml: string): FederalMemberVote[] {
  const votes: FederalMemberVote[] = [];
  for (const match of xml.matchAll(/<recorded-vote>([\s\S]*?)<\/recorded-vote>/g)) {
    const row = match[1]!;
    const legislator = /<legislator(\s[^>]*)?>/.exec(row);
    if (!legislator) {
      throw new Error(`House member row has no <legislator>: ${row.slice(0, 160)}`);
    }
    const tag = legislator[1] ?? "";
    votes.push({
      chamber: "house",
      memberId: required(attribute(tag, "name-id"), "House name-id", row),
      name: required(firstTagText(row, "legislator"), "House legislator name", row),
      state: required(attribute(tag, "state"), "House state", row),
      party: attribute(tag, "party"),
      vote: required(firstTagText(row, "vote"), "House vote", row),
    });
  }
  return votes;
}

/** Every `<member>` of a Senate LIS file, in file order. */
export function parseSenateMemberVotes(xml: string): FederalMemberVote[] {
  const votes: FederalMemberVote[] = [];
  for (const match of xml.matchAll(/<member>([\s\S]*?)<\/member>/g)) {
    const row = match[1]!;
    votes.push({
      chamber: "senate",
      memberId: required(firstTagText(row, "lis_member_id"), "Senate lis_member_id", row),
      name: required(firstTagText(row, "member_full"), "Senate member_full", row),
      state: required(firstTagText(row, "state"), "Senate state", row),
      party: firstTagText(row, "party"),
      vote: required(firstTagText(row, "vote_cast"), "Senate vote_cast", row),
    });
  }
  return votes;
}

export function parseFederalMemberVotes(chamber: LegislativeVoteChamber, xml: string): FederalMemberVote[] {
  return chamber === "house" ? parseHouseMemberVotes(xml) : parseSenateMemberVotes(xml);
}
