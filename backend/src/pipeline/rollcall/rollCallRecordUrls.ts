import { parseFederalMeasure, type FederalMeasure } from "./federalMeasures.js";
import type { LegislativeVoteChamber } from "./legislativeVotes.js";

// Which roll call a candidate_records.source_url points at, for the
// fan-out's duplicate scan (docs/plans/roll-call-vote-import.md §3). The
// hand-written rows cite the same vote three ways — the Clerk's XML file,
// the Clerk's vote page, and the Senate page in .htm or .xml — so URLs are
// folded to one key per roll call and compared on that, never as strings.

export type RollCallUrlKey = {
  chamber: LegislativeVoteChamber;
  // `house:<year>:<roll>` or `senate:<congress>-<session>:<roll>`.
  key: string;
};

const HOUSE_XML = /^https?:\/\/clerk\.house\.gov\/evs\/(\d{4})\/roll(\d{1,4})\.xml$/i;
// `https://clerk.house.gov/Votes/2025145`, sometimes with a query string.
const HOUSE_PAGE = /^https?:\/\/clerk\.house\.gov\/Votes\/(\d{4})(\d{1,4})(?:[?#].*)?$/i;
const SENATE =
  /^https?:\/\/(?:www\.)?senate\.gov\/legislative\/LIS\/roll_call_votes\/vote\d+\/vote_(\d+)_([12])_(\d+)\.(?:htm|xml)(?:[?#].*)?$/i;

/** The roll call a URL cites, or null when it is not a roll-call URL. */
export function rollCallUrlKey(url: string): RollCallUrlKey | null {
  const trimmed = url.trim();
  const houseXml = HOUSE_XML.exec(trimmed) ?? HOUSE_PAGE.exec(trimmed);
  if (houseXml) {
    return { chamber: "house", key: `house:${houseXml[1]}:${Number(houseXml[2])}` };
  }
  const senate = SENATE.exec(trimmed);
  if (senate) {
    return { chamber: "senate", key: `senate:${senate[1]}-${senate[2]}:${Number(senate[3])}` };
  }
  return null;
}

// The Clerk's member-vote search page: it names the roll only by number
// (`RollCallNum=102`), with no year or congress.
const HOUSE_MEMBER_VOTES = /^https?:\/\/clerk\.house\.gov\/Votes\/MemberVotes\?[^#]*\bRollCallNum=(\d+)\b/i;

/**
 * True when the URL cites the given roll call. On top of rollCallUrlKey's
 * exact shapes, this folds the Clerk's MemberVotes search page — which
 * carries only the roll number, so the comparison is exact ONLY among rows
 * already on the vote's own date (House roll numbers restart each year);
 * the fan-out's duplicate scan is date-scoped, its only caller.
 */
export function citesSameRollCall(url: string, rollCallKey: string): boolean {
  const key = rollCallUrlKey(url);
  if (key) {
    return key.key === rollCallKey;
  }
  const memberVotes = HOUSE_MEMBER_VOTES.exec(url.trim());
  if (!memberVotes) {
    return false;
  }
  const houseKey = /^house:\d{4}:(\d+)$/.exec(rollCallKey);
  return houseKey !== null && Number(memberVotes[1]) === Number(houseKey[1]);
}

/** True when the URL is any roll-call citation this module can recognize. */
export function citesAnyRollCall(url: string): boolean {
  return rollCallUrlKey(url) !== null || HOUSE_MEMBER_VOTES.test(url.trim());
}

/**
 * True when the text reads like a claim about voting. Same-day rows that
 * make one without citing any recognizable roll call may be this vote
 * described off a press release; the pilot's misses ("voting for final
 * passage of the Laken Riley Act", "voted in favor of the 2025 budget
 * reconciliation bill") name neither a bill id nor the feed's title, so
 * words are the only signal left. Report-only.
 */
export function looksLikeVoteClaim(description: string): boolean {
  return /\bvot(?:e|ed|es|ing)\b/i.test(description);
}

// A measure as a description spells it: `H.R. 29`, `H. R. 29`, `HR 29`,
// `H.Res. 863`, `H. Con. Res. 86`, `S. 5`, `S.J.Res. 3`. The lookbehind
// keeps `U.S. 5` from reading as `S. 5`.
const MEASURE_MENTION =
  /(?<![A-Za-z.])(H\.?\s?(?:CON\.?\s?RES|J\.?\s?RES|RES|R)\.?|S\.?\s?(?:CON\.?\s?RES|J\.?\s?RES|RES)\.?|S\.)\s?(\d+)\b/gi;

/**
 * True when the text names `measure` (any spelling). Report-only signal: a
 * same-day record that cites a press release but describes this bill may be
 * the same vote or a different one (a recommit vote on the same bill), and
 * free text cannot tell, so the importer lists such rows for a human rather
 * than rewriting them.
 */
export function descriptionMentionsMeasure(description: string, measure: FederalMeasure): boolean {
  for (const match of description.matchAll(MEASURE_MENTION)) {
    const parsed = parseFederalMeasure(`${match[1]}${match[2]}`);
    if (parsed && parsed.type === measure.type && parsed.number === measure.number) {
      return true;
    }
  }
  return false;
}
