import { partyBucket } from "./partyBucket";

// candidate.party is ballot-facing: nonpartisan contests store the
// placeholder "Nonpartisan" (and unresearched rows "Unknown"), which is
// correct next to a ballot but reads as the candidate's personal
// affiliation on profile-style views (profile header, share text,
// followed-candidates list, candidate search results). Those views hide
// the placeholder; election/ballot views keep showing it, matching the
// ballot itself. Lives here so web and mobile share one policy, like
// partyBucket.
const HIDDEN_PROFILE_PARTY_LABELS = new Set(["nonpartisan", "unknown"]);

export function profilePartyLabel(party: string | null | undefined): string {
  const trimmed = party?.trim() ?? "";
  return HIDDEN_PROFILE_PARTY_LABELS.has(trimmed.toLowerCase()) ? "" : trimmed;
}

// Brand color for a party label, reusing the filter buckets so affiliate
// and registration labels ("Democratic-Farmer-Labor", "Registered
// Republican") color like their major party. Everything else returns ""
// and keeps the surrounding text color (gray metadata).
export function partyColorClass(party: string | null | undefined): string {
  switch (partyBucket(party)) {
    case "democratic":
      return "text-dem-blue";
    case "republican":
      return "text-gop-red";
    default:
      return "";
  }
}
