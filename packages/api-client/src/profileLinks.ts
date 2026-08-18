// Outbound links shown under the candidate profile header, in display
// order. Web and mobile render the same list, so the X URL construction
// and the "which fields count as links" policy live here once.
//
// twitter_handle is stored as a bare lowercase handle (normalizeTwitterHandle
// in the backend strips the @ and any x.com/twitter.com URL wrapper); the
// client rebuilds the URL. The handle shape is re-checked here so a stray
// malformed row can never become an href — it is dropped instead.
//
// linkedin_url is only validated as http(s) on the write path, but the
// label asserts the domain, so a non-LinkedIn host is dropped the same way
// rather than shown as "LinkedIn". Host must be linkedin.com or a subdomain
// (www., uk., …) and end right there — "linkedin.com.evil.example" fails.
const TWITTER_HANDLE = /^[A-Za-z0-9_]{1,15}$/;
const LINKEDIN_URL = /^https?:\/\/([a-z0-9-]+\.)*linkedin\.com(?:[/?#]|$)/i;

export type CandidateProfileLink = { href: string; label: string };

export function candidateProfileLinks(candidate: {
  official_website_url: string | null;
  twitter_handle: string | null;
  linkedin_url: string | null;
}): CandidateProfileLink[] {
  const links: CandidateProfileLink[] = [];
  if (candidate.official_website_url) {
    links.push({ href: candidate.official_website_url, label: "Official website" });
  }
  if (candidate.twitter_handle && TWITTER_HANDLE.test(candidate.twitter_handle)) {
    links.push({ href: `https://x.com/${candidate.twitter_handle}`, label: "X (Twitter)" });
  }
  if (candidate.linkedin_url && LINKEDIN_URL.test(candidate.linkedin_url)) {
    links.push({ href: candidate.linkedin_url, label: "LinkedIn" });
  }
  return links;
}
