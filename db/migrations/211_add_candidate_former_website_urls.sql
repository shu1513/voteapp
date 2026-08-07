BEGIN;

-- Campaign sites change when a candidate enters a new race, so a stored
-- official_website_url goes stale. Profile re-writes now replace the stored
-- URL with the freshly researched one and archive the previous URL here
-- (jsonb array of strings). Identity matching treats archived URLs as hard
-- identifiers alongside the current one: a payload carrying either the old
-- or the new site still matches the person instead of minting a duplicate
-- row.
ALTER TABLE candidates
  ADD COLUMN former_website_urls jsonb;

COMMENT ON COLUMN candidates.former_website_urls IS
  'Previous official/campaign website URLs (jsonb array of strings), archived when a profile re-write replaces official_website_url. Matched as hard identifiers alongside the current URL.';

COMMIT;
