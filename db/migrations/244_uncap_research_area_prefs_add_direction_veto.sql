-- Auto-pick by issue alignment, PR 1 (docs/plans/auto-pick-by-issues.md).
--
-- 1. user_research_area_preferences: drop the rank ceiling (was 1..7, paired
--    with the deleted MAX_USER_RESEARCH_AREA_PREFERENCES constant) so a user
--    can rank every selectable issue; add per-issue `direction` (support or
--    oppose the issue's goal — catalog rows are goals, so "I care about gun
--    policy" alone does not say which way) and `hard_veto` (the "line in the
--    sand": a candidate who opposes this issue is never auto-picked).
-- 2. user_election_choices: `origin` marks auto-picked rows so the UI can
--    badge them and "clear my auto picks" can find them. Manual writes keep
--    stamping 'manual' (default + explicit on upsert).
BEGIN;

ALTER TABLE public.user_research_area_preferences
  DROP CONSTRAINT IF EXISTS chk_user_research_area_preferences_rank,
  ADD CONSTRAINT chk_user_research_area_preferences_rank
    CHECK (rank IS NULL OR rank >= 1),
  ADD COLUMN IF NOT EXISTS direction text NOT NULL DEFAULT 'support',
  ADD COLUMN IF NOT EXISTS hard_veto boolean NOT NULL DEFAULT false;

ALTER TABLE public.user_research_area_preferences
  DROP CONSTRAINT IF EXISTS chk_user_research_area_preferences_direction,
  ADD CONSTRAINT chk_user_research_area_preferences_direction
    CHECK (direction IN ('support', 'oppose'));

COMMENT ON COLUMN public.user_research_area_preferences.direction IS
  'Whether the user supports or opposes this research area''s stated goal; drives auto-pick alignment.';
COMMENT ON COLUMN public.user_research_area_preferences.hard_veto IS
  'Line in the sand: a candidate or measure that opposes this issue (per direction) is never auto-picked.';

ALTER TABLE public.user_election_choices
  ADD COLUMN IF NOT EXISTS origin text NOT NULL DEFAULT 'manual';

ALTER TABLE public.user_election_choices
  DROP CONSTRAINT IF EXISTS chk_user_election_choices_origin,
  ADD CONSTRAINT chk_user_election_choices_origin
    CHECK (origin IN ('manual', 'auto'));

COMMENT ON COLUMN public.user_election_choices.origin IS
  'manual = the user picked it; auto = written by auto-pick. A manual pick on the same row resets it to manual.';

COMMIT;
