BEGIN;

ALTER TABLE public.research_areas
  ADD COLUMN IF NOT EXISTS is_user_selectable boolean NOT NULL DEFAULT true;

COMMENT ON COLUMN public.research_areas.is_user_selectable IS
  'Whether this research area can be selected as a user preference.';

UPDATE public.research_areas
SET is_user_selectable = false
WHERE slug IN ('general', 'impartiality');

CREATE TABLE IF NOT EXISTS public.user_research_area_preferences (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL,
  research_area_id uuid NOT NULL,
  rank integer,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fk_user_research_area_preferences_user
    FOREIGN KEY (user_id)
    REFERENCES public.users (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_user_research_area_preferences_research_area
    FOREIGN KEY (research_area_id)
    REFERENCES public.research_areas (id)
    ON DELETE CASCADE,
  CONSTRAINT uq_user_research_area_preferences_user_area
    UNIQUE (user_id, research_area_id),
  CONSTRAINT chk_user_research_area_preferences_rank
    CHECK (rank IS NULL OR rank BETWEEN 1 AND 7)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_user_research_area_preferences_user_rank
  ON public.user_research_area_preferences (user_id, rank)
  WHERE rank IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_user_research_area_preferences_user_id
  ON public.user_research_area_preferences (user_id);

CREATE INDEX IF NOT EXISTS idx_user_research_area_preferences_research_area_id
  ON public.user_research_area_preferences (research_area_id);

DROP TRIGGER IF EXISTS trg_user_research_area_preferences_set_updated_at
  ON public.user_research_area_preferences;
CREATE TRIGGER trg_user_research_area_preferences_set_updated_at
BEFORE UPDATE ON public.user_research_area_preferences
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
