BEGIN;

-- Current-cycle competitiveness rating per election, researched manually.
-- The vote-power decisiveness axis prefers a fresh, confident row here over
-- historical_contest_margins; see docs/plans/current-decisiveness-2026-08.md.
CREATE TABLE IF NOT EXISTS public.current_race_ratings (
  election_id uuid PRIMARY KEY REFERENCES public.elections(id) ON DELETE CASCADE,
  schema_version text NOT NULL,
  competitiveness_label text,
  method text NOT NULL,
  confidence text,
  evidence_status text NOT NULL,
  as_of date,
  decisive_round text,
  evidence jsonb NOT NULL,
  source_url text NOT NULL,
  researched_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT current_race_ratings_schema_check
    CHECK (schema_version = 'current_race_rating.v1'),
  CONSTRAINT current_race_ratings_status_check
    CHECK (evidence_status IN ('rated', 'none_found')),
  CONSTRAINT current_race_ratings_label_check
    CHECK (
      competitiveness_label IS NULL
      OR competitiveness_label IN (
        'toss_up',
        'very_competitive',
        'competitive',
        'somewhat_competitive',
        'safe'
      )
    ),
  CONSTRAINT current_race_ratings_rated_fields_check
    CHECK (
      (
        evidence_status = 'rated'
        AND competitiveness_label IS NOT NULL
        AND confidence IS NOT NULL
        AND as_of IS NOT NULL
      )
      OR (
        evidence_status = 'none_found'
        AND competitiveness_label IS NULL
        AND confidence IS NULL
        AND as_of IS NULL
      )
    ),
  CONSTRAINT current_race_ratings_method_check
    CHECK (method IN ('outlet_consensus', 'mayoral_rubric')),
  CONSTRAINT current_race_ratings_confidence_check
    CHECK (confidence IS NULL OR confidence IN ('high', 'medium', 'low')),
  CONSTRAINT current_race_ratings_decisive_round_check
    CHECK (
      decisive_round IS NULL
      OR (method = 'mayoral_rubric' AND btrim(decisive_round) <> '')
    ),
  CONSTRAINT current_race_ratings_source_url_check
    CHECK (btrim(source_url) <> ''),
  CONSTRAINT current_race_ratings_evidence_check
    CHECK (jsonb_typeof(evidence) = 'object')
);

DROP TRIGGER IF EXISTS trg_current_race_ratings_set_updated_at
  ON public.current_race_ratings;
CREATE TRIGGER trg_current_race_ratings_set_updated_at
BEFORE UPDATE ON public.current_race_ratings
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
