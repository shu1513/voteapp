BEGIN;

CREATE TABLE public.ballot_measure_research_area_tags (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  ballot_measure_id uuid NOT NULL,
  research_area_id uuid NOT NULL,
  stance text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_ballot_measure_research_area_tags_measure_area
    UNIQUE (ballot_measure_id, research_area_id),
  CONSTRAINT fk_ballot_measure_research_area_tags_measure
    FOREIGN KEY (ballot_measure_id)
    REFERENCES public.ballot_measures (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_ballot_measure_research_area_tags_research_area
    FOREIGN KEY (research_area_id)
    REFERENCES public.research_areas (id)
    ON DELETE CASCADE,
  CONSTRAINT chk_ballot_measure_research_area_tags_stance
    CHECK (stance IN ('for', 'against'))
);

CREATE INDEX idx_ballot_measure_research_area_tags_measure_id
  ON public.ballot_measure_research_area_tags (ballot_measure_id);

CREATE INDEX idx_ballot_measure_research_area_tags_research_area_id
  ON public.ballot_measure_research_area_tags (research_area_id);

CREATE INDEX idx_ballot_measure_research_area_tags_stance
  ON public.ballot_measure_research_area_tags (stance);

DROP TRIGGER IF EXISTS trg_ballot_measure_research_area_tags_set_updated_at
  ON public.ballot_measure_research_area_tags;
CREATE TRIGGER trg_ballot_measure_research_area_tags_set_updated_at
BEFORE UPDATE ON public.ballot_measure_research_area_tags
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
