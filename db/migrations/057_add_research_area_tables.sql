BEGIN;

CREATE TABLE IF NOT EXISTS public.research_areas (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  slug text NOT NULL,
  name text NOT NULL,
  description text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_research_areas_slug
    UNIQUE (slug),
  CONSTRAINT chk_research_areas_slug_nonempty
    CHECK (length(trim(slug)) > 0),
  CONSTRAINT chk_research_areas_name_nonempty
    CHECK (length(trim(name)) > 0),
  CONSTRAINT chk_research_areas_slug_format
    CHECK (slug ~ '^[a-z0-9]+(?:_[a-z0-9]+)*$')
);

DROP TRIGGER IF EXISTS trg_research_areas_set_updated_at ON public.research_areas;
CREATE TRIGGER trg_research_areas_set_updated_at
BEFORE UPDATE ON public.research_areas
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.office_research_areas (
  office_id uuid NOT NULL,
  research_area_id uuid NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT pk_office_research_areas
    PRIMARY KEY (office_id, research_area_id),
  CONSTRAINT fk_office_research_areas_office
    FOREIGN KEY (office_id)
    REFERENCES public.offices (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_office_research_areas_research_area
    FOREIGN KEY (research_area_id)
    REFERENCES public.research_areas (id)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_office_research_areas_research_area_id
  ON public.office_research_areas (research_area_id);

CREATE TABLE IF NOT EXISTS public.candidate_record_area_tags (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_record_id uuid NOT NULL,
  research_area_id uuid NOT NULL,
  stance text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_candidate_record_area_tags_record_area
    UNIQUE (candidate_record_id, research_area_id),
  CONSTRAINT fk_candidate_record_area_tags_record
    FOREIGN KEY (candidate_record_id)
    REFERENCES public.candidate_records (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_candidate_record_area_tags_research_area
    FOREIGN KEY (research_area_id)
    REFERENCES public.research_areas (id)
    ON DELETE CASCADE,
  CONSTRAINT chk_candidate_record_area_tags_stance
    CHECK (stance IN ('for', 'against', 'neutral', 'mixed', 'unknown'))
);

CREATE INDEX IF NOT EXISTS idx_candidate_record_area_tags_record_id
  ON public.candidate_record_area_tags (candidate_record_id);

CREATE INDEX IF NOT EXISTS idx_candidate_record_area_tags_research_area_id
  ON public.candidate_record_area_tags (research_area_id);

CREATE INDEX IF NOT EXISTS idx_candidate_record_area_tags_stance
  ON public.candidate_record_area_tags (stance);

DROP TRIGGER IF EXISTS trg_candidate_record_area_tags_set_updated_at ON public.candidate_record_area_tags;
CREATE TRIGGER trg_candidate_record_area_tags_set_updated_at
BEFORE UPDATE ON public.candidate_record_area_tags
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
