BEGIN;

CREATE TABLE IF NOT EXISTS public.offices (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  scope text NOT NULL,
  canonical_name text NOT NULL,
  summary text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chk_offices_scope
    CHECK (
      scope IN (
        'statewide',
        'us_house',
        'state_upper',
        'state_lower',
        'county',
        'place',
        'school_elementary',
        'school_secondary',
        'school_unified'
      )
    ),
  CONSTRAINT chk_offices_canonical_name_nonempty
    CHECK (length(trim(canonical_name)) > 0),
  CONSTRAINT chk_offices_summary_nonempty
    CHECK (length(trim(summary)) > 0),
  CONSTRAINT uq_offices_scope_canonical_name
    UNIQUE (scope, canonical_name),
  CONSTRAINT uq_offices_id_scope
    UNIQUE (id, scope)
);

CREATE INDEX IF NOT EXISTS idx_offices_scope
  ON public.offices (scope);

DROP TRIGGER IF EXISTS trg_offices_set_updated_at ON public.offices;
CREATE TRIGGER trg_offices_set_updated_at
BEFORE UPDATE ON public.offices
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.office_title_aliases (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  office_id uuid NOT NULL,
  scope text NOT NULL,
  alias_text text NOT NULL,
  normalized_alias text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fk_office_title_aliases_office_scope
    FOREIGN KEY (office_id, scope)
    REFERENCES public.offices (id, scope)
    ON DELETE CASCADE,
  CONSTRAINT chk_office_title_aliases_scope
    CHECK (
      scope IN (
        'statewide',
        'us_house',
        'state_upper',
        'state_lower',
        'county',
        'place',
        'school_elementary',
        'school_secondary',
        'school_unified'
      )
    ),
  CONSTRAINT chk_office_title_aliases_alias_text_nonempty
    CHECK (length(trim(alias_text)) > 0),
  CONSTRAINT chk_office_title_aliases_normalized_alias_nonempty
    CHECK (length(trim(normalized_alias)) > 0),
  CONSTRAINT uq_office_title_aliases_scope_normalized_alias
    UNIQUE (scope, normalized_alias)
);

CREATE INDEX IF NOT EXISTS idx_office_title_aliases_office_id
  ON public.office_title_aliases (office_id);

DROP TRIGGER IF EXISTS trg_office_title_aliases_set_updated_at ON public.office_title_aliases;
CREATE TRIGGER trg_office_title_aliases_set_updated_at
BEFORE UPDATE ON public.office_title_aliases
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

ALTER TABLE public.elections
  ADD COLUMN IF NOT EXISTS office_id uuid;

ALTER TABLE public.elections
  DROP CONSTRAINT IF EXISTS fk_elections_office;

ALTER TABLE public.elections
  ADD CONSTRAINT fk_elections_office
  FOREIGN KEY (office_id)
  REFERENCES public.offices(id)
  ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS idx_elections_office_id
  ON public.elections (office_id);

COMMIT;
