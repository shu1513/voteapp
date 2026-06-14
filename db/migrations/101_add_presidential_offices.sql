BEGIN;

ALTER TABLE public.offices
  DROP CONSTRAINT IF EXISTS chk_offices_scope;

ALTER TABLE public.offices
  ADD CONSTRAINT chk_offices_scope
    CHECK (
      scope IN (
        'presidential',
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
    );

ALTER TABLE public.office_title_aliases
  DROP CONSTRAINT IF EXISTS chk_office_title_aliases_scope;

ALTER TABLE public.office_title_aliases
  ADD CONSTRAINT chk_office_title_aliases_scope
    CHECK (
      scope IN (
        'presidential',
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
    );

COMMIT;
