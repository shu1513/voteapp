BEGIN;

ALTER TABLE public.historical_contest_margins
  DROP CONSTRAINT IF EXISTS chk_historical_contest_margins_office_type,
  DROP CONSTRAINT IF EXISTS chk_historical_contest_margins_district_type;

ALTER TABLE public.historical_contest_margins
  ADD CONSTRAINT chk_historical_contest_margins_office_type
    CHECK (
      office_type IN (
        'US_PRESIDENT',
        'US_SENATE',
        'US_HOUSE',
        'GOVERNOR',
        'LIEUTENANT_GOVERNOR',
        'SECRETARY_OF_STATE',
        'ATTORNEY_GENERAL',
        'STATE_TREASURER',
        'STATE_AUDITOR',
        'COMPTROLLER',
        'SUPERINTENDENT_OF_PUBLIC_INSTRUCTION',
        'COMMISSIONER_OF_AGRICULTURE',
        'COMMISSIONER_OF_INSURANCE',
        'LABOR_COMMISSIONER',
        'LAND_COMMISSIONER',
        'STATE_SENATE',
        'STATE_HOUSE',
        'COUNTY_SHERIFF',
        'DISTRICT_ATTORNEY',
        'COUNTY_CLERK',
        'COUNTY_ASSESSOR',
        'COUNTY_AUDITOR',
        'COUNTY_TREASURER',
        'COUNTY_RECORDER',
        'COUNTY_CORONER'
      )
    ),
  ADD CONSTRAINT chk_historical_contest_margins_district_type
    CHECK (district_type IN ('statewide', 'us_house', 'state_upper', 'state_lower', 'county'));

COMMIT;
