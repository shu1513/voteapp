BEGIN;

ALTER TABLE user_districts
  DROP CONSTRAINT IF EXISTS fk_user_districts_district;

ALTER TABLE districts
  DROP CONSTRAINT IF EXISTS chk_district_type;

ALTER TABLE user_districts
  DROP CONSTRAINT IF EXISTS chk_user_districts_type;

ALTER TABLE user_districts
  DROP CONSTRAINT IF EXISTS chk_user_district_type;

UPDATE districts
SET district_type = 'statewide'
WHERE district_type = 'us_senate';

UPDATE user_districts
SET district_type = 'statewide'
WHERE district_type = 'us_senate';

ALTER TABLE districts
  ADD CONSTRAINT chk_district_type
  CHECK (
    district_type IN (
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

ALTER TABLE user_districts
  ADD CONSTRAINT chk_user_districts_type
  CHECK (
    district_type IN (
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

ALTER TABLE user_districts
  ADD CONSTRAINT fk_user_districts_district
  FOREIGN KEY (district_id, district_type)
  REFERENCES districts(id, district_type)
  ON DELETE CASCADE;

COMMIT;
