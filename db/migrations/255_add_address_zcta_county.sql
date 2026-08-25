BEGIN;

-- ZCTA -> county crosswalk for the ZIP partial-ballot path
-- (docs/plans/partial-address-scope.md). Loaded from the Census 2020
-- ZCTA/county relationship file by `npm run import:zcta-county-crosswalk`;
-- the resolver only offers county races when a ZCTA has exactly one county
-- row, so no overlap-share column is stored. Read-only for the API (SELECT
-- arrives through the role's default privileges; docs/postgres-api-role.md).
CREATE TABLE IF NOT EXISTS public.address_zcta_county (
  zcta5 text NOT NULL,
  county_geoid text NOT NULL,
  PRIMARY KEY (zcta5, county_geoid),
  CONSTRAINT chk_address_zcta_county_zcta5_shape CHECK (zcta5 ~ '^[0-9]{5}$'),
  CONSTRAINT chk_address_zcta_county_geoid_shape CHECK (county_geoid ~ '^[0-9]{5}$')
);

COMMIT;
