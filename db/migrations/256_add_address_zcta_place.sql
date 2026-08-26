BEGIN;

-- ZIP partial-ballot place upgrade (docs/plans/partial-address-scope.md):
-- one row per 2020 ZCTA that lies WHOLLY inside a single legally incorporated
-- place (Census tab20_zcta520_place20_natl relationship file; CDPs excluded,
-- zero land outside the place). Such ZIPs honestly get the place's races
-- (mayor etc.) on top of statewide/county. Loaded by
-- `npm run import:zcta-place-crosswalk`; one-time per decade. Read-only for
-- the API (SELECT arrives through the role's default privileges;
-- docs/postgres-api-role.md).
CREATE TABLE IF NOT EXISTS public.address_zcta_place (
  zcta5 text PRIMARY KEY,
  place_geoid text NOT NULL,
  CONSTRAINT chk_address_zcta_place_zcta5_shape CHECK (zcta5 ~ '^[0-9]{5}$'),
  CONSTRAINT chk_address_zcta_place_geoid_shape CHECK (place_geoid ~ '^[0-9]{7}$')
);

COMMIT;
