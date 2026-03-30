ALTER TABLE districts
  DROP COLUMN IF EXISTS registered_voters,
  DROP COLUMN IF EXISTS boundary_data;
