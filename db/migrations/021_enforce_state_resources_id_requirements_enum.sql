UPDATE state_resources
SET id_requirements = CASE
  WHEN lower(id_requirements) ~ '\b(id\s+is\s+not\s+required|no\s+(photo\s+)?id\s+(is\s+)?required|no\s+document\s+required|does\s+not\s+require\s+id)\b'
    THEN 'No document required to vote'
  WHEN lower(id_requirements) ~ '\bstrict\b' AND lower(id_requirements) ~ '\bphoto\b'
    THEN 'Strict photo ID'
  WHEN lower(id_requirements) ~ '\bstrict\b' AND lower(id_requirements) ~ '\b(non[- ]?photo|without\s+photo|no\s+photo)\b'
    THEN 'Strict non-photo ID'
  WHEN lower(id_requirements) ~ '\bnon[- ]?strict\b' AND lower(id_requirements) ~ '\bphoto\b'
    THEN 'Non-strict photo ID'
  WHEN lower(id_requirements) ~ '\bnon[- ]?strict\b' AND lower(id_requirements) ~ '\b(non[- ]?photo|without\s+photo|no\s+photo)\b'
    THEN 'Non-strict, non-photo ID'
  ELSE id_requirements
END;

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_id_requirements_text;

-- NOT VALID keeps migration safe for legacy rows while enforcing all new/updated rows.
ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_id_requirements_text
  CHECK (
    id_requirements IN (
      'Strict photo ID',
      'Strict non-photo ID',
      'Non-strict photo ID',
      'Non-strict, non-photo ID',
      'No document required to vote'
    )
  )
  NOT VALID;
