UPDATE state_resources
SET id_requirements = CASE
  WHEN lower(id_requirements) ~ '\y(id\s+is\s+not\s+required|no\s+(photo\s+)?id\s+(is\s+)?required|no\s+document\s+required|does\s+not\s+require\s+id)\y'
    THEN 'No document required to vote'
  WHEN lower(id_requirements) ~ '\ynon[- ]?strict\y' AND lower(id_requirements) ~ '\y(non[- ]?photo|without\s+photo|no\s+photo)\y'
    THEN 'Non-strict, non-photo ID'
  WHEN lower(id_requirements) ~ '\ynon[- ]?strict\y' AND lower(id_requirements) ~ '\yphoto\y'
    THEN 'Non-strict photo ID'
  WHEN lower(id_requirements) ~ '\ystrict\y' AND lower(id_requirements) ~ '\y(non[- ]?photo|without\s+photo|no\s+photo)\y'
    THEN 'Strict non-photo ID'
  WHEN lower(id_requirements) ~ '\ystrict\y' AND lower(id_requirements) ~ '\yphoto\y'
    THEN 'Strict photo ID'
  ELSE id_requirements
END;

ALTER TABLE state_resources
  VALIDATE CONSTRAINT chk_state_resources_id_requirements_text;
