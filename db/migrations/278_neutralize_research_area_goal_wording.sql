-- Reword three research-area goals so they state a goal, not an argument.
-- research_areas.description is fed verbatim into the record-labeling prompt
-- as the goal that stance is measured against, and shown to users in the
-- issue picker. The old text bundled contested policy prescriptions (tariff
-- cuts, "instead of war") or a justification ("to reduce gun violence") into
-- the goal itself. Slugs are unchanged; existing tags and preferences stay.
BEGIN;

UPDATE public.research_areas
SET description = 'Lower household costs for housing, energy, prescription drugs, food, and everyday and imported goods.',
    updated_at = now()
WHERE slug = 'cost_of_living_reduction';

UPDATE public.research_areas
SET description = 'Favor diplomacy over military force in foreign conflicts.',
    updated_at = now()
WHERE slug = 'peaceful_foreign_policy';

UPDATE public.research_areas
SET description = 'Regulate firearm access through background checks, licensing, and safe-storage rules.',
    updated_at = now()
WHERE slug = 'gun_control';

COMMIT;
