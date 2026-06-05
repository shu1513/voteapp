BEGIN;

INSERT INTO public.research_areas (slug, name, description)
VALUES
  (
    'environment_and_public_health',
    'Environment and Public Health',
    'Protect air, water, climate, and community health through standards, enforcement, and prevention.'
  ),
  (
    'healthcare_affordability',
    'Healthcare Affordability',
    'Reduce out-of-pocket costs and improve access to affordable, quality care.'
  ),
  (
    'public_safety_and_crime_control',
    'Public Safety and Crime Control',
    'Improve safety through effective policing, prevention, accountability, and justice system performance.'
  ),
  (
    'womens_reproductive_rights',
    'Women''s Reproductive Rights',
    'Protect legal access to reproductive healthcare and individual bodily autonomy.'
  ),
  (
    'immigration',
    'Immigration',
    'Welcome immigration through a lawful, orderly, and humane system, with priority for skilled workers, researchers, and other high-impact contributors.'
  ),
  (
    'election_integrity',
    'Election Integrity',
    'Ensure elections are secure, accurate, auditable, and trusted by the public.'
  ),
  (
    'social_programs_and_welfare',
    'Social Programs and Welfare',
    'Support vulnerable populations through effective safety-net and anti-poverty programs.'
  ),
  (
    'data_privacy',
    'Data Privacy',
    'Protect personal data rights through clear limits on collection, sharing, and misuse.'
  ),
  (
    'corporate_accountability',
    'Corporate Accountability',
    'Hold companies accountable for legal compliance, consumer protection, and public impact.'
  ),
  (
    'anti_corruption',
    'Anti-Corruption',
    'Prevent abuse of public office through transparency, ethics rules, and enforcement.'
  ),
  (
    'government_efficiency',
    'Government Efficiency',
    'Improve service delivery, reduce waste, and modernize administrative operations.'
  ),
  (
    'housing_affordability',
    'Housing Affordability',
    'Increase housing supply and reduce cost burdens for renters and homebuyers.'
  ),
  (
    'civil_rights',
    'Civil Rights',
    'Protect equal rights, anti-discrimination enforcement, and fair treatment under law.'
  ),
  (
    'public_education_quality',
    'Public Education Quality',
    'Strengthen student outcomes through effective teaching, standards, funding, and accountability.'
  )
ON CONFLICT (slug)
DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  updated_at = now();

COMMIT;
