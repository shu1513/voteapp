UPDATE public.finance_label_classifications
SET industry_slug = 'lawyers_and_legal_services',
    updated_at = now()
WHERE industry_slug = 'legal';

UPDATE public.finance_label_classifications
SET industry_slug = 'agriculture_and_food',
    updated_at = now()
WHERE industry_slug = 'agriculture';

UPDATE public.finance_label_classifications
SET industry_slug = NULL,
    confidence = 'unknown',
    classification_source = 'unknown',
    updated_at = now()
WHERE industry_slug IN ('telecom', 'retail', 'public_sector');

UPDATE public.candidate_finance_direct_breakdowns AS old_breakdown
SET category_name = 'lawyers_and_legal_services',
    updated_at = now()
WHERE old_breakdown.category_type = 'industry'
  AND old_breakdown.category_name = 'legal'
  AND NOT EXISTS (
    SELECT 1
    FROM public.candidate_finance_direct_breakdowns AS existing
    WHERE existing.fec_candidate_id = old_breakdown.fec_candidate_id
      AND existing.election_year = old_breakdown.election_year
      AND existing.category_type = old_breakdown.category_type
      AND existing.category_name = 'lawyers_and_legal_services'
  );

DELETE FROM public.candidate_finance_direct_breakdowns AS old_breakdown
WHERE old_breakdown.category_type = 'industry'
  AND old_breakdown.category_name = 'legal';

UPDATE public.candidate_finance_direct_breakdowns AS old_breakdown
SET category_name = 'agriculture_and_food',
    updated_at = now()
WHERE old_breakdown.category_type = 'industry'
  AND old_breakdown.category_name = 'agriculture'
  AND NOT EXISTS (
    SELECT 1
    FROM public.candidate_finance_direct_breakdowns AS existing
    WHERE existing.fec_candidate_id = old_breakdown.fec_candidate_id
      AND existing.election_year = old_breakdown.election_year
      AND existing.category_type = old_breakdown.category_type
      AND existing.category_name = 'agriculture_and_food'
  );

DELETE FROM public.candidate_finance_direct_breakdowns AS old_breakdown
WHERE old_breakdown.category_type = 'industry'
  AND old_breakdown.category_name IN ('agriculture', 'telecom', 'retail', 'public_sector');

UPDATE public.candidate_finance_outside_group_breakdowns AS old_breakdown
SET category_name = 'lawyers_and_legal_services',
    updated_at = now()
WHERE old_breakdown.category_type = 'industry'
  AND old_breakdown.category_name = 'legal'
  AND NOT EXISTS (
    SELECT 1
    FROM public.candidate_finance_outside_group_breakdowns AS existing
    WHERE existing.fec_candidate_id = old_breakdown.fec_candidate_id
      AND existing.election_year = old_breakdown.election_year
      AND existing.committee_id = old_breakdown.committee_id
      AND existing.support_oppose = old_breakdown.support_oppose
      AND existing.category_type = old_breakdown.category_type
      AND existing.category_name = 'lawyers_and_legal_services'
  );

DELETE FROM public.candidate_finance_outside_group_breakdowns AS old_breakdown
WHERE old_breakdown.category_type = 'industry'
  AND old_breakdown.category_name = 'legal';

UPDATE public.candidate_finance_outside_group_breakdowns AS old_breakdown
SET category_name = 'agriculture_and_food',
    updated_at = now()
WHERE old_breakdown.category_type = 'industry'
  AND old_breakdown.category_name = 'agriculture'
  AND NOT EXISTS (
    SELECT 1
    FROM public.candidate_finance_outside_group_breakdowns AS existing
    WHERE existing.fec_candidate_id = old_breakdown.fec_candidate_id
      AND existing.election_year = old_breakdown.election_year
      AND existing.committee_id = old_breakdown.committee_id
      AND existing.support_oppose = old_breakdown.support_oppose
      AND existing.category_type = old_breakdown.category_type
      AND existing.category_name = 'agriculture_and_food'
  );

DELETE FROM public.candidate_finance_outside_group_breakdowns AS old_breakdown
WHERE old_breakdown.category_type = 'industry'
  AND old_breakdown.category_name IN ('agriculture', 'telecom', 'retail', 'public_sector');
