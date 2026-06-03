BEGIN;

UPDATE public.research_areas
SET
  slug = CASE slug
    WHEN 'cost_of_living_and_inflation_reduction' THEN 'cost_of_living_reduction'
    WHEN 'government_spending_and_deficit_reduction' THEN 'government_spending_reduction'
    WHEN 'personal_income_tax_relief' THEN 'personal_income_tax_reduction'
    ELSE slug
  END,
  name = CASE slug
    WHEN 'cost_of_living_and_inflation_reduction' THEN 'Cost of Living Reduction'
    WHEN 'government_spending_and_deficit_reduction' THEN 'Government Spending Reduction'
    WHEN 'personal_income_tax_relief' THEN 'Personal Income Tax Reduction'
    ELSE name
  END,
  updated_at = now()
WHERE slug IN (
  'cost_of_living_and_inflation_reduction',
  'government_spending_and_deficit_reduction',
  'personal_income_tax_relief'
);

COMMIT;
