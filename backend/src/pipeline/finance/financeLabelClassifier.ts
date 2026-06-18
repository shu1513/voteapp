export type FinanceLabelType = "employer" | "occupation" | "donor" | "committee";

export const FINANCE_INDUSTRY_SLUGS = [
  "technology",
  "oil_gas_energy",
  "healthcare",
  "pharmaceuticals",
  "finance_investment",
  "lawyers_and_legal_services",
  "real_estate",
  "construction",
  "education",
  "defense_aerospace",
  "agriculture_and_food",
  "manufacturing",
  "insurance",
  "hospitality",
  "transportation",
  "labor_unions",
  "environmental_group",
] as const;

export type FinanceIndustrySlug = (typeof FINANCE_INDUSTRY_SLUGS)[number];

export type FinanceClassificationConfidence = "high" | "medium" | "low" | "unknown";
export type FinanceClassificationSource = "rule" | "manual" | "ai" | "unknown";

export type FinanceLabelClassification = {
  rawLabel: string;
  labelType: FinanceLabelType;
  normalizedLabel: string;
  industrySlug: FinanceIndustrySlug | null;
  confidence: FinanceClassificationConfidence;
  classificationSource: FinanceClassificationSource;
  matchedRule: string | null;
};

type ClassificationRule = {
  industrySlug: FinanceIndustrySlug | null;
  confidence: Exclude<FinanceClassificationConfidence, "unknown">;
  name: string;
};

type PatternRule = ClassificationRule & {
  pattern: RegExp;
};

const BUSINESS_SUFFIX_PATTERN = /\b(INC|INCORPORATED|LLC|L L C|LP|L P|LLP|L L P|LTD|LIMITED|CO|COMPANY|CORP|CORPORATION|PLC)\b/g;

const KNOWN_NON_INDUSTRY_OCCUPATIONS = new Set([
  "HOMEMAKER",
  "NONE",
  "NOT EMPLOYED",
  "NOT-EMPLOYED",
  "RETIRED",
  "SELF EMPLOYED",
  "SELF-EMPLOYED",
  "UNEMPLOYED",
]);

const EXACT_OCCUPATION_RULES = new Map<string, ClassificationRule>([
  ["ATTORNEY", { industrySlug: "lawyers_and_legal_services", confidence: "high", name: "occupation_exact_attorney" }],
  ["LAWYER", { industrySlug: "lawyers_and_legal_services", confidence: "high", name: "occupation_exact_lawyer" }],
  ["PHYSICIAN", { industrySlug: "healthcare", confidence: "high", name: "occupation_exact_physician" }],
  ["DOCTOR", { industrySlug: "healthcare", confidence: "high", name: "occupation_exact_doctor" }],
  ["NURSE", { industrySlug: "healthcare", confidence: "high", name: "occupation_exact_nurse" }],
  ["DENTIST", { industrySlug: "healthcare", confidence: "high", name: "occupation_exact_dentist" }],
  ["SOFTWARE ENGINEER", { industrySlug: "technology", confidence: "high", name: "occupation_exact_software_engineer" }],
  ["PROGRAMMER", { industrySlug: "technology", confidence: "high", name: "occupation_exact_programmer" }],
  ["REAL ESTATE", { industrySlug: "real_estate", confidence: "high", name: "occupation_exact_real_estate" }],
  ["REALTOR", { industrySlug: "real_estate", confidence: "high", name: "occupation_exact_realtor" }],
  ["CONSTRUCTION", { industrySlug: "construction", confidence: "high", name: "occupation_exact_construction" }],
  ["CONTRACTOR", { industrySlug: "construction", confidence: "medium", name: "occupation_exact_contractor" }],
  ["OIL AND GAS", { industrySlug: "oil_gas_energy", confidence: "high", name: "occupation_exact_oil_gas" }],
  ["OIL & GAS", { industrySlug: "oil_gas_energy", confidence: "high", name: "occupation_exact_oil_gas_ampersand" }],
  ["FARMER", { industrySlug: "agriculture_and_food", confidence: "high", name: "occupation_exact_farmer" }],
  ["TEACHER", { industrySlug: "education", confidence: "high", name: "occupation_exact_teacher" }],
  ["PROFESSOR", { industrySlug: "education", confidence: "high", name: "occupation_exact_professor" }],
]);

const EXACT_ORGANIZATION_RULES = new Map<string, ClassificationRule>([
  ["GOOGLE", { industrySlug: "technology", confidence: "high", name: "organization_exact_google" }],
  ["MICROSOFT", { industrySlug: "technology", confidence: "high", name: "organization_exact_microsoft" }],
  ["META", { industrySlug: "technology", confidence: "high", name: "organization_exact_meta" }],
  ["FACEBOOK", { industrySlug: "technology", confidence: "high", name: "organization_exact_facebook" }],
  ["APPLE", { industrySlug: "technology", confidence: "high", name: "organization_exact_apple" }],
  ["AMAZON", { industrySlug: "technology", confidence: "high", name: "organization_exact_amazon" }],
  ["PALANTIR", { industrySlug: "technology", confidence: "high", name: "organization_exact_palantir" }],
  ["ENERGY TRANSFER", { industrySlug: "oil_gas_energy", confidence: "high", name: "organization_exact_energy_transfer" }],
  ["CONTINENTAL RESOURCES", { industrySlug: "oil_gas_energy", confidence: "high", name: "organization_exact_continental_resources" }],
  ["GEO SOUTHERN ENERGY", { industrySlug: "oil_gas_energy", confidence: "high", name: "organization_exact_geo_southern" }],
  ["MIDLAND ENERGY", { industrySlug: "oil_gas_energy", confidence: "high", name: "organization_exact_midland_energy" }],
  ["KAISER", { industrySlug: "healthcare", confidence: "high", name: "organization_exact_kaiser" }],
  ["HCA", { industrySlug: "healthcare", confidence: "high", name: "organization_exact_hca" }],
  ["UNITEDHEALTH", { industrySlug: "healthcare", confidence: "high", name: "organization_exact_unitedhealth" }],
  ["PFIZER", { industrySlug: "pharmaceuticals", confidence: "high", name: "organization_exact_pfizer" }],
  ["MERCK", { industrySlug: "pharmaceuticals", confidence: "high", name: "organization_exact_merck" }],
  ["ELI LILLY", { industrySlug: "pharmaceuticals", confidence: "high", name: "organization_exact_eli_lilly" }],
  ["JOHNSON AND JOHNSON", { industrySlug: "pharmaceuticals", confidence: "high", name: "organization_exact_johnson_and_johnson" }],
  ["CANTOR FITZGERALD", { industrySlug: "finance_investment", confidence: "high", name: "organization_exact_cantor" }],
  ["ELLIOTT INVESTMENT MANAGEMENT", { industrySlug: "finance_investment", confidence: "high", name: "organization_exact_elliott" }],
  ["GREYLOCK", { industrySlug: "finance_investment", confidence: "high", name: "organization_exact_greylock" }],
  ["PAULSON CAPITAL", { industrySlug: "finance_investment", confidence: "high", name: "organization_exact_paulson" }],
  ["WINKLEVOSS CAPITAL", { industrySlug: "finance_investment", confidence: "high", name: "organization_exact_winklevoss" }],
  ["LOCKHEED MARTIN", { industrySlug: "defense_aerospace", confidence: "high", name: "organization_exact_lockheed" }],
  ["BOEING", { industrySlug: "defense_aerospace", confidence: "high", name: "organization_exact_boeing" }],
  ["NORTHROP GRUMMAN", { industrySlug: "defense_aerospace", confidence: "high", name: "organization_exact_northrop" }],
  ["PRATT INDUSTRIES", { industrySlug: "manufacturing", confidence: "high", name: "organization_exact_pratt" }],
  ["ULINE", { industrySlug: "manufacturing", confidence: "high", name: "organization_exact_uline" }],
  ["AFL-CIO", { industrySlug: "labor_unions", confidence: "high", name: "organization_exact_afl_cio" }],
  ["SEIU", { industrySlug: "labor_unions", confidence: "high", name: "organization_exact_seiu" }],
  ["TEAMSTERS", { industrySlug: "labor_unions", confidence: "high", name: "organization_exact_teamsters" }],
  ["SIERRA CLUB", { industrySlug: "environmental_group", confidence: "high", name: "organization_exact_sierra_club" }],
  ["LEAGUE OF CONSERVATION VOTERS", { industrySlug: "environmental_group", confidence: "high", name: "organization_exact_lcv" }],
]);

const OCCUPATION_PATTERN_RULES: readonly PatternRule[] = [
  { pattern: /\b(ATTORNEY|LAWYER|LAW FIRM|LEGAL)\b/, industrySlug: "lawyers_and_legal_services", confidence: "medium", name: "occupation_pattern_legal" },
  { pattern: /\b(PHYSICIAN|DOCTOR|NURSE|SURGEON|DENTIST|MEDICAL|HEALTHCARE|HEALTH CARE)\b/, industrySlug: "healthcare", confidence: "medium", name: "occupation_pattern_healthcare" },
  { pattern: /\b(SOFTWARE|PROGRAMMER|COMPUTER|TECHNOLOGY|TECH)\b/, industrySlug: "technology", confidence: "medium", name: "occupation_pattern_technology" },
  { pattern: /\b(OIL|GAS|PETROLEUM|ENERGY)\b/, industrySlug: "oil_gas_energy", confidence: "medium", name: "occupation_pattern_energy" },
  { pattern: /\b(REAL ESTATE|REALTOR|PROPERTY|DEVELOPER)\b/, industrySlug: "real_estate", confidence: "medium", name: "occupation_pattern_real_estate" },
  { pattern: /\b(CONSTRUCTION|CONTRACTOR|BUILDER)\b/, industrySlug: "construction", confidence: "medium", name: "occupation_pattern_construction" },
  { pattern: /\b(FARM|FARMER|RANCH|AGRICULTURE|FOOD)\b/, industrySlug: "agriculture_and_food", confidence: "medium", name: "occupation_pattern_agriculture" },
  { pattern: /\b(TEACHER|PROFESSOR|EDUCATION|EDUCATOR|UNIVERSITY|SCHOOL)\b/, industrySlug: "education", confidence: "medium", name: "occupation_pattern_education" },
];

const ORGANIZATION_PATTERN_RULES: readonly PatternRule[] = [
  { pattern: /\b(GOOGLE|MICROSOFT|META|FACEBOOK|APPLE|AMAZON|SOFTWARE|TECHNOLOGIES|TECHNOLOGY|CLOUD|DATA|AI)\b/, industrySlug: "technology", confidence: "medium", name: "organization_pattern_technology" },
  { pattern: /\b(OIL|GAS|PETROLEUM|ENERGY|PIPELINE|DRILLING)\b/, industrySlug: "oil_gas_energy", confidence: "medium", name: "organization_pattern_energy" },
  { pattern: /\b(PHARMA|PHARMACEUTICAL|BIOTECH|BIOPHARMA|PFIZER|MERCK|ELI LILLY)\b/, industrySlug: "pharmaceuticals", confidence: "medium", name: "organization_pattern_pharmaceuticals" },
  { pattern: /\b(HOSPITAL|HEALTH|MEDICAL|CLINIC|KAISER)\b/, industrySlug: "healthcare", confidence: "medium", name: "organization_pattern_healthcare" },
  { pattern: /\b(CAPITAL|INVESTMENT|INVESTMENTS|BANK|FINANCIAL|SECURITIES|VENTURES|HEDGE FUND|PRIVATE EQUITY)\b/, industrySlug: "finance_investment", confidence: "medium", name: "organization_pattern_finance" },
  { pattern: /\b(REAL ESTATE|REALTY|PROPERTIES|PROPERTY|DEVELOPMENT|DEVELOPER)\b/, industrySlug: "real_estate", confidence: "medium", name: "organization_pattern_real_estate" },
  { pattern: /\b(CONSTRUCTION|BUILDERS|CONTRACTORS|ENGINEERING)\b/, industrySlug: "construction", confidence: "medium", name: "organization_pattern_construction" },
  { pattern: /\b(UNION|AFL-CIO|SEIU|TEAMSTERS|AFSCME|UAW|IBEW|LABORERS)\b/, industrySlug: "labor_unions", confidence: "medium", name: "organization_pattern_labor_unions" },
  { pattern: /\b(UNIVERSITY|COLLEGE|SCHOOL|EDUCATION)\b/, industrySlug: "education", confidence: "medium", name: "organization_pattern_education" },
  { pattern: /\b(AEROSPACE|DEFENSE|MISSILE|LOCKHEED|BOEING|NORTHROP)\b/, industrySlug: "defense_aerospace", confidence: "medium", name: "organization_pattern_defense" },
  { pattern: /\b(FARM|RANCH|AGRICULTURE|FOODS?|DAIRY|MEAT|GRAIN)\b/, industrySlug: "agriculture_and_food", confidence: "medium", name: "organization_pattern_agriculture" },
  { pattern: /\b(MANUFACTURING|INDUSTRIES|FACTORY|STEEL|MOTORS)\b/, industrySlug: "manufacturing", confidence: "medium", name: "organization_pattern_manufacturing" },
  { pattern: /\b(INSURANCE|ASSURANCE)\b/, industrySlug: "insurance", confidence: "medium", name: "organization_pattern_insurance" },
  { pattern: /\b(ENVIRONMENTAL|CONSERVATION|CLIMATE|SIERRA CLUB)\b/, industrySlug: "environmental_group", confidence: "medium", name: "organization_pattern_environmental_group" },
];

function collapseWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

export function normalizeFinanceLabel(rawLabel: string, labelType: FinanceLabelType): string {
  const base = collapseWhitespace(
    rawLabel
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/&/g, " AND ")
      .replace(/[^A-Za-z0-9]+/g, " ")
      .toUpperCase()
  );

  if (labelType === "occupation") {
    return base;
  }

  const withoutSuffixes = collapseWhitespace(base.replace(BUSINESS_SUFFIX_PATTERN, " "));
  return withoutSuffixes.length > 0 ? withoutSuffixes : base;
}

function unknown(rawLabel: string, labelType: FinanceLabelType, normalizedLabel: string): FinanceLabelClassification {
  return {
    rawLabel,
    labelType,
    normalizedLabel,
    industrySlug: null,
    confidence: "unknown",
    classificationSource: "unknown",
    matchedRule: null,
  };
}

function fromRule(
  rawLabel: string,
  labelType: FinanceLabelType,
  normalizedLabel: string,
  rule: ClassificationRule
): FinanceLabelClassification {
  return {
    rawLabel,
    labelType,
    normalizedLabel,
    industrySlug: rule.industrySlug,
    confidence: rule.confidence,
    classificationSource: "rule",
    matchedRule: rule.name,
  };
}

function classifyOccupation(
  rawLabel: string,
  labelType: FinanceLabelType,
  normalizedLabel: string
): FinanceLabelClassification | null {
  if (KNOWN_NON_INDUSTRY_OCCUPATIONS.has(normalizedLabel)) {
    return fromRule(rawLabel, labelType, normalizedLabel, {
      industrySlug: null,
      confidence: "high",
      name: "occupation_known_non_industry",
    });
  }

  const exact = EXACT_OCCUPATION_RULES.get(normalizedLabel);
  if (exact) {
    return fromRule(rawLabel, labelType, normalizedLabel, exact);
  }

  const pattern = OCCUPATION_PATTERN_RULES.find((rule) => rule.pattern.test(normalizedLabel));
  return pattern ? fromRule(rawLabel, labelType, normalizedLabel, pattern) : null;
}

function classifyOrganization(
  rawLabel: string,
  labelType: FinanceLabelType,
  normalizedLabel: string
): FinanceLabelClassification | null {
  const exact = EXACT_ORGANIZATION_RULES.get(normalizedLabel);
  if (exact) {
    return fromRule(rawLabel, labelType, normalizedLabel, exact);
  }

  const pattern = ORGANIZATION_PATTERN_RULES.find((rule) => rule.pattern.test(normalizedLabel));
  return pattern ? fromRule(rawLabel, labelType, normalizedLabel, pattern) : null;
}

export function classifyFinanceLabel(input: {
  rawLabel: string;
  labelType: FinanceLabelType;
}): FinanceLabelClassification {
  const rawLabel = input.rawLabel.trim();
  const normalizedLabel = normalizeFinanceLabel(rawLabel, input.labelType);

  if (rawLabel.length === 0 || normalizedLabel.length === 0) {
    return unknown(input.rawLabel, input.labelType, normalizedLabel);
  }

  const classification =
    input.labelType === "occupation"
      ? classifyOccupation(rawLabel, input.labelType, normalizedLabel)
      : classifyOrganization(rawLabel, input.labelType, normalizedLabel);

  return classification ?? unknown(rawLabel, input.labelType, normalizedLabel);
}
