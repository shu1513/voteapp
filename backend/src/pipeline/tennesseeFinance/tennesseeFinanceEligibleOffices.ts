export type TennesseeFinanceEligibleOfficeKey = `${string}::${string}`;

export type TennesseeCampOfficeSearchInput = {
  officeSelection: string;
};

export const TENNESSEE_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly TennesseeFinanceEligibleOfficeKey[];

const TENNESSEE_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(TENNESSEE_FINANCE_ELIGIBLE_OFFICE_KEYS);

const TENNESSEE_APP_OFFICE_TO_CAMP = new Map<string, TennesseeCampOfficeSearchInput>([
  ["statewide::Governor", { officeSelection: "2" }],
  ["state_upper::State Senator", { officeSelection: "3" }],
  ["state_lower::State Lower Chamber Legislator", { officeSelection: "4" }],
]);

export function toTennesseeFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): TennesseeFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isTennesseeFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toTennesseeFinanceOfficeKey(input);
  return key !== null && TENNESSEE_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export function toTennesseeCampOfficeSearchInput(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): TennesseeCampOfficeSearchInput | null {
  const key = toTennesseeFinanceOfficeKey(input);
  return key ? TENNESSEE_APP_OFFICE_TO_CAMP.get(key) ?? null : null;
}

export function normalizeTennesseeCampDistrict(value: string | null | undefined): string | null {
  const trimmed = value?.trim();
  if (!trimmed) {
    return null;
  }
  const numeric = trimmed.match(/\d+/)?.[0];
  if (!numeric) {
    return trimmed.toUpperCase();
  }
  return String(Number.parseInt(numeric, 10));
}

export function tennesseeCampOfficeLabelForAppOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): string | null {
  const key = toTennesseeFinanceOfficeKey(input);
  if (key === "statewide::Governor") {
    return "Governor";
  }
  if (key === "state_upper::State Senator") {
    return "Senate";
  }
  if (key === "state_lower::State Lower Chamber Legislator") {
    return "House of Representatives";
  }
  return null;
}
