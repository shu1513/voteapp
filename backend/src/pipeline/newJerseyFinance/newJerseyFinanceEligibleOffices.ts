export const NEW_JERSEY_FINANCE_ELIGIBLE_OFFICE_KEYS = new Set([
  "statewide::governor",
  "statewide::lieutenant governor",
  "state_upper::state senator",
  "state_lower::state lower chamber legislator",
]);

function normalizeOfficeName(value: string | null | undefined): string {
  return (value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

export function isNewJerseyFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = `${normalizeOfficeName(input.officeScope)}::${normalizeOfficeName(input.officeCanonicalName)}`;
  return NEW_JERSEY_FINANCE_ELIGIBLE_OFFICE_KEYS.has(key);
}
