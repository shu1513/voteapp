export type AlabamaFinanceEligibleOfficeKey = `${string}::${string}`;

// V1 scope: statewide constitutional offices, both legislative chambers, and
// the statewide appellate courts (plan-alabama-finance.md, Phase 3). United
// States Senator and Representative are federal races filed with the FEC.
// County and municipal filers use the same FCPA portal and can be enabled
// later by widening this list (and the office-label map below) only.
export const ALABAMA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Attorney General",
  "statewide::Secretary of State",
  "statewide::State Treasurer",
  "statewide::State Auditor",
  "statewide::Commissioner of Agriculture",
  "statewide::Public Service Commissioner",
  "statewide::State Level Judge",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly AlabamaFinanceEligibleOfficeKey[];

const ALABAMA_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  ALABAMA_FINANCE_ELIGIBLE_OFFICE_KEYS
);

export function toAlabamaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): AlabamaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isAlabamaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toAlabamaFinanceOfficeKey(input);
  return key !== null && ALABAMA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

// FCPA race-search office dropdown labels, pinned from the live portal
// 2026-08-28 (the raw option text HTML-decodes "&amp;" to "&"; the client's
// dropdown parser does not decode, so lookups normalize first — see
// alabamaFcpaOfficeIdForLabel).
const ALABAMA_FCPA_OFFICE_LABELS: Record<string, string> = {
  "statewide::Governor": "Governor",
  "statewide::Lieutenant Governor": "Lt. Governor",
  "statewide::Attorney General": "Attorney General",
  "statewide::Secretary of State": "Secretary of State",
  "statewide::State Treasurer": "State Treasurer",
  "statewide::State Auditor": "State Auditor",
  "statewide::Commissioner of Agriculture": "Commissioner of Agriculture & Industries",
  "statewide::Public Service Commissioner": "Public Service Commissioner",
  "state_upper::State Senator": "State Senator",
  "state_lower::State Lower Chamber Legislator": "State Representative",
};

// "State Level Judge" bundles every statewide appellate court; the FCPA
// splits them per court, so the ballot title picks the label. Order matters:
// Chief Justice before the Supreme Court catch-all.
const ALABAMA_JUDICIAL_TITLE_RULES: readonly { pattern: RegExp; label: string }[] = [
  { pattern: /chief justice/i, label: "Supreme Court Chief Justice" },
  { pattern: /supreme court/i, label: "Supreme Court Associate Justice" },
  { pattern: /court of civil appeals/i, label: "Court of Civil Appeals Judge" },
  { pattern: /court of criminal appeals/i, label: "Court of Criminal Appeals Judge" },
];

/**
 * FCPA race-search office label for a VoteApp race; null when the office is
 * outside the v1 map or a judicial ballot title matches no court rule —
 * callers fail closed to manual review on null.
 */
export function alabamaFcpaOfficeLabelForRace(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  ballotTitle: string | null | undefined;
}): string | null {
  const key = toAlabamaFinanceOfficeKey(input);
  if (key === null) {
    return null;
  }
  if (key === "statewide::State Level Judge") {
    const title = input.ballotTitle?.trim();
    if (!title) {
      return null;
    }
    for (const rule of ALABAMA_JUDICIAL_TITLE_RULES) {
      if (rule.pattern.test(title)) {
        return rule.label;
      }
    }
    return null;
  }
  return ALABAMA_FCPA_OFFICE_LABELS[key] ?? null;
}

/**
 * Dropdown option id for an FCPA office label; null when the live dropdown
 * lacks it. Raw option text keeps "&amp;" — normalize both sides.
 */
export function alabamaFcpaOfficeIdForLabel(
  label: string,
  options: readonly { id: string; label: string }[]
): string | null {
  const decode = (value: string) => value.replace(/&amp;/g, "&").trim();
  const wanted = decode(label);
  for (const option of options) {
    if (decode(option.label) === wanted) {
      return option.id;
    }
  }
  return null;
}

/** "2026 ELECTION CYCLE" dropdown id for an election year; null when absent. */
export function alabamaFcpaElectionCycleIdForYear(
  electionYear: number,
  options: readonly { id: string; label: string }[]
): string | null {
  const wanted = `${electionYear} ELECTION CYCLE`;
  for (const option of options) {
    if (option.label.trim().toUpperCase() === wanted) {
      return option.id;
    }
  }
  return null;
}
