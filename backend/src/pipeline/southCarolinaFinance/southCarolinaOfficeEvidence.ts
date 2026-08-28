// Office-consistency evidence for South Carolina finance identity.
//
// The API's per-FILER office labels are useless for identity (a filer-search
// row lists every registration the filer ever had; 2026 statewide labels are
// the literal string "4"). But each CONTRIBUTION row carries the office label
// of its RUN, and for legislative runs that label is real and district-
// scoped (live-pinned: Chandra Dillard's 2026 rows say "SC House of
// Representatives District 23"). That makes row labels usable as a VETO:
// when the accepted runs' rows cleanly describe an office class or district
// that contradicts the linked race, the filer is the wrong person — a
// same-named House filer must never publish money under a Senate candidate.
//
// Veto-only by design: an uninterpretable label ("4", empty, unknown text)
// is NO evidence and never blocks, so the broken statewide labels cannot
// cause false vetoes.

export type SouthCarolinaOfficeEvidence = {
  officeClass: "statewide" | "state_upper" | "state_lower" | "local";
  district: number | null;
};

const STATEWIDE_LABELS = [
  "GOVERNOR",
  "LIEUTENANT GOVERNOR",
  "ATTORNEY GENERAL",
  "SECRETARY OF STATE",
  "STATE TREASURER",
  "COMPTROLLER",
  "SUPERINTENDENT OF EDUCATION",
  "COMMISSIONER OF AGRICULTURE",
  "ADJUTANT GENERAL",
];

const LOCAL_MARKERS = ["COUNCIL", "SHERIFF", "MAYOR", "COUNTY", "CITY", "TOWN", "SCHOOL"];

function normalizeLabel(value: string): string {
  return value
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function districtNumber(normalized: string): number | null {
  const match = /\bDISTRICT\s+(\d+)\b/.exec(normalized);
  return match ? Number.parseInt(match[1]!, 10) : null;
}

/** null = no usable evidence (broken, empty, or unrecognized label). */
export function classifySouthCarolinaRunOfficeLabel(label: string): SouthCarolinaOfficeEvidence | null {
  const normalized = normalizeLabel(label);
  if (!normalized) {
    return null;
  }
  // Federal races never file with the State Ethics Commission; a label that
  // mentions them is noise, not evidence.
  if (normalized.includes("UNITED STATES") || /\bUS\b/.test(normalized) || normalized.includes("CONGRESS")) {
    return null;
  }
  const district = districtNumber(normalized);
  if (/\bHOUSE OF REPRESENTATIVES\b/.test(normalized) || /\bHOUSE\b/.test(normalized)) {
    return { officeClass: "state_lower", district };
  }
  if (/\bSENATE\b/.test(normalized) || /\bSENATOR\b/.test(normalized)) {
    return { officeClass: "state_upper", district };
  }
  for (const statewide of STATEWIDE_LABELS) {
    if (normalized.includes(statewide)) {
      return { officeClass: "statewide", district: null };
    }
  }
  if (LOCAL_MARKERS.some((marker) => new RegExp(`\\b${marker}\\b`).test(normalized))) {
    return { officeClass: "local", district };
  }
  return null;
}

export function southCarolinaLinkedDistrictNumber(district: string | null | undefined): number | null {
  if (!district) {
    return null;
  }
  return districtNumber(normalizeLabel(district));
}

/**
 * Labels among the accepted runs' contribution rows that contradict the
 * linked race: a different office class, or the same legislative chamber
 * with a different district number. Empty result = no conflict (including
 * the no-evidence case).
 */
export function southCarolinaConflictingOfficeLabels(input: {
  officeScope: string;
  district: string | null | undefined;
  rowOfficeLabels: Iterable<string>;
}): string[] {
  const linkedDistrict = southCarolinaLinkedDistrictNumber(input.district);
  const conflicting = new Set<string>();
  for (const label of input.rowOfficeLabels) {
    const evidence = classifySouthCarolinaRunOfficeLabel(label);
    if (evidence === null) {
      continue;
    }
    if (evidence.officeClass !== input.officeScope) {
      conflicting.add(label);
      continue;
    }
    if (
      (evidence.officeClass === "state_lower" || evidence.officeClass === "state_upper") &&
      evidence.district !== null &&
      linkedDistrict !== null &&
      evidence.district !== linkedDistrict
    ) {
      conflicting.add(label);
    }
  }
  return [...conflicting].sort();
}
