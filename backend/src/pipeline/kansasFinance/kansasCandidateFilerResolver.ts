// Candidate -> Kansas CFR viewer filer resolution (plan-kansas-finance.md,
// Phase 3). Pure functions: the auto-link enumerates an office's filings
// from the viewer and calls these.
//
// Kansas has no filer id and no registration list: the viewer's grids list
// FILINGS, each carrying the name as typed on that filing. Live 2026-09-01
// (House, 1,127 report rows / 458 distinct spellings): the same person files
// as "BRUNK STEVEN" and "BRUNK STEVE", "WILLIAMS MARY" and "WILLIAMS MARY T",
// "CLINTON JERRY" and "CLINTON JERY" (typo); a few filings leave the
// district blank; statewide rows may carry a stray district ("ROGERS STACY"
// Governor, district 4). Names render as "LAST FIRST [MIDDLE ...]" in one
// uppercase string for every grid and channel.
//
// Fail-closed rules (plan "Identity and matching rules"):
// - exact office (the search is per office) and exact district when the
//   office is districted; statewide rows ignore the district column;
// - full-name evidence through the shared middle-name gate with one-sided
//   roster->filing nickname expansion; a bare surname never links;
// - every filing whose name aligns with the candidate is the same person
//   UNLESS two aligned spellings contradict each other on middle names or
//   generational suffix — then nothing links (ambiguous);
// - a candidate whose only aligned filings lack a district goes to manual
//   review, never to an automatic link.

import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import {
  hasMiddleNameConflict,
  personNamesMatchWithMiddleEvidence,
} from "../finance/personNameMiddleEvidence.js";
import { normalizeKansasNameForStorage } from "./kansasFinanceWriter.js";

export type KansasFilerFilingKind = "report" | "appointment_of_treasurer" | "affidavit";

export type KansasFilerRow = {
  /** Filed name exactly as the grid shows it: "LAST FIRST [MIDDLE ...]". */
  filedName: string;
  /** Grid district text ("1"; "" when the filing left it blank). */
  district: string;
  officeSought: string;
  filingKind: KansasFilerFilingKind;
  fileDate: string;
};

export type KansasFilerMatch = {
  /** Suffix-stripped, normalized surname of the most frequent aligned spelling (recipe key). */
  surname: string;
  /** Suffix-stripped, normalized first token of the most frequent aligned spelling (recipe key). */
  firstName: string;
  /** Most frequent filed display name (stored as committee_name). */
  committeeName: string;
  /** Distinct filed spellings that aligned, most frequent first. */
  filedNames: string[];
  rowCount: number;
  /** "nickname" marks a match that needed the one-sided first-name expansion. */
  confidence: "name_exact" | "name_nickname";
};

export type KansasFilerResolution =
  | { status: "matched"; match: KansasFilerMatch }
  | { status: "ambiguous"; reason: "conflicting_filed_names"; filedNames: string[] }
  | { status: "manual_confirm_required"; reason: "filings_missing_district"; filedNames: string[] }
  | { status: "unmatched"; reason: "missing_candidate_name" | "no_matching_filer" };

/** Match-time person-name normalization (suffixes stripped; storage keeps them). */
export function normalizeKansasPersonNameForMatching(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(JR|SR|II|III|IV)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Grid district text to a number ("/ 1" is already stripped by the parser); null when blank or non-numeric. */
export function kansasDistrictNumberFromGrid(value: string | null | undefined): number | null {
  const match = /^\s*(\d+)\s*$/.exec(value ?? "");
  if (!match) return null;
  const parsed = Number.parseInt(match[1]!, 10);
  return parsed > 0 ? parsed : null;
}

/**
 * Comma-form readings of a "LAST FIRST [MIDDLE ...]" grid name: the surname
 * may span one or more leading tokens ("VAN DYKE MARY"), so every split is
 * offered and the shared matcher tries them all — a wrong split can only
 * fail to align, never manufacture evidence. A name that already carries a
 * comma is taken as written.
 */
export function kansasFiledNameCommaVariants(filedName: string): string[] {
  const trimmed = filedName.trim();
  if (!trimmed) return [];
  if (trimmed.includes(",")) return [trimmed];
  const tokens = trimmed.split(/\s+/);
  if (tokens.length < 2) return [];
  const variants: string[] = [];
  for (let surnameTokens = 1; surnameTokens < tokens.length; surnameTokens += 1) {
    variants.push(`${tokens.slice(0, surnameTokens).join(" ")}, ${tokens.slice(surnameTokens).join(" ")}`);
  }
  return variants;
}

// One-sided nickname expansion (roster side only, the shared module's rule):
// filings carry legal names ("STEVEN", "JOSEPH") while the roster carries
// campaign names ("Steve", "Joe"). Surname, office, and district still must
// agree exactly.
function rosterFirstNameMatchesFiling(candidateFirst: string, rowFirst: string): boolean {
  return candidateFirst === rowFirst || firstNameVariants(candidateFirst).includes(rowFirst);
}

type AlignedSpelling = {
  filedName: string;
  /** The comma-form reading that aligned with the candidate. */
  commaForm: string;
  surname: string;
  firstName: string;
  count: number;
  strict: boolean;
};

function alignFiledName(candidateName: string, filedName: string): Omit<AlignedSpelling, "count"> | null {
  for (const commaForm of kansasFiledNameCommaVariants(filedName)) {
    const aligned = personNamesMatchWithMiddleEvidence({
      candidateName,
      rowNames: [commaForm],
      normalizePersonName: normalizeKansasPersonNameForMatching,
      firstNamesEquivalent: rosterFirstNameMatchesFiling,
    });
    if (!aligned) continue;
    const strict = personNamesMatchWithMiddleEvidence({
      candidateName,
      rowNames: [commaForm],
      normalizePersonName: normalizeKansasPersonNameForMatching,
    });
    // Recipe-key parts use the MATCHING normalizer so a generational suffix
    // typed into the surname cell ("JR ROBERTSON BOBBY JOE", live 2026)
    // never reaches the key the sync will search by.
    const commaIndex = commaForm.indexOf(",");
    const surname = normalizeKansasPersonNameForMatching(commaForm.slice(0, commaIndex));
    const firstName = normalizeKansasPersonNameForMatching(commaForm.slice(commaIndex + 1)).split(" ")[0] ?? "";
    if (!surname || !firstName) continue;
    return { filedName, commaForm, surname, firstName, strict };
  }
  return null;
}

function sortByFrequency(spellings: readonly AlignedSpelling[]): AlignedSpelling[] {
  return [...spellings].sort(
    (left, right) => right.count - left.count || left.filedName.localeCompare(right.filedName)
  );
}

export function resolveKansasCandidateFiler(input: {
  candidateName: string;
  /** Required for districted offices; null for statewide (district column ignored). */
  districtNumber: number | null;
  /** Filings already scoped to the viewer office. */
  rows: readonly KansasFilerRow[];
}): KansasFilerResolution {
  const candidateName = input.candidateName.trim();
  if (!candidateName) {
    return { status: "unmatched", reason: "missing_candidate_name" };
  }

  const confirmed = new Map<string, AlignedSpelling>();
  const blankDistrict = new Map<string, AlignedSpelling>();
  const alignmentCache = new Map<string, Omit<AlignedSpelling, "count"> | null>();
  for (const row of input.rows) {
    let pool = confirmed;
    if (input.districtNumber !== null) {
      const rowDistrict = kansasDistrictNumberFromGrid(row.district);
      if (rowDistrict === null) {
        pool = blankDistrict;
      } else if (rowDistrict !== input.districtNumber) {
        continue;
      }
    }
    const spellingKey = normalizeKansasNameForStorage(row.filedName);
    if (!spellingKey) continue;
    let aligned = alignmentCache.get(spellingKey);
    if (aligned === undefined) {
      aligned = alignFiledName(candidateName, row.filedName);
      alignmentCache.set(spellingKey, aligned);
    }
    if (aligned === null) continue;
    const existing = pool.get(spellingKey);
    if (existing) {
      existing.count += 1;
    } else {
      pool.set(spellingKey, { ...aligned, count: 1 });
    }
  }

  if (confirmed.size === 0) {
    if (blankDistrict.size > 0) {
      return {
        status: "manual_confirm_required",
        reason: "filings_missing_district",
        filedNames: sortByFrequency([...blankDistrict.values()]).map((spelling) => spelling.filedName),
      };
    }
    return { status: "unmatched", reason: "no_matching_filer" };
  }

  const spellings = sortByFrequency([...confirmed.values()]);
  // Every aligned spelling is one person unless two of them contradict each
  // other (middle initials "T" vs "B", or Jr vs Sr). Spellings that differ
  // only in first-name form ("STEVEN"/"STEVE") do not align with each other
  // and so carry no evidence either way.
  for (let left = 0; left < spellings.length; left += 1) {
    for (let right = left + 1; right < spellings.length; right += 1) {
      if (
        hasMiddleNameConflict({
          candidateName: spellings[left]!.commaForm,
          rowNames: [spellings[right]!.commaForm],
          normalizePersonName: normalizeKansasPersonNameForMatching,
        })
      ) {
        return {
          status: "ambiguous",
          reason: "conflicting_filed_names",
          filedNames: spellings.map((spelling) => spelling.filedName),
        };
      }
    }
  }

  const primary = spellings[0]!;
  return {
    status: "matched",
    match: {
      surname: primary.surname,
      firstName: primary.firstName,
      committeeName: primary.filedName,
      filedNames: spellings.map((spelling) => spelling.filedName),
      rowCount: spellings.reduce((sum, spelling) => sum + spelling.count, 0),
      confidence: spellings.every((spelling) => spelling.strict) ? "name_exact" : "name_nickname",
    },
  };
}
