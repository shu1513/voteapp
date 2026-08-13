// Outside (independent-expenditure) aggregation for one Phoenix roster
// candidate (plan Phase 3). Pure over the run-level Schedule B(6) pool that
// the sync module extracts from city-filing IE PACs' report PDFs, plus the
// curated supplements for the channels the portal cannot measure.
//
// Channel decisions (Phase 0 census + the 2026-08-12 Spotlight check):
//   1. Phoenix-registered city-filing PACs — SYSTEMATIC (this module).
//   2. Standing PACs (SOS-filing) — Spotlight's IE grid names the VENDOR,
//      not the candidate; targets live in free-text memos → curated.
//   3. Non-committee IE entities (scanned fillable forms) — curated.
//   4. EFD "dark money" filings — curated.
//
// Attribution is FAIL-CLOSED end to end (live dirt: the pinned real B(6)
// filing, PAC-22-14 d7118529, has a BLANK candidate name with % and office
// filled). The disclosed Election Month/Year is checked FIRST, per entry: an
// expenditure filed for another election is not this contest's money, so it
// never reaches the per-candidate diagnostics — otherwise the whole live
// pool (four 2023/2024-cycle entries, two of them blank-named) would read
// as a coverage gap in every 2026 candidate's note. An entry then books to
// this candidate only when, for the direction block in question:
//   - the block discloses a name and it matches under the shared Phoenix
//     person-name gates (token-based, never substring; the shared parser
//     accepts the "Last, First" comma form, so a comma alone is not a
//     multi-candidate marker),
//   - the block names ONE candidate at 100% (a blank % reads as 100 for a
//     single name; any partial % or multi-candidate split is excluded and
//     disclosed — v1 never pro-rates, and only for candidates the split
//     actually names),
//   - the disclosed Office Sought agrees with the candidate's office (the
//     pinned live format is "City Council" — no district — so a district
//     veto applies only when the field carries digits),
// Anything else increments a diagnostic; the sync's outside_coverage_note
// discloses per-candidate exclusions.

import {
  phoenixPersonNameMatchesCandidate,
  normalizePhoenixTextKey,
} from "./phoenixCandidateCommitteeResolver.js";
import { phoenixCandidateCycleForDate } from "./phoenixEfilingClient.js";
import type { PhoenixB6Entry } from "./phoenixReportPdfParser.js";
import {
  validatePhoenixOutsideSupplements,
  type PhoenixOutsideSupplement,
} from "./phoenixOutsideSpendingSupplements.js";

export type PhoenixOutsideTargetCandidate = {
  displayName: string;
  officeName: "Mayor" | "City Council Member";
  /** Council district (1–8); null for Mayor. */
  districtNumber: number | null;
  /** ISO election date — anchors the cycle gate. */
  electionDate: string;
};

/** One parsed B(6) entry tagged with its spender (the run-level pool). */
export type PhoenixOutsidePoolEntry = {
  spenderCopId: string;
  spenderName: string;
  reportPackageId: string;
  entry: PhoenixB6Entry;
};

export type PhoenixOutsideSpendingGroup = {
  spenderFilerId: string;
  spenderName: string;
  direction: "support" | "oppose";
  amountCents: number;
  expenditureCount: number;
};

export type PhoenixOutsideSpendingAggregate = {
  supportTotalCents: number;
  opposeTotalCents: number;
  /** All groups, largest first; the writer stores them all. */
  groups: PhoenixOutsideSpendingGroup[];
  diagnostics: {
    poolEntries: number;
    /** Entries disclosed for a different election cycle — not this contest's
     * money, so they are never disclosed as a gap in this race's coverage. */
    outOfCycleEntries: number;
    /** Entries disclosing no usable Election Month/Year — unplaceable in any
     * contest, so unplaceable here. */
    undatedEntries: number;
    /** In-cycle entries whose supported AND opposed blocks disclose no name —
     * the expenditure exists but cannot be attributed to anyone (live dirt). */
    unattributableEntries: number;
    unattributableCents: number;
    /** Direction blocks naming several candidates or a partial % — excluded
     * un-pro-rated (v1 rule), disclosed per candidate when name-matched. */
    partialAttributionRows: number;
    /** Name matched but office/district/cycle vetoed the row. */
    vetoedRows: number;
    /** Name disclosed but it is some other candidate. */
    otherCandidateRows: number;
    supplementRows: number;
    supplementRowsIncluded: number;
  };
};

/** "Election Month/Year" text → the portal cycle its date falls in. A
 * disclosed month anchors the cycle exactly (a "3/2027" runoff belongs to
 * the 2025 cycle); a bare year ("2024", the pinned live format) anchors on
 * November — Phoenix candidate elections are November races with runoffs
 * the following March, and a bare year can only mean the November date. */
function electionTextCycleStartYear(text: string | null): number | null {
  if (text === null) return null;
  const trimmed = text.trim();
  const monthYear = /^(\d{1,2})\s*\/\s*(\d{4})$/.exec(trimmed);
  const bareYear = /^(\d{4})$/.exec(trimmed);
  let year: number;
  let month: number;
  if (monthYear !== null) {
    month = Number(monthYear[1]);
    year = Number(monthYear[2]);
    if (month < 1 || month > 12) return null;
  } else if (bareYear !== null) {
    month = 11;
    year = Number(bareYear[1]);
  } else {
    return null;
  }
  return phoenixCandidateCycleForDate(
    `${year}-${String(month).padStart(2, "0")}-01`,
  ).startYear;
}

function officeTextAgrees(
  officeText: string,
  candidate: PhoenixOutsideTargetCandidate,
): boolean {
  const normalized = normalizePhoenixTextKey(officeText);
  const districtMatch = /\bDISTRICT (?:NO )?(\d{1,2})\b/.exec(normalized);
  if (candidate.officeName === "Mayor") {
    return /\bMAYOR\b/.test(normalized) && districtMatch === null;
  }
  if (!/\bCOUNCIL\b/.test(normalized)) return false;
  // The pinned live format ("City Council") has no district; the veto bites
  // only when digits are disclosed.
  return districtMatch === null || Number(districtMatch[1]) === candidate.districtNumber;
}

export function aggregatePhoenixOutsideSpending(input: {
  candidate: PhoenixOutsideTargetCandidate;
  pool: readonly PhoenixOutsidePoolEntry[];
  /** Curated entries for THIS cycle (sync filters by election year); they
   * run through the same name/office/district gates as pool entries. */
  supplements?: readonly PhoenixOutsideSupplement[];
}): PhoenixOutsideSpendingAggregate {
  const { candidate } = input;
  if (candidate.officeName === "City Council Member" && candidate.districtNumber === null) {
    throw new Error(
      `Phoenix outside-spending aggregation needs a district for council candidate ${candidate.displayName}`,
    );
  }
  const cycleStartYear = phoenixCandidateCycleForDate(candidate.electionDate).startYear;
  const supplements = input.supplements ?? [];
  validatePhoenixOutsideSupplements(supplements);

  const diagnostics: PhoenixOutsideSpendingAggregate["diagnostics"] = {
    poolEntries: input.pool.length,
    outOfCycleEntries: 0,
    undatedEntries: 0,
    unattributableEntries: 0,
    unattributableCents: 0,
    partialAttributionRows: 0,
    vetoedRows: 0,
    otherCandidateRows: 0,
    supplementRows: supplements.length,
    supplementRowsIncluded: 0,
  };
  const included: {
    spenderFilerId: string;
    spenderName: string;
    direction: "support" | "oppose";
    amountCents: number;
  }[] = [];

  for (const poolEntry of input.pool) {
    const { entry } = poolEntry;
    // Cycle gate FIRST, at entry level: an expenditure disclosed for another
    // election is not this contest's money at all. Running it before the
    // name checks keeps prior-cycle entries — including blank-name ones,
    // which the live pool is full of — out of this candidate's
    // unattributable disclosure, which would otherwise read as a gap in
    // THIS race's coverage. Blank/unparseable still fails closed.
    const entryCycleStartYear = electionTextCycleStartYear(entry.electionText);
    if (entryCycleStartYear === null) {
      // No usable election disclosed: the expenditure cannot be placed in
      // any contest. Counted with the other unplaceable entries, not with
      // the name-matched vetoes.
      diagnostics.undatedEntries += 1;
      diagnostics.unattributableCents += entry.amountCents;
      continue;
    }
    if (entryCycleStartYear !== cycleStartYear) {
      diagnostics.outOfCycleEntries += 1;
      continue;
    }
    const blocks = [
      { direction: "support" as const, names: entry.supportedNames, percents: entry.supportedPercents },
      { direction: "oppose" as const, names: entry.opposedNames, percents: entry.opposedPercents },
    ];
    if (blocks.every((block) => block.names.length === 0)) {
      diagnostics.unattributableEntries += 1;
      diagnostics.unattributableCents += entry.amountCents;
      continue;
    }
    for (const block of blocks) {
      if (block.names.length === 0) continue;
      // Cell fragments of ONE name join into a single string. The whole
      // string is matched FIRST: the shared parser reads a comma as a
      // "Last, First" surname boundary, so a filer's "Hermes, Ed" matches
      // Ed Hermes — a separator alone must not veto a single inverted name.
      // A genuine multi-candidate string cannot fully match (the extra
      // name's tokens block the parser's first+last alignment), so it falls
      // to the split below: it books to nobody (never pro-rated, v1), and it
      // counts as partial attribution only for a candidate actually named in
      // one of its segments — for everyone else it is just another
      // candidate's row, not a gap in THIS candidate's coverage.
      const joined = block.names
        .join(" ")
        .replace(/\s+/g, " ")
        // Leading/trailing separators are cell punctuation, not a split —
        // "Ed Hermes," must parse as one name, not "Last, <empty>".
        .replace(/^[,;&\s]+|[,;&\s]+$/g, "");
      if (!phoenixPersonNameMatchesCandidate(joined, candidate.displayName)) {
        const segments = joined
          .split(/,|&|;|\band\b/i)
          .map((segment) => segment.trim())
          .filter((segment) => segment.length > 0);
        const namesThisCandidate =
          segments.length > 1 &&
          segments.some((segment) =>
            phoenixPersonNameMatchesCandidate(segment, candidate.displayName),
          );
        if (namesThisCandidate) {
          diagnostics.partialAttributionRows += 1;
        } else {
          diagnostics.otherCandidateRows += 1;
        }
        continue;
      }
      if (
        block.percents.length > 1 ||
        (block.percents.length === 1 && block.percents[0] !== 100)
      ) {
        diagnostics.partialAttributionRows += 1;
        continue;
      }
      // Office veto: blank fails closed (a same-named person in another
      // race must never book money here).
      if (entry.officeText === null || !officeTextAgrees(entry.officeText, candidate)) {
        diagnostics.vetoedRows += 1;
        continue;
      }
      included.push({
        spenderFilerId: poolEntry.spenderCopId,
        spenderName: poolEntry.spenderName,
        direction: block.direction,
        amountCents: entry.amountCents,
      });
    }
  }

  for (const supplement of supplements) {
    if (!phoenixPersonNameMatchesCandidate(supplement.candidateName, candidate.displayName)) {
      diagnostics.otherCandidateRows += 1;
      continue;
    }
    if (!officeTextAgrees(supplement.officeSought, candidate)) {
      diagnostics.vetoedRows += 1;
      continue;
    }
    if (
      supplement.districtNumber !== null &&
      supplement.districtNumber !== candidate.districtNumber
    ) {
      diagnostics.vetoedRows += 1;
      continue;
    }
    diagnostics.supplementRowsIncluded += 1;
    included.push({
      spenderFilerId: supplement.spenderFilerId,
      spenderName: supplement.spenderName,
      direction: supplement.direction,
      amountCents: supplement.amountCents,
    });
  }

  // --- Group by (spender identity, direction); longest observed spelling. ---
  const groups = new Map<
    string,
    { spenderFilerId: string; names: Set<string>; direction: "support" | "oppose"; cents: number; count: number }
  >();
  for (const row of included) {
    const key = JSON.stringify([row.spenderFilerId, row.direction]);
    const group = groups.get(key) ?? {
      spenderFilerId: row.spenderFilerId,
      names: new Set<string>(),
      direction: row.direction,
      cents: 0,
      count: 0,
    };
    group.names.add(row.spenderName);
    group.cents += row.amountCents;
    group.count += 1;
    groups.set(key, group);
  }
  const mapped: PhoenixOutsideSpendingGroup[] = [...groups.values()]
    .map((group) => ({
      spenderFilerId: group.spenderFilerId,
      spenderName: [...group.names].sort((a, b) => b.length - a.length || a.localeCompare(b))[0]!,
      direction: group.direction,
      amountCents: group.cents,
      expenditureCount: group.count,
    }))
    .sort((a, b) => b.amountCents - a.amountCents || a.spenderName.localeCompare(b.spenderName));

  return {
    supportTotalCents: mapped
      .filter((group) => group.direction === "support")
      .reduce((sum, group) => sum + group.amountCents, 0),
    opposeTotalCents: mapped
      .filter((group) => group.direction === "oppose")
      .reduce((sum, group) => sum + group.amountCents, 0),
    groups: mapped,
    diagnostics,
  };
}
