import type { NcsbeDocumentRow } from "./northCarolinaNcsbeParsers.js";

// Decision-8 report selection for NCSBE inventories: assemble filings from
// split inventory rows, group originals with their amendments, and pick the
// currently-in-force filing per group — quarantining every ambiguous lineage
// instead of guessing. Pure inventory logic: the caller pre-filters rows to
// the document/report types it aggregates (regular Disclosure Reports for
// direct money, the two IE report types for outside money) and this module
// never looks at report contents.
//
// Two key levels (do not conflate — decision 8):
// - Row-merge key INCLUDES IsAmendment: the same filing can appear as a DATA
//   row and a separate IMAGE row (spike results item 8), merged only when
//   unambiguous.
// - Selection-group key EXCLUDES IsAmendment: originals and amendments must
//   share a group or an amendment could never supersede its original.

export type NcsbeFiling = {
  filerKey: string;
  committeeName: string;
  sboeId: string | null;
  documentType: string;
  reportType: string | null;
  periodStartRaw: string;
  periodEndRaw: string;
  periodStartIso: string | null;
  periodEndIso: string | null;
  isAmendment: boolean;
  // Structured report id (DataLink); null = image-only filing.
  reportId: string | null;
  // Newest ImageReceiptDate across the filing's rows — the legally-filed
  // chronology evidence the selector ordered by. Null when no row carries one
  // (DATA rows often have an empty image date, spike results item 8).
  filedDateIso: string | null;
  rows: NcsbeDocumentRow[];
};

export type NcsbeQuarantinedGroupReason =
  // A row's IsAmendment flag is blank. Observed only on correspondence noise
  // rows so far, but on a report row it means the lineage is unknowable —
  // null is never "not an amendment" (decision 8).
  | "null_amendment_flag"
  // More than one non-amendment original shares the period.
  | "multiple_original_filings"
  // A DATA/IMAGE row mix that cannot be paired unambiguously (an extra IMAGE
  // row could be a newer image-only amendment hiding behind an older
  // structured one).
  | "ambiguous_row_merge"
  // Filing chronology put a non-amendment newest while the group holds
  // amendments — contradictory lineage evidence.
  | "original_newer_than_amendment"
  // Two distinct filings (necessarily two amendments — multiple originals
  // already quarantined) tie on every chronology key. Report ids are NOT
  // evidence: nothing pins them as monotonic with filing time, so a tie is
  // genuinely ambiguous and money must not be selected by id ordering.
  | "ambiguous_filing_chronology";

export type NcsbeQuarantinedGroup = {
  filerKey: string;
  committeeName: string;
  documentType: string;
  reportType: string | null;
  periodStartRaw: string;
  periodEndRaw: string;
  reason: NcsbeQuarantinedGroupReason;
  rowCount: number;
};

export type NcsbeReportSelectionResult = {
  // The current filing per unambiguous group, structured (reportId set).
  selected: NcsbeFiling[];
  // Groups whose current filing is image-only: the period is
  // superseded-unavailable — never fall back to an older structured filing
  // (decision 8).
  supersededUnavailable: NcsbeFiling[];
  quarantinedGroups: NcsbeQuarantinedGroup[];
  groupCount: number;
  // Rows dropped as duplicates of an already-seen row — the same filing can
  // be listed by more than one inventory (both IE cycle years; a committee
  // inventory and the IE doc-type inventory).
  duplicateRowCount: number;
};

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// Unregistered filers all carry a null SBoEID, so the committee name is the
// only identity separating them (decision 6); the NAME: prefix keeps a
// name-keyed filer from ever colliding with a real SBoEID.
export function ncsbeFilerKeyForDocumentRow(row: Pick<NcsbeDocumentRow, "sboeId" | "committeeName">): string {
  return row.sboeId ? row.sboeId.toUpperCase() : `NAME:${normalizeTextKey(row.committeeName)}`;
}

type SelectionGroup = {
  filerKey: string;
  committeeName: string;
  documentType: string;
  reportType: string | null;
  periodStartRaw: string;
  periodEndRaw: string;
  rows: NcsbeDocumentRow[];
};

function maxIso(values: Array<string | null>): string | null {
  let max: string | null = null;
  for (const value of values) {
    if (value !== null && (max === null || value > max)) {
      max = value;
    }
  }
  return max;
}

function toFiling(group: SelectionGroup, isAmendment: boolean, rows: NcsbeDocumentRow[]): NcsbeFiling {
  const dataRow = rows.find((row) => row.dataLink !== null) ?? null;
  return {
    filerKey: group.filerKey,
    committeeName: group.committeeName,
    sboeId: rows[0]!.sboeId,
    documentType: group.documentType,
    reportType: group.reportType,
    periodStartRaw: group.periodStartRaw,
    periodEndRaw: group.periodEndRaw,
    periodStartIso: rows[0]!.periodStartDate.iso,
    periodEndIso: rows[0]!.periodEndDate.iso,
    isAmendment,
    reportId: dataRow?.dataLink ?? null,
    filedDateIso: maxIso(rows.map((row) => row.imageReceiptDate.iso)),
    rows,
  };
}

// Row-merge (decision 8): rows sharing (group, IsAmendment) assemble into
// filings. Allowed shapes: one DATA row with at most one IMAGE row (one
// filing, merged); several DATA rows with no IMAGE row (a chain of distinct
// structured filings — IsAmendment is a flag, not a counter, so two
// amendments of one period land here); a single IMAGE row (one image-only
// filing). Every other mix is ambiguous lineage.
function buildFilings(
  group: SelectionGroup,
  isAmendment: boolean,
  rows: NcsbeDocumentRow[]
): NcsbeFiling[] | "ambiguous" {
  const dataRows = rows.filter((row) => row.dataLink !== null);
  const imageRows = rows.filter((row) => row.dataLink === null);
  if (rows.length === 0) {
    return [];
  }
  if (dataRows.length === 1 && imageRows.length <= 1) {
    return [toFiling(group, isAmendment, rows)];
  }
  if (dataRows.length >= 2 && imageRows.length === 0) {
    return dataRows.map((row) => toFiling(group, isAmendment, [row]));
  }
  if (dataRows.length === 0 && imageRows.length === 1) {
    return [toFiling(group, isAmendment, rows)];
  }
  return "ambiguous";
}

// Chronology (decision 8): newest ImageReceiptDate first — what was legally
// filed last — with DataImportDate only as tie-break (import order is
// administrative and can lag or reorder). When both dates tie, the amendment
// flag decides: an amendment supersedes its original by definition, no
// chronology needed (a same-day amendment is still the amendment). What is
// deliberately NOT a key is the report id — nothing pins ids as monotonic
// with filing time, so two filings tying on this whole key are ambiguous
// (quarantined below), never id-ordered.
function chronologyKey(filing: NcsbeFiling): [string, string, string] {
  return [
    filing.filedDateIso ?? "",
    maxIso(filing.rows.map((row) => row.dataImportDate.iso)) ?? "",
    filing.isAmendment ? "1" : "0",
  ];
}

function compareChronology(left: NcsbeFiling, right: NcsbeFiling): number {
  const leftKey = chronologyKey(left);
  const rightKey = chronologyKey(right);
  for (let index = 0; index < leftKey.length; index += 1) {
    if (leftKey[index]! !== rightKey[index]!) {
      return leftKey[index]! < rightKey[index]! ? -1 : 1;
    }
  }
  return 0;
}

export function selectNcsbeCurrentFilings(input: {
  rows: readonly NcsbeDocumentRow[];
}): NcsbeReportSelectionResult {
  // Dedup: a structured row is the same filing wherever it is listed (one
  // report id = one filing); image-only rows dedup on their full identity.
  const seenReportIds = new Set<string>();
  const seenImageRowKeys = new Set<string>();
  const rows: NcsbeDocumentRow[] = [];
  let duplicateRowCount = 0;
  for (const row of input.rows) {
    if (row.dataLink !== null) {
      if (seenReportIds.has(row.dataLink)) {
        duplicateRowCount += 1;
        continue;
      }
      seenReportIds.add(row.dataLink);
    } else {
      const key = JSON.stringify([
        ncsbeFilerKeyForDocumentRow(row),
        row.documentType,
        row.reportType,
        row.isAmendment,
        row.periodStartDate.raw,
        row.periodEndDate.raw,
        row.imageReceiptDate.raw,
        row.dataImportDate.raw,
        row.imageLink,
      ]);
      if (seenImageRowKeys.has(key)) {
        duplicateRowCount += 1;
        continue;
      }
      seenImageRowKeys.add(key);
    }
    rows.push(row);
  }

  // Selection groups: key WITHOUT IsAmendment.
  const groups = new Map<string, SelectionGroup>();
  for (const row of rows) {
    const filerKey = ncsbeFilerKeyForDocumentRow(row);
    const key = JSON.stringify([
      filerKey,
      row.documentType,
      row.reportType,
      row.periodStartDate.raw,
      row.periodEndDate.raw,
    ]);
    const existing = groups.get(key);
    if (existing) {
      existing.rows.push(row);
      continue;
    }
    groups.set(key, {
      filerKey,
      committeeName: row.committeeName,
      documentType: row.documentType,
      reportType: row.reportType,
      periodStartRaw: row.periodStartDate.raw,
      periodEndRaw: row.periodEndDate.raw,
      rows: [row],
    });
  }

  const selected: NcsbeFiling[] = [];
  const supersededUnavailable: NcsbeFiling[] = [];
  const quarantinedGroups: NcsbeQuarantinedGroup[] = [];

  function quarantine(group: SelectionGroup, reason: NcsbeQuarantinedGroupReason): void {
    quarantinedGroups.push({
      filerKey: group.filerKey,
      committeeName: group.committeeName,
      documentType: group.documentType,
      reportType: group.reportType,
      periodStartRaw: group.periodStartRaw,
      periodEndRaw: group.periodEndRaw,
      reason,
      rowCount: group.rows.length,
    });
  }

  for (const group of groups.values()) {
    if (group.rows.some((row) => row.isAmendment === null)) {
      quarantine(group, "null_amendment_flag");
      continue;
    }
    const originals = buildFilings(
      group,
      false,
      group.rows.filter((row) => row.isAmendment === false)
    );
    const amendments = buildFilings(
      group,
      true,
      group.rows.filter((row) => row.isAmendment === true)
    );
    if (originals === "ambiguous" || amendments === "ambiguous") {
      quarantine(group, "ambiguous_row_merge");
      continue;
    }
    if (originals.length > 1) {
      quarantine(group, "multiple_original_filings");
      continue;
    }
    const filings = [...originals, ...amendments].sort(compareChronology);
    const current = filings[filings.length - 1]!;
    // A non-amendment picked over existing amendments means the chronology
    // evidence contradicts the amendment flags (or an amendment carries no
    // usable dates); stale money must not win a coin toss.
    if (!current.isAmendment && amendments.length > 0) {
      quarantine(group, "original_newer_than_amendment");
      continue;
    }
    // Two amendments tying on the full chronology key (same image date, same
    // import date) cannot be ordered by any pinned evidence — their money can
    // differ (a real amendment moved $6,800 on Berger's chain), so the group
    // fails closed instead of picking by id.
    if (filings.length > 1 && compareChronology(current, filings[filings.length - 2]!) === 0) {
      quarantine(group, "ambiguous_filing_chronology");
      continue;
    }
    if (current.reportId === null) {
      supersededUnavailable.push(current);
    } else {
      selected.push(current);
    }
  }

  const byGroupOrder = (left: NcsbeFiling, right: NcsbeFiling): number =>
    left.filerKey.localeCompare(right.filerKey) ||
    (left.periodStartIso ?? left.periodStartRaw).localeCompare(right.periodStartIso ?? right.periodStartRaw) ||
    (left.periodEndIso ?? left.periodEndRaw).localeCompare(right.periodEndIso ?? right.periodEndRaw) ||
    (left.reportId ?? "").localeCompare(right.reportId ?? "");
  selected.sort(byGroupOrder);
  supersededUnavailable.sort(byGroupOrder);
  quarantinedGroups.sort(
    (left, right) =>
      left.filerKey.localeCompare(right.filerKey) || left.periodStartRaw.localeCompare(right.periodStartRaw)
  );

  return {
    selected,
    supersededUnavailable,
    quarantinedGroups,
    groupCount: groups.size,
    duplicateRowCount,
  };
}
