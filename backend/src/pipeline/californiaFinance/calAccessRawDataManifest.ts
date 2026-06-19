import type { CalAccessRawDataFileSample, CalAccessRawDataProbeResult } from "./calAccessRawDataProbe.js";

export type CalAccessRawDataTableKey =
  | "filer_names"
  | "campaign_cover"
  | "receipts"
  | "late_independent_expenditures"
  | "late_contributions"
  | "summary_lines";

export type CalAccessRawDataTableManifestEntry = {
  key: CalAccessRawDataTableKey;
  fileName: string;
  description: string;
  requiredColumns: readonly string[];
};

export type CalAccessRawDataManifestTableValidation = {
  key: CalAccessRawDataTableKey;
  fileName: string;
  present: boolean;
  missingColumns: string[];
};

export type CalAccessRawDataManifestValidation = {
  ok: boolean;
  tables: CalAccessRawDataManifestTableValidation[];
  missingFiles: string[];
};

export const CAL_ACCESS_RAW_DATA_TABLE_MANIFEST = [
  {
    key: "filer_names",
    fileName: "CalAccess/DATA/FILERNAME_CD.TSV",
    description: "Filer names and contact metadata used to resolve committee IDs to committee names.",
    requiredColumns: ["FILER_ID", "FILER_TYPE", "STATUS", "NAML", "NAMF", "CITY", "ST"],
  },
  {
    key: "campaign_cover",
    fileName: "CalAccess/DATA/CVR_CAMPAIGN_DISCLOSURE_CD.TSV",
    description: "Campaign filing cover records used to link committees, candidates, offices, and filing periods.",
    requiredColumns: [
      "FILING_ID",
      "FORM_TYPE",
      "FILER_ID",
      "FILER_NAML",
      "RPT_DATE",
      "FROM_DATE",
      "THRU_DATE",
      "ELECT_DATE",
      "CMTTE_ID",
      "CMTTE_TYPE",
      "CONTROL_YN",
      "CAND_NAML",
      "CAND_NAMF",
      "OFFICE_CD",
      "OFFIC_DSCR",
      "JURIS_CD",
      "DIST_NO",
      "SUP_OPP_CD",
    ],
  },
  {
    key: "receipts",
    fileName: "CalAccess/DATA/RCPT_CD.TSV",
    description: "Receipt rows used for direct contributions and donor backtrace into outside spender committees.",
    requiredColumns: [
      "FILING_ID",
      "FORM_TYPE",
      "TRAN_ID",
      "ENTITY_CD",
      "CTRIB_NAML",
      "CTRIB_NAMF",
      "CTRIB_CITY",
      "CTRIB_ST",
      "CTRIB_EMP",
      "CTRIB_OCC",
      "RCPT_DATE",
      "AMOUNT",
      "CMTE_ID",
      "CAND_NAML",
      "CAND_NAMF",
      "OFFICE_CD",
      "OFFIC_DSCR",
      "SUP_OPP_CD",
    ],
  },
  {
    key: "late_independent_expenditures",
    fileName: "CalAccess/DATA/S496_CD.TSV",
    description: "Form 496 late independent expenditure line items.",
    requiredColumns: ["FILING_ID", "FORM_TYPE", "TRAN_ID", "AMOUNT", "EXP_DATE", "EXPN_DSCR"],
  },
  {
    key: "late_contributions",
    fileName: "CalAccess/DATA/S497_CD.TSV",
    description: "Form 497 late contribution line items, including candidate and support/oppose fields when reported.",
    requiredColumns: [
      "FILING_ID",
      "FORM_TYPE",
      "TRAN_ID",
      "ENTITY_CD",
      "ENTY_NAML",
      "ENTY_NAMF",
      "CTRIB_EMP",
      "CTRIB_OCC",
      "CTRIB_DATE",
      "AMOUNT",
      "CMTE_ID",
      "CAND_NAML",
      "CAND_NAMF",
      "OFFICE_CD",
      "OFFIC_DSCR",
      "SUP_OPP_CD",
    ],
  },
  {
    key: "summary_lines",
    fileName: "CalAccess/DATA/SMRY_CD.TSV",
    description: "Form summary line amounts used as a future source for totals when line mapping is known.",
    requiredColumns: ["FILING_ID", "LINE_ITEM", "FORM_TYPE", "AMOUNT_A", "AMOUNT_B", "AMOUNT_C"],
  },
] as const satisfies readonly CalAccessRawDataTableManifestEntry[];

export function listCalAccessRawDataManifestFileNames(): string[] {
  return CAL_ACCESS_RAW_DATA_TABLE_MANIFEST.map((entry) => entry.fileName);
}

function headersByFileName(samples: readonly CalAccessRawDataFileSample[]): Map<string, Set<string>> {
  return new Map(samples.map((sample) => [sample.fileName, new Set(sample.headers)]));
}

export function validateCalAccessRawDataManifest(
  probe: Pick<CalAccessRawDataProbeResult, "entries" | "samples" | "missingFileNames">
): CalAccessRawDataManifestValidation {
  const entryNames = new Set(probe.entries.map((entry) => entry.fileName));
  const sampleHeaders = headersByFileName(probe.samples);
  const missingFiles = new Set(probe.missingFileNames);

  const tables = CAL_ACCESS_RAW_DATA_TABLE_MANIFEST.map((entry) => {
    const present = entryNames.has(entry.fileName) && !missingFiles.has(entry.fileName);
    const headers = sampleHeaders.get(entry.fileName);
    const missingColumns = headers
      ? entry.requiredColumns.filter((column) => !headers.has(column))
      : [...entry.requiredColumns];

    return {
      key: entry.key,
      fileName: entry.fileName,
      present,
      missingColumns: present ? missingColumns : [...entry.requiredColumns],
    };
  });

  return {
    ok: tables.every((table) => table.present && table.missingColumns.length === 0),
    tables,
    missingFiles: tables.filter((table) => !table.present).map((table) => table.fileName),
  };
}
