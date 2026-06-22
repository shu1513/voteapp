import {
  probeOklahomaGuardianIeReportDocument,
  searchOklahomaGuardianIeReports,
  type OklahomaGuardianIeReportClientOptions,
  type OklahomaGuardianIeReportDocumentProbeResult,
  type OklahomaGuardianIeReportSearchInput,
  type OklahomaGuardianIeReportSearchResult,
  type OklahomaGuardianIeReportSearchRow,
} from "./oklahomaGuardianIeReportClient.js";
import {
  decodeOklahomaGuardianIeReportPdfArtifact,
  evaluateOklahomaGuardianIeReportForCandidate,
  extractOklahomaGuardianIeReportPdfText,
  parseOklahomaGuardianIeReportText,
  type OklahomaGuardianIeReportCandidateEvaluation,
  type OklahomaGuardianIeReportCandidateStance,
  type OklahomaGuardianIeSupportOppose,
} from "./oklahomaGuardianIeReportParser.js";

export type OklahomaGuardianIeOutsideSpendingDiscoveryInput = OklahomaGuardianIeReportSearchInput & {
  maxReports?: number;
};

export type OklahomaGuardianIeOutsideSpendingReport = {
  rowIndex: number;
  sourceRow: OklahomaGuardianIeReportSearchRow;
  spenderName: string;
  candidateName: string;
  officeName: string | null;
  supportOppose: OklahomaGuardianIeSupportOppose;
  amount: number;
  reportingPeriodBegin: string | null;
  reportingPeriodEnd: string | null;
  reportDescription: string | null;
  amended: boolean | null;
  sourceUrl: string;
  pdfByteLength: number;
};

export type OklahomaGuardianIeOutsideSpendingSkipReason =
  | Extract<OklahomaGuardianIeReportCandidateEvaluation, { status: "skipped" }>["reason"]
  | "missing_pdf_artifact"
  | "multiple_pdf_artifacts"
  | "empty_pdf_text"
  | "probe_failed";

export type OklahomaGuardianIeOutsideSpendingSkippedReport = {
  rowIndex: number;
  sourceRow: OklahomaGuardianIeReportSearchRow;
  reason: OklahomaGuardianIeOutsideSpendingSkipReason;
  errorMessage?: string;
  candidateStances?: OklahomaGuardianIeReportCandidateStance[];
  matchingCandidateStances?: OklahomaGuardianIeReportCandidateStance[];
};

export type OklahomaGuardianIeOutsideSpendingDiscoveryResult = {
  search: OklahomaGuardianIeReportSearchResult;
  reportsExamined: number;
  usableReports: OklahomaGuardianIeOutsideSpendingReport[];
  skippedReports: OklahomaGuardianIeOutsideSpendingSkippedReport[];
};

function normalizeMaxReports(value: number | undefined): number {
  if (value === undefined) {
    return 10;
  }
  if (!Number.isInteger(value) || value <= 0 || value > 100) {
    throw new Error(`Invalid Oklahoma Guardian IE maxReports: ${value}`);
  }
  return value;
}

function skippedFromEvaluation(input: {
  rowIndex: number;
  sourceRow: OklahomaGuardianIeReportSearchRow;
  evaluation: Extract<OklahomaGuardianIeReportCandidateEvaluation, { status: "skipped" }>;
}): OklahomaGuardianIeOutsideSpendingSkippedReport {
  return {
    rowIndex: input.rowIndex,
    sourceRow: input.sourceRow,
    reason: input.evaluation.reason,
    candidateStances: input.evaluation.candidateStances,
    matchingCandidateStances: input.evaluation.matchingCandidateStances,
  };
}

function reportFromMatchedEvaluation(input: {
  rowIndex: number;
  probe: OklahomaGuardianIeReportDocumentProbeResult;
  evaluation: Extract<OklahomaGuardianIeReportCandidateEvaluation, { status: "matched" }>;
  parsed: ReturnType<typeof parseOklahomaGuardianIeReportText>;
}): OklahomaGuardianIeOutsideSpendingReport {
  return {
    rowIndex: input.rowIndex,
    sourceRow: input.probe.selectedRow,
    spenderName: input.evaluation.spenderName,
    candidateName: input.evaluation.candidateName,
    officeName: input.evaluation.officeName,
    supportOppose: input.evaluation.supportOppose,
    amount: input.evaluation.amount,
    reportingPeriodBegin: input.parsed.reportingPeriodBegin,
    reportingPeriodEnd: input.parsed.reportingPeriodEnd,
    reportDescription: input.parsed.reportDescription,
    amended: input.parsed.amended,
    sourceUrl: input.probe.sourceUrl,
    pdfByteLength: input.probe.pdfArtifacts[0]?.byteLength ?? 0,
  };
}

async function inspectOneReport(input: {
  searchInput: OklahomaGuardianIeReportSearchInput;
  rowIndex: number;
  sourceRow: OklahomaGuardianIeReportSearchRow;
  options: OklahomaGuardianIeReportClientOptions;
}): Promise<
  | { usable: OklahomaGuardianIeOutsideSpendingReport; skipped?: never }
  | { usable?: never; skipped: OklahomaGuardianIeOutsideSpendingSkippedReport }
> {
  let probe: OklahomaGuardianIeReportDocumentProbeResult;
  try {
    probe = await probeOklahomaGuardianIeReportDocument(
      { ...input.searchInput, rowIndex: input.rowIndex },
      input.options
    );
  } catch (error) {
    return {
      skipped: {
        rowIndex: input.rowIndex,
        sourceRow: input.sourceRow,
        reason: "probe_failed",
        errorMessage: error instanceof Error ? error.message : String(error),
      },
    };
  }

  if (probe.pdfArtifacts.length === 0) {
    return { skipped: { rowIndex: input.rowIndex, sourceRow: input.sourceRow, reason: "missing_pdf_artifact" } };
  }
  if (probe.pdfArtifacts.length > 1) {
    return { skipped: { rowIndex: input.rowIndex, sourceRow: input.sourceRow, reason: "multiple_pdf_artifacts" } };
  }

  const text = extractOklahomaGuardianIeReportPdfText(
    decodeOklahomaGuardianIeReportPdfArtifact(probe.pdfArtifacts[0])
  );
  if (!text.trim()) {
    return { skipped: { rowIndex: input.rowIndex, sourceRow: input.sourceRow, reason: "empty_pdf_text" } };
  }

  const parsed = parseOklahomaGuardianIeReportText(text);
  const evaluation = evaluateOklahomaGuardianIeReportForCandidate({
    parsed,
    candidateName: input.searchInput.candidateName,
  });

  if (evaluation.status === "skipped") {
    return { skipped: skippedFromEvaluation({ rowIndex: input.rowIndex, sourceRow: input.sourceRow, evaluation }) };
  }

  return { usable: reportFromMatchedEvaluation({ rowIndex: input.rowIndex, probe, evaluation, parsed }) };
}

export async function discoverOklahomaGuardianIeOutsideSpendingReports(
  input: OklahomaGuardianIeOutsideSpendingDiscoveryInput,
  options: OklahomaGuardianIeReportClientOptions = {}
): Promise<OklahomaGuardianIeOutsideSpendingDiscoveryResult> {
  const maxReports = normalizeMaxReports(input.maxReports);
  const search = await searchOklahomaGuardianIeReports(input, options);
  const rowsToInspect = search.rows.slice(0, maxReports);
  const usableReports: OklahomaGuardianIeOutsideSpendingReport[] = [];
  const skippedReports: OklahomaGuardianIeOutsideSpendingSkippedReport[] = [];

  for (const [rowIndex, sourceRow] of rowsToInspect.entries()) {
    const result = await inspectOneReport({ searchInput: input, rowIndex, sourceRow, options });
    if (result.usable) {
      usableReports.push(result.usable);
    } else {
      skippedReports.push(result.skipped);
    }
  }

  return {
    search,
    reportsExamined: rowsToInspect.length,
    usableReports,
    skippedReports,
  };
}
