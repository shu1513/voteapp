import { describe, expect, it } from "vitest";

import type { CandidateRecordDroppedRecord } from "../../src/ai/enrichCandidateRecords.js";
import {
  applyConfirmedGaps,
  buildCandidateRecordQualityGaps,
  droppedRecordToGap,
  isBlockingCandidateRecordQualityGap,
  qualityDroppedRecordsToGaps,
} from "../../src/scripts/writeManualCandidateRecords.js";
import { buildManualResearchRepairReport } from "../../src/scripts/manualResearchRepairReport.js";

describe("writeManualCandidateRecords quality repair gaps", () => {
  it("turns quality-dropped records into deeper record-only repair instructions", () => {
    const dropped: CandidateRecordDroppedRecord = {
      record: {
        description: "The Secretary of State lists Jane Doe as a candidate for Governor.",
        source_url: "https://sos.example/candidates",
        event_date: "2026-05-01",
      },
      reason: "candidate record quality rejected row: pure_candidacy",
      failureType: "permanent",
      failureKind: "quality_gap",
    };

    const gap = droppedRecordToGap(dropped, 0);

    expect(gap).toEqual(
      expect.objectContaining({
        id: "candidate_records.dropped.0",
        stage: "candidate_records",
        objectType: "candidate_record",
        outcome: "needs_repair",
        failureKind: "quality_gap",
        promptFile: "src/ai/providers/candidateRecordDiscoveryPrompt.ts",
      })
    );
    expect(gap.focusedResearchPass).toContain("Do a deeper record-only research pass");
    expect(gap.focusedResearchPass).toContain("Do not replace this with another candidacy");
    expect(gap.focusedResearchPass).toContain("ballot-listing");
  });

  it("includes quality_gap dropped records in the repair report shape", () => {
    const dropped: CandidateRecordDroppedRecord = {
      record: {
        description: "The Secretary of State lists Jane Doe as a candidate for Governor.",
        source_url: "https://sos.example/candidates",
        event_date: "2026-05-01",
      },
      reason: "candidate record quality rejected row: pure_candidacy",
      failureType: "permanent",
      failureKind: "quality_gap",
    };
    const gap = droppedRecordToGap(dropped, 0);

    const report = buildManualResearchRepairReport({
      command: "manual:candidate-records:write",
      manualKey: "manual:candidate-records:election-1:candidate-1",
      target: {
        candidateId: "candidate-1",
        electionId: "election-1",
        recordsFile: "records.json",
        labelsFile: "labels.json",
      },
      gaps: [gap],
    });

    expect(report.status).toBe("needs_repair");
    expect(report.gaps).toContainEqual(
      expect.objectContaining({
        id: "candidate_records.dropped.0",
        failureKind: "quality_gap",
      })
    );
  });

  it("keeps source and schema drops pointed at source/schema repair", () => {
    const dropped: CandidateRecordDroppedRecord = {
      record: {
        description: "Served on the budget committee.",
        source_url: "https://bad.example/404",
        event_date: "2024-01-01",
      },
      reason: "citation fetch returned status 404",
      failureType: "permanent",
      failureKind: "source_url",
    };

    const gap = droppedRecordToGap(dropped, 1);

    expect(gap.promptFile).toBe("src/ai/providers/candidateRecordSourceRepairPrompt.ts");
    expect(gap.focusedResearchPass).toContain("source/schema repair pass");
  });

  it("keeps quality dropped gap ids stable when mixed with source drops", () => {
    const sourceDrop: CandidateRecordDroppedRecord = {
      record: {
        description: "Served on the budget committee.",
        source_url: "https://bad.example/404",
        event_date: "2024-01-01",
      },
      reason: "citation fetch returned status 404",
      failureType: "permanent",
      failureKind: "source_url",
    };
    const qualityDrop: CandidateRecordDroppedRecord = {
      record: {
        description: "The Secretary of State lists Jane Doe as a candidate for Governor.",
        source_url: "https://sos.example/candidates",
        event_date: "2026-05-01",
      },
      reason: "candidate record quality rejected row: pure_candidacy",
      failureType: "permanent",
      failureKind: "quality_gap",
    };

    expect(qualityDroppedRecordsToGaps([sourceDrop, qualityDrop])).toEqual([
      expect.objectContaining({
        id: "candidate_records.dropped.1",
        failureKind: "quality_gap",
      }),
    ]);
  });

  it("blocks strict mode on no-record and only-general set gaps", () => {
    const noRecordGap = buildCandidateRecordQualityGaps({
      recordCount: 0,
      labels: [],
    })[0];
    const onlyGeneralGap = buildCandidateRecordQualityGaps({
      recordCount: 1,
      labels: [{ research_area_slug: "general" }],
    })[0];

    expect(noRecordGap?.id).toBe("candidate_records.no_records_found");
    expect(onlyGeneralGap?.id).toBe("candidate_records.only_general_labels");
    expect(noRecordGap && isBlockingCandidateRecordQualityGap(noRecordGap)).toBe(true);
    expect(onlyGeneralGap && isBlockingCandidateRecordQualityGap(onlyGeneralGap)).toBe(true);
  });

  it("does not let a dropped quality row itself block once accepted records exist", () => {
    const dropped: CandidateRecordDroppedRecord = {
      record: {
        description: "The Secretary of State lists Jane Doe as a candidate for Governor.",
        source_url: "https://sos.example/candidates",
        event_date: "2026-05-01",
      },
      reason: "candidate record quality rejected row: pure_candidacy",
      failureType: "permanent",
      failureKind: "quality_gap",
    };

    const gap = droppedRecordToGap(dropped, 0);

    expect(isBlockingCandidateRecordQualityGap(gap)).toBe(false);
  });

  it("lets confirmed no-record and only-general gaps stop blocking after focused repair", () => {
    const gaps = [
      ...buildCandidateRecordQualityGaps({ recordCount: 0, labels: [] }),
      ...buildCandidateRecordQualityGaps({
        recordCount: 1,
        labels: [{ research_area_slug: "general" }],
      }),
    ];

    const confirmed = applyConfirmedGaps(
      gaps,
      new Set(["candidate_records.no_records_found", "candidate_records.only_general_labels"])
    );

    expect(confirmed.every((gap) => isBlockingCandidateRecordQualityGap(gap) === false)).toBe(true);
    expect(confirmed).toContainEqual(
      expect.objectContaining({
        id: "candidate_records.no_records_found",
        outcome: "confirmed_null",
      })
    );
    expect(confirmed).toContainEqual(
      expect.objectContaining({
        id: "candidate_records.only_general_labels",
        outcome: "confirmed_neutral",
      })
    );
  });

  it("keeps all-general labels blocking until candidate_records.only_general_labels is confirmed", () => {
    const gaps = buildCandidateRecordQualityGaps({
      recordCount: 1,
      labels: [{ research_area_slug: "general" }],
    });

    const wrongConfirmation = applyConfirmedGaps(
      gaps,
      new Set(["candidate_records.no_records_found"])
    );
    const rightConfirmation = applyConfirmedGaps(
      gaps,
      new Set(["candidate_records.only_general_labels"])
    );

    expect(wrongConfirmation.some(isBlockingCandidateRecordQualityGap)).toBe(true);
    expect(rightConfirmation.some(isBlockingCandidateRecordQualityGap)).toBe(false);
    expect(rightConfirmation).toContainEqual(
      expect.objectContaining({
        id: "candidate_records.only_general_labels",
        outcome: "confirmed_neutral",
      })
    );
  });

  it("keeps zero records blocking until candidate_records.no_records_found is confirmed", () => {
    const gaps = buildCandidateRecordQualityGaps({
      recordCount: 0,
      labels: [],
    });

    const wrongConfirmation = applyConfirmedGaps(
      gaps,
      new Set(["candidate_records.only_general_labels"])
    );
    const rightConfirmation = applyConfirmedGaps(
      gaps,
      new Set(["candidate_records.no_records_found"])
    );

    expect(wrongConfirmation.some(isBlockingCandidateRecordQualityGap)).toBe(true);
    expect(rightConfirmation.some(isBlockingCandidateRecordQualityGap)).toBe(false);
    expect(rightConfirmation).toContainEqual(
      expect.objectContaining({
        id: "candidate_records.no_records_found",
        outcome: "confirmed_null",
      })
    );
  });
});
