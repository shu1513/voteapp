import { describe, expect, it } from "vitest";
import {
  aggregateHoustonTexasGpacOutsideSpending,
} from "../../../src/pipeline/houstonFinance/houstonTexasGpacOutsideSpendingAggregator.js";
import {
  TEXAS_TEC_CANDIDATE_COLUMNS,
  TEXAS_TEC_EXPENDITURE_COLUMNS,
  TEXAS_TEC_PURPOSE_COLUMNS,
  type TexasTecCandidateRow,
  type TexasTecExpenditureRow,
  type TexasTecPurposeRow,
} from "../../../src/pipeline/texasFinance/texasTecCsvDatabaseReader.js";

function row<T extends readonly string[]>(columns: T, values: Partial<Record<T[number], string>>): Record<T[number], string> {
  return Object.fromEntries(columns.map((column) => [column, values[column] ?? ""])) as Record<T[number], string>;
}
const purpose = (values: Partial<TexasTecPurposeRow>) => row(TEXAS_TEC_PURPOSE_COLUMNS, values);
const candidate = (values: Partial<TexasTecCandidateRow>) => row(TEXAS_TEC_CANDIDATE_COLUMNS, values);
const expenditure = (values: Partial<TexasTecExpenditureRow>) => row(TEXAS_TEC_EXPENDITURE_COLUMNS, values);

describe("Houston Texas GPAC outside spending", () => {
  it("requires the exact report relation, Houston Mayor context, and explicit stance", () => {
    const result = aggregateHoustonTexasGpacOutsideSpending({
      candidateName: "John Whitmire",
      electionYear: 2023,
      maxGroups: 1,
      purposeRows: [
        purpose({ filerIdent: "100", filerTypeCd: "GPAC", reportInfoIdent: "10", subjectCategoryCd: "CANDIDATE", subjectPositionCd: "SUPPORT", commActivityName: "Whitmire, John", activitySeekOfficeDescr: "Mayor", activitySeekOfficePlace: "Houston" }),
        purpose({ filerIdent: "200", filerTypeCd: "GPAC", reportInfoIdent: "20", subjectCategoryCd: "CANDIDATE", subjectPositionCd: "OPPOSE", commActivityName: "Whitmire, John", activitySeekOfficeDescr: "Mayor", activitySeekOfficePlace: "Houston" }),
        purpose({ filerIdent: "300", filerTypeCd: "GPAC", reportInfoIdent: "30", subjectCategoryCd: "CANDIDATE", subjectPositionCd: "SUPPORT", commActivityName: "Whitmire, John", activitySeekOfficeDescr: "Mayor", activitySeekOfficePlace: "Dallas" }),
      ],
      candidateRows: [
        candidate({ filerIdent: "100", filerName: "Large Group", reportInfoIdent: "10", expendInfoId: "1", candidateNameFirst: "John", candidateNameLast: "Whitmire", candidateSeekOfficeDescr: "Mayor", candidateSeekOfficePlace: "Houston" }),
        candidate({ filerIdent: "200", filerName: "Small Group", reportInfoIdent: "20", expendInfoId: "2", candidateNameFirst: "John", candidateNameLast: "Whitmire", candidateSeekOfficeDescr: "Mayor", candidateSeekOfficePlace: "Houston" }),
        candidate({ filerIdent: "300", filerName: "Wrong City", reportInfoIdent: "30", expendInfoId: "3", candidateNameFirst: "John", candidateNameLast: "Whitmire", candidateSeekOfficeDescr: "Mayor", candidateSeekOfficePlace: "Dallas" }),
      ],
      expenditureRows: [
        expenditure({ filerIdent: "100", expendInfoId: "1", expendDt: "20231001", expendAmount: "100000" }),
        expenditure({ filerIdent: "200", expendInfoId: "2", expendDt: "20231002", expendAmount: "50000" }),
        expenditure({ filerIdent: "300", expendInfoId: "3", expendDt: "20231003", expendAmount: "900000" }),
      ],
    });
    expect(result.summary?.groups).toHaveLength(1);
    expect(result.summary?.groups[0]?.committeeName).toBe("Large Group");
    expect(result.summary?.supportTotal).toBe(100000);
    expect(result.summary?.opposeTotal).toBe(50000);
  });

  it("skips blank or conflicting report direction", () => {
    const result = aggregateHoustonTexasGpacOutsideSpending({
      candidateName: "John Whitmire", electionYear: 2023,
      purposeRows: [
        purpose({ filerIdent: "100", filerTypeCd: "GPAC", reportInfoIdent: "10", subjectCategoryCd: "CANDIDATE", subjectPositionCd: "SUPPORT", commActivityName: "John Whitmire", activitySeekOfficeDescr: "Houston Mayor" }),
        purpose({ filerIdent: "100", filerTypeCd: "GPAC", reportInfoIdent: "10", subjectCategoryCd: "CANDIDATE", subjectPositionCd: "OPPOSE", commActivityName: "John Whitmire", activitySeekOfficeDescr: "Houston Mayor" }),
      ],
      candidateRows: [candidate({ filerIdent: "100", filerName: "Ambiguous", reportInfoIdent: "10", expendInfoId: "1", candidateNameFirst: "John", candidateNameLast: "Whitmire", candidateSeekOfficeDescr: "Houston Mayor" })],
      expenditureRows: [expenditure({ filerIdent: "100", expendInfoId: "1", expendDt: "20231001", expendAmount: "100" })],
    });
    expect(result.summary).toBeNull();
  });
});
