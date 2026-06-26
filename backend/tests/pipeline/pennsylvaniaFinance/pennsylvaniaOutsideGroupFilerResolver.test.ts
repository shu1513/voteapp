import { describe, expect, it } from "vitest";

import {
  normalizePennsylvaniaOutsideGroupNameKey,
  resolvePennsylvaniaOutsideGroupFiler,
} from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaOutsideGroupFilerResolver.js";
import type { PennsylvaniaCampaignFinanceFilerRow } from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCampaignFinanceReader.js";

function filerRow(overrides: Partial<PennsylvaniaCampaignFinanceFilerRow> = {}): PennsylvaniaCampaignFinanceFilerRow {
  return {
    CampaignfinanceID: "100",
    FILERID: "PAC123",
    EYEAR: "2026",
    SubmittedDate: "20260501",
    CYCLE: "2",
    AMMEND: "",
    TERMINATE: "",
    FILERTYPE: "4",
    FILERNAME: "PENNSYLVANIANS FOR ACTION",
    OFFICE: "",
    DISTRICT: "",
    PARTY: "",
    ADDRESS1: "",
    ADDRESS2: "",
    CITY: "",
    STATE: "PA",
    ZIPCODE: "",
    COUNTY: "",
    PHONE: "",
    BEGINNING: "",
    MONETARY: "",
    INKIND: "",
    ...overrides,
  };
}

describe("pennsylvaniaOutsideGroupFilerResolver", () => {
  it("normalizes outside group names without broad fuzzy matching", () => {
    expect(normalizePennsylvaniaOutsideGroupNameKey("The Pennsylvanians for Action PAC, Inc.")).toBe(
      "PENNSYLVANIANS FOR ACTION"
    );
  });

  it("matches an outside organization to a filer by exact normalized name", () => {
    expect(
      resolvePennsylvaniaOutsideGroupFiler({
        organizationName: "Pennsylvanians for Action",
        filerRows: [filerRow()],
      })
    ).toEqual({
      status: "matched",
      filerId: "PAC123",
      filerName: "PENNSYLVANIANS FOR ACTION",
      filerType: "4",
      confidence: "exact",
      source: "pa_bulk",
      matchedFilerRowCount: 1,
    });
  });

  it("matches a small alias table entry", () => {
    expect(
      resolvePennsylvaniaOutsideGroupFiler({
        organizationName: "PA State Education Association",
        filerRows: [
          filerRow({
            FILERID: "PSEA1",
            FILERNAME: "PENNSYLVANIA STATE EDUCATION ASSOCIATION",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "PSEA1",
    });
  });

  it("does not substring-match unmatched organizations", () => {
    expect(
      resolvePennsylvaniaOutsideGroupFiler({
        organizationName: "Pennsylvanians",
        filerRows: [filerRow()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "no_filer_match",
      organizationNameNormalized: "PENNSYLVANIANS",
    });
  });

  it("returns ambiguous when exact normalized name maps to multiple filers", () => {
    expect(
      resolvePennsylvaniaOutsideGroupFiler({
        organizationName: "Pennsylvanians for Action",
        filerRows: [filerRow(), filerRow({ FILERID: "PAC999" })],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_filers",
      organizationNameNormalized: "PENNSYLVANIANS FOR ACTION",
      matches: [
        {
          filerId: "PAC123",
          filerName: "PENNSYLVANIANS FOR ACTION",
          filerType: "4",
          confidence: "exact",
          source: "pa_bulk",
          matchedFilerRowCount: 1,
        },
        {
          filerId: "PAC999",
          filerName: "PENNSYLVANIANS FOR ACTION",
          filerType: "4",
          confidence: "exact",
          source: "pa_bulk",
          matchedFilerRowCount: 1,
        },
      ],
    });
  });
});
