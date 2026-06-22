import { describe, expect, it, vi } from "vitest";

import type { FinanceLabelClassification } from "../../../src/pipeline/finance/financeLabelClassifier.js";
import {
  buildFinanceIndustryBreakdownsFromClassifications,
  financeClassificationKey,
  mergeFinanceLabelClassification,
  resolveFinanceIndustryClassifications,
} from "../../../src/pipeline/finance/financeIndustryClassificationService.js";

function aiClassification(input: Partial<FinanceLabelClassification> = {}): FinanceLabelClassification {
  return {
    rawLabel: "Acme Quantum Labs LLC",
    labelType: "employer",
    normalizedLabel: "ACME QUANTUM LABS",
    industrySlug: "technology",
    confidence: "medium",
    classificationSource: "ai",
    matchedRule: null,
    ...input,
  };
}

function createDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

describe("financeIndustryClassificationService", () => {
  it("loads cached classifications before calling AI", async () => {
    const db = createDb([
      {
        raw_label: "Acme Quantum Labs LLC",
        label_type: "employer",
        normalized_label: "ACME QUANTUM LABS",
        industry_slug: "technology",
        confidence: "medium",
        classification_source: "ai",
      },
    ]);
    const classifications = new Map<string, FinanceLabelClassification>();
    const classifier = vi.fn();

    await resolveFinanceIndustryClassifications({
      db,
      directBreakdowns: [
        {
          categoryType: "employer",
          categoryName: "Acme Quantum Labs LLC",
          amount: 150_000,
        },
      ],
      outsideBreakdowns: [],
      classifications,
      classifier,
      minAmount: 100_000,
      dryRun: false,
    });

    expect(classifier).not.toHaveBeenCalled();
    expect(classifications.get(financeClassificationKey("employer", "ACME QUANTUM LABS"))).toMatchObject({
      industrySlug: "technology",
      classificationSource: "ai",
    });
  });

  it("calls AI for high-value unknown employers", async () => {
    const db = createDb();
    const classifications = new Map<string, FinanceLabelClassification>();
    const classifier = vi.fn().mockResolvedValue([aiClassification()]);

    await resolveFinanceIndustryClassifications({
      db,
      directBreakdowns: [
        {
          categoryType: "employer",
          categoryName: "Acme Quantum Labs LLC",
          amount: 150_000,
        },
      ],
      outsideBreakdowns: [],
      classifications,
      classifier,
      minAmount: 100_000,
      dryRun: false,
    });

    expect(classifier).toHaveBeenCalledWith({
      labels: [
        {
          rawLabel: "Acme Quantum Labs LLC",
          labelType: "employer",
          normalizedLabel: "ACME QUANTUM LABS",
          amount: 150_000,
        },
      ],
    });
    expect(classifications.get(financeClassificationKey("employer", "ACME QUANTUM LABS"))).toMatchObject({
      industrySlug: "technology",
      classificationSource: "ai",
    });
  });

  it("calls AI for high-value unknown donor organizations", async () => {
    const db = createDb();
    const classifications = new Map<string, FinanceLabelClassification>();
    const classifier = vi.fn().mockResolvedValue([
      aiClassification({
        rawLabel: "Stand for New Mexico",
        labelType: "donor",
        normalizedLabel: "STAND FOR NEW MEXICO",
        industrySlug: "finance_investment",
      }),
    ]);

    await resolveFinanceIndustryClassifications({
      db,
      directBreakdowns: [],
      outsideBreakdowns: [
        {
          committeeId: "1036307",
          supportOppose: "oppose",
          categoryType: "donor",
          categoryName: "Stand for New Mexico",
          amount: 860_000,
        },
      ],
      classifications,
      classifier,
      minAmount: 25_000,
      dryRun: false,
    });

    expect(classifier).toHaveBeenCalledWith({
      labels: [
        {
          rawLabel: "Stand for New Mexico",
          labelType: "donor",
          normalizedLabel: "STAND FOR NEW MEXICO",
          amount: 860_000,
        },
      ],
    });
    expect(classifications.get(financeClassificationKey("donor", "STAND FOR NEW MEXICO"))).toMatchObject({
      industrySlug: "finance_investment",
      classificationSource: "ai",
    });
  });

  it("does not call AI for labels below the configured amount threshold", async () => {
    const db = createDb();
    const classifications = new Map<string, FinanceLabelClassification>();
    const classifier = vi.fn();

    await resolveFinanceIndustryClassifications({
      db,
      directBreakdowns: [
        {
          categoryType: "employer",
          categoryName: "Acme Quantum Labs LLC",
          amount: 99_999,
        },
      ],
      outsideBreakdowns: [],
      classifications,
      classifier,
      minAmount: 100_000,
      dryRun: false,
    });

    expect(db.query).not.toHaveBeenCalled();
    expect(classifier).not.toHaveBeenCalled();
    expect(classifications.size).toBe(0);
  });

  it("applies the AI classification threshold after aggregating split employer amounts", async () => {
    const db = createDb();
    const classifications = new Map<string, FinanceLabelClassification>();
    const classifier = vi.fn().mockResolvedValue([aiClassification()]);

    await resolveFinanceIndustryClassifications({
      db,
      directBreakdowns: [
        {
          categoryType: "employer",
          categoryName: "Acme Quantum Labs LLC",
          amount: 60_000,
        },
        {
          categoryType: "employer",
          categoryName: "ACME QUANTUM LABS",
          amount: 50_000,
        },
      ],
      outsideBreakdowns: [],
      classifications,
      classifier,
      minAmount: 100_000,
      dryRun: false,
    });

    expect(classifier).toHaveBeenCalledWith({
      labels: [
        {
          rawLabel: "Acme Quantum Labs LLC",
          labelType: "employer",
          normalizedLabel: "ACME QUANTUM LABS",
          amount: 110_000,
        },
      ],
    });
  });

  it("keeps finance syncs successful when AI classification fails", async () => {
    const db = createDb();
    const classifications = new Map<string, FinanceLabelClassification>();
    const classifier = vi.fn().mockRejectedValue(new Error("provider unavailable"));

    await expect(
      resolveFinanceIndustryClassifications({
        db,
        directBreakdowns: [
          {
            categoryType: "employer",
            categoryName: "Acme Quantum Labs LLC",
            amount: 150_000,
          },
        ],
        outsideBreakdowns: [],
        classifications,
        classifier,
        minAmount: 100_000,
        dryRun: false,
      })
    ).resolves.toBeUndefined();

    expect(classifier).toHaveBeenCalledOnce();
    expect(classifications.size).toBe(0);
  });

  it("builds direct and outside industry breakdowns from employer classifications", () => {
    const classifications = new Map<string, FinanceLabelClassification>();
    mergeFinanceLabelClassification(classifications, aiClassification());
    mergeFinanceLabelClassification(
      classifications,
      aiClassification({
        rawLabel: "Energy Transfer LP",
        normalizedLabel: "ENERGY TRANSFER",
        industrySlug: "oil_gas_energy",
        confidence: "high",
        classificationSource: "rule",
      })
    );

    const result = buildFinanceIndustryBreakdownsFromClassifications({
      directBreakdowns: [
        {
          categoryType: "employer",
          categoryName: "Acme Quantum Labs LLC",
          amount: 150_000,
          contributorCount: 4,
          sourceUrl: "https://www.fec.gov/direct",
        },
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 50_000,
          contributorCount: 2,
          sourceUrl: "https://www.fec.gov/direct",
        },
      ],
      outsideBreakdowns: [
        {
          committeeId: "C00000001",
          supportOppose: "support",
          categoryType: "employer",
          categoryName: "Energy Transfer LP",
          amount: 200_000,
          contributorCount: 3,
          sourceUrl: "https://www.fec.gov/outside",
        },
      ],
      classifications,
    });

    expect(result.directIndustryBreakdowns).toEqual([
      {
        categoryType: "industry",
        categoryName: "technology",
        amount: 150_000,
        contributorCount: 4,
        sourceUrl: "https://www.fec.gov/direct",
      },
    ]);
    expect(result.outsideIndustryBreakdowns).toEqual([
      {
        committeeId: "C00000001",
        supportOppose: "support",
        categoryType: "industry",
        categoryName: "oil_gas_energy",
        amount: 200_000,
        contributorCount: 3,
        sourceUrl: "https://www.fec.gov/outside",
      },
    ]);
  });

  it("builds outside industry breakdowns from donor classifications", () => {
    const classifications = new Map<string, FinanceLabelClassification>();
    mergeFinanceLabelClassification(
      classifications,
      aiClassification({
        rawLabel: "Guzman Construction Solutions LLC",
        labelType: "donor",
        normalizedLabel: "GUZMAN CONSTRUCTION SOLUTIONS",
        industrySlug: "construction",
        confidence: "medium",
        classificationSource: "rule",
      })
    );

    const result = buildFinanceIndustryBreakdownsFromClassifications({
      directBreakdowns: [],
      outsideBreakdowns: [
        {
          committeeId: "1036307",
          supportOppose: "oppose",
          categoryType: "donor",
          categoryName: "Guzman Construction Solutions LLC",
          amount: 25_000,
          contributorCount: 1,
          sourceUrl: "https://login.cfis.sos.state.nm.us/",
        },
      ],
      classifications,
    });

    expect(result.outsideIndustryBreakdowns).toEqual([
      {
        committeeId: "1036307",
        supportOppose: "oppose",
        categoryType: "industry",
        categoryName: "construction",
        amount: 25_000,
        contributorCount: 1,
        sourceUrl: "https://login.cfis.sos.state.nm.us/",
      },
    ]);
  });
});
