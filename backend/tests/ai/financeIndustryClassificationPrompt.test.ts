import { describe, expect, it } from "vitest";

import { buildFinanceIndustryClassificationPrompt } from "../../src/ai/providers/financeIndustryClassificationPrompt.js";

describe("financeIndustryClassificationPrompt", () => {
  it("constrains AI industry classification to allowed slugs or unknown", () => {
    const prompt = buildFinanceIndustryClassificationPrompt({
      labels: [
        {
          rawLabel: "Acme Quantum Labs LLC",
          labelType: "employer",
          normalizedLabel: "ACME QUANTUM LABS",
        },
      ],
    });

    expect(prompt).toContain("Classify campaign-finance employer, donor, or organization labels");
    expect(prompt).not.toContain("Classify FEC campaign-finance");
    expect(prompt).toContain("Use only the allowed industry_slug values below, or unknown.");
    expect(prompt).toContain("Prefer unknown over guessing.");
    expect(prompt).toContain("political committee, campaign committee, PAC");
    expect(prompt).toContain("technology");
    expect(prompt).toContain("oil_gas_energy");
    expect(prompt).toContain("pharmaceuticals");
    expect(prompt).toContain("lawyers_and_legal_services");
    expect(prompt).toContain("agriculture_and_food");
    expect(prompt).toContain("waste_management");
    expect(prompt).toContain("labor_unions");
    expect(prompt).toContain("environmental_group");
    expect(prompt).toContain("media_entertainment");
    expect(prompt).toContain("retail");
    expect(prompt).not.toContain("public_sector");
    expect(prompt).not.toContain("telecom");
    expect(prompt).not.toContain("crypto");
    expect(prompt).toContain("Return exactly one classification for each input id.");
    expect(prompt).toContain('"id": "1"');
    expect(prompt).toContain('"label": "Acme Quantum Labs LLC"');
    expect(prompt).not.toContain('"amount"');
    expect(prompt).not.toContain('"label_type"');
    expect(prompt).not.toContain('"normalized_label"');
  });
});
