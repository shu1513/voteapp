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
          amount: 50_000,
        },
      ],
    });

    expect(prompt).toContain("Use only the allowed industry_slug values below, or unknown.");
    expect(prompt).toContain("Prefer unknown over guessing.");
    expect(prompt).toContain("political committee, campaign committee, PAC");
    expect(prompt).toContain("technology");
    expect(prompt).toContain("oil_gas_energy");
    expect(prompt).toContain("environmental_group");
    expect(prompt).not.toContain("media_entertainment");
    expect(prompt).not.toContain("crypto");
    expect(prompt).not.toContain("labor");
    expect(prompt).toContain("Return exactly one classification for each (label_type, normalized_label) input pair.");
    expect(prompt).toContain('"label_type": "employer"');
    expect(prompt).toContain('"normalized_label": "ACME QUANTUM LABS"');
  });
});
