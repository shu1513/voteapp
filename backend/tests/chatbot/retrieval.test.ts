import { describe, expect, it } from "vitest";

import { expandOfficeAliases } from "../../src/chatbot/retrieval.js";

describe("expandOfficeAliases", () => {
  it("appends the corpus office phrase for common federal Senate phrasings", () => {
    expect(expandOfficeAliases("Who has raised more money in the Georgia Senate race?")).toBe(
      "Who has raised more money in the Georgia Senate race? United States Senator"
    );
    expect(expandOfficeAliases("Who is running for US Senate in Georgia?")).toBe(
      "Who is running for US Senate in Georgia? United States Senator"
    );
    expect(expandOfficeAliases("Tell me about the U.S. Senate election")).toBe(
      "Tell me about the U.S. Senate election United States Senator"
    );
    expect(expandOfficeAliases("the United States Senate seat in Ohio")).toBe(
      "the United States Senate seat in Ohio United States Senator"
    );
  });

  it("appends each alias at most once", () => {
    expect(expandOfficeAliases("US Senate race or United States Senate seat?")).toBe(
      "US Senate race or United States Senate seat? United States Senator"
    );
  });

  it("leaves state-senate questions on the state races", () => {
    expect(expandOfficeAliases("Who won the state Senate race in Georgia?")).toBe(
      "Who won the state Senate race in Georgia?"
    );
    expect(expandOfficeAliases("Who's running for State Senate District 2?")).toBe(
      "Who's running for State Senate District 2?"
    );
  });

  it("leaves questions without a federal phrasing untouched", () => {
    expect(expandOfficeAliases("Who is Jon Ossoff?")).toBe("Who is Jon Ossoff?");
    expect(expandOfficeAliases("Who are the candidates for Los Angeles mayor?")).toBe(
      "Who are the candidates for Los Angeles mayor?"
    );
    // "Senate" alone (no race/seat/election frame, no US prefix) stays as-is:
    // "my senator" / "the Senate" questions are not reliably federal races.
    expect(expandOfficeAliases("What does the Senate do?")).toBe("What does the Senate do?");
  });
});
