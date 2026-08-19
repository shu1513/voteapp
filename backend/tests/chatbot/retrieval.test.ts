import { describe, expect, it } from "vitest";

import { classifyRaceQuestion, expandOfficeAliases, RACE_COLLECTIVE_RE } from "../../src/chatbot/retrieval.js";

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

describe("classifyRaceQuestion", () => {
  it("classifies money questions", () => {
    expect(classifyRaceQuestion("Who has raised more money in the Georgia Senate race?")).toBe("money");
    expect(classifyRaceQuestion("How much has the Republican candidate raised?")).toBe("money");
    expect(classifyRaceQuestion("Compare their fundraising")).toBe("money");
    expect(classifyRaceQuestion("Who spent the most cash on hand?")).toBe("money");
  });

  it("classifies records questions, winning over money words", () => {
    expect(classifyRaceQuestion("What are the candidates' records in the Georgia Senate race?")).toBe("records");
    expect(classifyRaceQuestion("How did they vote on the budget?")).toBe("records");
    expect(classifyRaceQuestion("What bills has she sponsored?")).toBe("records");
    // Both kinds of words → records: the asked-for artifact is the record.
    expect(classifyRaceQuestion("What is their voting record on campaign finance bills?")).toBe("records");
  });

  it("classifies everything else neutral", () => {
    expect(classifyRaceQuestion("Who is running for US Senate in Georgia?")).toBe("neutral");
    expect(classifyRaceQuestion("Tell me about the Los Angeles mayor race")).toBe("neutral");
  });
});

describe("RACE_COLLECTIVE_RE", () => {
  it("matches questions about a race's field as a whole", () => {
    expect(RACE_COLLECTIVE_RE.test("Compare the candidates for me.")).toBe(true);
    expect(RACE_COLLECTIVE_RE.test("What are the differences between them?")).toBe(true);
    expect(RACE_COLLECTIVE_RE.test("Who else is running?")).toBe(true);
    expect(RACE_COLLECTIVE_RE.test("who's running")).toBe(true);
    expect(RACE_COLLECTIVE_RE.test("Who has raised the most money in this race?")).toBe(true);
    expect(RACE_COLLECTIVE_RE.test("How do they stack up against each other?")).toBe(true);
  });

  it("does not leak the answerability-gate bypass to off-topic questions", () => {
    // These get page context applied if they match — so a match here means
    // an off-topic question would pass the gate on the page's chunks.
    expect(RACE_COLLECTIVE_RE.test("What will the weather be on election day?")).toBe(false);
    expect(RACE_COLLECTIVE_RE.test("Who is Jon Ossoff?")).toBe(false);
    expect(RACE_COLLECTIVE_RE.test("How do I contest a parking ticket in Atlanta?")).toBe(false);
    expect(RACE_COLLECTIVE_RE.test("Is the racetrack referendum on the ballot?")).toBe(false);
  });
});
