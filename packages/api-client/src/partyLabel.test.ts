import { describe, expect, it } from "vitest";
import { partyColorClass, profilePartyLabel } from "./partyLabel";

describe("profilePartyLabel", () => {
  it("passes real party labels through", () => {
    expect(profilePartyLabel("Democratic")).toBe("Democratic");
    expect(profilePartyLabel("Republican")).toBe("Republican");
    expect(profilePartyLabel("Independent")).toBe("Independent");
    expect(profilePartyLabel("No Party Preference")).toBe("No Party Preference");
  });

  it("hides ballot-facing placeholders", () => {
    expect(profilePartyLabel("Nonpartisan")).toBe("");
    expect(profilePartyLabel("nonpartisan")).toBe("");
    expect(profilePartyLabel("Unknown")).toBe("");
  });

  it("coalesces missing values to empty string", () => {
    expect(profilePartyLabel(null)).toBe("");
    expect(profilePartyLabel(undefined)).toBe("");
    expect(profilePartyLabel("  ")).toBe("");
  });
});

describe("partyColorClass", () => {
  it("colors major parties, including affiliates and registration labels", () => {
    expect(partyColorClass("Democratic")).toBe("text-dem-blue");
    expect(partyColorClass("Democratic-Farmer-Labor")).toBe("text-dem-blue");
    expect(partyColorClass("Republican")).toBe("text-gop-red");
    expect(partyColorClass("Registered Republican")).toBe("text-gop-red");
  });

  it("leaves everything else uncolored", () => {
    expect(partyColorClass("Independent")).toBe("");
    expect(partyColorClass("Green")).toBe("");
    expect(partyColorClass("Nonpartisan")).toBe("");
    expect(partyColorClass(null)).toBe("");
  });
});
