import { describe, expect, it } from "vitest";

import { firstNameVariants } from "../../../src/pipeline/finance/personFirstNameNicknames.js";

describe("firstNameVariants", () => {
  it("maps formal names and nicknames in both directions", () => {
    expect(firstNameVariants("MICHAEL")).toContain("MIKE");
    expect(firstNameVariants("MIKE")).toContain("MICHAEL");
    expect(firstNameVariants("FRANCES")).toContain("FRAN");
    expect(firstNameVariants("WILLIAM")).toEqual(expect.arrayContaining(["BILL", "BILLY", "WILL"]));
  });

  it("returns the union of groups for shared nicknames", () => {
    expect(firstNameVariants("PAT")).toEqual(expect.arrayContaining(["PATRICK", "PATRICIA"]));
    expect(firstNameVariants("STEVE")).toEqual(expect.arrayContaining(["STEPHEN", "STEVEN"]));
    expect(firstNameVariants("HARRY")).toEqual(expect.arrayContaining(["HAROLD", "HENRY", "HANK"]));
    expect(firstNameVariants("TED")).toEqual(
      expect.arrayContaining(["EDWARD", "ED", "EDDIE", "THEODORE", "TEDDY"])
    );
  });

  it("does not relate distinct formal names through a shared nickname", () => {
    // PATRICK and PATRICIA both map to PAT, but not to each other.
    expect(firstNameVariants("PATRICK")).not.toContain("PATRICIA");
    expect(firstNameVariants("STEPHEN")).not.toContain("STEVEN");
  });

  it("returns nothing for names without a known nickname", () => {
    expect(firstNameVariants("ZELDA")).toEqual([]);
    expect(firstNameVariants("")).toEqual([]);
  });
});
