import { describe, expect, it, vi } from "vitest";

import {
  buildKansasFilerKey,
  normalizeKansasFilerKey,
  normalizeKansasNameForStorage,
  upsertKansasFinanceLink,
} from "../../../src/pipeline/kansasFinance/kansasFinanceWriter.js";

describe("Kansas filer recipe keys", () => {
  it("builds <officeCode>:<district>:<SURNAME>:<FIRST> with storage normalization", () => {
    expect(buildKansasFilerKey({ officeCode: "7", districtNumber: 85, surname: "Brunson", firstName: "Steven" })).toBe("7:85:BRUNSON:STEVEN");
    expect(buildKansasFilerKey({ officeCode: "1", districtNumber: null, surname: "Rowan", firstName: "Stacy" })).toBe("1::ROWAN:STACY");
    expect(buildKansasFilerKey({ officeCode: "7", districtNumber: 12, surname: "Van Dyke", firstName: "Mary-Ann" })).toBe("7:12:VAN DYKE:MARY ANN");
    expect(normalizeKansasNameForStorage("O'Brien, José")).toBe("O BRIEN JOSE");
  });

  it("rejects keys outside the recipe shape", () => {
    expect(normalizeKansasFilerKey(" 7:85:brunson:steven ")).toBe("7:85:BRUNSON:STEVEN");
    expect(() => normalizeKansasFilerKey("12345")).toThrow("Invalid Kansas filer key");
    expect(() => normalizeKansasFilerKey("7:85:BRUNSON")).toThrow("Invalid Kansas filer key");
    expect(() => normalizeKansasFilerKey("7:85::STEVEN")).toThrow("Invalid Kansas filer key");
    expect(() => buildKansasFilerKey({ officeCode: "7", districtNumber: 85, surname: "", firstName: "Steven" })).toThrow("Invalid Kansas filer key");
  });
});

describe("upsertKansasFinanceLink", () => {
  it("canonicalizes the key and writes through the standard writer", async () => {
    const db = {
      query: vi.fn((sql: unknown) =>
        String(sql).includes("INSERT INTO public.ks_candidate_finance_links")
          ? Promise.resolve({ rows: [{ id: "link-1" }], rowCount: 1 })
          : Promise.resolve({ rows: [], rowCount: 0 })
      ),
    };
    const result = await upsertKansasFinanceLink({
      db,
      link: {
        candidateId: "11111111-1111-4111-8111-111111111111",
        electionId: "22222222-2222-4222-8222-222222222222",
        electionYear: 2026,
        candidateNameNormalized: "STEVEN BRUNSON",
        officeName: "State Lower Chamber Legislator",
        district: "85",
        committeeId: "7:85:brunson:steven",
        committeeName: "BRUNSON STEVEN",
        linkSource: "cfr_viewer",
      },
    });
    expect(result).toEqual({ linkId: "link-1" });
    const insert = db.query.mock.calls.find(([sql]) => String(sql).includes("INSERT INTO public.ks_candidate_finance_links"));
    expect(insert?.[1]?.[6]).toBe("7:85:BRUNSON:STEVEN");
    expect(insert?.[1]?.[9]).toBe("cfr_viewer");
  });

  it("refuses a link whose id is not a recipe key", async () => {
    const db = { query: vi.fn() };
    await expect(
      upsertKansasFinanceLink({
        db,
        link: {
          candidateId: "11111111-1111-4111-8111-111111111111",
          electionId: "22222222-2222-4222-8222-222222222222",
          electionYear: 2026,
          candidateNameNormalized: "STEVEN BRUNSON",
          officeName: "State Lower Chamber Legislator",
          district: "85",
          committeeId: "H085SB",
          committeeName: "BRUNSON STEVEN",
        },
      })
    ).rejects.toThrow("Invalid Kansas filer key");
    expect(db.query).not.toHaveBeenCalled();
  });
});
