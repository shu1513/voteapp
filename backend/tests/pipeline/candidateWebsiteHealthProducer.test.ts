import { describe, expect, it } from "vitest";

import {
  buildFormerWebsitesAfterRetire,
  classifyCandidateWebsiteCheckResult,
  isOffDomainRedirect,
  isRetireEligible,
  rootUrlOfDeepLink,
} from "../../src/pipeline/candidates/candidateWebsiteHealthProducer.js";

describe("classifyCandidateWebsiteCheckResult", () => {
  it("classifies 404 as hard_fail", () => {
    const classified = classifyCandidateWebsiteCheckResult({
      ok: false,
      reason: "citation fetch returned status 404",
    });
    expect(classified.outcome).toBe("hard_fail");
    expect(classified.statusCode).toBe(404);
  });

  it("upgrades unresolved hostname to hard_fail (dead campaign domain)", () => {
    const classified = classifyCandidateWebsiteCheckResult({
      ok: false,
      reason: "citation URL hostname could not be resolved",
    });
    expect(classified.outcome).toBe("hard_fail");
    expect(classified.statusCode).toBeNull();
  });

  it("keeps transient DNS resolver failures transient", () => {
    const classified = classifyCandidateWebsiteCheckResult({
      ok: false,
      reason: "citation URL DNS lookup failed transiently: EAI_AGAIN",
    });
    expect(classified.outcome).toBe("transient_fail");
  });

  it("keeps timeout transient", () => {
    const classified = classifyCandidateWebsiteCheckResult({
      ok: false,
      reason: "citation URL fetch timed out",
    });
    expect(classified.outcome).toBe("transient_fail");
  });

  it("classifies ok response as healthy", () => {
    const classified = classifyCandidateWebsiteCheckResult({
      ok: true,
      normalizedUrl: "https://example.com",
      finalUrl: "https://example.com",
      status: 200,
    });
    expect(classified.outcome).toBe("healthy");
    expect(classified.statusCode).toBe(200);
  });
});

describe("isOffDomainRedirect", () => {
  it("flags a redirect onto a different host", () => {
    expect(
      isOffDomainRedirect("https://smithformayor.com", "https://someparkinglot.com/lander")
    ).toBe(true);
  });

  it("ignores www vs bare-host differences", () => {
    expect(
      isOffDomainRedirect("https://smithformayor.com", "https://www.smithformayor.com/home")
    ).toBe(false);
  });

  it("ignores same-host path redirects", () => {
    expect(
      isOffDomainRedirect("https://smithformayor.com/old", "https://smithformayor.com/new")
    ).toBe(false);
  });
});

describe("rootUrlOfDeepLink", () => {
  it("returns the origin root for a subpage", () => {
    expect(rootUrlOfDeepLink("https://www.manyformercer.com/about")).toBe(
      "https://www.manyformercer.com/"
    );
  });

  it("returns null for a URL that is already a root", () => {
    expect(rootUrlOfDeepLink("https://www.mandelabarnes.com/")).toBeNull();
    expect(rootUrlOfDeepLink("https://www.mandelabarnes.com")).toBeNull();
  });

  it("treats a query-only URL as worth trimming", () => {
    expect(rootUrlOfDeepLink("https://kentoncounty.org/?EID=58")).toBe("https://kentoncounty.org/");
  });

  it("returns null for an unparseable URL", () => {
    expect(rootUrlOfDeepLink("not a url")).toBeNull();
  });
});

describe("isRetireEligible", () => {
  const asOf = new Date("2026-08-21T00:00:00.000Z");
  const base = {
    consecutiveHardFailures: 3,
    firstHardFailedAt: new Date("2026-08-01T00:00:00.000Z"),
    lastHttpStatus: 404 as number | null,
    lastError: "citation fetch returned status 404" as string | null,
    hardFailureThreshold: 3,
    hardFailureWindowDays: 14,
    asOf,
  };

  it("eligible on 404 streak past the window", () => {
    expect(isRetireEligible(base)).toBe(true);
  });

  it("eligible on unresolved-hostname streak (no status code)", () => {
    expect(
      isRetireEligible({
        ...base,
        lastHttpStatus: null,
        lastError: "citation URL hostname could not be resolved",
      })
    ).toBe(true);
  });

  it("not eligible on null status with a non-DNS error", () => {
    expect(
      isRetireEligible({
        ...base,
        lastHttpStatus: null,
        lastError: "citation URL fetch timed out",
      })
    ).toBe(false);
  });

  it("not eligible below the streak threshold", () => {
    expect(isRetireEligible({ ...base, consecutiveHardFailures: 2 })).toBe(false);
  });

  it("not eligible inside the age window", () => {
    expect(
      isRetireEligible({ ...base, firstHardFailedAt: new Date("2026-08-15T00:00:00.000Z") })
    ).toBe(false);
  });

  it("not eligible on soft statuses (403/500)", () => {
    expect(isRetireEligible({ ...base, lastHttpStatus: 500 })).toBe(false);
    expect(isRetireEligible({ ...base, lastHttpStatus: 403 })).toBe(false);
  });
});

describe("buildFormerWebsitesAfterRetire", () => {
  it("appends the retired URL to the archive", () => {
    expect(
      buildFormerWebsitesAfterRetire({
        retiredUrl: "https://smithformayor.com",
        storedFormerWebsites: ["https://smith2022.com"],
      })
    ).toEqual(["https://smith2022.com", "https://smithformayor.com"]);
  });

  it("dedupes on the normalized URL", () => {
    expect(
      buildFormerWebsitesAfterRetire({
        retiredUrl: "https://smithformayor.com/",
        storedFormerWebsites: ["https://smithformayor.com"],
      })
    ).toEqual(["https://smithformayor.com"]);
  });

  it("drops blank entries", () => {
    expect(
      buildFormerWebsitesAfterRetire({
        retiredUrl: "https://smithformayor.com",
        storedFormerWebsites: ["  ", "https://smith2022.com"],
      })
    ).toEqual(["https://smith2022.com", "https://smithformayor.com"]);
  });
});
