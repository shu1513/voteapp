import { describe, expect, it } from "vitest";
import { aggregateSanFranciscoHeadlineTotals } from "../../src/pipeline/sanFranciscoFinance/sanFranciscoHeadlineTotals.js";
import type {
  SanFranciscoContestManifest,
  SanFranciscoManifestCandidate,
  SanFranciscoManifestOutsideRelation,
} from "../../src/pipeline/sanFranciscoFinance/sanFranciscoDashboardManifestClient.js";

const wong: SanFranciscoManifestCandidate = {
  filerNid: "n-wong",
  fppcId: "1481230",
  committeeName: "Alan Wong for Supervisor 2026",
  candidateName: "ALAN WONG",
  fundsCents: 41_237_100,
  expensesCents: 41_072_779,
};

const lee: SanFranciscoManifestCandidate = {
  filerNid: "n-lee",
  fppcId: "1482000",
  committeeName: "David Lee for Supervisor 2026",
  candidateName: "DAVID LEE",
  fundsCents: 10_000_000,
  expensesCents: 9_000_000,
};

function manifest(
  outsideRelations: SanFranciscoManifestOutsideRelation[],
): SanFranciscoContestManifest {
  return {
    electionDate: "2026-06-02",
    contestCode: "bos04",
    title: "Member, Board of Supervisors, District 4",
    candidates: [wong, lee],
    outsideRelations,
    sourceUrl: "https://example.test/bos04.md",
    schemaFingerprint: "top:|candidate:|ie_candidate:|ie_committee:",
  };
}

describe("aggregateSanFranciscoHeadlineTotals", () => {
  it("passes the manifest funds/expenses through and sums relations by direction", () => {
    const result = aggregateSanFranciscoHeadlineTotals({
      manifest: manifest([
        {
          candidateName: "ALAN WONG",
          candidateFppcId: "1481230",
          position: "support",
          spenderFppcId: "1490001",
          spenderName: "GROWSF",
          amountCents: 70_000_000,
        },
        {
          candidateName: "ALAN WONG",
          candidateFppcId: "1481230",
          position: "oppose",
          spenderFppcId: "1490002",
          spenderName: "NEIGHBORS PAC",
          amountCents: 2_475_392,
        },
        // Same spender opposing the OTHER candidate — must not leak in.
        {
          candidateName: "DAVID LEE",
          candidateFppcId: "1482000",
          position: "oppose",
          spenderFppcId: "1490001",
          spenderName: "GROWSF",
          amountCents: 1_000_000,
        },
      ]),
      candidate: wong,
    });
    expect(result.totalRaisedCents).toBe(41_237_100);
    expect(result.totalSpentCents).toBe(41_072_779);
    expect(result.outsideSupportCents).toBe(70_000_000);
    expect(result.outsideOpposeCents).toBe(2_475_392);
    expect(result.groups).toEqual([
      {
        spenderId: "1490001",
        spenderName: "GROWSF",
        supportOppose: "support",
        amountCents: 70_000_000,
        sourceUrl: "https://example.test/bos04.md",
      },
      {
        spenderId: "1490002",
        spenderName: "NEIGHBORS PAC",
        supportOppose: "oppose",
        amountCents: 2_475_392,
        sourceUrl: "https://example.test/bos04.md",
      },
    ]);
  });

  it("targets id-less relations by name and derives synthetic spender ids", () => {
    const result = aggregateSanFranciscoHeadlineTotals({
      manifest: manifest([
        {
          candidateName: "Alan Wong",
          candidateFppcId: null,
          position: "support",
          spenderFppcId: null,
          spenderName: "Friends of the Sunset",
          amountCents: 5_000_000,
        },
      ]),
      candidate: wong,
    });
    expect(result.groups).toEqual([
      {
        spenderId: "name:FRIENDS OF THE SUNSET",
        spenderName: "Friends of the Sunset",
        supportOppose: "support",
        amountCents: 5_000_000,
        sourceUrl: "https://example.test/bos04.md",
      },
    ]);
  });

  it("a relation id naming another committee is decisive over an equal name", () => {
    const result = aggregateSanFranciscoHeadlineTotals({
      manifest: manifest([
        {
          candidateName: "ALAN WONG",
          candidateFppcId: "1482000", // David Lee's committee id
          position: "support",
          spenderFppcId: "1490001",
          spenderName: "GROWSF",
          amountCents: 5_000_000,
        },
      ]),
      candidate: wong,
    });
    expect(result.groups).toEqual([]);
    expect(result.outsideSupportCents).toBe(0);
  });

  it("sums duplicate rows for the same spender and direction", () => {
    const result = aggregateSanFranciscoHeadlineTotals({
      manifest: manifest([
        {
          candidateName: "ALAN WONG",
          candidateFppcId: "1481230",
          position: "support",
          spenderFppcId: "1490001",
          spenderName: "GROWSF",
          amountCents: 1_000_000,
        },
        {
          candidateName: "ALAN WONG",
          candidateFppcId: "1481230",
          position: "support",
          spenderFppcId: "1490001",
          spenderName: "GROWSF",
          amountCents: 2_000_000,
        },
      ]),
      candidate: wong,
    });
    expect(result.groups.length).toBe(1);
    expect(result.groups[0]!.amountCents).toBe(3_000_000);
  });
});
