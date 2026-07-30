import { beforeEach, describe, expect, it } from "vitest";

import {
  buildDomainFirstSeenMap,
  buildSourceTierSweep,
  CROSS_CANDIDATE_DOMAIN_MIN_CANDIDATES,
  listCrossCandidateDomainBursts,
  listNewlySeenDomainConcentrations,
  listPreElectionDamagingBursts,
  NEWLY_SEEN_DOMAIN_MIN_RECORDS,
  PRE_ELECTION_DAMAGING_MIN_RECORDS,
  type SourceAuditRecordRow,
} from "../../src/scripts/candidateRecordSourceAudit.js";

const NOW = new Date("2026-07-24T12:00:00.000Z");

let recordCounter = 0;

beforeEach(() => {
  recordCounter = 0;
});

function makeRecord(overrides: Partial<SourceAuditRecordRow>): SourceAuditRecordRow {
  recordCounter += 1;
  return {
    record_id: `record-${recordCounter}`,
    candidate_id: "cand-1",
    display_name: "Jane Candidate",
    description: "Voted for the state budget in 2026.",
    source_url: "https://smalltownweekly.com/news/1",
    created_at: "2026-07-20T00:00:00.000Z",
    origin: "ai_enricher",
    origin_run_id: "run-1",
    ...overrides,
  };
}

describe("listCrossCandidateDomainBursts", () => {
  it("flags an unlisted domain feeding several candidates inside the window", () => {
    const records = ["cand-1", "cand-2", "cand-3"].map((candidateId) =>
      makeRecord({
        candidate_id: candidateId,
        source_url: "https://patriot-watchdog-report.com/story",
        created_at: "2026-07-10T00:00:00.000Z",
      })
    );
    const bursts = listCrossCandidateDomainBursts(records, NOW);
    expect(bursts).toHaveLength(1);
    expect(bursts[0]).toMatchObject({
      domain: "patriot-watchdog-report.com",
      candidateCount: CROSS_CANDIDATE_DOMAIN_MIN_CANDIDATES,
      recordCount: 3,
    });
  });

  it("groups www and apex forms of the same domain together", () => {
    const records = [
      makeRecord({ candidate_id: "cand-1", source_url: "https://www.newsite.com/a" }),
      makeRecord({ candidate_id: "cand-2", source_url: "https://newsite.com/b" }),
      makeRecord({ candidate_id: "cand-3", source_url: "https://newsite.com/c" }),
    ];
    const bursts = listCrossCandidateDomainBursts(records, NOW);
    expect(bursts).toHaveLength(1);
    expect(bursts[0]?.domain).toBe("newsite.com");
  });

  it("exempts listed domains no matter how many candidates they feed", () => {
    const records = ["cand-1", "cand-2", "cand-3", "cand-4"].map((candidateId) =>
      makeRecord({
        candidate_id: candidateId,
        source_url: "https://apnews.com/article/x",
        created_at: "2026-07-10T00:00:00.000Z",
      })
    );
    expect(listCrossCandidateDomainBursts(records, NOW)).toEqual([]);
  });

  it("ignores records created before the recent window and repeat records from one candidate", () => {
    const records = [
      // Outside the 30-day window.
      makeRecord({ candidate_id: "cand-1", created_at: "2026-05-01T00:00:00.000Z" }),
      // Same candidate cited twice = one distinct candidate.
      makeRecord({ candidate_id: "cand-2" }),
      makeRecord({ candidate_id: "cand-2" }),
      makeRecord({ candidate_id: "cand-3" }),
    ];
    expect(listCrossCandidateDomainBursts(records, NOW)).toEqual([]);
  });
});

describe("listPreElectionDamagingBursts", () => {
  const election = {
    candidate_id: "cand-1",
    election_id: "election-1",
    election_date: "2026-08-04",
    official_ballot_title: "Governor",
  };

  it("flags a candidate gaining damaging records inside the pre-election window", () => {
    const records = [
      makeRecord({
        description: "Was indicted on federal bribery charges.",
        created_at: "2026-07-20T00:00:00.000Z",
      }),
      makeRecord({
        description: "Accused of misusing campaign funds.",
        created_at: "2026-07-22T00:00:00.000Z",
      }),
    ];
    const bursts = listPreElectionDamagingBursts(records, [election]);
    expect(bursts).toHaveLength(1);
    expect(bursts[0]).toMatchObject({
      candidateId: "cand-1",
      electionId: "election-1",
      damagingRecordCount: PRE_ELECTION_DAMAGING_MIN_RECORDS,
    });
  });

  it("does not flag benign records or damaging records created outside the window", () => {
    const records = [
      // Benign, in window.
      makeRecord({ created_at: "2026-07-20T00:00:00.000Z" }),
      makeRecord({ created_at: "2026-07-21T00:00:00.000Z" }),
      // Damaging, but imported long before the window (historical research).
      makeRecord({
        description: "Was indicted on federal bribery charges.",
        created_at: "2026-01-10T00:00:00.000Z",
      }),
      // Damaging, in window, but only one such record.
      makeRecord({
        description: "Accused of misusing campaign funds.",
        created_at: "2026-07-22T00:00:00.000Z",
      }),
    ];
    expect(listPreElectionDamagingBursts(records, [election])).toEqual([]);
  });

  it("does not count records imported after the election date", () => {
    const records = [
      makeRecord({
        description: "Was indicted on federal bribery charges.",
        created_at: "2026-08-10T00:00:00.000Z",
      }),
      makeRecord({
        description: "Accused of misusing campaign funds.",
        created_at: "2026-08-11T00:00:00.000Z",
      }),
    ];
    expect(listPreElectionDamagingBursts(records, [election])).toEqual([]);
  });

  it("keeps only the earliest election when overlapping windows capture the identical record set", () => {
    const records = [
      makeRecord({
        description: "Was indicted on federal bribery charges.",
        created_at: "2026-07-20T00:00:00.000Z",
      }),
      makeRecord({
        description: "Accused of misusing campaign funds.",
        created_at: "2026-07-22T00:00:00.000Z",
      }),
    ];
    // Aug 4 window: Jul 5 - Aug 4. Aug 18 window: Jul 19 - Aug 18. Both
    // capture exactly the same two records.
    const laterElection = {
      candidate_id: "cand-1",
      election_id: "election-2",
      election_date: "2026-08-18",
      official_ballot_title: "Governor runoff",
    };
    const bursts = listPreElectionDamagingBursts(records, [laterElection, election]);
    expect(bursts).toHaveLength(1);
    expect(bursts[0]).toMatchObject({ electionId: "election-1", electionDate: "2026-08-04" });
  });

  it("emits separate bursts when overlapping windows capture different record sets", () => {
    const records = [
      // Only inside the Aug 4 window (Jul 5 - Aug 4).
      makeRecord({
        description: "Was indicted on federal bribery charges.",
        created_at: "2026-07-10T00:00:00.000Z",
      }),
      makeRecord({
        description: "Accused of misusing campaign funds.",
        created_at: "2026-07-12T00:00:00.000Z",
      }),
      // Inside both windows.
      makeRecord({
        description: "Was censured by the state senate over misuse of funds.",
        created_at: "2026-07-25T00:00:00.000Z",
      }),
      // Only inside the Aug 18 window (Jul 19 - Aug 18).
      makeRecord({
        description: "Pleaded guilty to campaign-finance violations.",
        created_at: "2026-08-10T00:00:00.000Z",
      }),
    ];
    const laterElection = {
      candidate_id: "cand-1",
      election_id: "election-2",
      election_date: "2026-08-18",
      official_ballot_title: "Governor runoff",
    };
    const bursts = listPreElectionDamagingBursts(records, [election, laterElection]);
    expect(bursts).toHaveLength(2);
    expect(bursts.map((burst) => burst.electionId).sort()).toEqual(["election-1", "election-2"]);
  });
});

describe("listNewlySeenDomainConcentrations", () => {
  it("flags a domain first seen recently that already has several records", () => {
    const records = [
      makeRecord({ source_url: "https://fresh-local-news.com/a", created_at: "2026-07-18T00:00:00.000Z" }),
      makeRecord({
        candidate_id: "cand-2",
        source_url: "https://fresh-local-news.com/b",
        created_at: "2026-07-19T00:00:00.000Z",
      }),
      makeRecord({
        candidate_id: "cand-3",
        source_url: "https://fresh-local-news.com/c",
        created_at: "2026-07-20T00:00:00.000Z",
      }),
    ];
    const concentrations = listNewlySeenDomainConcentrations(
      records,
      buildDomainFirstSeenMap(records),
      NOW
    );
    expect(concentrations).toHaveLength(1);
    expect(concentrations[0]).toMatchObject({
      domain: "fresh-local-news.com",
      tier: "unlisted",
      firstSeenAt: "2026-07-18T00:00:00.000Z",
      recordCount: NEWLY_SEEN_DOMAIN_MIN_RECORDS,
      candidateCount: 3,
    });
  });

  it("does not flag a domain whose first record predates the window", () => {
    const records = [
      makeRecord({ source_url: "https://old-outlet.com/a", created_at: "2025-01-01T00:00:00.000Z" }),
      makeRecord({ source_url: "https://old-outlet.com/b", created_at: "2026-07-19T00:00:00.000Z" }),
      makeRecord({ source_url: "https://old-outlet.com/c", created_at: "2026-07-20T00:00:00.000Z" }),
    ];
    expect(
      listNewlySeenDomainConcentrations(records, buildDomainFirstSeenMap(records), NOW)
    ).toEqual([]);
  });

  it("uses corpus-wide first-seen, not the scoped slice, in filtered audit runs", () => {
    // The scoped slice (e.g. --district-id) only contains recent records for
    // the domain, but the corpus has an older record from another district:
    // the domain is NOT newly seen and must not flag.
    const scopedRecords = [
      makeRecord({ source_url: "https://old-outlet.com/a", created_at: "2026-07-18T00:00:00.000Z" }),
      makeRecord({ source_url: "https://old-outlet.com/b", created_at: "2026-07-19T00:00:00.000Z" }),
      makeRecord({ source_url: "https://old-outlet.com/c", created_at: "2026-07-20T00:00:00.000Z" }),
    ];
    const corpus = [
      ...scopedRecords,
      { source_url: "https://www.old-outlet.com/elsewhere", created_at: "2024-03-01T00:00:00.000Z" },
    ];
    expect(
      listNewlySeenDomainConcentrations(scopedRecords, buildDomainFirstSeenMap(corpus), NOW)
    ).toEqual([]);
    // Sanity: the same scoped slice DOES flag when the corpus confirms the
    // domain is genuinely new.
    expect(
      listNewlySeenDomainConcentrations(scopedRecords, buildDomainFirstSeenMap(scopedRecords), NOW)
    ).toHaveLength(1);
  });

  it("exempts listed domains (a newly allowlisted outlet is not suspicious)", () => {
    const records = [
      makeRecord({ source_url: "https://apnews.com/a", created_at: "2026-07-18T00:00:00.000Z" }),
      makeRecord({ source_url: "https://apnews.com/b", created_at: "2026-07-19T00:00:00.000Z" }),
      makeRecord({ source_url: "https://apnews.com/c", created_at: "2026-07-20T00:00:00.000Z" }),
    ];
    expect(
      listNewlySeenDomainConcentrations(records, buildDomainFirstSeenMap(records), NOW)
    ).toEqual([]);
  });
});

describe("buildSourceTierSweep", () => {
  it("counts tiers, groups unlisted domains, and lists blocked-domain records", () => {
    const records = [
      makeRecord({ source_url: "https://apnews.com/article/x" }),
      makeRecord({ source_url: "https://sos.ca.gov/elections" }),
      makeRecord({ source_url: "https://smalltownweekly.com/news/1" }),
      makeRecord({
        candidate_id: "cand-2",
        source_url: "https://smalltownweekly.com/news/2",
        description: "Was indicted on federal bribery charges.",
      }),
      makeRecord({ source_url: "https://www.reddit.com/r/politics/x" }),
    ];
    const sweep = buildSourceTierSweep(records);
    expect(sweep.tierCounts).toEqual({ listed: 2, unlisted: 2, blocked: 1 });
    expect(sweep.unlistedDomains).toHaveLength(1);
    expect(sweep.unlistedDomains[0]).toMatchObject({
      domain: "smalltownweekly.com",
      recordCount: 2,
      candidateCount: 2,
      damagingRecordCount: 1,
    });
    expect(sweep.blockedDomainRecords).toHaveLength(1);
    expect(sweep.blockedDomainRecords[0]?.sourceUrl).toBe("https://www.reddit.com/r/politics/x");
  });

  it("separates blocked rows by class and carries the matching repair", () => {
    const records = [
      makeRecord({ source_url: "https://www.reddit.com/r/politics/x" }),
      makeRecord({ source_url: "https://www.civoren.com/candidate/some-person" }),
      makeRecord({
        source_url:
          "https://validate.perfdrive.com/?ssa=abc&ssc=https%3A%2F%2Fwww.sos.mn.gov%2Fnews%2Fx",
      }),
    ];
    const sweep = buildSourceTierSweep(records);

    expect(sweep.tierCounts.blocked).toBe(3);
    expect(sweep.blockedKindCounts).toEqual({
      ugc_social: 1,
      generated_candidate_directory: 1,
      bot_check_interstitial: 1,
    });

    const byKind = new Map(sweep.blockedDomainRecords.map((record) => [record.blockedKind, record]));
    // The interstitial repair must point at the embedded URL, NOT tell the
    // operator the source is untrustworthy — the underlying page is a .gov
    // press release and discarding it would lose a good citation.
    expect(byKind.get("bot_check_interstitial")?.repair).toContain("ssc=");
    expect(byKind.get("generated_candidate_directory")?.repair).toContain("lead only");
    expect(byKind.get("ugc_social")?.repair).toContain("secondary coverage");
    expect(byKind.get("bot_check_interstitial")?.repair).not.toContain("secondary coverage");
  });

  it("sorts unlisted domains by record count descending", () => {
    const records = [
      makeRecord({ source_url: "https://one-record.com/a" }),
      makeRecord({ source_url: "https://two-records.com/a" }),
      makeRecord({ source_url: "https://two-records.com/b" }),
    ];
    const sweep = buildSourceTierSweep(records);
    expect(sweep.unlistedDomains.map((domain) => domain.domain)).toEqual([
      "two-records.com",
      "one-record.com",
    ]);
  });
});
