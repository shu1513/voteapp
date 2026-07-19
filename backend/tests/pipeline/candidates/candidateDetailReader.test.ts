import { describe, expect, it, vi } from "vitest";

import {
  CandidateDetailReaderError,
  lookupCandidateDetailById,
} from "../../../src/pipeline/candidates/candidateDetailReader.js";

const candidateId = "22222222-2222-4222-8222-222222222222";
const userId = "11111111-1111-4111-8111-111111111111";

function createMockQueryable() {
  return {
    query: vi.fn(),
  };
}

function expectCandidateDetailError(error: unknown, code: string): void {
  expect(error).toBeInstanceOf(CandidateDetailReaderError);
  expect((error as CandidateDetailReaderError).code).toBe(code);
}

describe("lookupCandidateDetailById", () => {
  it("rejects invalid candidate IDs before querying", async () => {
    const db = createMockQueryable();

    await expect(lookupCandidateDetailById(db, { candidateId: "not-a-uuid" })).rejects.toSatisfy((error) => {
      expectCandidateDetailError(error, "invalid_candidate_id");
      return true;
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid user IDs before querying", async () => {
    const db = createMockQueryable();

    await expect(lookupCandidateDetailById(db, { candidateId, userId: "not-a-uuid" })).rejects.toSatisfy((error) => {
      expectCandidateDetailError(error, "invalid_user_id");
      return true;
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("returns null for missing, deleted, or merged candidates", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({ rows: [] });

    await expect(lookupCandidateDetailById(db, { candidateId })).resolves.toBeNull();

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(db.query.mock.calls[0]?.[1]).toEqual([candidateId]);
    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("candidate.deleted_at IS NULL");
    expect(sql).toContain("candidate.merged_into_candidate_id IS NULL");
  });

  it("returns candidate basics without follow state for anonymous lookups", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({
      rows: [
        {
          candidate_id: candidateId,
          display_name: "Jane Smith",
          first_name: "Jane",
          last_name: "Smith",
          date_of_birth: "1980-01-02",
          party: "Democratic",
          state: "CA",
          current_office: "Mayor",
          summary: "Incumbent mayor.",
          twitter_handle: "janesmith",
          linkedin_url: "https://www.linkedin.com/in/janesmith",
          official_website_url: "https://janesmith.example",
          fec_ids: ["H4CA00001"],
          state_filing_ids: ["CA-123"],
          profile_sources: ["https://janesmith.example/about", "https://city.example/mayor"],
          last_researched: "2026-06-30 12:00:00+00",
          records_researched_through: "2026-06-30",
        },
      ],
    });

    db.query.mockResolvedValueOnce({ rows: [] });
    db.query.mockResolvedValueOnce({ rows: [] });

    await expect(lookupCandidateDetailById(db, { candidateId })).resolves.toEqual({
      candidate: {
        candidate_id: candidateId,
        display_name: "Jane Smith",
        first_name: "Jane",
        last_name: "Smith",
        date_of_birth: "1980-01-02",
        party: "Democratic",
        state: "CA",
        current_office: "Mayor",
        summary: "Incumbent mayor.",
        twitter_handle: "janesmith",
        linkedin_url: "https://www.linkedin.com/in/janesmith",
        official_website_url: "https://janesmith.example",
        fec_ids: ["H4CA00001"],
        state_filing_ids: ["CA-123"],
        profile_sources: ["https://janesmith.example/about", "https://city.example/mayor"],
        last_researched: "2026-06-30 12:00:00+00",
        records_researched_through: "2026-06-30",
        records: [],
        elections: [],
        is_following: false,
        follow: null,
      },
    });

    expect(db.query).toHaveBeenCalledTimes(3);
    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("COALESCE(");
    expect(sql).toContain("NULLIF(trim(candidate.display_name), '')");
    expect(sql).toContain("candidate.profile_sources");
  });

  it("returns candidate records with research-area tags in display order", async () => {
    const db = createMockQueryable();
    const recordIdA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const recordIdB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    db.query.mockResolvedValueOnce({
      rows: [
        {
          candidate_id: candidateId,
          display_name: "Jane Smith",
          first_name: "Jane",
          last_name: "Smith",
          party: "Democratic",
          state: "CA",
          current_office: "Mayor",
          summary: null,
          fec_ids: [],
          state_filing_ids: [],
        },
      ],
    });
    db.query.mockResolvedValueOnce({
      rows: [
        {
          candidate_record_id: recordIdA,
          description: "Sponsored a housing affordability bill.",
          source_url: "https://example.test/record-a",
          event_date: "2026-01-15",
          created_at: "2026-01-16T00:00:00.000Z",
        },
        {
          candidate_record_id: recordIdB,
          description: "Voted against a tax increase.",
          source_url: "https://example.test/record-b",
          event_date: "2025-12-01",
          created_at: "2025-12-02T00:00:00.000Z",
        },
      ],
    });
    db.query.mockResolvedValueOnce({
      rows: [
        {
          candidate_record_id: recordIdA,
          research_area_id: "99999999-9999-4999-8999-999999999999",
          slug: "housing_affordability",
          name: "Housing Affordability",
          stance: "for",
        },
        {
          candidate_record_id: recordIdA,
          research_area_id: "88888888-8888-4888-8888-888888888888",
          slug: "taxes",
          name: "Taxes",
          stance: "neutral",
        },
      ],
    });

    db.query.mockResolvedValueOnce({ rows: [] });

    await expect(lookupCandidateDetailById(db, { candidateId })).resolves.toMatchObject({
      candidate: {
        records: [
          {
            id: recordIdA,
            description: "Sponsored a housing affordability bill.",
            source_url: "https://example.test/record-a",
            event_date: "2026-01-15",
            created_at: "2026-01-16T00:00:00.000Z",
            research_area_tags: [
              {
                research_area_id: "99999999-9999-4999-8999-999999999999",
                slug: "housing_affordability",
                name: "Housing Affordability",
                stance: "for",
              },
              {
                research_area_id: "88888888-8888-4888-8888-888888888888",
                slug: "taxes",
                name: "Taxes",
                stance: null,
              },
            ],
          },
          {
            id: recordIdB,
            description: "Voted against a tax increase.",
            source_url: "https://example.test/record-b",
            event_date: "2025-12-01",
            created_at: "2025-12-02T00:00:00.000Z",
            research_area_tags: [],
          },
        ],
      },
    });

    expect(db.query).toHaveBeenCalledTimes(4);
    expect(db.query.mock.calls[1]?.[1]).toEqual([candidateId]);
    expect(String(db.query.mock.calls[1]?.[0])).toContain("ORDER BY record.event_date DESC");
    expect(db.query.mock.calls[2]?.[1]).toEqual([[recordIdA, recordIdB]]);
    expect(String(db.query.mock.calls[2]?.[0])).toContain("JOIN public.research_areas AS research_area");
  });

  it("returns compact candidate election links with upcoming elections ordered before past elections", async () => {
    const db = createMockQueryable();
    const candidateElectionId = "44444444-4444-4444-8444-444444444444";
    const pastCandidateElectionId = "77777777-7777-4777-8777-777777777777";
    const electionId = "55555555-5555-4555-8555-555555555555";
    const pastElectionId = "99999999-9999-4999-8999-999999999999";
    const districtId = "66666666-6666-4666-8666-666666666666";
    db.query.mockResolvedValueOnce({
      rows: [
        {
          candidate_id: candidateId,
          display_name: "Jane Smith",
          first_name: "Jane",
          last_name: "Smith",
          party: "Democratic",
          state: "CA",
          current_office: "Mayor",
          summary: null,
          fec_ids: [],
          state_filing_ids: [],
        },
      ],
    });
    db.query.mockResolvedValueOnce({ rows: [] });
    db.query.mockResolvedValueOnce({
      rows: [
        {
          candidate_election_id: candidateElectionId,
          election_id: electionId,
          district_id: districtId,
          district_type: "place",
          district_name: "Example City",
          district_state: "CA",
          race_type: "office",
          official_ballot_title: "Mayor",
          election_date: "2026-11-03",
          election_stage: "general",
          is_partisan: false,
          is_incumbent: true,
          status: "declared",
          office_scope: "place",
          office_canonical_name: "Mayor",
        },
        {
          candidate_election_id: pastCandidateElectionId,
          election_id: pastElectionId,
          district_id: districtId,
          district_type: "place",
          district_name: "Example City",
          district_state: "CA",
          race_type: "office",
          official_ballot_title: "Mayor",
          election_date: "2024-11-05",
          election_stage: "general",
          is_partisan: false,
          is_incumbent: false,
          status: "lost",
          office_scope: "place",
          office_canonical_name: "Mayor",
        },
      ],
    });

    const result = await lookupCandidateDetailById(db, { candidateId });

    expect(result).toMatchObject({
      candidate: {
        elections: [
          {
            candidate_election_id: candidateElectionId,
            election_id: electionId,
            district: {
              id: districtId,
              name: "Example City",
              district_type: "place",
              state: "CA",
            },
            race_type: "office",
            official_ballot_title: "Mayor",
            election_date: "2026-11-03",
            election_stage: "general",
            is_partisan: false,
            is_incumbent: true,
            status: "declared",
            office_scope: "place",
            office_canonical_name: "Mayor",
          },
          {
            candidate_election_id: pastCandidateElectionId,
            election_id: pastElectionId,
            district: {
              id: districtId,
              name: "Example City",
              district_type: "place",
              state: "CA",
            },
            race_type: "office",
            official_ballot_title: "Mayor",
            election_date: "2024-11-05",
            election_stage: "general",
            is_partisan: false,
            is_incumbent: false,
            status: "lost",
            office_scope: "place",
            office_canonical_name: "Mayor",
          },
        ],
      },
    });

    expect(result?.candidate.elections[0]).not.toHaveProperty("finance_summary");
    expect(db.query).toHaveBeenCalledTimes(3);
    expect(db.query.mock.calls[2]?.[1]).toEqual([candidateId]);
    const sql = String(db.query.mock.calls[2]?.[0]);
    const normalizedSql = sql.replace(/\s+/g, " ");
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    // Running mates must resolve their ticket's election too, but without
    // inheriting the lead row's incumbency, and without duplicating an
    // election where the candidate also holds their own row.
    expect(normalizedSql).toContain(
      "WHERE (candidate_election.candidate_id = $1::uuid"
    );
    expect(normalizedSql).toContain(
      "OR (candidate_election.running_mate_candidate_id = $1::uuid AND NOT EXISTS"
    );
    expect(normalizedSql).toContain(
      "WHERE own_link.candidate_id = $1::uuid AND own_link.election_id = candidate_election.election_id"
    );
    expect(normalizedSql).toContain(
      "CASE WHEN candidate_election.candidate_id = $1::uuid THEN candidate_election.is_incumbent ELSE FALSE END AS is_incumbent"
    );
    expect(sql).toContain("LEFT JOIN public.offices AS office");
    expect(sql).toContain("CASE WHEN election.election_date >= (now() AT TIME ZONE 'Pacific/Honolulu')::date THEN 0 ELSE 1 END ASC");
    expect(sql).toContain("CASE WHEN election.election_date >= (now() AT TIME ZONE 'Pacific/Honolulu')::date THEN election.election_date END ASC");
    expect(sql).toContain("CASE WHEN election.election_date < (now() AT TIME ZONE 'Pacific/Honolulu')::date THEN election.election_date END DESC");
  });

  it("returns authenticated follow state when the user follows the candidate", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({
      rows: [
        {
          candidate_id: candidateId,
          display_name: "Jane Smith",
          first_name: "Jane",
          last_name: "Smith",
          party: "Democratic",
          state: "CA",
          current_office: null,
          summary: null,
          fec_ids: [],
          state_filing_ids: [],
        },
      ],
    });
    db.query.mockResolvedValueOnce({ rows: [] });
    db.query.mockResolvedValueOnce({ rows: [] });
    db.query.mockResolvedValueOnce({
      rows: [
        {
          notify_elections: true,
          notify_updates: false,
          created_at: new Date("2026-01-02T03:04:05.000Z"),
        },
      ],
    });

    await expect(lookupCandidateDetailById(db, { candidateId, userId })).resolves.toMatchObject({
      candidate: {
        candidate_id: candidateId,
        is_following: true,
        follow: {
          notify_elections: true,
          notify_updates: false,
          created_at: "2026-01-02T03:04:05.000Z",
        },
      },
    });

    expect(db.query).toHaveBeenCalledTimes(4);
    expect(db.query.mock.calls[3]?.[1]).toEqual([userId, candidateId]);
    expect(String(db.query.mock.calls[3]?.[0])).toContain("user_row.deleted_at IS NULL");
  });

  it("returns not-following state when the authenticated user has no follow row", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({
      rows: [
        {
          candidate_id: candidateId,
          display_name: "Jane Smith",
          first_name: null,
          last_name: null,
          date_of_birth: null,
          party: null,
          state: null,
          current_office: null,
          summary: null,
          twitter_handle: null,
          linkedin_url: null,
          official_website_url: null,
          fec_ids: null,
          state_filing_ids: null,
          profile_sources: null,
          last_researched: null,
          records_researched_through: null,
        },
      ],
    });
    db.query.mockResolvedValueOnce({ rows: [] });
    db.query.mockResolvedValueOnce({ rows: [] });
    db.query.mockResolvedValueOnce({ rows: [] });

    await expect(lookupCandidateDetailById(db, { candidateId, userId })).resolves.toEqual({
      candidate: {
        candidate_id: candidateId,
        display_name: "Jane Smith",
        first_name: null,
        last_name: null,
        date_of_birth: null,
        party: "",
        state: "",
        current_office: null,
        summary: null,
        twitter_handle: null,
        linkedin_url: null,
        official_website_url: null,
        fec_ids: [],
        state_filing_ids: [],
        profile_sources: [],
        last_researched: null,
        records_researched_through: null,
        records: [],
        elections: [],
        is_following: false,
        follow: null,
      },
    });
  });
});
