import { describe, expect, it, vi } from "vitest";

import { OfficeMatcher } from "../../src/pipeline/elections/officeMatcher.js";
import { normalizeElectionTitleKey } from "../../src/utils/normalizeElectionTitleKey.js";

type QueryResultRow = Record<string, unknown>;

function createMatcherDataClient(input: {
  aliasesByScope: Record<string, Array<{ office_id: string; normalized_alias: string }>>;
  officesByScope: Record<string, Array<{ id: string; canonical_name: string }>>;
}) {
  const query = vi.fn(async (sql: string, params?: unknown[]) => {
    const scope = String(params?.[0] ?? "");
    if (sql.includes("FROM public.office_title_aliases")) {
      return { rows: (input.aliasesByScope[scope] ?? []) as QueryResultRow[] };
    }
    if (sql.includes("FROM public.offices")) {
      return { rows: (input.officesByScope[scope] ?? []) as QueryResultRow[] };
    }
    throw new Error(`unexpected query in officeMatcher test: ${sql.slice(0, 120)}`);
  });
  return { query };
}

describe("OfficeMatcher", () => {
  it("returns exact alias match when normalized alias exists", async () => {
    const title = "California State Governor";
    const client = createMatcherDataClient({
      aliasesByScope: {
        statewide: [
          { office_id: "office-governor", normalized_alias: normalizeElectionTitleKey(title) },
        ],
      },
      officesByScope: {
        statewide: [{ id: "office-governor", canonical_name: "Governor" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "statewide",
      districtName: "California",
      state: "CA",
      officialBallotTitle: title,
    });

    expect(result.officeId).toBe("office-governor");
    expect(result.method).toBe("alias_exact");
    expect(result.shouldPersistAlias).toBe(false);
  });

  it("uses deterministic fallback for lieutenant governor vs governor", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { statewide: [] },
      officesByScope: {
        statewide: [
          { id: "office-governor", canonical_name: "Governor" },
          { id: "office-lt-governor", canonical_name: "Lieutenant Governor" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "statewide",
      districtName: "California",
      state: "CA",
      officialBallotTitle: "California State Lieutenant Governor",
    });

    expect(result.officeId).toBe("office-lt-governor");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("respects scope and avoids cross-scope office matching", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: {
        county: [],
        statewide: [],
      },
      officesByScope: {
        county: [{ id: "office-board-supervisors", canonical_name: "Board of Supervisors" }],
        statewide: [{ id: "office-governor", canonical_name: "Governor" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);

    const statewideResult = await matcher.resolve({
      scope: "statewide",
      districtName: "California",
      state: "CA",
      officialBallotTitle: "Board of Supervisors, District 1",
    });
    expect(statewideResult.officeId).toBeNull();
    expect(statewideResult.method).toBe("none");

    const countyResult = await matcher.resolve({
      scope: "county",
      districtName: "Los Angeles County",
      state: "CA",
      officialBallotTitle: "Los Angeles County Board of Supervisors, District 1",
    });
    expect(countyResult.officeId).toBe("office-board-supervisors");
    expect(countyResult.method).toBe("deterministic_fallback");
  });

  it("ignores election-stage words when matching office titles", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { statewide: [] },
      officesByScope: {
        statewide: [{ id: "office-governor", canonical_name: "Governor" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "statewide",
      districtName: "California",
      state: "CA",
      officialBallotTitle: "Primary Election for California Governor",
    });

    expect(result.officeId).toBe("office-governor");
    expect(result.method).toBe("deterministic_fallback");
  });

  it("returns ambiguous when top office scores are too close", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { place: [] },
      officesByScope: {
        place: [
          { id: "office-city-clerk", canonical_name: "City Clerk" },
          { id: "office-town-clerk", canonical_name: "Town Clerk" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "place",
      districtName: "Springfield",
      state: "IL",
      officialBallotTitle: "Clerk",
    });

    expect(result.officeId).toBeNull();
    expect(result.method).toBe("ambiguous");
    expect(result.shouldPersistAlias).toBe(false);
  });

  it("normalizes U.S. marker and punctuation for senate title matching", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { statewide: [] },
      officesByScope: {
        statewide: [{ id: "office-us-senator", canonical_name: "United States Senator" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "statewide",
      districtName: "California",
      state: "CA",
      officialBallotTitle: "U.S. Senator (Unexpired Term)",
    });

    expect(result.officeId).toBe("office-us-senator");
    expect(result.method).toBe("deterministic_fallback");
  });

  it("uses stripped matcher key for alias-memory persistence", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { us_house: [] },
      officesByScope: {
        us_house: [{ id: "office-us-house", canonical_name: "United States Representative" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "us_house",
      districtName: "Congressional District 31 (119th Congress), California",
      state: "CA",
      officialBallotTitle: "United States Representative, 31st District",
    });

    expect(result.officeId).toBe("office-us-house");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.normalizedAlias).toBe("united states representative 31st district");
    expect(result.aliasMemoryKey).toBe("united states representative");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("reads learned alias memory from stripped key on subsequent resolve", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { us_house: [] },
      officesByScope: {
        us_house: [{ id: "office-us-house", canonical_name: "United States Representative" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);

    const first = await matcher.resolve({
      scope: "us_house",
      districtName: "Congressional District 31 (119th Congress), California",
      state: "CA",
      officialBallotTitle: "United States Representative, 31st District",
    });
    expect(first.method).toBe("deterministic_fallback");
    expect(first.officeId).toBe("office-us-house");
    expect(first.aliasMemoryKey).toBe("united states representative");

    matcher.rememberAlias("us_house", first.aliasMemoryKey, "office-us-house");

    const second = await matcher.resolve({
      scope: "us_house",
      districtName: "Congressional District 31 (119th Congress), California",
      state: "CA",
      officialBallotTitle: "United States Representative, 31st District",
    });
    expect(second.method).toBe("alias_exact");
    expect(second.officeId).toBe("office-us-house");
    expect(second.shouldPersistAlias).toBe(false);
  });
});
