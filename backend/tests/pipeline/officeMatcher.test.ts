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
        county: [
          {
            office_id: "office-county-supervisor",
            normalized_alias: normalizeElectionTitleKey("Los Angeles County Board of Supervisors, District 1"),
          },
        ],
        statewide: [],
      },
      officesByScope: {
        county: [{ id: "office-county-supervisor", canonical_name: "County Supervisor" }],
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
    expect(countyResult.officeId).toBe("office-county-supervisor");
    expect(countyResult.method).toBe("alias_exact");
  });

  it("does not match township supervisor to county supervisor", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { county: [] },
      officesByScope: {
        county: [{ id: "office-county-supervisor", canonical_name: "County Supervisor" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "county",
      districtName: "Example County",
      state: "OH",
      officialBallotTitle: "Township Supervisor",
    });

    expect(result.officeId).toBeNull();
    expect(result.method).toBe("none");
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

  it("maps clear statewide U.S. Senate titles to United States Senator and persists alias memory", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { statewide: [] },
      officesByScope: {
        statewide: [
          { id: "office-governor", canonical_name: "Governor" },
          { id: "office-us-senator", canonical_name: "United States Senator" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "statewide",
      districtName: "California",
      state: "CA",
      officialBallotTitle: "U.S. Senate (Special Election)",
    });

    expect(result.officeId).toBe("office-us-senator");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.confidence).toBe(1);
    expect(result.aliasMemoryKey).toBe("united states senate special election");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("maps us_senate family entries to United States Senator even with generic senate title", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { statewide: [] },
      officesByScope: {
        statewide: [
          { id: "office-governor", canonical_name: "Governor" },
          { id: "office-us-senator", canonical_name: "United States Senator" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "statewide",
      districtName: "California",
      state: "CA",
      officialBallotTitle: "Senator",
      discoveryContestFamily: "us_senate",
    });

    expect(result.officeId).toBe("office-us-senator");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.confidence).toBe(1);
    expect(result.aliasMemoryKey).toBe("senator");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("does not force non-Senate statewide titles to United States Senator", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { statewide: [] },
      officesByScope: {
        statewide: [
          { id: "office-governor", canonical_name: "Governor" },
          { id: "office-us-senator", canonical_name: "United States Senator" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "statewide",
      districtName: "California",
      state: "CA",
      officialBallotTitle: "California Governor",
    });

    expect(result.officeId).toBe("office-governor");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.officeId).not.toBe("office-us-senator");
  });

  it("does not let us_senate family provenance override a clearly different office title", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { statewide: [] },
      officesByScope: {
        statewide: [
          { id: "office-governor", canonical_name: "Governor" },
          { id: "office-us-senator", canonical_name: "United States Senator" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "statewide",
      districtName: "California",
      state: "CA",
      officialBallotTitle: "Governor",
      discoveryContestFamily: "us_senate",
    });

    expect(result.officeId).toBe("office-governor");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.officeId).not.toBe("office-us-senator");
  });

  it("does not treat state senate titles as compatible with us_senate provenance", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { statewide: [] },
      officesByScope: {
        statewide: [
          { id: "office-governor", canonical_name: "Governor" },
          { id: "office-us-senator", canonical_name: "United States Senator" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "statewide",
      districtName: "California",
      state: "CA",
      officialBallotTitle: "State Senator",
      discoveryContestFamily: "us_senate",
    });

    expect(result.officeId).toBeNull();
    expect(result.method).toBe("none");
    expect(result.officeId).not.toBe("office-us-senator");
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

  it("maps us_house office titles to United States Representative and persists alias memory", async () => {
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
      officialBallotTitle: "Member of Congress, 31st District",
    });

    expect(result.officeId).toBe("office-us-house");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.confidence).toBe(1);
    expect(result.aliasMemoryKey).toBe("member of congress");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("returns none for us_house when United States Representative office row is missing", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { us_house: [] },
      officesByScope: {
        us_house: [],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "us_house",
      districtName: "Congressional District 31 (119th Congress), California",
      state: "CA",
      officialBallotTitle: "Member of Congress, 31st District",
    });

    expect(result.officeId).toBeNull();
    expect(result.method).toBe("none");
    expect(result.shouldPersistAlias).toBe(false);
  });

  it("maps state_upper office titles to State Senator and persists alias memory", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { state_upper: [] },
      officesByScope: {
        state_upper: [{ id: "office-state-senator", canonical_name: "State Senator" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "state_upper",
      districtName: "California State Senate District 12",
      state: "CA",
      officialBallotTitle: "Member of the Legislature, District 12",
    });

    expect(result.officeId).toBe("office-state-senator");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.confidence).toBe(1);
    expect(result.aliasMemoryKey).toBe("member of the legislature");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("returns none for state_upper when State Senator office row is missing", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { state_upper: [] },
      officesByScope: {
        state_upper: [],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "state_upper",
      districtName: "California State Senate District 12",
      state: "CA",
      officialBallotTitle: "Member of the Legislature, District 12",
    });

    expect(result.officeId).toBeNull();
    expect(result.method).toBe("none");
    expect(result.shouldPersistAlias).toBe(false);
  });

  it("maps state_lower office titles to State Lower Chamber Legislator and persists alias memory", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { state_lower: [] },
      officesByScope: {
        state_lower: [
          { id: "office-state-lower", canonical_name: "State Lower Chamber Legislator" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "state_lower",
      districtName: "Massachusetts State House District 7",
      state: "MA",
      officialBallotTitle: "Representative in General Court, District 7",
    });

    expect(result.officeId).toBe("office-state-lower");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.confidence).toBe(1);
    expect(result.aliasMemoryKey).toBe("representative in general court");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("returns none for state_lower when State Lower Chamber Legislator office row is missing", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { state_lower: [] },
      officesByScope: {
        state_lower: [],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "state_lower",
      districtName: "Massachusetts State House District 7",
      state: "MA",
      officialBallotTitle: "Representative in General Court, District 7",
    });

    expect(result.officeId).toBeNull();
    expect(result.method).toBe("none");
    expect(result.shouldPersistAlias).toBe(false);
  });

  it("maps statewide judicial-family titles to State Level Judge and persists alias memory", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { statewide: [] },
      officesByScope: {
        statewide: [{ id: "office-state-judge", canonical_name: "State Level Judge" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "statewide",
      districtName: "California",
      state: "CA",
      officialBallotTitle: "Justice of the Supreme Court",
      discoveryContestFamily: "judicial_office",
    });

    expect(result.officeId).toBe("office-state-judge");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.confidence).toBe(1);
    expect(result.aliasMemoryKey).toBe("justice of the supreme court");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("maps county judicial-family titles to County Level Judge and persists alias memory", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { county: [] },
      officesByScope: {
        county: [{ id: "office-county-judge", canonical_name: "County Level Judge" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "county",
      districtName: "Los Angeles County",
      state: "CA",
      officialBallotTitle: "Superior Court Judge, Office No. 7",
      discoveryContestFamily: "judicial_office",
    });

    expect(result.officeId).toBe("office-county-judge");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.confidence).toBe(1);
    expect(result.aliasMemoryKey).toBe("superior court judge");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("maps place judicial-family titles to Place Level Judge and persists alias memory", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { place: [] },
      officesByScope: {
        place: [{ id: "office-place-judge", canonical_name: "Place Level Judge" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "place",
      districtName: "Sample City",
      state: "CA",
      officialBallotTitle: "Municipal Judge",
      discoveryContestFamily: "judicial_office",
    });

    expect(result.officeId).toBe("office-place-judge");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.confidence).toBe(1);
    expect(result.aliasMemoryKey).toBe("municipal judge");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("does not force-map a mis-stamped statewide non-judicial title to State Level Judge", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { statewide: [] },
      officesByScope: {
        statewide: [
          { id: "office-state-judge", canonical_name: "State Level Judge" },
          { id: "office-governor", canonical_name: "Governor" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "statewide",
      districtName: "California",
      state: "CA",
      officialBallotTitle: "Governor",
      discoveryContestFamily: "judicial_office",
    });

    expect(result.officeId).toBe("office-governor");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("does not force-map District Attorney to County Level Judge under judicial provenance", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { county: [] },
      officesByScope: {
        county: [
          { id: "office-county-judge", canonical_name: "County Level Judge" },
          { id: "office-district-attorney", canonical_name: "District Attorney" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "county",
      districtName: "Washoe County",
      state: "NV",
      officialBallotTitle: "District Attorney",
      discoveryContestFamily: "judicial_office",
    });

    expect(result.officeId).toBe("office-district-attorney");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("returns none for a judicial-family title with no judicial marker or office match", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { county: [] },
      officesByScope: {
        county: [{ id: "office-county-judge", canonical_name: "County Level Judge" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "county",
      districtName: "Washoe County",
      state: "NV",
      officialBallotTitle: "Public Administrator",
      discoveryContestFamily: "judicial_office",
    });

    expect(result.officeId).toBeNull();
    expect(result.method).toBe("none");
    expect(result.shouldPersistAlias).toBe(false);
  });

  it("maps school-scope office titles to School Board Member and persists alias memory", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { school_unified: [] },
      officesByScope: {
        school_unified: [{ id: "office-school-board", canonical_name: "School Board Member" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "school_unified",
      districtName: "Baldwin Park Unified School District",
      state: "CA",
      officialBallotTitle: "Governing Board Trustee, Area 3",
    });

    expect(result.officeId).toBe("office-school-board");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.confidence).toBe(1);
    expect(result.aliasMemoryKey).toBe("governing board trustee area 3");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("returns none for school scopes when School Board Member office row is missing", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { school_elementary: [] },
      officesByScope: {
        school_elementary: [],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "school_elementary",
      districtName: "Sample Elementary School District",
      state: "CA",
      officialBallotTitle: "Governing Board Member",
    });

    expect(result.officeId).toBeNull();
    expect(result.method).toBe("none");
    expect(result.shouldPersistAlias).toBe(false);
  });
});
