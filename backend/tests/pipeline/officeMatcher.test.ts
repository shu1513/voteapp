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
  it("does not score a non-judicial county contest into a judge office (TX County Judge)", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { county: [] },
      officesByScope: {
        county: [
          { id: "office-county-judge-judicial", canonical_name: "County Level Judge" },
          { id: "office-county-executive", canonical_name: "County Executive" },
          { id: "office-sheriff", canonical_name: "Sheriff" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "county",
      districtName: "Bexar County, Texas",
      state: "TX",
      officialBallotTitle: "County Judge",
      discoveryContestFamily: "non_judicial_office",
    });

    // "county judge" does not clear the scoring threshold against County
    // Executive; the seeded alias (migration 145) resolves real traffic. The
    // essential assertion is that the judge office is no longer reachable.
    expect(result.officeId).not.toBe("office-county-judge-judicial");
    expect(result.officeId).toBeNull();
  });

  it("ignores a learned judge-office alias when the entry family is non-judicial", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: {
        county: [
          { office_id: "office-county-judge-judicial", normalized_alias: normalizeElectionTitleKey("County Judge") },
        ],
      },
      officesByScope: {
        county: [
          { id: "office-county-judge-judicial", canonical_name: "County Level Judge" },
          { id: "office-county-executive", canonical_name: "County Executive" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "county",
      districtName: "Bexar County, Texas",
      state: "TX",
      officialBallotTitle: "County Judge",
      discoveryContestFamily: "non_judicial_office",
    });

    expect(result.officeId).not.toBe("office-county-judge-judicial");
  });

  it("resolves official state schools-chief title variants via the seeded aliases", async () => {
    // Guards the normalizer-parity invariant for migration 164's aliases: the
    // seed layer stores normalizeElectionTitleKey(alias) while the matcher
    // looks titles up under its own normalization — if the two ever diverge
    // for these official ballot titles (GA/SC/WI-CA forms), the alias silently
    // stops matching and the shell strands with office_id = NULL again.
    const superintendentOfficeId = "office-superintendent-of-public-instruction";
    const aliasTitles = [
      "State School Superintendent",
      "State Superintendent of Public Instruction",
      "State Superintendent of Education",
    ];
    const client = createMatcherDataClient({
      aliasesByScope: {
        statewide: aliasTitles.map((title) => ({
          office_id: superintendentOfficeId,
          normalized_alias: normalizeElectionTitleKey(title),
        })),
      },
      officesByScope: {
        statewide: [
          { id: superintendentOfficeId, canonical_name: "Superintendent of Public Instruction" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    for (const title of aliasTitles) {
      const result = await matcher.resolve({
        scope: "statewide",
        districtName: "Georgia",
        state: "GA",
        officialBallotTitle: title,
      });
      expect(result.officeId).toBe(superintendentOfficeId);
      expect(result.method).toBe("alias_exact");
    }
  });

  it("resolves the SF county Assessor-Recorder and Public Defender titles via the seeded aliases", async () => {
    // Guards the normalizer-parity invariant for migration 165's aliases: the
    // seed layer stores normalizeElectionTitleKey(alias) while the matcher
    // looks titles up under its own normalization — if the two ever diverge
    // for these official ballot titles (SF's hyphenated combined office and
    // the elected Public Defender), the alias silently stops matching and the
    // shell strands with office_id = NULL again.
    const assessorRecorderOfficeId = "office-county-assessor-recorder";
    const publicDefenderOfficeId = "office-public-defender";

    // Pin migration 165's hand-written normalized_alias literals to the
    // current normalizer output. Without these, both sides of the test below
    // run through normalizeElectionTitleKey, so the test would keep passing
    // even after a normalizer change left the migration's stored DB rows
    // stale.
    expect(normalizeElectionTitleKey("Assessor-Recorder")).toBe("assessor recorder");
    expect(normalizeElectionTitleKey("County Assessor-Recorder")).toBe("county assessor recorder");
    expect(normalizeElectionTitleKey("Public Defender")).toBe("public defender");

    const aliasCases: Array<{ title: string; officeId: string }> = [
      { title: "Assessor-Recorder", officeId: assessorRecorderOfficeId },
      { title: "County Assessor-Recorder", officeId: assessorRecorderOfficeId },
      { title: "Public Defender", officeId: publicDefenderOfficeId },
    ];
    const client = createMatcherDataClient({
      aliasesByScope: {
        county: aliasCases.map(({ title, officeId }) => ({
          office_id: officeId,
          normalized_alias: normalizeElectionTitleKey(title),
        })),
      },
      officesByScope: {
        county: [
          { id: assessorRecorderOfficeId, canonical_name: "County Assessor-Recorder" },
          { id: publicDefenderOfficeId, canonical_name: "Public Defender" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    for (const { title, officeId } of aliasCases) {
      const result = await matcher.resolve({
        scope: "county",
        districtName: "San Francisco County, California",
        state: "CA",
        officialBallotTitle: title,
      });
      expect(result.officeId).toBe(officeId);
      expect(result.method).toBe("alias_exact");
    }
  });

  it("resolves the migration 169 alias gaps: City Representative, County Mayor, NY judicial districts", async () => {
    // Guards the normalizer-parity invariant for migration 169's aliases,
    // same as the 164/165 tests above: pin the migration's hand-written
    // normalized_alias literals to the current normalizer output, then prove
    // each official title resolves through the matcher.
    expect(normalizeElectionTitleKey("City Representative")).toBe("city representative");
    expect(normalizeElectionTitleKey("County Mayor")).toBe("county mayor");
    expect(normalizeElectionTitleKey("Supreme Court Justice - 1st Judicial District")).toBe(
      "supreme court justice 1st judicial district"
    );

    const cityCouncilOfficeId = "office-city-council-member";
    const countyExecutiveOfficeId = "office-county-executive";
    const countyJudgeOfficeId = "office-county-judge-judicial";

    const client = createMatcherDataClient({
      aliasesByScope: {
        place: [
          { office_id: cityCouncilOfficeId, normalized_alias: normalizeElectionTitleKey("City Representative") },
        ],
        county: [
          { office_id: countyExecutiveOfficeId, normalized_alias: normalizeElectionTitleKey("County Mayor") },
          {
            office_id: countyJudgeOfficeId,
            normalized_alias: normalizeElectionTitleKey("Supreme Court Justice - 1st Judicial District"),
          },
        ],
      },
      officesByScope: {
        place: [{ id: cityCouncilOfficeId, canonical_name: "City Council Member" }],
        county: [
          { id: countyExecutiveOfficeId, canonical_name: "County Executive" },
          { id: countyJudgeOfficeId, canonical_name: "County Level Judge" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);

    // El Paso's full seat title reduces to the generic "city representative"
    // key via jurisdiction + seat stripping, so one alias covers every seat.
    const elPaso = await matcher.resolve({
      scope: "place",
      districtName: "El Paso city, Texas",
      state: "TX",
      officialBallotTitle: "City Representative District 1, City of El Paso",
      discoveryContestFamily: "non_judicial_office",
    });
    expect(elPaso.officeId).toBe(cityCouncilOfficeId);
    expect(elPaso.method).toBe("alias_exact");

    const countyMayor = await matcher.resolve({
      scope: "county",
      districtName: "Orange County, Florida",
      state: "FL",
      officialBallotTitle: "County Mayor",
      discoveryContestFamily: "non_judicial_office",
    });
    expect(countyMayor.officeId).toBe(countyExecutiveOfficeId);
    expect(countyMayor.method).toBe("alias_exact");

    const nyJustice = await matcher.resolve({
      scope: "county",
      districtName: "New York County, New York",
      state: "NY",
      officialBallotTitle: "Supreme Court Justice - 1st Judicial District",
      discoveryContestFamily: "judicial_office",
    });
    expect(nyJustice.officeId).toBe(countyJudgeOfficeId);
    expect(nyJustice.method).toBe("alias_exact");
  });

  it("still honors a judge-office alias when the entry family is judicial", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: {
        county: [
          { office_id: "office-county-judge-judicial", normalized_alias: normalizeElectionTitleKey("Judge of the County Court") },
        ],
      },
      officesByScope: {
        county: [{ id: "office-county-judge-judicial", canonical_name: "County Level Judge" }],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "county",
      districtName: "Erie County, New York",
      state: "NY",
      officialBallotTitle: "Judge of the County Court",
      discoveryContestFamily: "judicial_office",
    });

    expect(result.officeId).toBe("office-county-judge-judicial");
    expect(result.method).toBe("alias_exact");
  });


  it("scores a joint governor ticket into the Governor office, not Lieutenant Governor", async () => {
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
      districtName: "Ohio",
      state: "OH",
      officialBallotTitle: "Governor and Lieutenant Governor",
      discoveryContestFamily: "non_judicial_office",
    });

    expect(result.officeId).toBe("office-governor");
    expect(result.method).toBe("deterministic_fallback");
    expect(result.shouldPersistAlias).toBe(true);
  });

  it("scores a slash-form joint governor ticket into the Governor office", async () => {
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
      districtName: "Alabama",
      state: "AL",
      officialBallotTitle: "Governor / Lieutenant Governor",
    });

    expect(result.officeId).toBe("office-governor");
    expect(result.method).toBe("deterministic_fallback");
  });

  it("still scores a standalone Lieutenant Governor title into the Lieutenant Governor office", async () => {
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
      districtName: "Alabama",
      state: "AL",
      officialBallotTitle: "Lieutenant Governor",
      discoveryContestFamily: "non_judicial_office",
    });

    expect(result.officeId).toBe("office-lt-governor");
    expect(result.method).toBe("deterministic_fallback");
  });

  it("keeps a learned alias authoritative for joint governor tickets", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: {
        statewide: [
          {
            office_id: "office-governor",
            normalized_alias: normalizeElectionTitleKey("Governor and Lieutenant Governor"),
          },
        ],
      },
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
      districtName: "Ohio",
      state: "OH",
      officialBallotTitle: "Governor and Lieutenant Governor",
      discoveryContestFamily: "non_judicial_office",
    });

    expect(result.officeId).toBe("office-governor");
    expect(result.method).toBe("alias_exact");
    expect(result.shouldPersistAlias).toBe(false);
  });

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

  it("matches a county-name-prefixed executive title via the bare-title alias (Harris County Judge)", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: {
        county: [{ office_id: "office-county-executive", normalized_alias: "county judge" }],
      },
      officesByScope: {
        county: [
          { id: "office-county-executive", canonical_name: "County Executive" },
          { id: "office-county-judge-judicial", canonical_name: "County Level Judge" },
          { id: "office-sheriff", canonical_name: "Sheriff" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "county",
      districtName: "Harris County, Texas",
      state: "TX",
      officialBallotTitle: "Harris County Judge",
      discoveryContestFamily: "non_judicial_office",
    });

    expect(result.officeId).toBe("office-county-executive");
    expect(result.method).toBe("alias_exact");
  });

  it("matches a county board president title via alias after stripping the county name", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: {
        county: [
          {
            office_id: "office-county-executive",
            normalized_alias: "president of the county board of commissioners",
          },
        ],
      },
      officesByScope: {
        county: [
          { id: "office-county-executive", canonical_name: "County Executive" },
          { id: "office-county-commissioner", canonical_name: "County Commissioner" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "county",
      districtName: "Cook County, Illinois",
      state: "IL",
      officialBallotTitle: "President of the Cook County Board of Commissioners",
      discoveryContestFamily: "non_judicial_office",
    });

    expect(result.officeId).toBe("office-county-executive");
    expect(result.method).toBe("alias_exact");
  });

  it("scores a county-name-prefixed standard title into the right office without an alias", async () => {
    const client = createMatcherDataClient({
      aliasesByScope: { county: [] },
      officesByScope: {
        county: [
          { id: "office-county-clerk", canonical_name: "County Clerk" },
          { id: "office-sheriff", canonical_name: "Sheriff" },
          { id: "office-county-treasurer", canonical_name: "County Treasurer" },
        ],
      },
    });

    const matcher = new OfficeMatcher(client as never);
    const result = await matcher.resolve({
      scope: "county",
      districtName: "Dallas County, Texas",
      state: "TX",
      officialBallotTitle: "Dallas County Clerk",
      discoveryContestFamily: "non_judicial_office",
    });

    expect(result.officeId).toBe("office-county-clerk");
  });
});
