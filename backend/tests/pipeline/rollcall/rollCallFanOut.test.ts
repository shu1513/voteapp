import { describe, expect, it, vi } from "vitest";

import {
  insertRollCallRecord,
  labelsForSide,
  loadExistingRecordsForDate,
  memberVoteSide,
  NOTIFY_WITHIN_DAYS,
  parseRollCallLabels,
  planCandidateRecord,
  refreshRollCallRecord,
  rewriteRollCallRecord,
  shouldNotifyForVoteDate,
  syncRollCallRecordTags,
  type ExistingCandidateRecord,
} from "../../../src/pipeline/rollcall/rollCallFanOut.js";
import { migrationTableColumns } from "../../helpers/migrationTableColumns.js";

const SLUGS = new Set(["general", "integrity_and_ethics", "immigration", "gun_control"]);

describe("memberVoteSide", () => {
  it("maps both feeds' spellings and skips non-positions", () => {
    expect(memberVoteSide("Yea")).toBe("yea");
    expect(memberVoteSide("Aye")).toBe("yea");
    expect(memberVoteSide("Nay")).toBe("nay");
    expect(memberVoteSide("No")).toBe("nay");
    expect(memberVoteSide("Present")).toBeNull();
    expect(memberVoteSide("Not Voting")).toBeNull();
    expect(memberVoteSide(" not  voting ")).toBeNull();
  });

  it("fails on a value the floor feeds never print", () => {
    expect(() => memberVoteSide("Guilty")).toThrow(/unknown vote value: Guilty/);
    expect(() => memberVoteSide("")).toThrow(/unknown vote value/);
  });
});

describe("parseRollCallLabels / labelsForSide", () => {
  it("takes the authored stance per side and never inverts yea for nay voters", () => {
    const labels = parseRollCallLabels(
      [
        { slug: "immigration", yea: "for", nay: "against" },
        { slug: "Gun_Control", yea: "against", nay: null },
        { slug: "general" },
      ],
      SLUGS
    );
    expect(labels).toEqual([
      { slug: "immigration", yea: "for", nay: "against" },
      { slug: "gun_control", yea: "against", nay: null },
      { slug: "general", yea: null, nay: null },
    ]);
    expect(labelsForSide(labels, "yea")).toEqual([
      { researchAreaSlug: "immigration", stance: "for" },
      { researchAreaSlug: "gun_control", stance: "against" },
      { researchAreaSlug: "general", stance: null },
    ]);
    // The nay side takes only what the judgment stated: gun_control's nay
    // is null, so nay voters get NO gun_control tag — not the inverse of
    // yea. The non-stance area still tags both sides topically.
    expect(labelsForSide(labels, "nay")).toEqual([
      { researchAreaSlug: "immigration", stance: "against" },
      { researchAreaSlug: "general", stance: null },
    ]);
  });

  it("reads a pre-nay stored label as an unstated nay side, never as the old inversion", () => {
    const labels = parseRollCallLabels([{ slug: "immigration", yea: "for" }, { slug: "general" }], SLUGS);
    expect(labels).toEqual([
      { slug: "immigration", yea: "for", nay: null },
      { slug: "general", yea: null, nay: null },
    ]);
    expect(labelsForSide(labels, "nay")).toEqual([{ researchAreaSlug: "general", stance: null }]);
    // A judgment whose only label is a stance area leaves nay voters with
    // no tags at all — an untagged record, not a flipped claim.
    expect(labelsForSide(parseRollCallLabels([{ slug: "immigration", yea: "for" }], SLUGS), "nay")).toEqual([]);
  });

  it("requires an explicit nay decision on stance areas when the authoring gate asks for it", () => {
    const explicit = { requireExplicitNay: true };
    expect(() => parseRollCallLabels([{ slug: "immigration", yea: "for" }], SLUGS, explicit)).toThrow(
      /\[0\]\.nay must be stated for immigration/
    );
    // Non-stance areas have no nay decision to state.
    expect(
      parseRollCallLabels([{ slug: "immigration", yea: "for", nay: null }, { slug: "general" }], SLUGS, explicit)
    ).toEqual([
      { slug: "immigration", yea: "for", nay: null },
      { slug: "general", yea: null, nay: null },
    ]);
  });

  it("rejects bad shapes, unknown slugs, and stance rules broken on either side", () => {
    expect(() => parseRollCallLabels(null, SLUGS)).toThrow(/not a non-empty array/);
    expect(() => parseRollCallLabels([], SLUGS)).toThrow(/not a non-empty array/);
    expect(() => parseRollCallLabels(["immigration"], SLUGS)).toThrow(/\[0\] is not an object/);
    expect(() => parseRollCallLabels([{ yea: "for" }], SLUGS)).toThrow(/\[0\]\.slug is not a string/);
    expect(() => parseRollCallLabels([{ slug: "immigration", yea: "yes" }], SLUGS)).toThrow(/\[0\]\.yea must be/);
    expect(() => parseRollCallLabels([{ slug: "immigration", yea: "for", nay: "no" }], SLUGS)).toThrow(/\[0\]\.nay must be/);
    expect(() => parseRollCallLabels([{ slug: "immigration", yea: "for", nay: "for" }], SLUGS)).toThrow(
      /\[0\]\.nay restates yea/
    );
    expect(() =>
      parseRollCallLabels(
        [
          { slug: "immigration", yea: "for" },
          { slug: "immigration", yea: "against" },
        ],
        SLUGS
      )
    ).toThrow(/names immigration twice/);
    expect(() => parseRollCallLabels([{ slug: "housing", yea: "for" }], SLUGS)).toThrow(/'housing' is not allowed/);
    expect(() => parseRollCallLabels([{ slug: "immigration", yea: null }], SLUGS)).toThrow(
      /invalid for yea voters: .*requires stance/
    );
    expect(() => parseRollCallLabels([{ slug: "general", yea: "for" }], SLUGS)).toThrow(/must not include stance/);
    expect(() => parseRollCallLabels([{ slug: "general", nay: "for" }], SLUGS)).toThrow(
      /invalid for nay voters: .*must not include stance/
    );
  });
});

describe("planCandidateRecord", () => {
  const KEY = "house:2025:145";
  const NEW_KEY = "v3_new";
  const HR1 = { type: "hr" as const, number: "1" };

  function record(overrides: Partial<ExistingCandidateRecord> & { id: string }): ExistingCandidateRecord {
    return {
      description: "Voted for the One Big Beautiful Bill Act. It passed 215-214.",
      source_url: "https://clerk.house.gov/Votes/2025145",
      origin: "rollcall_import",
      record_identity_key: `v3_${overrides.id}`,
      retired_at: null,
      ...overrides,
    };
  }

  // The text and URL this run would write. `record()` defaults to the same
  // values, so a same-key row is `unchanged` unless a test edits it.
  const INCOMING = {
    description: "Voted for the One Big Beautiful Bill Act. It passed 215-214.",
    sourceUrl: "https://clerk.house.gov/Votes/2025145",
  };

  function plan(existing: ExistingCandidateRecord[], skipExisting = false) {
    return planCandidateRecord({ existing, identityKey: NEW_KEY, ...INCOMING, rollCallKey: KEY, measure: HR1, skipExisting });
  }

  it("inserts when the candidate has nothing for this roll call", () => {
    expect(plan([])).toEqual({ plan: { action: "insert" }, relatedRecordIds: [] });
    const other = record({ id: "a", source_url: "https://clerk.house.gov/Votes/2025144", description: "Voted on the rule." });
    expect(plan([other]).plan).toEqual({ action: "insert" });
  });

  it("is unchanged on a re-run and rewrites exactly one live hand-written duplicate, whatever its URL spelling", () => {
    expect(plan([record({ id: "done", record_identity_key: NEW_KEY })]).plan).toEqual({ action: "unchanged", recordId: "done" });
    const xml = record({ id: "old", source_url: "https://clerk.house.gov/evs/2025/roll145.xml" });
    expect(plan([xml]).plan).toEqual({
      action: "rewrite",
      recordId: "old",
      oldIdentityKey: "v3_old",
      oldSourceUrl: xml.source_url,
      oldDescription: xml.description,
    });
    expect(plan([record({ id: "page" })]).plan.action).toBe("rewrite");
    // The Clerk's MemberVotes search page names the roll only by number.
    expect(
      plan([record({ id: "mv", source_url: "https://clerk.house.gov/Votes/MemberVotes?BillNum=H.R.1&RollCallNum=145&Session=1st" })])
        .plan.action
    ).toBe("rewrite");
    expect(plan([record({ id: "page" })], true).plan).toEqual({ action: "skip_existing", recordId: "page" });
  });

  it("refreshes its own same-key row whose text differs only in ways the key ignores, and leaves other rows alone", () => {
    // The key hashes normalized text, so a punctuation or case edit keeps
    // the key; a re-run must still update the stored text. The old values
    // ride along to guard the write.
    const edited = record({
      id: "done",
      record_identity_key: NEW_KEY,
      description: "Voted for the one big beautiful bill act; it passed 215-214",
    });
    const refresh = { action: "refresh", recordId: "done", oldDescription: edited.description, oldSourceUrl: edited.source_url };
    expect(plan([edited]).plan).toEqual(refresh);
    // --skip-existing guards hand-written duplicates, not this pipeline's own rows.
    expect(plan([edited], true).plan).toEqual(refresh);
    const slash = record({ id: "done", record_identity_key: NEW_KEY, source_url: "https://clerk.house.gov/Votes/2025145/" });
    expect(plan([slash]).plan).toEqual({ ...refresh, oldDescription: slash.description, oldSourceUrl: slash.source_url });
    // A hand-written or pre-provenance row with the same key is untouched.
    expect(plan([{ ...edited, origin: "manual" }]).plan).toEqual({ action: "unchanged", recordId: "done" });
    expect(plan([{ ...edited, origin: null }]).plan).toEqual({ action: "unchanged", recordId: "done" });
    // A retired same-key row is still retired, never refreshed.
    expect(plan([{ ...edited, retired_at: "2026-01-01" }]).plan).toEqual({ action: "retired", recordId: "done" });
  });

  it("never resurrects a retired claim, but a retired copy beside a live row is only history", () => {
    expect(plan([record({ id: "r1", record_identity_key: NEW_KEY, retired_at: "2026-01-01T00:00:00Z" })]).plan).toEqual({
      action: "retired",
      recordId: "r1",
    });
    // Live row for the vote + a retired old copy: the claim is not
    // withdrawn, so the live row is still written (and stays writable when
    // the judgment changes later).
    expect(plan([record({ id: "live" }), record({ id: "r2", retired_at: "2026-01-01T00:00:00Z" })]).plan).toEqual({
      action: "rewrite",
      recordId: "live",
      oldIdentityKey: "v3_live",
      oldSourceUrl: "https://clerk.house.gov/Votes/2025145",
      oldDescription: record({ id: "live" }).description,
    });
    expect(
      plan([record({ id: "done", record_identity_key: NEW_KEY }), record({ id: "r3", retired_at: "2026-01-01T00:00:00Z" })]).plan
    ).toEqual({ action: "unchanged", recordId: "done" });
  });

  it("stops on more than one live row for the vote, including an imported row beside a later hand-written one", () => {
    expect(plan([record({ id: "a" }), record({ id: "b" })]).plan).toEqual({ action: "ambiguous", recordIds: ["a", "b"] });
    expect(plan([record({ id: "done", record_identity_key: NEW_KEY }), record({ id: "hand" })]).plan).toEqual({
      action: "ambiguous",
      recordIds: ["done", "hand"],
    });
  });

  it("lists same-day rows that name the measure or make an uncited vote claim, and only those", () => {
    const press = record({
      id: "press",
      source_url: "https://hinson.house.gov/media/press-releases/x",
      description: "Voted for H.R. 1, the One Big Beautiful Bill Act.",
    });
    // No bill id, no roll-call URL — the pilot's missed press-release rows.
    const paraphrase = record({
      id: "paraphrase",
      source_url: "https://fulcher.house.gov/media/press-releases/y",
      description: "Voted in favor of the 2025 budget reconciliation bill enacting the President's agenda.",
    });
    // A vote claim citing ANOTHER roll call is a different, known vote —
    // this is what keeps same-day imported roll calls from cross-flagging
    // each other's records.
    const otherRoll = record({
      id: "other-roll",
      source_url: "https://clerk.house.gov/evs/2025/roll143.xml",
      description: "Voted to pass S.J. Res. 31, disapproving an EPA rule. It passed the House 216-212.",
    });
    // Same guard on the measure branch: an amendment vote on THIS bill
    // cites its own roll call, so naming the bill must not flag it (the
    // Aderholt case from expansion batch 5).
    const amendment = record({
      id: "amendment",
      source_url: "https://clerk.house.gov/Votes/2025143",
      description: "Voted yes on an amendment to H.R. 1 during House consideration.",
    });
    const nonVote = record({
      id: "chair",
      source_url: "https://collins.house.gov/media/press-releases/z",
      description: "Was appointed chair of the Subcommittee on Water Resources.",
    });
    const retiredPress = { ...press, id: "retired-press", retired_at: "2026-01-01T00:00:00Z" };
    const decision = plan([press, paraphrase, otherRoll, amendment, nonVote, retiredPress]);
    expect(decision).toEqual({ plan: { action: "insert" }, relatedRecordIds: ["press", "paraphrase"] });
    // No measure (quorum call): the bill-id rule is off, the vote-claim
    // rule still works.
    expect(
      planCandidateRecord({ existing: [press, paraphrase], identityKey: NEW_KEY, rollCallKey: KEY, measure: null, skipExisting: false })
        .relatedRecordIds
    ).toEqual(["press", "paraphrase"]);
  });
});

describe("shouldNotifyForVoteDate", () => {
  it("notifies only for votes within the last 30 days", () => {
    expect(NOTIFY_WITHIN_DAYS).toBe(30);
    expect(shouldNotifyForVoteDate("2026-08-23", "2026-08-23")).toBe(true);
    expect(shouldNotifyForVoteDate("2026-07-24", "2026-08-23")).toBe(true);
    expect(shouldNotifyForVoteDate("2026-07-23", "2026-08-23")).toBe(false);
    expect(shouldNotifyForVoteDate("2025-05-22", "2026-08-23")).toBe(false);
  });
});

describe("database writes", () => {
  const content = {
    candidateId: "cand-1",
    description: "Voted to pass H.R. 1. It passed the House 215-214.",
    sourceUrl: "https://clerk.house.gov/evs/2025/roll145.xml",
    eventDate: "2025-05-22",
    identityKey: "v3_new",
    originRunId: "rollcall:US:house:119-1:145:2026-08-23T00:00:00.000Z",
  };

  it("loads every same-day row of the named candidates grouped by candidate, using migration columns only", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        { candidate_id: "c1", id: "r1", description: "d", source_url: "u", record_identity_key: "k1", retired_at: null },
        { candidate_id: "c1", id: "r2", description: "d", source_url: "u", record_identity_key: "k2", retired_at: "2026-01-01" },
        { candidate_id: "c2", id: "r3", description: "d", source_url: "u", record_identity_key: "k3", retired_at: null },
      ],
    });
    const loaded = await loadExistingRecordsForDate(
      { query },
      ["c1", "c2", "c3"],
      ["2025-05-22", "2025-05-21"],
      "rollcall:US:house:119-1:145:"
    );
    expect(query.mock.calls[0]?.[1]).toEqual([["c1", "c2", "c3"], ["2025-05-22", "2025-05-21"], "rollcall:US:house:119-1:145:"]);
    expect([...loaded.keys()]).toEqual(["c1", "c2"]);
    expect(loaded.get("c1")?.map((record) => record.id)).toEqual(["r1", "r2"]);
    const columns = migrationTableColumns("candidate_records");
    const sql = query.mock.calls[0]?.[0] as string;
    for (const column of [
      "candidate_id",
      "id",
      "description",
      "source_url",
      "record_identity_key",
      "retired_at",
      "event_date",
      "origin",
      "origin_run_id",
    ]) {
      expect(sql).toContain(column);
      expect(columns.has(column), column).toBe(true);
    }
    // The run-id prefix net: a changed or cleared official-date override
    // leaves this pipeline's rows on a date outside the scan window, and
    // they must still be found so the plan rewrites instead of inserting.
    expect(sql).toMatch(/event_date = ANY\(\$2::date\[\]\)\s+OR \(origin = 'rollcall_import' AND starts_with\(origin_run_id, \$3\)\)/);
    expect(await loadExistingRecordsForDate({ query }, [], ["2025-05-22"], "rollcall:US:house:119-1:145:")).toEqual(new Map());
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("inserts with rollcall_import provenance and refuses a silent conflict", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [{ id: "new-id" }] }).mockResolvedValueOnce({ rows: [] });
    expect(await insertRollCallRecord({ query }, content)).toBe("new-id");
    const [sql, params] = query.mock.calls[0]!;
    expect(sql).toMatch(/'rollcall_import'/);
    expect(params).toEqual([content.candidateId, content.description, content.sourceUrl, "2025-05-22", "v3_new", content.originRunId]);
    await expect(insertRollCallRecord({ query }, content)).rejects.toThrow(/already holds record key v3_new/);
  });

  it("rewrites in place guarded on the old key, then logs the rollcall_normalization transition", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rowCount: 1 }).mockResolvedValueOnce({ rowCount: 1 });
    await rewriteRollCallRecord({ query }, { ...content, recordId: "old-id", oldIdentityKey: "v3_old" });
    expect(query).toHaveBeenCalledTimes(2);
    const [updateSql, updateParams] = query.mock.calls[0]!;
    expect(updateSql).toMatch(/UPDATE public\.candidate_records/);
    expect(updateSql).toMatch(/origin = 'rollcall_import'/);
    expect(updateSql).toMatch(/WHERE id = \$1\s+AND record_identity_key = \$2\s+AND retired_at IS NULL/);
    // The date moves with the content: identity keys embed event_date, so a
    // row found on the raw source date after an official-date override lands
    // on the official date.
    expect(updateSql).toMatch(/event_date = \$5::date/);
    expect(updateParams).toEqual([
      "old-id",
      "v3_old",
      content.description,
      content.sourceUrl,
      "2025-05-22",
      "v3_new",
      content.originRunId,
    ]);
    const [transitionSql, transitionParams] = query.mock.calls[1]!;
    expect(transitionSql).toMatch(/candidate_record_identity_transitions/);
    expect(transitionParams).toEqual(["cand-1", "v3_old", "v3_new", "rollcall_normalization"]);

    const stale = vi.fn().mockResolvedValueOnce({ rowCount: 0 });
    await expect(
      rewriteRollCallRecord({ query: stale }, { ...content, recordId: "old-id", oldIdentityKey: "v3_old" })
    ).rejects.toThrow(/changed under the rewrite/);
    expect(stale).toHaveBeenCalledTimes(1);
  });

  it("refreshes text in place guarded on the key and the old text, touching only description, source_url, and updated_at", async () => {
    const old = { oldDescription: "Voted to pass H.R. 1; it passed the House 215-214", oldSourceUrl: content.sourceUrl };
    const query = vi.fn().mockResolvedValueOnce({ rowCount: 1 });
    await refreshRollCallRecord({ query }, { ...content, ...old, recordId: "done-id" });
    expect(query).toHaveBeenCalledTimes(1);
    const [sql, params] = query.mock.calls[0]!;
    expect(sql).toMatch(/UPDATE public\.candidate_records/);
    expect(sql).toMatch(/SET description = \$3,\s+source_url = \$4,\s+updated_at = now\(\)/);
    expect(sql).toMatch(
      /WHERE id = \$1\s+AND record_identity_key = \$2\s+AND description = \$5\s+AND source_url = \$6\s+AND retired_at IS NULL/
    );
    // The key does not change, so no identity transition and no touch of
    // event_date, origin, or origin_run_id in the SET clause.
    expect(sql.slice(0, sql.indexOf("WHERE"))).not.toMatch(/record_identity_key|event_date|origin/);
    expect(params).toEqual(["done-id", "v3_new", content.description, content.sourceUrl, old.oldDescription, old.oldSourceUrl]);

    const stale = vi.fn().mockResolvedValueOnce({ rowCount: 0 });
    await expect(refreshRollCallRecord({ query: stale }, { ...content, ...old, recordId: "done-id" })).rejects.toThrow(
      /changed under the refresh/
    );
  });

  it("makes the record's tags exactly the side's labels", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });
    const ids = new Map([
      ["immigration", "ra-imm"],
      ["general", "ra-gen"],
    ]);
    const result = await syncRollCallRecordTags(
      { query },
      "rec-1",
      [
        { researchAreaSlug: "immigration", stance: "against" },
        { researchAreaSlug: "general", stance: null },
      ],
      ids
    );
    expect(result).toEqual({ deleted: 1 });
    expect(query.mock.calls[0]?.[0]).toMatch(/DELETE FROM public\.candidate_record_area_tags/);
    expect(query.mock.calls[0]?.[1]).toEqual(["rec-1", ["ra-imm", "ra-gen"]]);
    expect(query.mock.calls.slice(1).map((call) => call[1])).toEqual([
      ["rec-1", "ra-imm", "against"],
      ["rec-1", "ra-gen", null],
    ]);
    await expect(syncRollCallRecordTags({ query }, "rec-1", [{ researchAreaSlug: "housing", stance: "for" }], ids)).rejects.toThrow(
      /no research area id for slug housing/
    );
  });
});
