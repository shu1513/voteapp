import { describe, expect, it, vi } from "vitest";

import { runMergeCandidates } from "../../src/scripts/mergeManualCandidates.js";

const MERGED = "11111111-1111-1111-1111-111111111111";
const SURVIVOR = "22222222-2222-2222-2222-222222222222";
const ELECTION_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const ELECTION_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const LINK_ID = "44444444-4444-4444-4444-444444444444";
const SURVIVOR_LINK_ID = "55555555-5555-5555-5555-555555555555";
const USER_ID = "99999999-9999-9999-9999-999999999999";

function candidateRow(id: string, overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id,
    display_name: id === MERGED ? "Pat O'Brien" : "Pat OBrien",
    first_name: "Pat",
    last_name: "OBrien",
    party: "Nonpartisan",
    state: "AZ",
    deleted_at: null,
    merged_into_candidate_id: null,
    fec_ids: [],
    state_filing_ids: [],
    summary: null,
    current_office: null,
    date_of_birth: null,
    twitter_handle: null,
    linkedin_url: null,
    official_website_url: null,
    profile_sources: [],
    ...overrides,
  };
}

function linkRow(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: LINK_ID,
    candidate_id: MERGED,
    election_id: ELECTION_A,
    is_incumbent: false,
    status: "declared",
    running_mate_candidate_id: null,
    ...overrides,
  };
}

function manualFinanceTargetFkRows() {
  const common = {
    constraint_name: "manual_candidate_finance_filing_targets_candidate_election_fk",
    table_name: "manual_candidate_finance_filing_targets",
    column_count: 2,
  };
  return [
    { ...common, column_name: "candidate_id", referenced_column: "candidate_id" },
    { ...common, column_name: "election_id", referenced_column: "election_id" },
  ];
}

// Query order: BEGIN, lock candidates, lock links, lock mate links,
// presidential pair guard, winners guard, [FK scan onto candidate_elections +
// per-table counts when links converge], link writes, records, sweep
// confirmations, follows, notification events, FK scan onto candidates +
// per-table counts, chain collapse, identifier union, mark merged,
// COMMIT/ROLLBACK.
function buildClient(responses: Record<string, unknown[][]>) {
  const calls: { text: string; values: unknown[] }[] = [];
  const queue = { ...responses };
  const query = vi.fn(async (text: string, values?: unknown[]) => {
    calls.push({ text, values: values ?? [] });
    for (const key of Object.keys(queue)) {
      if (text.includes(key)) {
        const rows = queue[key]!.shift();
        if (rows !== undefined) return { rows };
      }
    }
    return { rows: [] };
  });
  return { query, calls };
}

function happyResponses(overrides: Partial<Record<string, unknown[][]>> = {}) {
  return {
    "FROM public.candidates\n        WHERE id = ANY": [
      [candidateRow(MERGED), candidateRow(SURVIVOR)],
    ],
    "FROM public.candidate_elections\n        WHERE candidate_id = ANY": [[linkRow()]],
    "FROM public.candidate_records\n        WHERE candidate_id = ANY": [
      [
        { id: "r1", candidate_id: MERGED, record_identity_key: "k1" },
        { id: "r2", candidate_id: SURVIVOR, record_identity_key: "k2" },
      ],
    ],
    "FROM public.user_candidate_follows\n        WHERE candidate_id = ANY": [
      [{ id: "f1", candidate_id: MERGED, user_id: USER_ID }],
    ],
    "FROM public.user_candidate_follow_notification_events\n        WHERE candidate_id = ANY": [
      [
        {
          id: "e1",
          candidate_id: MERGED,
          user_id: USER_ID,
          event_type: "candidate_record_update",
          election_id: null,
        },
      ],
    ],
    "'public.candidates'::regclass": [
      [{ table_name: "public.az_candidate_finance_links", column_name: "candidate_id" }],
    ],
    "FROM public.az_candidate_finance_links WHERE candidate_id": [[{ n: "2" }], [{ n: "0" }]],
    ...overrides,
  };
}

function run(
  client: { query: ReturnType<typeof vi.fn> },
  overrides: Partial<{ candidateId: string; intoCandidateId: string; dryRun: boolean }> = {}
) {
  return runMergeCandidates(client, {
    candidateId: MERGED,
    intoCandidateId: SURVIVOR,
    dryRun: false,
    ...overrides,
  });
}

describe("runMergeCandidates", () => {
  it("rehomes links, records, follows, events, and finance rows, then marks the duplicate merged", async () => {
    const { query, calls } = buildClient(happyResponses());

    const result = await run({ query });

    expect(result.links).toEqual({ rehomed: 1, duplicatesDeleted: 0 });
    expect(result.records).toEqual({ rehomed: 1, duplicatesDeleted: 0, areaTagsCopied: 0, retirementsPropagated: 0 });
    expect(result.follows).toEqual({ rehomed: 1, duplicatesDeleted: 0 });
    expect(result.notificationEvents).toEqual({
      rehomed: 1,
      duplicatesDeleted: 0,
      remappedToSurvivorRecords: 0,
    });
    expect(result.profile).toEqual({
      fieldsFilled: [],
      sourcesAppended: 0,
      formerWebsiteUrlsAppended: 0,
    });
    expect(result.otherTables).toEqual([
      { table: "public.az_candidate_finance_links", column: "candidate_id", rowsRehomed: 2 },
    ]);
    expect(result.mergedCandidateName).toContain("Pat O'Brien");

    // Candidates locked first, in deterministic order, so two concurrent
    // merges serialize instead of deadlocking.
    const candidateLock = calls.find((call) => call.text.includes("FROM public.candidates\n"));
    expect(candidateLock?.text).toContain("ORDER BY id");
    expect(candidateLock?.text).toContain("FOR UPDATE");

    const linkRehome = calls.find((call) =>
      call.text.includes("UPDATE public.candidate_elections SET candidate_id")
    );
    expect(linkRehome?.values).toEqual([[LINK_ID], SURVIVOR]);
    const financeRehome = calls.find((call) =>
      call.text.includes("UPDATE public.az_candidate_finance_links SET candidate_id")
    );
    expect(financeRehome?.values).toEqual([MERGED, SURVIVOR]);
    const mark = calls.find((call) => call.text.includes("SET merged_into_candidate_id = $2::uuid,"));
    expect(mark?.values).toEqual([MERGED, SURVIVOR]);
    expect(mark?.text).toContain("COALESCE(deleted_at, now())");
    expect(calls.at(-1)?.text).toBe("COMMIT");
  });

  it("refuses only when immutable manual-finance targets reference links the merge would change", async () => {
    const { query, calls } = buildClient(happyResponses({
      "FROM public.manual_candidate_finance_filing_targets": [[{ n: "1" }]],
    }));

    await expect(run({ query })).rejects.toThrow(
      /1 manual candidate-finance filing target row\(s\).*immutable filing payloads/s
    );
    expect(calls.some((call) => call.text.includes("UPDATE public.candidate_elections SET candidate_id"))).toBe(false);
    expect(calls.at(-1)?.text).toBe("ROLLBACK");
  });

  it("dry-run reports the plan, writes nothing, and rolls back", async () => {
    const { query, calls } = buildClient(happyResponses());

    const result = await run({ query }, { dryRun: true });

    expect(result.dryRun).toBe(true);
    expect(result.links.rehomed).toBe(1);
    expect(result.otherTables[0]?.rowsRehomed).toBe(2);
    expect(calls.some((call) => /^\s*(UPDATE|DELETE)\b/i.test(call.text))).toBe(false);
    expect(calls.at(-1)?.text).toBe("ROLLBACK");
  });

  it("accepts uppercase UUID input by normalizing before row matching", async () => {
    const { query } = buildClient(happyResponses());

    const result = await run(
      { query },
      { candidateId: MERGED.toUpperCase(), intoCandidateId: SURVIVOR.toUpperCase() }
    );

    expect(result.mergedCandidateId).toBe(MERGED);
    expect(result.survivorCandidateId).toBe(SURVIVOR);
  });

  it("rejects identical ids before touching the database", async () => {
    const { query } = buildClient({});
    await expect(run({ query }, { intoCandidateId: MERGED })).rejects.toThrow(/must differ/);
    expect(query).not.toHaveBeenCalled();
  });

  it("refuses missing, already-merged, and dead candidates", async () => {
    const missing = buildClient(
      happyResponses({
        "FROM public.candidates\n        WHERE id = ANY": [[candidateRow(SURVIVOR)]],
      })
    );
    await expect(run({ query: missing.query })).rejects.toThrow(/Candidate not found: 11111111/);

    const alreadyMerged = buildClient(
      happyResponses({
        "FROM public.candidates\n        WHERE id = ANY": [
          [
            candidateRow(MERGED, { merged_into_candidate_id: SURVIVOR, deleted_at: "2026-01-01" }),
            candidateRow(SURVIVOR),
          ],
        ],
      })
    );
    await expect(run({ query: alreadyMerged.query })).rejects.toThrow(/already merged into/);

    const mergedSurvivor = buildClient(
      happyResponses({
        "FROM public.candidates\n        WHERE id = ANY": [
          [
            candidateRow(MERGED),
            candidateRow(SURVIVOR, {
              merged_into_candidate_id: "33333333-3333-3333-3333-333333333333",
              deleted_at: "2026-01-01",
            }),
          ],
        ],
      })
    );
    await expect(run({ query: mergedSurvivor.query })).rejects.toThrow(
      /itself merged into .*merge into that row instead/
    );

    const deadSurvivor = buildClient(
      happyResponses({
        "FROM public.candidates\n        WHERE id = ANY": [
          [candidateRow(MERGED), candidateRow(SURVIVOR, { deleted_at: "2026-01-01" })],
        ],
      })
    );
    await expect(run({ query: deadSurvivor.query })).rejects.toThrow(/soft-deleted/);
  });

  it("converges identical duplicate links and refuses disagreeing ones", async () => {
    const identical = buildClient(
      happyResponses({
        "FROM public.candidate_elections\n        WHERE candidate_id = ANY": [
          [linkRow(), linkRow({ id: SURVIVOR_LINK_ID, candidate_id: SURVIVOR })],
        ],
        "'public.candidate_elections'::regclass": [
          [
            {
              constraint_name: "fl_group_links_candidate_election_id_fkey",
              table_name: "public.fl_candidate_finance_outside_group_links",
              column_name: "candidate_election_id",
              referenced_column: "id",
              column_count: 1,
            },
            ...manualFinanceTargetFkRows(),
          ],
        ],
        "FROM public.fl_candidate_finance_outside_group_links WHERE candidate_election_id": [
          [{ n: "0" }],
        ],
      })
    );
    const result = await run({ query: identical.query });
    expect(result.links).toEqual({ rehomed: 0, duplicatesDeleted: 1 });
    const del = identical.calls.find((call) =>
      call.text.includes("DELETE FROM public.candidate_elections")
    );
    expect(del?.values).toEqual([[LINK_ID]]);

    const disagreeing = buildClient(
      happyResponses({
        "FROM public.candidate_elections\n        WHERE candidate_id = ANY": [
          [
            linkRow(),
            linkRow({ id: SURVIVOR_LINK_ID, candidate_id: SURVIVOR, status: "withdrawn" }),
          ],
        ],
      })
    );
    await expect(run({ query: disagreeing.query })).rejects.toThrow(/links disagree/);
  });

  it("refuses to delete a duplicate link that other tables still reference", async () => {
    const { query } = buildClient(
      happyResponses({
        "FROM public.candidate_elections\n        WHERE candidate_id = ANY": [
          [linkRow(), linkRow({ id: SURVIVOR_LINK_ID, candidate_id: SURVIVOR })],
        ],
        "'public.candidate_elections'::regclass": [
          [
            {
              constraint_name: "fl_group_links_candidate_election_id_fkey",
              table_name: "public.fl_candidate_finance_outside_group_links",
              column_name: "candidate_election_id",
              referenced_column: "id",
              column_count: 1,
            },
          ],
        ],
        "FROM public.fl_candidate_finance_outside_group_links WHERE candidate_election_id": [
          [{ n: "3" }],
        ],
      })
    );

    await expect(run({ query })).rejects.toThrow(
      /fl_candidate_finance_outside_group_links\.candidate_election_id \(3\)/
    );
  });

  it("reconciles user election choices on duplicate links before deleting them", async () => {
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.candidate_elections\n        WHERE candidate_id = ANY": [
          [linkRow(), linkRow({ id: SURVIVOR_LINK_ID, candidate_id: SURVIVOR })],
        ],
        // c1's user picked both candidates in the duplicate election (their
        // survivor pick wins); c2's user picked only the merged candidate
        // (repointed to the survivor's candidacy).
        "FROM public.user_election_choices AS merged_choice": [
          [
            { id: "c1", survivor_has_pick: true },
            { id: "c2", survivor_has_pick: false },
          ],
        ],
      })
    );

    const result = await run({ query });
    expect(result.choices).toEqual({ repointedToSurvivor: 1, duplicatesDeleted: 1 });

    const choiceDelete = calls.find((call) =>
      call.text.includes("DELETE FROM public.user_election_choices")
    );
    expect(choiceDelete?.values).toEqual([["c1"]]);
    const choiceRepoint = calls.find((call) =>
      call.text.includes("UPDATE public.user_election_choices")
    );
    expect(choiceRepoint?.values).toEqual([["c2"], SURVIVOR]);

    // The reconciliation must land before the duplicate-link delete, or the
    // FK cascade would have erased the choices first.
    const choiceDeleteIndex = calls.findIndex((call) =>
      call.text.includes("DELETE FROM public.user_election_choices")
    );
    const linkDeleteIndex = calls.findIndex((call) =>
      call.text.includes("DELETE FROM public.candidate_elections")
    );
    expect(choiceDeleteIndex).toBeGreaterThan(-1);
    expect(linkDeleteIndex).toBeGreaterThan(choiceDeleteIndex);
  });

  it("refuses duplicate-link deletion under an unknown composite FK onto candidate_elections", async () => {
    const { query } = buildClient(
      happyResponses({
        "FROM public.candidate_elections\n        WHERE candidate_id = ANY": [
          [linkRow(), linkRow({ id: SURVIVOR_LINK_ID, candidate_id: SURVIVOR })],
        ],
        // A future table with a composite FK this guard cannot count — must
        // refuse, not silently pass a wrong-column comparison. The known
        // choices FK is exempt (bespoke-reconciled).
        "'public.candidate_elections'::regclass": [
          [
            {
              constraint_name: "fk_user_election_choices_candidacy",
              table_name: "public.user_election_choices",
              column_name: "candidate_id",
              referenced_column: "candidate_id",
              column_count: 2,
            },
            {
              constraint_name: "fk_future_composite",
              table_name: "public.future_table",
              column_name: "candidate_id",
              referenced_column: "candidate_id",
              column_count: 2,
            },
          ],
        ],
      })
    );

    await expect(run({ query })).rejects.toThrow(
      /future_table\.fk_future_composite.*extend the guard/s
    );
  });

  it("refuses when persisted election_results winners reference the duplicate", async () => {
    const { query } = buildClient(
      happyResponses({
        jsonb_array_elements: [[{ id: "77777777-7777-7777-7777-777777777777" }]],
      })
    );

    await expect(run({ query })).rejects.toThrow(/persisted winners referencing candidate/);
  });

  it("refuses ticket conflicts: self-tickets, same-election mate collisions, and presidential pairs", async () => {
    const selfTicket = buildClient(
      happyResponses({
        "FROM public.candidate_elections\n        WHERE candidate_id = ANY": [
          [linkRow({ candidate_id: SURVIVOR, running_mate_candidate_id: MERGED })],
        ],
      })
    );
    await expect(run({ query: selfTicket.query })).rejects.toThrow(/head and running mate/);

    const sameElectionMates = buildClient(
      happyResponses({
        "FROM public.candidate_elections\n        WHERE candidate_id = ANY": [
          [
            linkRow({
              id: "m1",
              candidate_id: "66666666-6666-6666-6666-666666666666",
              election_id: ELECTION_A,
              running_mate_candidate_id: MERGED,
            }),
            linkRow({
              id: "m2",
              candidate_id: "77777777-7777-7777-7777-777777777777",
              election_id: ELECTION_A,
              running_mate_candidate_id: SURVIVOR,
            }),
          ],
        ],
      })
    );
    await expect(run({ query: sameElectionMates.query })).rejects.toThrow(
      /links carrying both candidates as running mates/
    );
    // Refusal must precede every write in the transaction.
    expect(
      sameElectionMates.calls.some((call) => /^\s*(UPDATE|DELETE|INSERT)\b/i.test(call.text))
    ).toBe(false);

    const presidentialPair = buildClient(
      happyResponses({
        "FROM public.presidential_cycle_candidates": [[{ id: "p1" }]],
      })
    );
    await expect(run({ query: presidentialPair.query })).rejects.toThrow(
      /presidential_cycle_candidates row p1/
    );
  });

  it("rehomes mate references, allowing both candidates as mates on different elections", async () => {
    // The classic duplicate shape: the duplicate rides as mate on one shell,
    // the survivor on a sibling — different elections, so the partial unique
    // key cannot collide and the merge must NOT refuse.
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.candidate_elections\n        WHERE candidate_id = ANY": [
          [
            linkRow({
              id: "m1",
              candidate_id: "66666666-6666-6666-6666-666666666666",
              election_id: ELECTION_A,
              running_mate_candidate_id: MERGED,
            }),
            linkRow({
              id: "m2",
              candidate_id: "77777777-7777-7777-7777-777777777777",
              election_id: ELECTION_B,
              running_mate_candidate_id: SURVIVOR,
            }),
          ],
        ],
      })
    );

    const result = await run({ query });

    expect(result.mateLinks).toEqual({ rehomed: 1 });
    const mateUpdate = calls.find((call) =>
      call.text.includes("SET running_mate_candidate_id = $2::uuid")
    );
    expect(mateUpdate?.values).toEqual([["m1"], SURVIVOR]);
  });

  it("deletes duplicate records by identity key and rehomes the rest", async () => {
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.candidate_records\n        WHERE candidate_id = ANY": [
          [
            { id: "r1", candidate_id: MERGED, record_identity_key: "k1" },
            { id: "r2", candidate_id: MERGED, record_identity_key: "shared" },
            { id: "r3", candidate_id: SURVIVOR, record_identity_key: "shared" },
          ],
        ],
      })
    );

    const result = await run({ query });

    expect(result.records).toEqual({ rehomed: 1, duplicatesDeleted: 1, areaTagsCopied: 0, retirementsPropagated: 0 });
    const del = calls.find((call) => call.text.includes("DELETE FROM public.candidate_records"));
    expect(del?.values).toEqual([["r2"]]);
    const rehome = calls.find((call) =>
      call.text.includes("UPDATE public.candidate_records SET candidate_id")
    );
    expect(rehome?.values).toEqual([["r1"], SURVIVOR]);
  });

  it("propagates a retired duplicate's retirement onto an active survivor copy", async () => {
    // Mixed retirement states on an identical-key pair are conflicting
    // operator decisions about the same claim; deleting the retired copy
    // while the survivor's stays active would silently resurrect a withdrawn
    // claim. Retirement wins.
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.candidate_records\n        WHERE candidate_id = ANY": [
          [
            {
              id: "r2",
              candidate_id: MERGED,
              record_identity_key: "shared",
              retired_at: "2026-07-30 00:00:00+00",
              retired_reason: "unsupported claim",
            },
            { id: "r3", candidate_id: SURVIVOR, record_identity_key: "shared", retired_at: null, retired_reason: null },
          ],
        ],
      })
    );

    const result = await run({ query });

    expect(result.records).toEqual({ rehomed: 0, duplicatesDeleted: 1, areaTagsCopied: 0, retirementsPropagated: 1 });
    const propagate = calls.find((call) => call.text.includes("SET retired_at = $2::timestamptz"));
    expect(propagate?.values).toEqual(["r3", "2026-07-30 00:00:00+00", "unsupported claim"]);
  });

  it("does not touch a survivor copy that is already retired", async () => {
    // The reverse direction: duplicate active, survivor retired. The
    // survivor's own retirement stands; no propagation UPDATE is issued.
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.candidate_records\n        WHERE candidate_id = ANY": [
          [
            { id: "r2", candidate_id: MERGED, record_identity_key: "shared", retired_at: null, retired_reason: null },
            {
              id: "r3",
              candidate_id: SURVIVOR,
              record_identity_key: "shared",
              retired_at: "2026-07-30 00:00:00+00",
              retired_reason: "wrong attribution",
            },
          ],
        ],
      })
    );

    const result = await run({ query });

    expect(result.records).toEqual({ rehomed: 0, duplicatesDeleted: 1, areaTagsCopied: 0, retirementsPropagated: 0 });
    expect(calls.some((call) => call.text.includes("SET retired_at = $2::timestamptz"))).toBe(false);
  });

  it("deletes the survivor's sweep confirmation only when records were rehomed", async () => {
    const withRehomes = buildClient(
      happyResponses({
        "SELECT candidate_id FROM public.candidate_record_sweep_confirmations": [
          [{ candidate_id: MERGED }, { candidate_id: SURVIVOR }],
        ],
      })
    );
    const rehomed = await run({ query: withRehomes.query });
    expect(rehomed.sweepConfirmations).toEqual({ mergedDeleted: true, survivorDeleted: true });
    const del = withRehomes.calls.find((call) =>
      call.text.includes("DELETE FROM public.candidate_record_sweep_confirmations")
    );
    expect(del?.values).toEqual([[MERGED, SURVIVOR]]);

    // Same confirmations, but the duplicate has no records to rehome — the
    // survivor's record set is unchanged, so its confirmation stands.
    const withoutRehomes = buildClient(
      happyResponses({
        "FROM public.candidate_records\n        WHERE candidate_id = ANY": [
          [{ id: "r2", candidate_id: SURVIVOR, record_identity_key: "k2" }],
        ],
        "SELECT candidate_id FROM public.candidate_record_sweep_confirmations": [
          [{ candidate_id: MERGED }, { candidate_id: SURVIVOR }],
        ],
      })
    );
    const untouched = await run({ query: withoutRehomes.query });
    expect(untouched.sweepConfirmations).toEqual({ mergedDeleted: true, survivorDeleted: false });
    const del2 = withoutRehomes.calls.find((call) =>
      call.text.includes("DELETE FROM public.candidate_record_sweep_confirmations")
    );
    expect(del2?.values).toEqual([[MERGED]]);
  });

  it("keeps the survivor's follow when a user follows both candidates", async () => {
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.user_candidate_follows\n        WHERE candidate_id = ANY": [
          [
            { id: "f1", candidate_id: MERGED, user_id: USER_ID },
            { id: "f2", candidate_id: SURVIVOR, user_id: USER_ID },
            { id: "f3", candidate_id: MERGED, user_id: "88888888-8888-8888-8888-888888888888" },
          ],
        ],
      })
    );

    const result = await run({ query });

    expect(result.follows).toEqual({ rehomed: 1, duplicatesDeleted: 1 });
    const del = calls.find((call) => call.text.includes("DELETE FROM public.user_candidate_follows"));
    expect(del?.values).toEqual([["f1"]]);
  });

  it("deletes colliding future-election events and rehomes the rest", async () => {
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.user_candidate_follow_notification_events\n        WHERE candidate_id = ANY": [
          [
            {
              id: "e1",
              candidate_id: MERGED,
              user_id: USER_ID,
              event_type: "candidate_future_election",
              election_id: ELECTION_A,
            },
            {
              id: "e2",
              candidate_id: SURVIVOR,
              user_id: USER_ID,
              event_type: "candidate_future_election",
              election_id: ELECTION_A,
            },
            {
              id: "e3",
              candidate_id: MERGED,
              user_id: USER_ID,
              event_type: "candidate_future_election",
              election_id: ELECTION_B,
            },
          ],
        ],
      })
    );

    const result = await run({ query });

    expect(result.notificationEvents).toEqual({
      rehomed: 1,
      duplicatesDeleted: 1,
      remappedToSurvivorRecords: 0,
    });
    const del = calls.find((call) =>
      call.text.includes("DELETE FROM public.user_candidate_follow_notification_events")
    );
    expect(del?.values).toEqual([["e1"]]);
    const rehome = calls.find((call) =>
      call.text.includes("UPDATE public.user_candidate_follow_notification_events")
    );
    expect(rehome?.values).toEqual([["e3"], SURVIVOR]);
  });

  it("dedupes withdrawal events per event type, not just future-election events", async () => {
    // Both election-scoped event types carry their own per-(user, candidate,
    // election) partial unique index; rehoming a duplicate's withdrawal event
    // onto a survivor that already has one would violate
    // uq_ucf_notification_events_withdrawal and roll back the whole merge.
    // A same-election event of the OTHER type is not a collision.
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.user_candidate_follow_notification_events\n        WHERE candidate_id = ANY": [
          [
            {
              id: "w1",
              candidate_id: MERGED,
              user_id: USER_ID,
              event_type: "candidate_election_withdrawal",
              election_id: ELECTION_A,
            },
            {
              id: "w2",
              candidate_id: SURVIVOR,
              user_id: USER_ID,
              event_type: "candidate_election_withdrawal",
              election_id: ELECTION_A,
            },
            // Survivor has a future-election event for ELECTION_B; the
            // duplicate's withdrawal for ELECTION_B must still rehome.
            {
              id: "f1",
              candidate_id: SURVIVOR,
              user_id: USER_ID,
              event_type: "candidate_future_election",
              election_id: ELECTION_B,
            },
            {
              id: "w3",
              candidate_id: MERGED,
              user_id: USER_ID,
              event_type: "candidate_election_withdrawal",
              election_id: ELECTION_B,
            },
          ],
        ],
      })
    );

    const result = await run({ query });

    expect(result.notificationEvents).toEqual({
      rehomed: 1,
      duplicatesDeleted: 1,
      remappedToSurvivorRecords: 0,
    });
    const del = calls.find((call) =>
      call.text.includes("DELETE FROM public.user_candidate_follow_notification_events")
    );
    expect(del?.values).toEqual([["w1"]]);
    const rehome = calls.find((call) =>
      call.text.includes("UPDATE public.user_candidate_follow_notification_events")
    );
    expect(rehome?.values).toEqual([["w3"], SURVIVOR]);
  });

  it("copies missing area tags to the survivor's record before deleting a duplicate record", async () => {
    const responses = () =>
      happyResponses({
        "FROM public.candidate_records\n        WHERE candidate_id = ANY": [
          [
            { id: "r2", candidate_id: MERGED, record_identity_key: "shared" },
            { id: "r3", candidate_id: SURVIVOR, record_identity_key: "shared" },
          ],
        ],
        "FROM public.candidate_record_area_tags t": [[{ id: "t1" }]],
      });

    const live = buildClient(responses());
    const result = await run({ query: live.query });
    expect(result.records).toEqual({ rehomed: 0, duplicatesDeleted: 1, areaTagsCopied: 1, retirementsPropagated: 0 });
    const insert = live.calls.find((call) =>
      call.text.includes("INSERT INTO public.candidate_record_area_tags")
    );
    expect(insert?.values).toEqual(["r2", "r3"]);
    expect(insert?.text).toContain("ON CONFLICT (candidate_record_id, research_area_id) DO NOTHING");
    // The copy must land before the duplicate record's delete in the same
    // transaction, or the tags cascade away first.
    const insertIndex = live.calls.findIndex((call) =>
      call.text.includes("INSERT INTO public.candidate_record_area_tags")
    );
    const deleteIndex = live.calls.findIndex((call) =>
      call.text.includes("DELETE FROM public.candidate_records")
    );
    expect(insertIndex).toBeGreaterThan(-1);
    expect(insertIndex).toBeLessThan(deleteIndex);

    const dry = buildClient(responses());
    const dryResult = await run({ query: dry.query }, { dryRun: true });
    expect(dryResult.records.areaTagsCopied).toBe(1);
    expect(dry.calls.some((call) => /^\s*INSERT\b/i.test(call.text))).toBe(false);
  });

  it("remaps events from duplicate-deleted records onto the survivor's copy", async () => {
    const responses = (survivorAlreadyNotified: boolean) =>
      happyResponses({
        "FROM public.candidate_records\n        WHERE candidate_id = ANY": [
          [
            { id: "r2", candidate_id: MERGED, record_identity_key: "shared" },
            { id: "r3", candidate_id: SURVIVOR, record_identity_key: "shared" },
          ],
        ],
        "FROM public.user_candidate_follow_notification_events\n        WHERE candidate_id = ANY": [
          [
            {
              id: "e1",
              candidate_id: MERGED,
              user_id: USER_ID,
              event_type: "candidate_record_update",
              election_id: null,
              candidate_record_id: "r2",
            },
            ...(survivorAlreadyNotified
              ? [
                  {
                    id: "e2",
                    candidate_id: SURVIVOR,
                    user_id: USER_ID,
                    event_type: "candidate_record_update",
                    election_id: null,
                    candidate_record_id: "r3",
                  },
                ]
              : []),
          ],
        ],
      });

    // No survivor-side event: the pending/history event follows the content.
    const remap = buildClient(responses(false));
    const remapResult = await run({ query: remap.query });
    expect(remapResult.notificationEvents).toEqual({
      rehomed: 0,
      duplicatesDeleted: 0,
      remappedToSurvivorRecords: 1,
    });
    const remapUpdate = remap.calls.find((call) =>
      call.text.includes("candidate_record_id = $3::uuid")
    );
    expect(remapUpdate?.values).toEqual(["e1", SURVIVOR, "r3"]);

    // Survivor-side event exists: remapping would trip the partial unique
    // key, and the user already heard about this content — delete instead.
    const collide = buildClient(responses(true));
    const collideResult = await run({ query: collide.query });
    expect(collideResult.notificationEvents).toEqual({
      rehomed: 0,
      duplicatesDeleted: 1,
      remappedToSurvivorRecords: 0,
    });
    const del = collide.calls.find((call) =>
      call.text.includes("DELETE FROM public.user_candidate_follow_notification_events")
    );
    expect(del?.values).toEqual([["e1"]]);
  });

  it("archives the duplicate's differing website into the survivor's former_website_urls", async () => {
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.candidates\n        WHERE id = ANY": [
          [
            candidateRow(MERGED, {
              official_website_url: "https://pat2024.example",
              former_website_urls: ["https://pat2022.example"],
            }),
            candidateRow(SURVIVOR, {
              official_website_url: "https://pat2026.example",
            }),
          ],
        ],
      })
    );

    const result = await run({ query });

    // The survivor's populated site wins; the duplicate's current + former
    // sites keep identifying the person via the archive.
    expect(result.profile).toEqual({
      fieldsFilled: [],
      sourcesAppended: 0,
      formerWebsiteUrlsAppended: 2,
    });
    const update = calls.find((call) => call.text.includes("former_website_urls = $"));
    expect(update?.values?.[0]).toBe(SURVIVOR);
    expect(JSON.parse(update?.values?.at(-1) as string)).toEqual([
      "https://pat2022.example",
      "https://pat2024.example",
    ]);
  });

  it("counts appended former websites against the survivor's normalized baseline", async () => {
    // The survivor's stored archive carries a normalized duplicate AND its
    // own current site; measured against the raw list those would offset the
    // genuinely new URL and report 0 appended.
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.candidates\n        WHERE id = ANY": [
          [
            candidateRow(MERGED, {
              official_website_url: "https://pat2024.example",
            }),
            candidateRow(SURVIVOR, {
              official_website_url: "https://pat2026.example",
              former_website_urls: [
                "https://pat2022.example",
                "https://pat2022.example/",
                "https://pat2026.example",
              ],
            }),
          ],
        ],
      })
    );

    const result = await run({ query });

    expect(result.profile.formerWebsiteUrlsAppended).toBe(1);
    const update = calls.find((call) => call.text.includes("former_website_urls = $"));
    expect(JSON.parse(update?.values?.at(-1) as string)).toEqual([
      "https://pat2022.example",
      "https://pat2024.example",
    ]);
  });

  it("fills blank survivor profile fields from the duplicate and unions profile sources", async () => {
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.candidates\n        WHERE id = ANY": [
          [
            candidateRow(MERGED, {
              summary: "Long-form summary written on the duplicate.",
              official_website_url: "https://example.gov",
              current_office: "City Council Member",
              profile_sources: ["https://sos.example.gov/filing", "https://example.gov"],
            }),
            candidateRow(SURVIVOR, {
              summary: "   ",
              current_office: "Mayor",
              profile_sources: ["https://example.gov"],
            }),
          ],
        ],
      })
    );

    const result = await run({ query });

    // Blank summary and missing website fill in; the survivor's populated
    // current_office wins over the duplicate's.
    expect(result.profile).toEqual({
      fieldsFilled: ["summary", "official_website_url"],
      sourcesAppended: 1,
      // The duplicate's site became the survivor's current site via the fill,
      // so nothing lands in the archive.
      formerWebsiteUrlsAppended: 0,
    });
    const update = calls.find((call) => call.text.includes("summary = $"));
    expect(update?.values?.[0]).toBe(SURVIVOR);
    expect(update?.values).toContain("Long-form summary written on the duplicate.");
    expect(update?.values).toContain("https://example.gov");
    expect(JSON.parse(update?.values?.at(-1) as string)).toEqual([
      "https://example.gov",
      "https://sos.example.gov/filing",
    ]);
    expect(update?.text).not.toContain("current_office");
  });

  it("refuses when both candidates have rows in the same referencing table", async () => {
    const { query } = buildClient(
      happyResponses({
        "FROM public.az_candidate_finance_links WHERE candidate_id": [[{ n: "2" }], [{ n: "1" }]],
      })
    );

    await expect(run({ query })).rejects.toThrow(
      /Both candidates have rows in public\.az_candidate_finance_links\.candidate_id \(duplicate: 2, survivor: 1\)/
    );
  });

  it("collapses merge chains onto the survivor", async () => {
    const { query, calls } = buildClient(
      happyResponses({
        "SELECT id FROM public.candidates": [[{ id: "33333333-3333-3333-3333-333333333333" }]],
      })
    );

    const result = await run({ query });

    expect(result.chainCollapsedCandidates).toBe(1);
    const collapse = calls.find((call) =>
      call.text.includes("UPDATE public.candidates SET merged_into_candidate_id")
    );
    expect(collapse?.values).toEqual([MERGED, SURVIVOR]);
  });

  it("unions hard identifiers onto the survivor, deduped case-insensitively", async () => {
    const { query, calls } = buildClient(
      happyResponses({
        "FROM public.candidates\n        WHERE id = ANY": [
          [
            candidateRow(MERGED, {
              fec_ids: ["S6AK00268", "s6ak00284"],
              state_filing_ids: ["orange-ca-1290"],
            }),
            candidateRow(SURVIVOR, {
              // Raw duplicate in the stored list must not mask the append.
              fec_ids: ["S6AK00268", "s6AK00268"],
              state_filing_ids: [],
            }),
          ],
        ],
      })
    );

    const result = await run({ query });

    expect(result.identifiers).toEqual({ fecIdsAppended: 1, stateFilingIdsAppended: 1 });
    const update = calls.find((call) => call.text.includes("SET fec_ids"));
    expect(update?.values?.[0]).toBe(SURVIVOR);
    expect(JSON.parse(update?.values?.[1] as string)).toEqual(["S6AK00268", "s6ak00284"]);
    expect(JSON.parse(update?.values?.[2] as string)).toEqual(["orange-ca-1290"]);
  });
});
