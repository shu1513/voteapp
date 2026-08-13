import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError, apiRequest } from "@voteapp/api-client";
import {
  clearBallotDraft,
  draftChoicesByElectionId,
  draftProgress,
  flushBallotDraftToAccount,
  hasDraftPicks,
  readBallotDraft,
  setDraftBallotContext,
  setDraftCandidateChoice,
  setDraftMeasureChoice,
} from "./ballotDraft";

vi.mock("@voteapp/api-client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@voteapp/api-client")>();
  return { ...actual, apiRequest: vi.fn() };
});

const mockedApiRequest = vi.mocked(apiRequest);

const RACE = { electionId: "e1", raceTitle: "Governor", electionDate: "2026-11-03" };

function pickJane(chosen = true, seatsToFill: number | null = null) {
  setDraftCandidateChoice({
    ...RACE,
    seatsToFill,
    candidateId: "c1",
    candidateName: "Jane Doe",
    chosen,
  });
}

beforeEach(() => {
  window.localStorage.clear();
  clearBallotDraft();
  mockedApiRequest.mockReset();
});

afterEach(() => {
  clearBallotDraft();
});

describe("ballotDraft store", () => {
  it("starts empty and persists a candidate pick to localStorage", () => {
    expect(hasDraftPicks(readBallotDraft())).toBe(false);
    pickJane();
    const stored = JSON.parse(window.localStorage.getItem("voteapp_ballot_draft") ?? "{}");
    expect(stored.choices.e1.picks).toEqual([
      { candidate_id: "c1", display_name: "Jane Doe", candidacy_status: "active" },
    ]);
    expect(hasDraftPicks(readBallotDraft())).toBe(true);
  });

  it("single-seat picks behave as a radio (replace), unpick removes the row", () => {
    pickJane();
    setDraftCandidateChoice({ ...RACE, seatsToFill: 1, candidateId: "c2", candidateName: "John Roe", chosen: true });
    const row = readBallotDraft().choices.e1;
    expect(row.picks.map((pick) => pick.candidate_id)).toEqual(["c2"]);
    setDraftCandidateChoice({ ...RACE, seatsToFill: 1, candidateId: "c2", candidateName: "John Roe", chosen: false });
    expect(readBallotDraft().choices.e1).toBeUndefined();
  });

  it("multi-seat picks append to the seat cap and no-op past it", () => {
    pickJane(true, 2);
    setDraftCandidateChoice({ ...RACE, seatsToFill: 2, candidateId: "c2", candidateName: "John Roe", chosen: true });
    setDraftCandidateChoice({ ...RACE, seatsToFill: 2, candidateId: "c3", candidateName: "Ann Poe", chosen: true });
    expect(readBallotDraft().choices.e1.picks.map((pick) => pick.candidate_id)).toEqual(["c1", "c2"]);
  });

  it("sets and clears a measure position; clearing removes the row", () => {
    setDraftMeasureChoice({ electionId: "m1", raceTitle: "Prop A", electionDate: "2026-11-03", position: "no" });
    expect(readBallotDraft().choices.m1.measure_position).toBe("no");
    expect(readBallotDraft().choices.m1.race_type).toBe("ballot_measure");
    setDraftMeasureChoice({ electionId: "m1", raceTitle: "Prop A", electionDate: "2026-11-03", position: null });
    expect(readBallotDraft().choices.m1).toBeUndefined();
  });

  it("treats corrupt or foreign storage as an empty draft", () => {
    window.localStorage.setItem("voteapp_ballot_draft", "{not json");
    clearBallotDraft();
    window.localStorage.setItem("voteapp_ballot_draft", "{not json");
    expect(readBallotDraft().choices).toEqual({});
    window.localStorage.setItem("voteapp_ballot_draft", JSON.stringify({ v: 99, choices: {} }));
    clearBallotDraft();
    window.localStorage.setItem("voteapp_ballot_draft", JSON.stringify({ v: 99, choices: {} }));
    expect(readBallotDraft().choices).toEqual({});
  });

  it("computes progress against the stored target", () => {
    setDraftBallotContext(["d1"], { election_date: "2026-11-03", election_ids: ["e1", "m1", "e9"] });
    expect(draftProgress(readBallotDraft())).toEqual({ picked: 0, total: 3, complete: false });
    pickJane();
    setDraftMeasureChoice({ electionId: "m1", raceTitle: "Prop A", electionDate: "2026-11-03", position: "yes" });
    expect(draftProgress(readBallotDraft())).toEqual({ picked: 2, total: 3, complete: false });
    setDraftCandidateChoice({
      electionId: "e9",
      raceTitle: "Sheriff",
      electionDate: "2026-11-03",
      seatsToFill: null,
      candidateId: "c9",
      candidateName: "Pat Law",
      chosen: true,
    });
    expect(draftProgress(readBallotDraft())).toEqual({ picked: 3, total: 3, complete: true });
    // Picks outside the target don't inflate the numerator.
    setDraftMeasureChoice({ electionId: "m2", raceTitle: "Prop B", electionDate: "2027-03-02", position: "no" });
    expect(draftProgress(readBallotDraft())?.picked).toBe(3);
  });

  it("exposes draft rows as an ElectionChoice map", () => {
    pickJane();
    const choice = draftChoicesByElectionId(readBallotDraft()).get("e1");
    expect(choice?.official_ballot_title).toBe("Governor");
    expect(choice?.race_type).toBe("office");
    expect(choice?.election_date).toBe("2026-11-03");
  });
});

describe("flushBallotDraftToAccount", () => {
  it("replays every decided row through PUT and clears the draft", async () => {
    pickJane();
    setDraftMeasureChoice({ electionId: "m1", raceTitle: "Prop A", electionDate: "2026-11-03", position: "yes" });
    mockedApiRequest.mockResolvedValue({});
    await flushBallotDraftToAccount();
    const bodies = mockedApiRequest.mock.calls.map(([, options]) => (options as { body: unknown }).body);
    expect(bodies).toEqual(
      expect.arrayContaining([
        { election_id: "e1", candidate_id: "c1", chosen: true },
        { election_id: "m1", measure_position: "yes" },
      ])
    );
    expect(hasDraftPicks(readBallotDraft())).toBe(false);
  });

  it("skips business rejections (ApiError) but keeps flushing, then clears", async () => {
    pickJane();
    setDraftMeasureChoice({ electionId: "m1", raceTitle: "Prop A", electionDate: "2026-11-03", position: "yes" });
    mockedApiRequest
      .mockRejectedValueOnce(new ApiError(400, "election_closed", "This election has ended"))
      .mockResolvedValue({});
    await flushBallotDraftToAccount();
    expect(mockedApiRequest).toHaveBeenCalledTimes(2);
    expect(hasDraftPicks(readBallotDraft())).toBe(false);
  });

  it("keeps the draft when transport fails so a later session can retry", async () => {
    pickJane();
    mockedApiRequest.mockRejectedValue(new TypeError("Failed to fetch"));
    await expect(flushBallotDraftToAccount()).rejects.toThrow("Failed to fetch");
    expect(hasDraftPicks(readBallotDraft())).toBe(true);
  });
});
