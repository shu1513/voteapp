import { describe, expect, it } from "vitest";

import {
  CANDIDATES_AI_CANDIDATES,
  DEFAULT_AI_CANDIDATE,
  ELECTIONS_AI_CANDIDATES,
  PRESIDENTIAL_PROFILE_AI_CANDIDATES,
  PRESIDENTIAL_ROSTER_AI_CANDIDATES,
  STATE_RESOURCES_AI_CANDIDATES,
} from "../../src/ai/aiCandidates.ts";

const expectedStateResourcesCandidates = [
  { provider: "gemini", model: "gemini-2.5-flash-lite" },
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

const expectedElectionsCandidates = [
  { provider: "gemini", model: "gemini-2.5-flash-lite" },
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.5" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

const expectedCandidatesWorkflowCandidates = [
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

const expectedPresidentialRosterCandidates = [
  { provider: "claude", model: "claude-opus-4-8" },
  { provider: "openai", model: "gpt-5.5" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

const expectedPresidentialProfileCandidates = [
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.5" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

describe("state/elections AI candidate lists", () => {
  it("contains the current ordered model cycle for state resources", () => {
    expect(STATE_RESOURCES_AI_CANDIDATES).toEqual(expectedStateResourcesCandidates);
  });

  it("contains the current ordered model cycle for elections", () => {
    expect(ELECTIONS_AI_CANDIDATES).toEqual(expectedElectionsCandidates);
  });

  it("contains the current ordered model cycle for candidates workflow", () => {
    expect(CANDIDATES_AI_CANDIDATES).toEqual(expectedCandidatesWorkflowCandidates);
  });

  it("contains the current ordered model cycle for presidential roster research", () => {
    expect(PRESIDENTIAL_ROSTER_AI_CANDIDATES).toEqual(expectedPresidentialRosterCandidates);
  });

  it("contains the current ordered model cycle for presidential profile research", () => {
    expect(PRESIDENTIAL_PROFILE_AI_CANDIDATES).toEqual(expectedPresidentialProfileCandidates);
  });

  it("exposes the first state-resources entry as default", () => {
    expect(DEFAULT_AI_CANDIDATE).toEqual(STATE_RESOURCES_AI_CANDIDATES[0]);
    expect(DEFAULT_AI_CANDIDATE).toEqual(
      { provider: "gemini", model: "gemini-2.5-flash-lite" },
    );
  });
});
