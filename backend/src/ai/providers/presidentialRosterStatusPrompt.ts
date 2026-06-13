export type PresidentialRosterStatusPromptStage = "primary" | "general";

export type PresidentialRosterStatusPromptCandidate = {
  candidateId: string;
  displayName: string;
  party: string;
  fecIds: readonly string[];
  sources?: readonly string[];
};

export type PresidentialRosterStatusPromptInput = {
  cycleId: string;
  electionYear: number;
  stage: PresidentialRosterStatusPromptStage;
  party: string | null;
  candidates: readonly PresidentialRosterStatusPromptCandidate[];
  reviewFeedbackLines?: readonly string[];
};

function q(value: string): string {
  return JSON.stringify(value);
}

function assertPresidentialElectionYear(electionYear: number): void {
  if (!Number.isInteger(electionYear) || electionYear < 2000 || electionYear > 2100 || electionYear % 4 !== 0) {
    throw new Error(`Invalid presidential roster status prompt election year: ${electionYear}`);
  }
}

function normalizeNonEmpty(value: string, fieldName: string): string {
  const normalized = value.trim();
  if (normalized.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return normalized;
}

function normalizeParty(input: PresidentialRosterStatusPromptInput): string | null {
  const party = input.party?.trim() ?? "";
  if (input.stage === "primary") {
    if (party.length === 0) {
      throw new Error("Presidential roster status prompt primary party is required");
    }
    return party;
  }
  if (input.stage === "general" && party.length > 0) {
    throw new Error("Presidential roster status prompt general party must be null");
  }
  return null;
}

function electionName(input: {
  electionYear: number;
  stage: PresidentialRosterStatusPromptStage;
  party: string | null;
}): string {
  if (input.stage === "primary") {
    return `${input.electionYear} ${input.party} presidential primary`;
  }
  return `${input.electionYear} presidential general election`;
}

function formatStringList(values: readonly string[]): string {
  if (values.length === 0) {
    return "[]";
  }
  return `[${values.map((value) => q(value)).join(", ")}]`;
}

function normalizeCandidate(input: PresidentialRosterStatusPromptCandidate): PresidentialRosterStatusPromptCandidate {
  const candidateId = normalizeNonEmpty(input.candidateId, "candidate_id");
  const displayName = normalizeNonEmpty(input.displayName, `candidate ${candidateId} display_name`);
  const party = normalizeNonEmpty(input.party, `candidate ${candidateId} party`);
  const fecIds = [...new Set(input.fecIds.map((value) => value.trim().toUpperCase()).filter((value) => value.length > 0))];
  if (fecIds.length === 0) {
    throw new Error(`candidate ${candidateId} must include at least one FEC ID`);
  }
  return {
    candidateId,
    displayName,
    party,
    fecIds,
    sources: [...new Set((input.sources ?? []).map((value) => value.trim()).filter((value) => value.length > 0))],
  };
}

function formatCandidate(candidate: PresidentialRosterStatusPromptCandidate): string[] {
  return [
    `- candidate_id: ${q(candidate.candidateId)}`,
    `  display_name: ${q(candidate.displayName)}`,
    `  party: ${q(candidate.party)}`,
    `  fec_ids: ${formatStringList(candidate.fecIds)}`,
    ...(candidate.sources && candidate.sources.length > 0
      ? [`  existing_sources: ${formatStringList(candidate.sources)}`]
      : []),
  ];
}

export function buildPresidentialRosterStatusPrompt(input: PresidentialRosterStatusPromptInput): string {
  assertPresidentialElectionYear(input.electionYear);
  const cycleId = normalizeNonEmpty(input.cycleId, "presidential_cycle_id");
  const party = normalizeParty(input);
  const candidates = input.candidates.map((candidate) => normalizeCandidate(candidate));
  if (candidates.length === 0) {
    throw new Error("At least one omitted presidential candidate is required");
  }
  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];
  const name = electionName({ electionYear: input.electionYear, stage: input.stage, party });

  return [
    "You are verifying the current status of presidential candidates omitted from the latest roster search.",
    "Return strict JSON only.",
    "",
    "Context:",
    `- presidential_cycle_id: ${q(cycleId)}`,
    `- election_name: ${q(name)}`,
    `- election_year: ${input.electionYear}`,
    `- stage: ${q(input.stage)}`,
    ...(party ? [`- party: ${q(party)}`] : []),
    "",
    "Candidates to verify:",
    ...candidates.flatMap((candidate) => formatCandidate(candidate)),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "candidates": [',
    "    {",
    '      "candidate_id": "one of the provided candidate_id values",',
    '      "status": "active|withdrawn|unknown",',
    '      "sources": ["https://..."],',
    '      "notes": "short reason"',
    "    }",
    "  ]",
    "}",
    "",
    "Rules:",
    "- Return exactly one result row for each provided candidate_id.",
    "- Do not return rows for candidates not listed in Candidates to verify.",
    "- Use status=withdrawn only when there is clear evidence the candidate suspended, ended, withdrew, or is no longer running for this cycle.",
    "- Use status=active when there is evidence the candidate is still running for this cycle.",
    "- Use status=unknown when evidence is unclear, stale, conflicting, or unavailable.",
    "- Do not infer withdrawal just because the candidate was missing from the latest roster list.",
    "- Each result must include at least one source URL for the page checked.",
    "- notes must briefly explain the evidence behind the status.",
    "- return JSON only (no prose, no markdown).",
    ...(reviewFeedbackLines.length > 0
      ? [
          "",
          "Previous feedback to fix:",
          ...reviewFeedbackLines.map((line, index) => `${index + 1}. ${line}`),
          "Fix only the issues above and keep valid content.",
        ]
      : []),
  ].join("\n");
}
