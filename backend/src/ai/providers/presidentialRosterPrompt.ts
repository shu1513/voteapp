export type PresidentialRosterPromptStage = "primary" | "general";

export type PresidentialRosterPromptInput = {
  cycleId: string;
  electionYear: number;
  stage: PresidentialRosterPromptStage;
  party: string | null;
  reviewFeedbackLines?: readonly string[];
};

function q(value: string): string {
  return JSON.stringify(value);
}

function assertPresidentialElectionYear(electionYear: number): void {
  if (!Number.isInteger(electionYear) || electionYear < 2000 || electionYear > 2100 || electionYear % 4 !== 0) {
    throw new Error(`Invalid presidential roster prompt election year: ${electionYear}`);
  }
}

function normalizeCycleId(cycleId: string): string {
  const normalized = cycleId.trim();
  if (normalized.length === 0) {
    throw new Error("Presidential roster prompt cycle_id is required");
  }
  return normalized;
}

function normalizeParty(input: PresidentialRosterPromptInput): string | null {
  const party = input.party?.trim() ?? "";
  if (input.stage === "primary") {
    if (party.length === 0) {
      throw new Error("Presidential roster prompt primary party is required");
    }
    return party;
  }
  if (input.stage === "general" && party.length > 0) {
    throw new Error("Presidential roster prompt general party must be null");
  }
  return null;
}

function electionName(input: {
  electionYear: number;
  stage: PresidentialRosterPromptStage;
  party: string | null;
}): string {
  if (input.stage === "primary") {
    return `${input.electionYear} ${input.party} presidential primary`;
  }
  return `${input.electionYear} presidential general election`;
}

export function buildPresidentialRosterPrompt(input: PresidentialRosterPromptInput): string {
  assertPresidentialElectionYear(input.electionYear);
  const cycleId = normalizeCycleId(input.cycleId);
  const party = normalizeParty(input);
  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];
  const name = electionName({ electionYear: input.electionYear, stage: input.stage, party });

  return [
    "You are researching the candidate roster for one U.S. presidential election cycle.",
    "Return strict JSON only.",
    "",
    "Cycle context:",
    `- presidential_cycle_id: ${q(cycleId)}`,
    `- election_name: ${q(name)}`,
    `- election_year: ${input.electionYear}`,
    `- stage: ${q(input.stage)}`,
    ...(party ? [`- party: ${q(party)}`] : []),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "candidates": [',
    "    {",
    '      "display_name": "candidate name as used publicly; ballot-listed name if available",',
    '      "party": "party label",',
    '      "fec_candidate_id": "FEC presidential candidate ID if known, otherwise omit",',
    '      "sources": ["https://..."],',
    '      "status": "active|withdrawn"',
    "    }",
    "  ]",
    "}",
    "",
    "Rules:",
    "- Actively search the public web for this presidential cycle.",
    ...(input.stage === "primary" && party
      ? [
          `- Return only candidates meaningfully running for the ${input.electionYear} ${party} presidential nomination.`,
          "- Do not return independent candidates, third-party candidates, or general-election-only candidates.",
          `- Every candidate.party must be ${q(party)} or an obvious equivalent label for that party.`,
        ]
      : [
          "- Return only candidates meaningfully running in the presidential general election.",
          "- Include major-party nominees and ballot-qualified third-party or independent candidates when clearly known.",
        ]),
    "- Do not return every person who filed an FEC statement; include candidates with meaningful public campaign activity or credible public/party coverage.",
    "- Include fec_candidate_id when you can identify a matching OpenFEC/FEC presidential candidate ID.",
    "- status must be active for currently running candidates and withdrawn for candidates who publicly suspended, ended, or withdrew from this cycle.",
    "- Each candidate must include at least one supporting source URL.",
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
