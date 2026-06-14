export type PresidentialNomineePromptCandidate = {
  candidateId: string;
  displayName: string;
  party: string;
  fecIds: readonly string[];
  sources?: readonly string[];
};

export type PresidentialNomineePromptInput = {
  cycleId: string;
  electionYear: number;
  party: string;
  candidates: readonly PresidentialNomineePromptCandidate[];
  reviewFeedbackLines?: readonly string[];
};

function q(value: string): string {
  return JSON.stringify(value);
}

function assertPresidentialElectionYear(electionYear: number): void {
  if (!Number.isInteger(electionYear) || electionYear < 2000 || electionYear > 2100 || electionYear % 4 !== 0) {
    throw new Error(`Invalid presidential nominee prompt election year: ${electionYear}`);
  }
}

function normalizeNonEmpty(value: string, fieldName: string): string {
  const normalized = value.trim();
  if (normalized.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return normalized;
}

function normalizeStringList(values: readonly string[] | undefined): string[] {
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const value of values ?? []) {
    const trimmed = value.trim();
    if (trimmed.length === 0 || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    normalized.push(trimmed);
  }
  return normalized;
}

function normalizeCandidate(input: PresidentialNomineePromptCandidate): PresidentialNomineePromptCandidate {
  const candidateId = normalizeNonEmpty(input.candidateId, "candidate_id");
  return {
    candidateId,
    displayName: normalizeNonEmpty(input.displayName, `candidate ${candidateId} display_name`),
    party: normalizeNonEmpty(input.party, `candidate ${candidateId} party`),
    fecIds: normalizeStringList(input.fecIds).map((value) => value.toUpperCase()),
    sources: normalizeStringList(input.sources),
  };
}

function formatStringList(values: readonly string[]): string {
  return `[${values.map((value) => q(value)).join(", ")}]`;
}

function formatCandidate(candidate: PresidentialNomineePromptCandidate): string[] {
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

export function buildPresidentialNomineePrompt(input: PresidentialNomineePromptInput): string {
  assertPresidentialElectionYear(input.electionYear);
  const cycleId = normalizeNonEmpty(input.cycleId, "presidential_cycle_id");
  const party = normalizeNonEmpty(input.party, "presidential nominee prompt party");
  const candidates = input.candidates.map((candidate) => normalizeCandidate(candidate));
  if (candidates.length === 0) {
    throw new Error("At least one presidential primary candidate is required");
  }
  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];
  const electionName = `${input.electionYear} ${party} presidential primary`;

  return [
    "You are researching whether one U.S. presidential primary has a nominee.",
    "Return strict JSON only.",
    "",
    "Cycle context:",
    `- presidential_cycle_id: ${q(cycleId)}`,
    `- election_name: ${q(electionName)}`,
    `- election_year: ${input.electionYear}`,
    '- stage: "primary"',
    `- party: ${q(party)}`,
    "",
    "Known active primary candidates:",
    ...candidates.flatMap((candidate) => formatCandidate(candidate)),
    "",
    "Return JSON with this exact shape when a nominee is known:",
    "{",
    '  "nominee_found": true,',
    '  "candidate_name": "nominee name",',
    '  "fec_candidate_id": "FEC presidential candidate ID if known, otherwise omit",',
    '  "sources": ["https://..."]',
    "}",
    "",
    "Return JSON with this exact shape when no nominee is known yet:",
    "{",
    '  "nominee_found": false,',
    '  "sources": ["https://..."]',
    "}",
    "",
    "Rules:",
    `- Research only the ${input.electionYear} ${party} presidential primary.`,
    "- nominee_found=true only when one of these is clearly true:",
    "  1. the party has officially nominated the candidate,",
    "  2. the candidate has clinched a majority of delegates needed for nomination,",
    "  3. all meaningful remaining competitors have suspended/withdrawn and credible sources describe this candidate as the presumptive nominee.",
    "- Do not set nominee_found=true based only on polling, fundraising, endorsements, debate performance, media momentum, betting markets, ballot access, or being the frontrunner.",
    "- Use nominee_found=false when the candidate is only leading, favored, likely, expected, projected by commentary, or when the evidence is unclear.",
    "- Use nominee_found=false when the primary is still unresolved or evidence is unclear.",
    "- Prefer nominees from the Known active primary candidates list, but do not invent a match.",
    "- Include fec_candidate_id when you can identify the nominee's OpenFEC/FEC presidential candidate ID.",
    "- Each response must include at least one source URL for the page checked.",
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
