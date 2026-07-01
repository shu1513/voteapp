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
    '      "display_name": "candidate name; ballot-listed name if available",',
    '      "party": "party label",',
    '      "fec_candidate_id": "required FEC presidential candidate ID",',
    '      "sources": ["https://..."],',
    '      "qualification_evidence": [',
    "        {",
    '          "kind": "official_campaign_website|public_campaign_launch|party_recognized_candidate_page|ballot_access|primary_ballot_listing",',
    '          "source_url": "https://...",',
    '          "description": "short source-backed reason this proves the person is a real campaign/ballot candidate beyond an FEC filing"',
    "        }",
    "      ],",
    '      "status": "active|withdrawn",',
    '      "running_mate": {',
    '        "display_name": "officially announced running mate name",',
    '        "fec_candidate_id": "optional FEC candidate ID if known",',
    '        "sources": ["https://..."]',
    "      }",
    "    }",
    "  ]",
    "}",
    "",
    "Rules:",
    "- Actively search the public web for this presidential cycle.",
    ...(input.stage === "primary" && party
      ? [
          `- Return only candidates formally running for the ${input.electionYear} ${party} presidential nomination.`,
        ]
      : [
          "- Return only candidates formally running in the presidential general election.",
          "- Include major-party nominees and ballot-qualified third-party or independent candidates when clearly known.",
        ]),
    "- FEC filing alone is not enough. Do not return a person whose only evidence is an FEC filing, FEC committee row, or OpenFEC candidate page.",
    "- Only return people with both a matching presidential FEC candidate ID and at least one additional source-backed qualification signal.",
    "- Acceptable qualification_evidence kinds are: official_campaign_website, public_campaign_launch, party_recognized_candidate_page, ballot_access, primary_ballot_listing.",
    "- qualification_evidence.source_url must support the qualification signal and must not be an FEC/OpenFEC URL.",
    "- Include fec_candidate_id for every candidate. If you cannot identify a matching presidential FEC candidate ID, omit that person.",
    "- Include running_mate only if the candidate has officially announced a running mate; omit running_mate if none is officially announced.",
    "- Do not include speculative, rumored, shortlist, possible, or expected running mates.",
    "- If running_mate is included, it must include at least one supporting source URL.",
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
