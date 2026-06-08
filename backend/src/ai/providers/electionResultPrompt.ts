import type { ElectionResultContext } from "../../pipeline/electionResults/electionResultContextLoader.js";
import type { ElectionResultPassType } from "../../types/electionResults.js";

export type ElectionResultPromptInput = {
  passType: ElectionResultPassType;
  scheduledFor: string;
  contexts: readonly ElectionResultContext[];
  reviewFeedbackLines?: readonly string[];
};

function formatStringList(values: readonly string[]): string {
  if (values.length === 0) {
    return "none";
  }
  return values.map((value) => `"${value}"`).join(", ");
}

function formatCandidateRoster(context: ElectionResultContext): string[] {
  if (context.candidates.length === 0) {
    return ["  candidates: []"];
  }

  return [
    "  candidates:",
    ...context.candidates.map((candidate) =>
      [
        `    - candidate_election_id: "${candidate.candidateElectionId}"`,
        `candidate_id: "${candidate.candidateId}"`,
        `name: "${candidate.displayName}"`,
        `party: "${candidate.party}"`,
        `status: "${candidate.status}"`,
        `fec_ids: [${formatStringList(candidate.fecIds)}]`,
        `state_filing_ids: [${formatStringList(candidate.stateFilingIds)}]`,
      ].join("; ")
    ),
  ];
}

function formatElectionContext(context: ElectionResultContext): string[] {
  return [
    `- election_id: "${context.electionId}"`,
    `  race_type: "${context.raceType}"`,
    `  official_ballot_title: "${context.officialBallotTitle}"`,
    `  election_date: "${context.electionDate}"`,
    `  district_name: "${context.district.name}"`,
    `  district_type: "${context.district.districtType}"`,
    `  state: "${context.district.state}"`,
    ...(context.electionStage ? [`  election_stage: "${context.electionStage}"`] : []),
    ...(context.discoveryContestFamily
      ? [`  discovery_contest_family: "${context.discoveryContestFamily}"`]
      : []),
    ...(context.sourceUrls.length > 0
      ? [`  known_election_source_urls: [${formatStringList(context.sourceUrls)}]`]
      : []),
    ...(context.ballotMeasure
      ? [
          `  ballot_measure_id: "${context.ballotMeasure.ballotMeasureId}"`,
          ...(context.ballotMeasure.officialMeasureUrl
            ? [`  official_measure_url: "${context.ballotMeasure.officialMeasureUrl}"`]
            : []),
        ]
      : []),
    ...formatCandidateRoster(context),
  ];
}

export function buildElectionResultPrompt(input: ElectionResultPromptInput): string {
  if (input.contexts.length === 0) {
    throw new Error("Election result prompt requires at least one election context");
  }
  if (input.contexts.length > 10) {
    throw new Error(`Election result prompt accepts at most 10 elections, got ${input.contexts.length}`);
  }

  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];

  return [
    "You are researching election results for a small batch of known elections.",
    "Return strict JSON only.",
    "",
    "Search context:",
    `- pass_type: "${input.passType}"`,
    `- scheduled_for: "${input.scheduledFor}"`,
    "",
    "Elections to research:",
    ...input.contexts.flatMap(formatElectionContext),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "results": [',
    "    {",
    '      "election_id": "provided election_id",',
    '      "result_status": "projected|unofficial|certified|not_found|not_final_yet",',
    '      "outcome": "won|advanced|runoff|too_close|unknown OR passed|failed|unknown for ballot measures",',
    '      "winners": [',
    "        {",
    '          "candidate_election_id": "provided candidate_election_id when matched",',
    '          "candidate_name": "required only when no candidate_election_id is available",',
    '          "party": "optional only when no candidate_election_id is available"',
    "        }",
    "      ],",
    '      "source_url": "https://...",',
    '      "source_type": "official|ap|news|other",',
    '      "notes": "short factual note, or empty string"',
    "    }",
    "  ]",
    "}",
    "",
    "Rules:",
    "- Return exactly one result row for each provided election_id, and no rows for any other election_id.",
    "- If results are not available, return result_status=\"not_found\", outcome=\"unknown\", winners=[], and cite the page/source checked.",
    "- If the certified-results pass is not complete yet, return result_status=\"not_final_yet\", outcome=\"unknown\", winners=[], and cite the official source showing certification is not final.",
    "- For unofficial, certified, and not_final_yet, use an official election-authority source.",
    "- AP/news sources are allowed only when result_status=\"projected\".",
    "- Candidate winners must use the provided candidate_election_id when the winner appears in the provided roster.",
    "- If a winner is a write-in or missing from the provided roster, include candidate_name and party if known, but omit candidate_election_id and candidate_id.",
    "- For office races, winners is the winner/advancer set; multi-winner races may include multiple winners.",
    "- For ballot measures, winners must be [] and outcome must be passed/failed/unknown.",
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
