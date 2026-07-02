import { isUsSenateOfficeTitle } from "../../utils/senateOffice.js";
import type { ElectionContestFamily } from "../../types/election.js";

export type CandidateRecordDiscoveryPromptInput = {
  candidateDisplayName: string;
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  discoveryContestFamily?: ElectionContestFamily | null;
  sinceDate?: string | null;
  reviewFeedbackLines?: readonly string[];
};

function shouldIncludeState(input: CandidateRecordDiscoveryPromptInput): boolean {
  return !(input.districtType === "presidential" && input.state.trim().toUpperCase() === "US");
}

export function buildCandidateRecordDiscoveryPrompt(input: CandidateRecordDiscoveryPromptInput): string {
  const includeSenateContext = isUsSenateOfficeTitle(input.officialBallotTitle);
  const useJudicialRecordObjective = input.discoveryContestFamily === "judicial_office";
  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];
  const objectiveRule = useJudicialRecordObjective
    ? "- Research the web for publicly available reliable records about this exact judicial candidate, covering each of: notable cases or rulings they handled (as judge, or as prosecutor/defense/counsel before taking the bench); any discipline, ethics complaints, reversals, or conduct-commission proceedings; and endorsements they made or received. Focus on evidence for evaluating legal competence, integrity and ethics, impartiality, and professional record."
    : "- Research reliable public records about this exact candidate. If the candidate holds or has EVER held public office, cover each of: major votes they cast and legislation they sponsored; actions taken with executive power (appointments, vetoes, budgets, agency decisions); any court, ethics, or disciplinary proceedings, campaign finance issues, or conflicts of interest; and committees, caucuses, or organizations they led. If the candidate never held public office, cover each of: their career record (companies founded or run, professional work with public impact); organizations, nonprofits, unions, boards, or commissions they led or served on and public advocacy they have done; and any court, legal, or regulatory records.";

  return [
    "You are researching substantive public records about one election candidate.",
    "Focus on records that evaluate fitness/competence for this office and the candidate's background relevant to office duties.",
    "Return strict JSON only.",
    "",
    "Candidate + election context:",
    `- candidate_display_name: "${input.candidateDisplayName}"`,
    `- district_name: "${input.districtName}"`,
    `- district_type: "${input.districtType}"`,
    ...(shouldIncludeState(input) ? [`- state: "${input.state}"`] : []),
    `- election_date: "${input.electionDate}"`,
    `- official_ballot_title: "${input.officialBallotTitle}"`,
    ...(input.discoveryContestFamily ? [`- discovery_contest_family: "${input.discoveryContestFamily}"`] : []),
    ...(input.electionStage ? [`- election_stage: "${input.electionStage}"`] : []),
    ...(includeSenateContext && input.senateClass ? [`- senate_class: "${input.senateClass}"`] : []),
    ...(includeSenateContext && input.termEndYear ? [`- term_end_year: "${input.termEndYear}"`] : []),
    ...(input.sinceDate ? [`- since_date: "${input.sinceDate}"`] : []),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "records": [',
    "    {",
    '      "description": "neutral factual description of the record",',
    '      "source_url": "https://...",',
    '      "event_date": "YYYY-MM-DD"',
    "    }",
    "  ]",
    "}",
    "",
    "Rules:",
    objectiveRule,
    ...(useJudicialRecordObjective
      ? [
          "- Describe what the candidate actually did in the case and its effects/impacts.",
        ]
      : []),
    ...(input.sinceDate ? ['- Include only records with event_date >= since_date.'] : []),
    "- Return the comprehensive set of substantive records across those categories, proportionate to the candidate's actual public career — do not stop at one or two representative records for a documented public figure. There is no target number of records.",
    "- Include both favorable and unfavorable records when they exist; a long public career with only one direction represented is incomplete research.",
    "- records may be an empty array if no reliable actual action/service/accountability records are found.",
    "- Official ballot, Secretary of State, election-office, or qualified-candidate listings are roster evidence, not candidate record evidence.",
    "- Do not include filing-to-run, candidacy announcements, ballot qualification, ballot listing, campaign launch, or campaign promise rows as records.",
    "- If the only reliable sources prove the person is running but do not show an actual action, public service, leadership role, vote, official decision, litigation/enforcement record, endorsement, or other accountability record, return {\"records\": []}.",
    "- Each record must include source_url and event_date.",
    "- event_date must be YYYY-MM-DD; use the action/event date when known, otherwise use the source publication date.",
    "- If neither action/event date nor publication date is available, omit that record.",
    "- Use one row per concrete record; do not duplicate the same source/event.",
    "- Keep descriptions neutral and factual.",
    "- For damaging claims, require official/legal sources or reputable news and do not state allegations as proven facts.",
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
