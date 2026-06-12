import { getStateNameByFips } from "../../constants/usStates.js";

export type PresidentialPrimaryDatePromptInput = {
  cycleId: string;
  electionName: string;
  electionYear: number;
  party: string;
  stateFipsList: readonly string[];
  scheduledFor: string;
  reviewFeedbackLines?: readonly string[];
};

export const PRESIDENTIAL_PRIMARY_DATE_PROMPT_MAX_STATES = 10;

function q(value: string): string {
  return JSON.stringify(value);
}

function assertPresidentialElectionYear(electionYear: number): void {
  if (!Number.isInteger(electionYear) || electionYear < 2000 || electionYear > 2100 || electionYear % 4 !== 0) {
    throw new Error(`Invalid presidential primary date prompt election year: ${electionYear}`);
  }
}

function normalizeParty(party: string): string {
  const normalized = party.trim();
  if (normalized.length === 0) {
    throw new Error("Presidential primary date prompt party is required");
  }
  return normalized;
}

function normalizeStateFipsList(stateFipsList: readonly string[]): string[] {
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const stateFips of stateFipsList) {
    const trimmed = stateFips.trim();
    if (!/^[0-9]{2}$/.test(trimmed)) {
      throw new Error(`Invalid presidential primary date prompt state_fips: ${stateFips}`);
    }
    getStateNameByFips(trimmed);
    if (seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    normalized.push(trimmed);
  }
  if (normalized.length === 0) {
    throw new Error("Presidential primary date prompt requires at least one state_fips");
  }
  if (normalized.length > PRESIDENTIAL_PRIMARY_DATE_PROMPT_MAX_STATES) {
    throw new Error(
      `Presidential primary date prompt accepts at most ${PRESIDENTIAL_PRIMARY_DATE_PROMPT_MAX_STATES} states, got ${normalized.length}`
    );
  }
  return normalized;
}

function formatStateContext(stateFips: string): string {
  return `- state_fips: ${q(stateFips)}; state_name: ${q(getStateNameByFips(stateFips))}`;
}

export function buildPresidentialPrimaryDatePrompt(input: PresidentialPrimaryDatePromptInput): string {
  assertPresidentialElectionYear(input.electionYear);
  const party = normalizeParty(input.party);
  const stateFipsList = normalizeStateFipsList(input.stateFipsList);
  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];
  const electionName = input.electionName.trim();
  if (electionName.length === 0) {
    throw new Error("Presidential primary date prompt election_name is required");
  }

  return [
    "You are researching official U.S. presidential primary election dates for a known election cycle.",
    "Return strict JSON only.",
    "",
    "Cycle context:",
    `- presidential_cycle_id: ${q(input.cycleId)}`,
    `- election_name: ${q(electionName)}`,
    `- election_year: ${input.electionYear}`,
    `- party: ${q(party)}`,
    `- scheduled_for: ${q(input.scheduledFor)}`,
    "",
    "States to research:",
    ...stateFipsList.map(formatStateContext),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "results": [',
    "    {",
    '      "state_fips": "copy exactly from the provided state_fips",',
    '      "state_name": "copy exactly from the provided state_name",',
    '      "status": "official_found|not_official_yet",',
    '      "primary_date": "YYYY-MM-DD when officially set, otherwise null",',
    '      "sources": ["https://..."],',
    '      "notes": "short factual note, or empty string"',
    "    }",
    "  ]",
    "}",
    "",
    "Rules:",
    "- Return exactly one result row for each provided state_fips; do not return rows for unprovided states.",
    `- Research only the ${input.electionYear} ${party} presidential primary or presidential preference primary date.`,
    "- Use official sources for official_found: state election office, secretary of state, official state election calendar, official national party page, or official statute/calendar page. News articles, blogs, Wikipedia, and unofficial calendars are not sufficient for official_found.",
    "- If an official date is set, return status=\"official_found\" and primary_date as YYYY-MM-DD.",
    "- If no official date is set yet, return status=\"not_official_yet\" and primary_date=null.",
    "- Do not infer a date from prior cycles unless an official source says that date applies to this election year and party.",
    "- primary_date, when present, must be in the election_year.",
    "- Each result must include at least one source URL for the official page checked.",
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
