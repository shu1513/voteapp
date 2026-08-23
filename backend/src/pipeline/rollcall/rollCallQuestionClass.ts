import type { LegislativeVoteChamber } from "./legislativeVotes.js";
import type { FederalMeasure } from "./federalMeasures.js";

// The question-class filter from docs/plans/roll-call-vote-import.md §1.
// Regex builds the queue; it never decides truth. A roll call is eligible
// (is_floor_vote = true) only when its question is a final-action class AND
// its measure is a bill or joint resolution. Simple/concurrent resolutions
// (rules for debate, budget resolutions, sense-of-Congress), nominations,
// and votes with no measure (quorum calls, Speaker election) are excluded by
// default; the plan admits exceptions only by explicit hand add.

export type RollCallQuestionClass =
  | "passage"
  | "suspension"
  | "concur_senate_amendment"
  | "conference_report"
  | "veto_override";

export type RollCallClassification = {
  isFloorVote: boolean;
  questionClass: RollCallQuestionClass | null;
  reason: string;
};

type QuestionRule = { pattern: RegExp; questionClass: RollCallQuestionClass };

// House Clerk `vote-question` values. Matched on the whitespace-collapsed,
// lower-cased text so a stray double space in the feed cannot drop a vote.
const HOUSE_RULES: readonly QuestionRule[] = [
  // "On Passage" and "On Passage, the Objections of the President to the
  // Contrary Notwithstanding" (veto override).
  { pattern: /^on passage, .*objections of the president/, questionClass: "veto_override" },
  { pattern: /^on passage(?:$|,)/, questionClass: "passage" },
  { pattern: /^on motion to suspend the rules and pass(?:$|,)/, questionClass: "suspension" },
  {
    pattern: /^on motion to (?:suspend the rules and )?concur in the senate amendments?$/,
    questionClass: "concur_senate_amendment",
  },
  {
    pattern: /^on (?:motion to suspend the rules and )?agree(?:ing)? to the conference report$/,
    questionClass: "conference_report",
  },
];

// Senate LIS `question` values.
const SENATE_RULES: readonly QuestionRule[] = [
  { pattern: /^on passage of the bill$/, questionClass: "passage" },
  { pattern: /^on the joint resolution$/, questionClass: "passage" },
  { pattern: /^on the conference report$/, questionClass: "conference_report" },
  { pattern: /^on overriding the veto$/, questionClass: "veto_override" },
];

const EXCLUDED_MEASURE_TYPES = new Set<FederalMeasure["type"]>(["hres", "hconres", "sres", "sconres", "pn"]);

function normalizeQuestion(question: string): string {
  return question.toLowerCase().replace(/\s+/g, " ").trim();
}

export function classifyFederalRollCall(input: {
  chamber: LegislativeVoteChamber;
  question: string;
  measure: FederalMeasure | null;
}): RollCallClassification {
  const rules = input.chamber === "house" ? HOUSE_RULES : SENATE_RULES;
  const normalized = normalizeQuestion(input.question);
  const rule = rules.find((candidate) => candidate.pattern.test(normalized));
  if (!rule) {
    return { isFloorVote: false, questionClass: null, reason: "excluded_question" };
  }
  if (!input.measure) {
    return { isFloorVote: false, questionClass: rule.questionClass, reason: "no_measure" };
  }
  if (EXCLUDED_MEASURE_TYPES.has(input.measure.type)) {
    return {
      isFloorVote: false,
      questionClass: rule.questionClass,
      reason: `excluded_measure:${input.measure.type}`,
    };
  }
  return { isFloorVote: true, questionClass: rule.questionClass, reason: `kept:${rule.questionClass}` };
}
