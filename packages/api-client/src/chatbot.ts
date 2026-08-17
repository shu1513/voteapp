// Chatbot "Ask" contract + call (docs/plans/chatbot-rag.md). Phase 1 is
// retrieval-only: deterministic template answers and search-result cards,
// no generated prose. One isolated module so removing the feature deletes
// this file and its index export line.

import { apiRequest } from "./client";

export type ChatbotAskOutcome = "template" | "retrieval" | "clarify" | "refuse_no_data" | "refuse_policy";

export type ChatbotResultCard = {
  title: string;
  /** Site-relative page URL, or an official state resource URL (absolute).
   * Always server-constructed. */
  url: string;
  snippet: string;
  source_type: string;
};

export type ChatbotAskResponse = {
  outcome: ChatbotAskOutcome;
  answer: string;
  results: ChatbotResultCard[];
  /** When the answer came from the search index: its build time. Null for
   * deterministic template answers. */
  data_current_as_of: string | null;
  /** True when the answer prose is model-generated (Phase 2). The widget
   * shows the AI label + report control for these. Absent on template,
   * clarify, refusal, and result-card answers. */
  ai_generated?: boolean;
  /** Opaque one-shot token for submitChatbotFeedback. Present on every
   * answer when the server mints them; absent → hide the thumbs. */
  feedback_token?: string;
};

export const CHATBOT_MAX_QUESTION_LENGTH = 500;

export const CHATBOT_PRIVACY_NOTE =
  "Don't include personal information (like your address) in questions.";

/** The candidate or election page the user is (or was last) looking at. The
 * server only applies it to questions that point at it ("this candidate"),
 * so sending the most recent one on every ask is safe. */
export type ChatbotAskContext =
  | { kind: "candidate"; id: string }
  | { kind: "election"; id: string };

export type AskChatbotOptions = {
  previousQuestion?: string | null;
  context?: ChatbotAskContext | null;
  signal?: AbortSignal;
};

export type ChatbotFeedbackVerdict = "up" | "down";

/** One-shot 👍/👎 on an answer. The token came from that answer's ask
 * response; the server ignores duplicate votes on the same token. */
export function submitChatbotFeedback(token: string, verdict: ChatbotFeedbackVerdict): Promise<{ status: "ok" }> {
  return apiRequest<{ status: "ok" }>("/api/chatbot/feedback", {
    method: "POST",
    body: { token, verdict },
  });
}

export function askChatbot(question: string, options: AskChatbotOptions = {}): Promise<ChatbotAskResponse> {
  const { previousQuestion, context, signal } = options;
  return apiRequest<ChatbotAskResponse>("/api/chatbot/ask", {
    method: "POST",
    body: {
      question,
      ...(previousQuestion ? { previous_question: previousQuestion } : {}),
      ...(context
        ? { context: context.kind === "candidate" ? { candidate_id: context.id } : { election_id: context.id } }
        : {}),
    },
    signal,
  });
}
