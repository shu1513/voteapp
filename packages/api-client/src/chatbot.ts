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
};

export const CHATBOT_MAX_QUESTION_LENGTH = 500;

export const CHATBOT_PRIVACY_NOTE =
  "Don't include personal information (like your address) in questions.";

export function askChatbot(
  question: string,
  previousQuestion?: string | null,
  signal?: AbortSignal
): Promise<ChatbotAskResponse> {
  return apiRequest<ChatbotAskResponse>("/api/chatbot/ask", {
    method: "POST",
    body: {
      question,
      ...(previousQuestion ? { previous_question: previousQuestion } : {}),
    },
    signal,
  });
}
