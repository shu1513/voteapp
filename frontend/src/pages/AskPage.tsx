import { useRef, useState } from "react";
import { Link } from "react-router";
import { useMutation } from "@tanstack/react-query";
import { askChatbot, CHATBOT_MAX_QUESTION_LENGTH, CHATBOT_PRIVACY_NOTE, ApiError } from "@voteapp/api-client";
import type { ChatbotAskResponse, ChatbotResultCard } from "@voteapp/api-client";
import { ErrorNotice } from "../components/Status";
import { useDocumentTitle } from "../lib/useDocumentTitle";

// Ask (docs/plans/chatbot-rag.md Phase 1): single-turn Q&A against VoteApp's
// own database — deterministic template answers and search-result cards, no
// generated prose. The previous question rides along on the next ask so
// follow-ups like "what about their voting record?" keep their scope
// (deterministic carry-over, no AI).

function ResultCardLink({ card }: { card: ChatbotResultCard }) {
  const body = (
    <>
      <p className="font-semibold text-ink">{card.title}</p>
      <p className="mt-1 text-sm text-ink-soft">{card.snippet}</p>
    </>
  );
  const className = "block rounded-xl border border-line px-4 py-3 transition hover:border-ink-soft hover:bg-surface";
  // Official state resources are absolute external URLs; everything else is
  // a site-relative page. Both are server-constructed.
  if (card.url.startsWith("http://") || card.url.startsWith("https://")) {
    return (
      <a href={card.url} target="_blank" rel="noopener noreferrer" className={className}>
        {body}
        <p className="mt-1 text-xs text-ink-soft">Official resource ↗</p>
      </a>
    );
  }
  return (
    <Link to={card.url} className={className}>
      {body}
    </Link>
  );
}

function formatDataCurrentAsOf(timestamp: string): string {
  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime())) {
    return timestamp;
  }
  return parsed.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" });
}

export default function AskPage() {
  useDocumentTitle("Ask", "Ask questions about the elections and candidates on your ballot.");
  const [question, setQuestion] = useState("");
  const [exchange, setExchange] = useState<{ question: string; response: ChatbotAskResponse } | null>(null);
  // The turn BEFORE the one on screen — sent as previous_question so a
  // follow-up resolves against the last answered scope.
  const previousQuestionRef = useRef<string | null>(null);

  const ask = useMutation({
    mutationFn: (input: { question: string; previousQuestion: string | null }) =>
      askChatbot(input.question, input.previousQuestion),
    onSuccess: (response, input) => {
      setExchange({ question: input.question, response });
      previousQuestionRef.current = input.question;
    },
  });

  function submit(event: React.FormEvent) {
    event.preventDefault();
    const trimmed = question.trim();
    if (trimmed.length === 0 || ask.isPending) {
      return;
    }
    ask.mutate({ question: trimmed, previousQuestion: previousQuestionRef.current });
    setQuestion("");
  }

  const notAvailable = ask.error instanceof ApiError && ask.error.status === 404;

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="text-2xl font-extrabold tracking-tight text-ink">Ask</h1>
      <p className="mt-2 text-ink-soft">
        Ask about the November 2026 elections we cover: candidates, their records, campaign finance, elections, and
        ballot measures. Answers come only from our database — never opinions or endorsements.
      </p>

      <form onSubmit={submit} className="mt-6 flex gap-2">
        <label htmlFor="ask-question" className="sr-only">
          Your question
        </label>
        <input
          id="ask-question"
          type="text"
          value={question}
          maxLength={CHATBOT_MAX_QUESTION_LENGTH}
          onChange={(event) => setQuestion(event.target.value)}
          placeholder="e.g. Who is running for US Senate in Georgia?"
          className="min-w-0 flex-1 rounded-xl border border-line px-4 py-2.5 text-ink placeholder:text-ink-soft focus:border-ink-soft focus:outline-none"
        />
        <button
          type="submit"
          disabled={ask.isPending || question.trim().length === 0}
          className="shrink-0 rounded-xl bg-rausch px-4 py-2.5 font-semibold text-white transition hover:bg-rausch-dark disabled:opacity-50"
        >
          {ask.isPending ? "Asking…" : "Ask"}
        </button>
      </form>
      <p className="mt-2 text-xs text-ink-soft">{CHATBOT_PRIVACY_NOTE}</p>

      {ask.isError &&
        (notAvailable ? (
          <p className="mt-6 rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink-soft">
            Ask isn't available right now. Please try again later.
          </p>
        ) : (
          <div className="mt-6">
            <ErrorNotice error={ask.error} />
          </div>
        ))}

      {exchange && !ask.isPending && (
        <section className="mt-8" aria-live="polite">
          <p className="text-sm font-semibold text-ink-soft">You asked</p>
          <p className="mt-1 text-ink">{exchange.question}</p>
          <p className="mt-4 whitespace-pre-line text-ink">{exchange.response.answer}</p>
          {exchange.response.results.length > 0 && (
            <ul className="mt-4 space-y-3">
              {exchange.response.results.map((card) => (
                <li key={card.url}>
                  <ResultCardLink card={card} />
                </li>
              ))}
            </ul>
          )}
          {exchange.response.data_current_as_of && (
            <p className="mt-4 text-xs text-ink-soft">
              Data current as of {formatDataCurrentAsOf(exchange.response.data_current_as_of)}. Always verify with
              official sources.
            </p>
          )}
        </section>
      )}
    </div>
  );
}
