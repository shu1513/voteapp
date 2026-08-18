import { useEffect, useRef, useState } from "react";
import { Link, useLocation } from "react-router";
import { useMutation } from "@tanstack/react-query";
import {
  askChatbot,
  ApiError,
  CHATBOT_MAX_QUESTION_LENGTH,
  CHATBOT_PRIVACY_NOTE,
  submitChatbotFeedback,
  useMe,
} from "@voteapp/api-client";
import type { ChatbotAskContext, ChatbotAskResponse, ChatbotResultCard, ContentReportEntityType } from "@voteapp/api-client";

import { ReportContentButton } from "../ReportContentButton";

// Floating "Ask" widget (docs/plans/chatbot-rag.md Phase 1): a minimized
// bubble in the lower-right on most pages, expanding to a small chat box.
// Answers come only from VoteApp's database — deterministic templates and
// search-result cards, no generated prose. The widget exists to answer
// questions about whatever the user encounters while browsing: it remembers
// the current (or most recent) candidate/election page and sends it as
// context, so "tell me more about this candidate" resolves deterministically
// server-side. Verified accounts only: logged-out visitors see a register
// prompt, unverified accounts a verify prompt (server enforces with 401/403).

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

/** Current page → widget context, or null on non-detail pages. */
export function contextFromPathname(pathname: string): ChatbotAskContext | null {
  const match = /^\/(candidates|elections)\/([^/]+)$/.exec(pathname);
  if (!match || !UUID_RE.test(match[2] as string)) {
    return null;
  }
  return { kind: match[1] === "candidates" ? "candidate" : "election", id: match[2] as string };
}

/** Tappable starter questions for an empty chat, tuned to the page being
 * viewed. Deictic phrasings on detail pages exercise the server's page
 * context ("this candidate" resolves to the profile being read). */
export function starterQuestions(context: ChatbotAskContext | null): string[] {
  if (context?.kind === "candidate") {
    return ["Tell me more about this candidate", "What is their voting record?", "Who is funding their campaign?"];
  }
  if (context?.kind === "election") {
    return ["Tell me more about this election", "Who is running in this election?"];
  }
  // No register chip: the site header already links voter registration —
  // the chip slot instead showcases the personalized saved-issues match.
  return ["What can you do?", "Which races affect issues I care about?"];
}

/**
 * Where the widget stays hidden: the logged-out home page (first-visit
 * pitch), the public pick-card share page (a friend's shared link, not a
 * member browsing), and the auth flows themselves.
 */
export function isChatWidgetHidden(pathname: string, isLoggedIn: boolean): boolean {
  if (pathname === "/" && !isLoggedIn) {
    return true;
  }
  if (pathname.startsWith("/picks/")) {
    return true;
  }
  return ["/login", "/register", "/forgot-password", "/reset-password", "/verify-email", "/verify-email-change"].some(
    (route) => pathname === route || pathname.startsWith(`${route}/`)
  );
}

function ResultCardLink({ card }: { card: ChatbotResultCard }) {
  const body = (
    <>
      <p className="text-sm font-semibold text-ink">{card.title}</p>
      <p className="mt-0.5 text-xs text-ink-soft">{card.snippet}</p>
    </>
  );
  const className = "block rounded-lg border border-line px-3 py-2 transition hover:border-ink-soft hover:bg-surface";
  // Official state resources are absolute external URLs; everything else is
  // a site-relative page. Both are server-constructed.
  if (card.url.startsWith("http://") || card.url.startsWith("https://")) {
    return (
      <a href={card.url} target="_blank" rel="noopener noreferrer" className={className}>
        {body}
        <p className="mt-0.5 text-xs text-ink-soft">Official resource ↗</p>
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

type Turn = { question: string; response: ChatbotAskResponse };

/** The entity behind an AI answer's first cited source — what a content
 * report about the answer should attach to. URLs are server-constructed
 * (/candidates/<id> or /elections/<id>), so parsing them is safe here. */
export function reportTargetFromResults(
  results: readonly ChatbotResultCard[]
): { entityType: ContentReportEntityType; entityId: string } | null {
  for (const card of results) {
    const match = /^\/(candidates|elections)\/([0-9a-f-]{36})$/i.exec(card.url);
    if (match) {
      return {
        entityType: match[1] === "candidates" ? "candidate" : "election",
        entityId: match[2] as string,
      };
    }
  }
  return null;
}

/** One-shot 👍/👎 under an answer (docs/plans/chatbot-improvements-2026-08.md
 * PR 2). The thanks copy appears only after the server confirmed the vote —
 * an optimistic "Thanks" would silently lose feedback on network failures.
 * A transient failure reverts to the buttons (they ARE the retry control);
 * a 400 means the token itself was rejected (it died with a server restart),
 * so retrying can never succeed — give up honestly instead. */
function FeedbackButtons({ token }: { token: string }) {
  const feedback = useMutation({
    mutationFn: (verdict: "up" | "down") => submitChatbotFeedback(token, verdict),
  });
  if (feedback.isSuccess) {
    return <p className="text-xs text-ink-soft">Thanks for the feedback.</p>;
  }
  if (feedback.error instanceof ApiError && feedback.error.status === 400) {
    return <p className="text-xs text-ink-soft">Couldn't record feedback for this answer.</p>;
  }
  return (
    <span className="inline-flex flex-wrap items-center gap-1">
      <span className="text-xs text-ink-soft">Helpful?</span>
      <button
        type="button"
        aria-label="Good answer"
        disabled={feedback.isPending}
        onClick={() => feedback.mutate("up")}
        className="rounded px-1 text-xs transition hover:bg-surface disabled:opacity-50"
      >
        👍
      </button>
      <button
        type="button"
        aria-label="Bad answer"
        disabled={feedback.isPending}
        onClick={() => feedback.mutate("down")}
        className="rounded px-1 text-xs transition hover:bg-surface disabled:opacity-50"
      >
        👎
      </button>
      {feedback.isError && <span className="text-xs text-rausch-dark">Couldn't save — try again.</span>}
    </span>
  );
}

function TurnView({ turn, reporterEmail }: { turn: Turn; reporterEmail: string | null }) {
  const isAi = turn.response.ai_generated === true;
  const reportTarget = isAi ? reportTargetFromResults(turn.response.results) : null;
  return (
    <div>
      <p className="ml-8 rounded-xl bg-surface px-3 py-2 text-sm text-ink">{turn.question}</p>
      <p className="mt-2 whitespace-pre-line text-sm text-ink">{turn.response.answer}</p>
      {turn.response.results.length > 0 && (
        <ul className="mt-2 space-y-2">
          {/* URL alone is NOT unique: a candidate's profile/finance/record
              cards deliberately share one page URL. Static list, so the
              index disambiguates safely. */}
          {turn.response.results.map((card, index) => (
            <li key={`${index}:${card.source_type}:${card.url}`}>
              <ResultCardLink card={card} />
            </li>
          ))}
        </ul>
      )}
      {isAi ? (
        // BEHAVIOR.md rule 9: every AI answer is labeled, dated, and
        // reportable. The report attaches to the first cited entity — the
        // data behind the answer lives on that page.
        <div className="mt-1.5 flex flex-wrap items-center gap-x-2 gap-y-0.5">
          <p className="text-xs text-ink-soft">
            AI-generated from our election data — may contain mistakes.
            {turn.response.data_current_as_of &&
              ` Data current as of ${formatDataCurrentAsOf(turn.response.data_current_as_of)}.`}
          </p>
          {reportTarget && (
            <ReportContentButton
              entityType={reportTarget.entityType}
              entityId={reportTarget.entityId}
              contextLabel="this AI answer"
              reporterEmail={reporterEmail}
            />
          )}
        </div>
      ) : (
        turn.response.data_current_as_of && (
          <p className="mt-1.5 text-xs text-ink-soft">
            Data current as of {formatDataCurrentAsOf(turn.response.data_current_as_of)}. Verify with official sources.
          </p>
        )
      )}
      {turn.response.feedback_token && (
        <div className="mt-1.5">
          <FeedbackButtons token={turn.response.feedback_token} />
        </div>
      )}
    </div>
  );
}

/** The register/verify walls. The widget stays visible to logged-out
 * visitors on purpose: opening it is the acquisition prompt. */
function AccessPrompt({ kind }: { kind: "register" | "verify" }) {
  if (kind === "register") {
    return (
      <div className="px-4 py-6 text-center">
        <p className="text-sm text-ink">
          Ask questions about the candidates and elections you're looking at — free with an account.
        </p>
        <div className="mt-4 flex justify-center gap-3">
          <Link
            to="/register"
            className="rounded-lg bg-rausch px-3 py-1.5 text-sm font-semibold text-white transition hover:bg-rausch-dark"
          >
            Sign up
          </Link>
          <Link to="/login" className="rounded-lg border border-line px-3 py-1.5 text-sm font-medium text-ink">
            Log in
          </Link>
        </div>
      </div>
    );
  }
  return (
    <div className="px-4 py-6 text-center">
      <p className="text-sm text-ink">Verify your email to use Ask — check your inbox for the verification link.</p>
      <Link to="/me/settings" className="mt-3 inline-block text-sm text-rausch underline">
        Account settings
      </Link>
    </div>
  );
}

/** Fresh auth identity → fresh widget. Remounting on login/logout/account
 * switch resets EVERYTHING at once — open state (a panel opened before the
 * login flow must not greet the new session already expanded), the
 * transcript and remembered page context (on a shared browser the previous
 * account's questions and viewed pages must not carry over — same reason
 * purgeAccountScopedQueries exists), and any in-flight ask (a slow answer
 * settling after the switch lands on the unmounted instance, a no-op,
 * instead of the next account's chat). */
export function ChatWidget() {
  const { me } = useMe();
  // The epoch bumps only when a RESOLVED identity CHANGES — never when the
  // initial /api/me load settles (undefined → user), which must not snap
  // shut a widget opened while that request was still in flight.
  const resolved = me === undefined ? null : (me?.email ?? "anon");
  const lastResolved = useRef<string | null>(null);
  const epoch = useRef(0);
  if (resolved !== null) {
    if (lastResolved.current !== null && resolved !== lastResolved.current) {
      epoch.current += 1;
    }
    lastResolved.current = resolved;
  }
  return <ChatWidgetSession key={epoch.current} />;
}

function ChatWidgetSession() {
  const location = useLocation();
  const { me } = useMe();
  const [open, setOpen] = useState(false);
  const [question, setQuestion] = useState("");
  const [turns, setTurns] = useState<Turn[]>([]);
  // The current or most recent candidate/election page this session — the
  // "what the user is looking at" the server resolves deictic questions
  // against. Kept after navigating away so "their record?" asked from the
  // ballot page still means the candidate just viewed.
  const [context, setContext] = useState<ChatbotAskContext | null>(null);
  const transcriptRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const pageContext = contextFromPathname(location.pathname);
    if (pageContext) {
      setContext(pageContext);
    }
  }, [location.pathname]);

  const ask = useMutation({
    mutationFn: (input: { question: string; previousQuestion: string | null; context: ChatbotAskContext | null }) =>
      askChatbot(input.question, { previousQuestion: input.previousQuestion, context: input.context }),
    onSuccess: (response, input) => {
      setTurns((previous) => [...previous, { question: input.question, response }]);
    },
  });

  useEffect(() => {
    // Optional-call: jsdom (tests) has no Element.scrollTo.
    transcriptRef.current?.scrollTo?.({ top: transcriptRef.current.scrollHeight });
  }, [turns, ask.isPending]);

  if (isChatWidgetHidden(location.pathname, Boolean(me))) {
    return null;
  }

  function sendQuestion(text: string) {
    const trimmed = text.trim();
    if (trimmed.length === 0 || ask.isPending) {
      return;
    }
    ask.mutate({
      question: trimmed,
      previousQuestion: turns.length > 0 ? (turns[turns.length - 1] as Turn).question : null,
      context,
    });
    setQuestion("");
  }

  function submit(event: React.FormEvent) {
    event.preventDefault();
    sendQuestion(question);
  }

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        aria-label="Open Ask"
        className="chat-launcher fixed bottom-4 right-4 z-30 rounded-full bg-rausch px-4 py-3 text-sm font-semibold text-white shadow-lg transition hover:bg-rausch-dark"
      >
        Ask
      </button>
    );
  }

  const accessKind = !me ? "register" : !me.email_verified ? "verify" : null;
  // A mid-session 401/403 (expired session, un-verified elsewhere) gets the
  // same walls as the client-side check.
  const errorAccessKind =
    ask.error instanceof ApiError && ask.error.status === 401
      ? "register"
      : ask.error instanceof ApiError && ask.error.status === 403
        ? "verify"
        : null;
  const notAvailable = ask.error instanceof ApiError && ask.error.status === 404;

  return (
    <div
      role="dialog"
      aria-label="Ask about elections and candidates"
      className="fixed bottom-4 right-4 z-30 flex max-h-[75vh] w-[22rem] max-w-[calc(100vw-2rem)] flex-col rounded-2xl border border-line bg-white shadow-2xl"
    >
      <div className="flex items-center justify-between rounded-t-2xl border-b border-line bg-surface px-3 py-2">
        <p className="text-sm font-bold text-ink">Ask</p>
        <div className="flex items-center gap-1">
          {/* Back to the empty-chat start screen (starter chips included) —
              without this, the only way out of a conversation was a page
              reload. A NEW chat must not inherit the old one's remembered
              context either: after navigating away, "their record?" would
              silently answer about a candidate no longer on screen. Reset to
              the CURRENT page's context (null off detail pages). */}
          {turns.length > 0 && (
            <button
              type="button"
              onClick={() => {
                if (ask.isPending) {
                  return;
                }
                setTurns([]);
                setQuestion("");
                setContext(contextFromPathname(location.pathname));
                ask.reset();
              }}
              className="rounded px-2 py-0.5 text-xs text-ink-soft hover:text-ink"
            >
              New chat
            </button>
          )}
          <button
            type="button"
            onClick={() => setOpen(false)}
            aria-label="Minimize Ask"
            className="rounded px-2 py-0.5 text-ink-soft hover:text-ink"
          >
            —
          </button>
        </div>
      </div>

      {accessKind ?? errorAccessKind ? (
        <AccessPrompt kind={(accessKind ?? errorAccessKind) as "register" | "verify"} />
      ) : (
        <>
          <div ref={transcriptRef} className="min-h-[8rem] flex-1 overflow-y-auto px-3 py-3">
            {turns.length === 0 && !ask.isPending && (
              <div>
                <p className="text-sm text-ink-soft">
                  Ask about the November 2026 elections we cover: candidates, records, campaign finance, elections, and
                  ballot measures. Answers come only from our data — never opinions or endorsements.
                </p>
                <div className="mt-3 flex flex-wrap gap-1.5">
                  {/* Chips describe the CURRENT page, not the remembered
                      context: after navigating away, "this candidate" would
                      point at a page the user can no longer see. The
                      remembered context still rides along for typed deictic
                      follow-ups; generic chips are non-deictic, so the
                      server ignores it for them. */}
                  {starterQuestions(contextFromPathname(location.pathname)).map((starter) => (
                    <button
                      key={starter}
                      type="button"
                      onClick={() => sendQuestion(starter)}
                      className="rounded-full border border-line px-2.5 py-1 text-xs text-ink transition hover:border-ink-soft hover:bg-surface"
                    >
                      {starter}
                    </button>
                  ))}
                </div>
              </div>
            )}
            <div className="space-y-4">
              {turns.map((turn, index) => (
                <TurnView key={index} turn={turn} reporterEmail={me?.email ?? null} />
              ))}
            </div>
            {ask.isPending && <p className="mt-3 text-sm text-ink-soft">Looking that up…</p>}
            {ask.isError &&
              !errorAccessKind &&
              (notAvailable ? (
                <p className="mt-3 text-sm text-ink-soft">Ask isn't available right now. Please try again later.</p>
              ) : (
                <p className="mt-3 rounded-lg border border-rausch/40 bg-rausch/5 px-2 py-1.5 text-xs text-rausch-dark">
                  Something went wrong. Please try again.
                </p>
              ))}
          </div>
          <form onSubmit={submit} className="border-t border-line p-2">
            <div className="flex gap-1.5">
              <label htmlFor="chat-widget-question" className="sr-only">
                Your question
              </label>
              <input
                id="chat-widget-question"
                type="text"
                value={question}
                maxLength={CHATBOT_MAX_QUESTION_LENGTH}
                onChange={(event) => setQuestion(event.target.value)}
                placeholder="Ask about what you're looking at…"
                className="min-w-0 flex-1 rounded-lg border border-line px-2.5 py-1.5 text-sm text-ink placeholder:text-ink-soft focus:border-ink-soft focus:outline-none"
              />
              <button
                type="submit"
                disabled={ask.isPending || question.trim().length === 0}
                className="shrink-0 rounded-lg bg-rausch px-3 py-1.5 text-sm font-semibold text-white transition hover:bg-rausch-dark disabled:opacity-50"
              >
                Ask
              </button>
            </div>
            <p className="mt-1 px-0.5 text-[10px] leading-tight text-ink-soft">{CHATBOT_PRIVACY_NOTE}</p>
          </form>
        </>
      )}
    </div>
  );
}
