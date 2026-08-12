// The "Ask" pipeline: intent router → answer cache → hybrid retrieval →
// answerability gate → LLM answer (flag/cap/budget permitting) or template
// answer / result cards — docs/plans/chatbot-rag.md.
//
// Every outcome logs one anonymous, redacted row to chatbot.questions
// (fire-and-forget; a failed insert never fails the answer).

import type { Pool } from "pg";

import { answerWithLlm, buildScopeKey, getCachedAskResponse, type LlmAnswering } from "./answer.js";
import { suggestClosestCandidates } from "./didYouMean.js";
import { detectIntent, detectStateInQuestion, type IntentMatch } from "./intents.js";
import { normalizeQuestion } from "./redact.js";
import {
  getActiveGeneration,
  isAnswerable,
  retrieveChunks,
  GATE_MIN_ENTITY_SIMILARITY,
  type CandidateEntityMatch,
  type RetrievalResult,
  type RetrievedChunk,
} from "./retrieval.js";
import { REFUSAL_NO_DATA_ANSWER, toResultCards, type AskResultCard } from "./shared.js";
import type { EmbeddingsClient } from "./embeddingsClient.js";

export type AskOutcome = "template" | "retrieval" | "clarify" | "refuse_no_data" | "refuse_policy";

export type { AskResultCard } from "./shared.js";

export type AskResponse = {
  outcome: AskOutcome;
  answer: string;
  results: AskResultCard[];
  /** Active generation activation time for retrieval answers; null for
   * deterministic templates (they are live, not indexed). */
  data_current_as_of: string | null;
  /** True only for model-generated prose (Phase 2). Absent/false everywhere
   * else — the widget uses it for the AI label + report control
   * (BEHAVIOR.md rule 9). */
  ai_generated?: boolean;
};

export type AskContext =
  | { kind: "candidate"; id: string }
  | { kind: "election"; id: string };

export type AskService = {
  /** userId enables the Phase 2 LLM path (per-user cap + provider abuse
   * identifier). The endpoint is verified-accounts-only, so the API always
   * has one; operator scripts (eval) may omit it — they stay retrieval-only. */
  ask: (
    question: string,
    previousQuestion?: string | null,
    context?: AskContext | null,
    userId?: string | null
  ) => Promise<AskResponse>;
};

// Context only applies to questions that point at it: "tell me more about
// THIS candidate", "what's THEIR voting record". A non-deictic question on a
// candidate page ("what will the weather be on election day?") must still be
// judged on its own evidence, or every off-topic question asked from a
// candidate page would pass the gate on the page's chunks.
const DEICTIC_RE = /\b(?:this|that|these|those|his|her|hers|their|theirs|its|he|she|they|him|them|it)\b/i;

type ResolvedContext = {
  kind: "candidate" | "election";
  id: string;
  state: string | null;
};

/** Validates the page context against the ACTIVE generation (a stale or
 * out-of-corpus id resolves to null and the question stands on its own). */
async function resolveContext(db: Pool, generationId: string, context: AskContext): Promise<ResolvedContext | null> {
  const result = await db.query<{ state: string | null }>(
    `
      SELECT state
      FROM chatbot.chunks
      WHERE generation_id = $1::uuid
        AND source_type = $2
        AND source_id = $3::uuid
      LIMIT 1
    `,
    [generationId, context.kind === "candidate" ? "candidate_profile" : "election", context.id]
  );
  const row = result.rows[0];
  return row ? { kind: context.kind, id: context.id, state: row.state } : null;
}

const POLICY_REFUSAL_ANSWER =
  "I can't recommend how to vote — no endorsements, ever. I can share neutral information from our data instead: who is running, their backgrounds and records, and campaign finance.";

// Fixed for the current cycle the corpus covers; revisit with the 2027+
// cohorts. Deterministic on purpose (BEHAVIOR.md rule 6).
const GENERAL_ELECTION_DATE_ANSWER =
  "The November 2026 general election is on Tuesday, November 3, 2026. Some places also have earlier primaries, runoffs, or special elections — check the election pages for exact dates.";

// Countdown to the same fixed date — pure date math, no data. Election day is
// a LOCAL calendar date and the server (or its clock) can sit in any
// timezone, so "today" is anchored to US Eastern (UTC-5; DST has ended by
// election week): without the client's timezone that is the least-wrong
// boundary — exact for the earliest US clocks, off only for a few
// late-evening hours further west. Uses only UTC accessors so the answer
// never depends on the server's own timezone.
const ELECTION_DAY_UTC = Date.UTC(2026, 10, 3);
const EASTERN_UTC_OFFSET_MS = 5 * 3_600_000;
export function electionCountdownAnswer(now: Date = new Date()): string {
  const eastern = new Date(now.getTime() - EASTERN_UTC_OFFSET_MS);
  const todayUtc = Date.UTC(eastern.getUTCFullYear(), eastern.getUTCMonth(), eastern.getUTCDate());
  const days = Math.round((ELECTION_DAY_UTC - todayUtc) / 86_400_000);
  if (days > 1) {
    return `The November 2026 general election is on Tuesday, November 3, 2026 — ${days} days from today.`;
  }
  if (days === 1) {
    return "The November 2026 general election is tomorrow: Tuesday, November 3, 2026.";
  }
  if (days === 0) {
    return "The November 2026 general election is today, Tuesday, November 3, 2026!";
  }
  return "The November 2026 general election was on Tuesday, November 3, 2026.";
}

type StateLogisticsRow = {
  state_abbreviation: string;
  state_name: string;
  polling_place_url: string;
  voter_registration_url: string;
  id_requirements: string;
  online_registration_available: boolean;
  online_registration_deadline_rule: string | null;
  in_person_registration_deadline_rule: string;
  same_day_registration_available: boolean;
  mail_voting_available: boolean;
  mail_ballot_request_url: string | null;
  mail_ballot_request_deadline_rule: string | null;
  /** First research-evidence URL behind id_requirements / the mail fields
   * (state_resources.sources is a field→[urls] map). May be third-party
   * (NCSL), so cards label these as sources, never as official. */
  id_requirements_source_url: string | null;
  mail_voting_source_url: string | null;
};

// Chatbot-local read of the manually researched official links (BEHAVIOR.md
// rule 5: logistics answers come only from state_resources). Deliberately not
// widening the shared stateVotingResources reader: this module must stay
// deletable without touching shared code.
async function loadStateLogistics(db: Pool, stateAbbreviation: string): Promise<StateLogisticsRow | null> {
  const result = await db.query<StateLogisticsRow>(
    `
      SELECT
        state_abbreviation,
        state_name,
        polling_place_url,
        voter_registration_url,
        id_requirements,
        online_registration_available,
        online_registration_deadline_rule,
        in_person_registration_deadline_rule,
        same_day_registration_available,
        mail_voting_available,
        mail_ballot_request_url,
        mail_ballot_request_deadline_rule,
        sources->'id_requirements'->>0 AS id_requirements_source_url,
        sources->'mail_voting_available'->>0 AS mail_voting_source_url
      FROM public.state_resources
      WHERE state_abbreviation = $1
    `,
    [stateAbbreviation]
  );
  return result.rows[0] ?? null;
}

// Researched deadline rules are sentences that usually end in a period; the
// templates add their own punctuation, so strip the trailing one ("…election..").
function trimRule(rule: string): string {
  return rule.trim().replace(/\.+$/, "");
}

const BALLOT_CARD: AskResultCard = {
  title: "Look up your ballot",
  url: "/ballot",
  snippet: "Enter your address to see every election and candidate on your ballot.",
  source_type: "page",
};

function describeEntityOption(match: CandidateEntityMatch): string {
  const officePart = match.currentOffice ? `, ${match.currentOffice}` : "";
  const partyPart = match.party ? ` (${match.party})` : "";
  return `${match.displayName}${partyPart} — ${match.state}${officePart}`;
}

// Scope-ambiguity heuristic (rule 7): a race-LISTING question ("who's
// running for…", "who's on the ballot for…") with no named state and no
// named candidate, whose election-title matches tie across 2+ states within
// a margin — "the sheriff race" fits dozens of counties equally. A question
// with one clearly dominant title match (its place tokens named, or an exact
// measure title) is not tied. Restricted to listing phrasings so entity and
// measure questions never trip it.
const SCOPE_TIE_RATIO = 0.85;
const RACE_LISTING_RE = /\bwho(?:'s| is| are)?\s+(?:running|on\s+the\s+ballot|the\s+candidates?)\b|\bcandidates\s+for\b/i;

function needsScopeClarification(question: string, retrieval: RetrievalResult, scopeState: string | null): boolean {
  if (scopeState) {
    return false;
  }
  if (retrieval.bestEntitySimilarity >= GATE_MIN_ENTITY_SIMILARITY) {
    return false;
  }
  if (!RACE_LISTING_RE.test(question)) {
    return false;
  }
  const [top] = retrieval.electionTitleMatches;
  if (!top || !top.state) {
    return false;
  }
  // The question names a place some matched race is in ("Los Angeles mayor",
  // "San Jose City Council District 5") → it IS scoped; retrieval decides.
  if (retrieval.electionTitleMatches.some((match) => match.placeSimilarity >= 0.4)) {
    return false;
  }
  const tiedStates = new Set(
    retrieval.electionTitleMatches
      .filter((match) => match.state && match.similarity / top.similarity >= SCOPE_TIE_RATIO)
      .map((match) => match.state)
  );
  return tiedStates.size >= 2;
}

async function renderIntentAnswer(db: Pool, intent: IntentMatch): Promise<AskResponse> {
  // Smalltalk: friendly fixed lines, no data, no cards (BEHAVIOR-neutral —
  // nothing here asserts a fact). Checked before the state-resources fetch.
  if (intent.kind === "greeting") {
    return {
      outcome: "template",
      answer:
        "Hi! Ask me about the November 2026 elections we cover — candidates, their records, campaign finance, elections, and ballot measures.",
      results: [],
      data_current_as_of: null,
    };
  }
  if (intent.kind === "thanks") {
    return { outcome: "template", answer: "My pleasure — ask any time.", results: [], data_current_as_of: null };
  }
  if (intent.kind === "goodbye") {
    return {
      outcome: "template",
      answer: "Goodbye! Come back whenever you have election questions.",
      results: [],
      data_current_as_of: null,
    };
  }
  if (intent.kind === "help") {
    return {
      outcome: "template",
      answer:
        'I answer questions from our November 2026 election data: who\'s running, candidates\' backgrounds and records, campaign finance, elections, and ballot measures. I can also link your state\'s official pages for registering and voting. Try: "Who is running for US Senate in Georgia?"',
      results: [BALLOT_CARD],
      data_current_as_of: null,
    };
  }
  if (intent.kind === "election_countdown") {
    return {
      outcome: "template",
      answer: electionCountdownAnswer(),
      results: [BALLOT_CARD],
      data_current_as_of: null,
    };
  }

  const resources = intent.state ? await loadStateLogistics(db, intent.state) : null;

  if (intent.kind === "policy_refusal") {
    return { outcome: "refuse_policy", answer: POLICY_REFUSAL_ANSWER, results: [], data_current_as_of: null };
  }

  if (intent.kind === "needs_scope") {
    return {
      outcome: "clarify",
      answer:
        "That depends on where you vote — runoff and primary dates differ by state and race. Which state, county, or city do you mean?",
      results: [],
      data_current_as_of: null,
    };
  }

  if (intent.kind === "untracked_data") {
    return {
      outcome: "refuse_no_data",
      answer:
        "I don't track candidates' social media posts. I can share what's in our data: candidate profiles, records, campaign finance, elections, and ballot measures.",
      results: [],
      data_current_as_of: null,
    };
  }

  if (intent.kind === "out_of_cycle") {
    return {
      outcome: "refuse_no_data",
      answer:
        "Our data covers the November 2026 elections, so I can't answer questions about other election years.",
      results: [],
      data_current_as_of: null,
    };
  }

  if (intent.kind === "results") {
    return {
      outcome: "template",
      answer:
        "Election results are posted on each election's page as official sources report and certify them. Look the election up from your ballot page or a candidate's page to see its current status.",
      results: [BALLOT_CARD],
      data_current_as_of: null,
    };
  }

  if (intent.kind === "ballot_lookup") {
    return {
      outcome: "template",
      answer:
        "Use the ballot page to enter your address and see everything on your ballot. Please don't share your address here in chat.",
      results: [BALLOT_CARD],
      data_current_as_of: null,
    };
  }

  if (intent.kind === "election_date") {
    return { outcome: "template", answer: GENERAL_ELECTION_DATE_ANSWER, results: [BALLOT_CARD], data_current_as_of: null };
  }

  // Primary/runoff/special dates vary by race and are not in the Nov-2026
  // corpus — never serve the general-election date for them (rule 6). No
  // card either: state_resources has no election-calendar URL, and a
  // registration link would not support a date claim.
  if (intent.kind === "other_election_date") {
    const stateName = resources?.state_name ?? "your state";
    return {
      outcome: "template",
      answer: `Primary, runoff, and special election dates vary by state and race, and our data covers the November 2026 general election. Check ${stateName}'s official election website (usually the Secretary of State) for those dates.`,
      results: [],
      data_current_as_of: null,
    };
  }

  if (intent.kind === "where_to_vote") {
    if (resources) {
      return {
        outcome: "template",
        answer: `${resources.state_name} lists polling places through its official lookup — that's the authoritative source for where to vote.`,
        results: [
          {
            title: `${resources.state_name} official polling place lookup`,
            url: resources.polling_place_url,
            snippet: "Official state resource.",
            source_type: "official_state_resource",
          },
          BALLOT_CARD,
        ],
        data_current_as_of: null,
      };
    }
    return {
      outcome: "template",
      answer:
        "Polling places are listed by each state's official lookup. Tell me which state you vote in, or use the ballot page to find your elections.",
      results: [BALLOT_CARD],
      data_current_as_of: null,
    };
  }

  if (intent.kind === "voter_registration") {
    if (resources) {
      const deadlineParts: string[] = [];
      if (resources.online_registration_available && resources.online_registration_deadline_rule) {
        deadlineParts.push(`Online registration: ${trimRule(resources.online_registration_deadline_rule)}`);
      }
      deadlineParts.push(`In person: ${trimRule(resources.in_person_registration_deadline_rule)}`);
      if (resources.same_day_registration_available) {
        deadlineParts.push(`${resources.state_name} offers same-day registration`);
      }
      return {
        outcome: "template",
        answer: `Register to vote in ${resources.state_name} through the official state site. ${deadlineParts.join(". ")}.`,
        results: [
          {
            title: `${resources.state_name} official voter registration`,
            url: resources.voter_registration_url,
            snippet: "Official state resource.",
            source_type: "official_state_resource",
          },
        ],
        data_current_as_of: null,
      };
    }
    return {
      outcome: "template",
      answer:
        "Each state runs its own voter registration with its own deadlines. Tell me which state you vote in and I'll link its official registration page.",
      results: [],
      data_current_as_of: null,
    };
  }

  if (intent.kind === "voter_id") {
    if (resources) {
      // The ID rules are our researched data; the card links the research
      // source behind them (possibly third-party, labeled as a source — a
      // registration portal would not support an ID claim). No source
      // recorded → answer text stands alone.
      return {
        outcome: "template",
        answer: `${resources.state_name} ID rules: ${resources.id_requirements}`,
        results: resources.id_requirements_source_url
          ? [
              {
                title: `Source for ${resources.state_name}'s voter ID rules`,
                url: resources.id_requirements_source_url,
                snippet: "Where this answer's ID information comes from.",
                source_type: "source_link",
              },
            ]
          : [],
        data_current_as_of: null,
      };
    }
    return {
      outcome: "template",
      answer: "Voter ID rules differ by state. Tell me which state you vote in and I'll share its official requirements.",
      results: [],
      data_current_as_of: null,
    };
  }

  // mail_voting. The dedicated request URL is the official destination; when
  // a state has none, fall back to the research source behind the mail
  // fields (labeled as a source, never as official — a registration or
  // polling link would not support a mail-voting claim).
  if (resources) {
    const mailCard: AskResultCard[] = resources.mail_ballot_request_url
      ? [
          {
            title: `${resources.state_name} official mail ballot information`,
            url: resources.mail_ballot_request_url,
            snippet: "Official state resource.",
            source_type: "official_state_resource",
          },
        ]
      : resources.mail_voting_source_url
        ? [
            {
              title: `Source for ${resources.state_name}'s mail voting rules`,
              url: resources.mail_voting_source_url,
              snippet: "Where this answer's mail voting information comes from.",
              source_type: "source_link",
            },
          ]
        : [];
    if (!resources.mail_voting_available) {
      return {
        outcome: "template",
        answer: `${resources.state_name} does not offer general mail voting according to its official resources. Check the official site for absentee eligibility rules.`,
        results: mailCard,
        data_current_as_of: null,
      };
    }
    const deadline = resources.mail_ballot_request_deadline_rule
      ? ` Request deadline: ${trimRule(resources.mail_ballot_request_deadline_rule)}.`
      : "";
    return {
      outcome: "template",
      answer: `${resources.state_name} offers voting by mail.${deadline}`,
      results: mailCard,
      data_current_as_of: null,
    };
  }
  return {
    outcome: "template",
    answer:
      "Mail voting rules differ by state. Tell me which state you vote in and I'll link its official mail ballot information.",
    results: [],
    data_current_as_of: null,
  };
}

type QuestionLogRow = {
  questionNorm: string;
  answeredBy: string;
  scopeKey: string | null;
  matchedChunkIds: string[];
  latencyMs: number;
  /** Actual billed tokens for LLM-answered asks; null everywhere else
   * (cache hits, templates, cards). */
  tokensIn: number | null;
  tokensOut: number | null;
};

function logQuestion(db: Pool, row: QuestionLogRow): void {
  // Fire-and-forget: the log must never delay or fail an answer.
  void db
    .query(
      `
        INSERT INTO chatbot.questions (question_norm, answered_by, scope_key, matched_chunk_ids, latency_ms, tokens_in, tokens_out)
        VALUES ($1, $2, $3, $4::bigint[], $5, $6, $7)
      `,
      [row.questionNorm, row.answeredBy, row.scopeKey, row.matchedChunkIds, row.latencyMs, row.tokensIn, row.tokensOut]
    )
    .catch((error: unknown) => {
      console.warn(
        "chatbot question log insert failed; answer unaffected:",
        error instanceof Error ? error.message : String(error)
      );
    });
}

export type CreateAskServiceOptions = {
  db: Pool;
  embeddings: EmbeddingsClient | null;
  /** Phase 2 LLM answering (adapter client + limits Redis + config). Absent
   * → Phase 1 behavior exactly: retrieval cards, no cache, no model. */
  llm?: LlmAnswering | null;
};

export function createAskService(options: CreateAskServiceOptions): AskService {
  const { db, embeddings } = options;
  const llm = options.llm ?? null;

  return {
    async ask(
      question: string,
      previousQuestion?: string | null,
      context?: AskContext | null,
      userId?: string | null
    ): Promise<AskResponse> {
      const startedAt = Date.now();
      const questionNorm = normalizeQuestion(question);
      const finish = (
        response: AskResponse,
        answeredBy: string,
        scopeKey: string | null = null,
        matchedChunkIds: string[] = [],
        tokensIn: number | null = null,
        tokensOut: number | null = null
      ): AskResponse => {
        logQuestion(db, {
          questionNorm,
          answeredBy,
          scopeKey,
          matchedChunkIds,
          latencyMs: Date.now() - startedAt,
          tokensIn,
          tokensOut,
        });
        return response;
      };

      // 1. Deterministic intents (policy refusals, logistics, results).
      const intent = detectIntent(question);
      if (intent) {
        const response = await renderIntentAnswer(db, intent);
        const answeredBy =
          response.outcome === "refuse_policy"
            ? "refused_policy"
            : response.outcome === "refuse_no_data"
              ? "refused"
              : response.outcome === "clarify"
                ? "clarify"
                : `intent:${intent.kind}`;
        return finish(response, answeredBy, intent.state);
      }

      // 2. Retrieval over the active generation.
      const generation = await getActiveGeneration(db);
      if (!generation) {
        return finish(
          { outcome: "refuse_no_data", answer: REFUSAL_NO_DATA_ANSWER, results: [], data_current_as_of: null },
          "refused"
        );
      }

      // Page context: applied only when the question points at it (deictic)
      // and the id still exists in the active generation.
      const resolvedContext =
        context && DEICTIC_RE.test(question) ? await resolveContext(db, generation.id, context) : null;

      // Deterministic follow-up scope carry-over (no LLM rewrite in v1): a
      // scopeless follow-up appends the previous turn's text so its district/
      // candidate tokens participate in matching — append-only, so nothing
      // from the current question can be dropped.
      let scopeState = detectStateInQuestion(question);
      let retrievalText = question;
      if (previousQuestion && !scopeState && !resolvedContext) {
        scopeState = detectStateInQuestion(previousQuestion);
        retrievalText = `${question} ${previousQuestion}`;
      }
      if (!scopeState && resolvedContext) {
        scopeState = resolvedContext.state;
      }

      // Phase 2 exact-answer cache, checked BEFORE retrieval is paid for (a
      // hit skips the query embedding too). The key text is the normalized
      // FULL retrieval text — a carried-over previous turn changes the
      // answer, so it must change the key. Only questions that passed the
      // gate ever get cached, and never time-sensitive ones (those returned
      // from the intent router above; BEHAVIOR.md rule 6).
      const carriedPrevious = retrievalText === question ? null : (previousQuestion ?? null);
      const cacheQuestionNorm = normalizeQuestion(retrievalText);
      const scopeKey = buildScopeKey(
        scopeState,
        resolvedContext ? { kind: resolvedContext.kind, id: resolvedContext.id } : null
      );
      if (llm && userId) {
        const cached = await getCachedAskResponse(llm, {
          questionNorm: cacheQuestionNorm,
          scopeKey,
          generationId: generation.id,
        });
        if (cached) {
          return finish(cached, "cache", scopeState);
        }
      }

      const retrieval = await retrieveChunks({
        db,
        embeddings,
        generationId: generation.id,
        question: retrievalText,
        scopeState,
        contextCandidateId: resolvedContext?.kind === "candidate" ? resolvedContext.id : null,
        contextElectionId: resolvedContext?.kind === "election" ? resolvedContext.id : null,
      });

      // 3. Same-name candidates → clarify, never silently pick (rule 7).
      if (retrieval.ambiguousEntities.length > 0) {
        const options_ = retrieval.ambiguousEntities.map(describeEntityOption);
        return finish(
          {
            outcome: "clarify",
            answer: `I found more than one candidate with that name. Which one do you mean? ${options_.join("; ")}.`,
            results: [],
            data_current_as_of: generation.activatedAt,
          },
          "clarify",
          scopeState
        );
      }

      // 4. Scopeless race questions that tie across states → clarify.
      if (needsScopeClarification(question, retrieval, scopeState)) {
        return finish(
          {
            outcome: "clarify",
            answer:
              "There are races like that in several places. Which state, county, or city do you mean?",
            results: [],
            data_current_as_of: generation.activatedAt,
          },
          "clarify",
          null
        );
      }

      // 5. Answerability gate on raw scores; a deictic question about the
      // viewed page is answerable on the page's own chunks.
      if (!isAnswerable(retrieval) && !retrieval.contextMatched) {
        // Before flatly refusing: a question span close to a matched
        // candidate's WHOLE name is a probable typo ("Jon Osoff") — offer
        // the closest names instead (didYouMean.ts filters the surname-only
        // noise that must keep refusing).
        const suggestions = suggestClosestCandidates(retrievalText, retrieval.entityMatches);
        if (suggestions.length > 0) {
          return finish(
            {
              outcome: "clarify",
              answer: `I couldn't find that exact name. Did you mean: ${suggestions
                .map(describeEntityOption)
                .join("; ")}?`,
              results: [],
              data_current_as_of: generation.activatedAt,
            },
            "clarify",
            scopeState
          );
        }
        return finish(
          { outcome: "refuse_no_data", answer: REFUSAL_NO_DATA_ANSWER, results: [], data_current_as_of: null },
          "refused",
          scopeState
        );
      }

      // 6. LLM answer over the gated chunks (Phase 2), when configured and
      // the ask is user-attributed. Every guard trip or failure inside falls
      // back to the Phase 1 cards below — the LLM can only ADD an answer.
      const matchedChunkIds = retrieval.chunks.map((chunk) => chunk.id);
      let cardsAnsweredBy = "retrieval";
      if (llm && userId) {
        const step = await answerWithLlm({
          db,
          llm,
          userId,
          question,
          previousQuestion: carriedPrevious,
          questionNorm: cacheQuestionNorm,
          scopeKey,
          generationId: generation.id,
          generationActivatedAt: generation.activatedAt,
          chunks: retrieval.chunks,
        });
        if (step.kind === "answered") {
          return finish(step.response, "llm", scopeState, matchedChunkIds, step.tokensIn, step.tokensOut);
        }
        if (step.reason === "rate_limited") {
          cardsAnsweredBy = "rate_limited";
        }
      }

      const cards = toResultCards(retrieval.chunks);
      return finish(
        {
          outcome: "retrieval",
          answer:
            "Here's what our data has on that. These summaries come from our election database — open a result for the full picture.",
          results: cards,
          data_current_as_of: generation.activatedAt,
        },
        cardsAnsweredBy,
        scopeState,
        matchedChunkIds
      );
    },
  };
}
