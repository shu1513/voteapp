import { afterEach, describe, expect, it, vi } from "vitest";
import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ChatWidget, contextFromPathname, isChatWidgetHidden, reportTargetFromResults, starterQuestions } from "./ChatWidget";
import { renderRoutes } from "../../test/render";
import { ME_UNVERIFIED, ME_VERIFIED } from "../../test/fixtures";
import { apiError, stubApiRoutes, type ApiRoute } from "../../test/mockApi";
import type { ChatbotAskResponse } from "@voteapp/api-client";

const RETRIEVAL_RESPONSE: ChatbotAskResponse = {
  outcome: "retrieval",
  answer: "Here's what our data has on that.",
  results: [
    {
      title: "Jon Ossoff — candidate, United States Senator (Georgia)",
      url: "/candidates/44444444-4444-4444-a444-444444444444",
      snippet: "Incumbent US Senator for Georgia.",
      source_type: "candidate_profile",
    },
  ],
  data_current_as_of: "2026-08-11T00:00:00Z",
};

function renderWidgetAt(pathname: string, askRoute: ApiRoute = { body: RETRIEVAL_RESPONSE }) {
  const fetchMock = stubApiRoutes({
    "/api/me": { body: ME_VERIFIED },
    "/api/chatbot/ask": askRoute,
  });
  const result = renderRoutes([{ path: "*", element: <ChatWidget /> }], pathname);
  return { fetchMock, ...result };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("visibility rules", () => {
  it("hides on the logged-out home page, shows there when logged in", () => {
    expect(isChatWidgetHidden("/", false)).toBe(true);
    expect(isChatWidgetHidden("/", true)).toBe(false);
  });

  it("hides on the public pick-card share page and auth flows for everyone", () => {
    expect(isChatWidgetHidden("/picks/some-token", true)).toBe(true);
    expect(isChatWidgetHidden("/login", true)).toBe(true);
    expect(isChatWidgetHidden("/register", false)).toBe(true);
    expect(isChatWidgetHidden("/verify-email", true)).toBe(true);
  });

  it("shows on candidate, election, and ballot pages", () => {
    expect(isChatWidgetHidden("/candidates/44444444-4444-4444-a444-444444444444", false)).toBe(false);
    expect(isChatWidgetHidden("/elections/11111111-1111-4111-9111-111111111111", true)).toBe(false);
    expect(isChatWidgetHidden("/ballot", true)).toBe(false);
  });
});

describe("contextFromPathname", () => {
  it("maps candidate and election detail pages, nothing else", () => {
    expect(contextFromPathname("/candidates/44444444-4444-4444-a444-444444444444")).toEqual({
      kind: "candidate",
      id: "44444444-4444-4444-a444-444444444444",
    });
    expect(contextFromPathname("/elections/11111111-1111-4111-9111-111111111111")).toEqual({
      kind: "election",
      id: "11111111-1111-4111-9111-111111111111",
    });
    expect(contextFromPathname("/ballot")).toBeNull();
    expect(contextFromPathname("/candidates/not-a-uuid")).toBeNull();
  });
});

describe("reportTargetFromResults", () => {
  it("maps the first candidate/election source card to a report entity", () => {
    expect(reportTargetFromResults(RETRIEVAL_RESPONSE.results)).toEqual({
      entityType: "candidate",
      entityId: "44444444-4444-4444-a444-444444444444",
    });
    expect(
      reportTargetFromResults([
        { title: "E", url: "/elections/11111111-1111-4111-9111-111111111111", snippet: "", source_type: "election" },
      ])
    ).toEqual({ entityType: "election", entityId: "11111111-1111-4111-9111-111111111111" });
    expect(reportTargetFromResults([{ title: "B", url: "/ballot", snippet: "", source_type: "page" }])).toBeNull();
    expect(reportTargetFromResults([])).toBeNull();
  });
});

describe("starterQuestions", () => {
  it("tunes suggestions to the page being viewed", () => {
    expect(starterQuestions({ kind: "candidate", id: "x" })).toContain("Tell me more about this candidate");
    expect(starterQuestions({ kind: "election", id: "x" })).toContain("Who is running in this election?");
    // Exact list on purpose: the generic chips are deliberate product copy
    // (the ballot and election-date chips were removed by request).
    expect(starterQuestions(null)).toEqual(["What can you do?", "How do I register to vote?"]);
  });
});

describe("ChatWidget", () => {
  it("starts minimized and expands on click", async () => {
    const user = userEvent.setup();
    renderWidgetAt("/ballot");
    const bubble = await screen.findByRole("button", { name: "Open Ask" });
    await user.click(bubble);
    expect(screen.getByRole("dialog", { name: /ask about elections/i })).toBeInTheDocument();
    expect(screen.getByText(/never opinions or endorsements/i)).toBeInTheDocument();
  });

  it("prompts logged-out visitors to register when opened", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    const user = userEvent.setup();
    renderRoutes([{ path: "*", element: <ChatWidget /> }], "/ballot");
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    expect(await screen.findByRole("link", { name: "Sign up" })).toHaveAttribute("href", "/register");
    expect(screen.queryByLabelText("Your question")).not.toBeInTheDocument();
  });

  it("prompts unverified accounts to verify instead of asking", async () => {
    stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    const user = userEvent.setup();
    renderRoutes([{ path: "*", element: <ChatWidget /> }], "/ballot");
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    expect(await screen.findByText(/verify your email/i)).toBeInTheDocument();
    expect(screen.queryByLabelText("Your question")).not.toBeInTheDocument();
  });

  it("sends the current candidate page as context and renders the answer", async () => {
    const user = userEvent.setup();
    const { fetchMock } = renderWidgetAt("/candidates/44444444-4444-4444-a444-444444444444");
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    await user.type(screen.getByLabelText("Your question"), "Tell me more about this candidate.");
    await user.click(screen.getByRole("button", { name: "Ask" }));

    expect(await screen.findByText("Here's what our data has on that.")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Jon Ossoff/ })).toHaveAttribute(
      "href",
      "/candidates/44444444-4444-4444-a444-444444444444"
    );
    const askCall = fetchMock.mock.calls.find(([input]) => String(input).includes("/api/chatbot/ask"));
    const body = JSON.parse((askCall as unknown as [string, RequestInit])[1].body as string);
    expect(body).toEqual({
      question: "Tell me more about this candidate.",
      context: { candidate_id: "44444444-4444-4444-a444-444444444444" },
    });
  });

  it("keeps the transcript across turns and sends the previous question", async () => {
    const user = userEvent.setup();
    const { fetchMock } = renderWidgetAt("/ballot");
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));

    const input = screen.getByLabelText("Your question");
    await user.type(input, "Tell me about Jesse Petrea.");
    await user.click(screen.getByRole("button", { name: "Ask" }));
    await screen.findByText("Tell me about Jesse Petrea.");

    await user.type(input, "What about their voting record?");
    await user.click(screen.getByRole("button", { name: "Ask" }));
    await waitFor(() => {
      const askCalls = fetchMock.mock.calls.filter(([request]) => String(request).includes("/api/chatbot/ask"));
      expect(askCalls).toHaveLength(2);
      const body = JSON.parse((askCalls[1] as unknown as [string, RequestInit])[1].body as string);
      expect(body).toEqual({
        question: "What about their voting record?",
        previous_question: "Tell me about Jesse Petrea.",
      });
    });
    // Both turns stay on screen.
    expect(screen.getByText("Tell me about Jesse Petrea.")).toBeInTheDocument();
    expect(screen.getByText("What about their voting record?")).toBeInTheDocument();
  });

  it("New chat clears the transcript back to the start screen", async () => {
    const user = userEvent.setup();
    renderWidgetAt("/ballot");
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    expect(screen.queryByRole("button", { name: "New chat" })).not.toBeInTheDocument();
    await user.type(screen.getByLabelText("Your question"), "Who is Jon Ossoff?");
    await user.click(screen.getByRole("button", { name: "Ask" }));
    await screen.findByText("Here's what our data has on that.");

    await user.click(screen.getByRole("button", { name: "New chat" }));
    expect(screen.queryByText("Here's what our data has on that.")).not.toBeInTheDocument();
    // The start screen (starter chips included) returns.
    expect(screen.getByRole("button", { name: "What can you do?" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "New chat" })).not.toBeInTheDocument();
  });

  it("New chat drops the remembered context from the old conversation", async () => {
    const user = userEvent.setup();
    const { fetchMock, router } = renderWidgetAt("/candidates/44444444-4444-4444-a444-444444444444");
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    await user.type(screen.getByLabelText("Your question"), "Tell me more about this candidate.");
    await user.click(screen.getByRole("button", { name: "Ask" }));
    await screen.findByText("Here's what our data has on that.");

    // Leave the candidate page, start over: the new chat must not inherit
    // the old candidate as context — "their record?" would answer about a
    // candidate no longer on screen.
    await router.navigate("/ballot");
    await user.click(screen.getByRole("button", { name: "New chat" }));
    await user.type(screen.getByLabelText("Your question"), "What is their voting record?");
    await user.click(screen.getByRole("button", { name: "Ask" }));
    await waitFor(() => {
      const askCalls = fetchMock.mock.calls.filter(([request]) => String(request).includes("/api/chatbot/ask"));
      expect(askCalls).toHaveLength(2);
      const body = JSON.parse((askCalls[1] as unknown as [string, RequestInit])[1].body as string);
      expect(body).toEqual({ question: "What is their voting record?" });
    });
  });

  it("collapses and clears the chat when the signed-in account changes", async () => {
    const user = userEvent.setup();
    const { queryClient } = renderWidgetAt("/ballot");
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    await user.type(screen.getByLabelText("Your question"), "Who is Jon Ossoff?");
    await user.click(screen.getByRole("button", { name: "Ask" }));
    await screen.findByText("Here's what our data has on that.");

    // Another account signs in on the same tab (the widget never unmounts,
    // so an open panel would otherwise survive the login round-trip): the
    // panel collapses and the previous account's conversation is gone.
    act(() => {
      queryClient.setQueryData(["me"], { ...ME_VERIFIED.user, email: "someone-else@example.com" });
    });
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    expect(screen.queryByText("Here's what our data has on that.")).not.toBeInTheDocument();
    expect(screen.getByText(/never opinions or endorsements/i)).toBeInTheDocument();
  });

  it("discards an answer that was still in flight when the account switched", async () => {
    const user = userEvent.setup();
    let release!: (value: { body: unknown }) => void;
    const held = new Promise<{ body: unknown }>((resolve) => {
      release = resolve;
    });
    const { queryClient } = renderWidgetAt("/ballot", () => held);
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    await user.type(screen.getByLabelText("Your question"), "Who is Jon Ossoff?");
    await user.click(screen.getByRole("button", { name: "Ask" }));

    // Account B signs in while A's question is still in flight...
    act(() => {
      queryClient.setQueryData(["me"], { ...ME_VERIFIED.user, email: "someone-else@example.com" });
    });
    // ...then A's slow answer arrives. It must land nowhere: the remount
    // discarded the widget instance the mutation would have appended to.
    await act(async () => {
      release({ body: RETRIEVAL_RESPONSE });
      await new Promise((resolve) => setTimeout(resolve, 20));
    });
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    expect(screen.queryByText("Who is Jon Ossoff?")).not.toBeInTheDocument();
    expect(screen.queryByText("Here's what our data has on that.")).not.toBeInTheDocument();
  });

  it("sends a starter chip as a question on click, then hides the chips", async () => {
    const user = userEvent.setup();
    const { fetchMock } = renderWidgetAt("/candidates/44444444-4444-4444-a444-444444444444");
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    await user.click(screen.getByRole("button", { name: "Tell me more about this candidate" }));

    expect(await screen.findByText("Here's what our data has on that.")).toBeInTheDocument();
    const askCall = fetchMock.mock.calls.find(([input]) => String(input).includes("/api/chatbot/ask"));
    const body = JSON.parse((askCall as unknown as [string, RequestInit])[1].body as string);
    expect(body).toEqual({
      question: "Tell me more about this candidate",
      context: { candidate_id: "44444444-4444-4444-a444-444444444444" },
    });
    // Chips only seed an empty chat.
    expect(screen.queryByRole("button", { name: "Tell me more about this candidate" })).not.toBeInTheDocument();
  });

  it("bases chips on the CURRENT page even when an older context is remembered", async () => {
    const user = userEvent.setup();
    const { router } = renderWidgetAt("/candidates/44444444-4444-4444-a444-444444444444");
    // Leave the candidate page; the remembered context stays for typed
    // follow-ups, but chips must describe what the user sees NOW.
    await router.navigate("/ballot");
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    expect(await screen.findByRole("button", { name: "What can you do?" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Tell me more about this candidate" })).not.toBeInTheDocument();
  });

  it("labels AI answers, dates them, and offers the report control (BEHAVIOR rule 9)", async () => {
    const user = userEvent.setup();
    const aiResponse: ChatbotAskResponse = {
      ...RETRIEVAL_RESPONSE,
      answer: "Jon Ossoff is the incumbent US Senator from Georgia.",
      ai_generated: true,
    };
    renderWidgetAt("/ballot", { body: aiResponse });
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    await user.type(screen.getByLabelText("Your question"), "Who is Jon Ossoff?");
    await user.click(screen.getByRole("button", { name: "Ask" }));

    expect(await screen.findByText("Jon Ossoff is the incumbent US Senator from Georgia.")).toBeInTheDocument();
    // One combined label line; the exact date rendering is timezone-local.
    expect(screen.getByText(/AI-generated from our election data/)).toHaveTextContent(/Data current as of/);
    // Report control attaches to the first cited entity.
    expect(screen.getByRole("button", { name: /report an issue with this ai answer/i })).toBeInTheDocument();
    // Sources still render as cards.
    expect(screen.getByRole("link", { name: /Jon Ossoff/ })).toBeInTheDocument();
  });

  it("shows no AI label or report control on plain retrieval-card answers", async () => {
    const user = userEvent.setup();
    renderWidgetAt("/ballot");
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    await user.type(screen.getByLabelText("Your question"), "Who is Jon Ossoff?");
    await user.click(screen.getByRole("button", { name: "Ask" }));
    expect(await screen.findByText("Here's what our data has on that.")).toBeInTheDocument();
    expect(screen.queryByText(/AI-generated/)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /report an issue/i })).not.toBeInTheDocument();
    expect(screen.getByText(/Data current as of/)).toBeInTheDocument();
  });

  it("shows the register wall when the server answers 401 mid-session", async () => {
    const user = userEvent.setup();
    renderWidgetAt("/ballot", apiError(401, "unauthorized", "Authentication is required"));
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    await user.type(screen.getByLabelText("Your question"), "Who is Jon Ossoff?");
    await user.click(screen.getByRole("button", { name: "Ask" }));
    expect(await screen.findByRole("link", { name: "Sign up" })).toBeInTheDocument();
  });
});
