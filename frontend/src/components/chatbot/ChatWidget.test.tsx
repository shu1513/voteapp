import { afterEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ChatWidget, contextFromPathname, isChatWidgetHidden } from "./ChatWidget";
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

  it("shows the register wall when the server answers 401 mid-session", async () => {
    const user = userEvent.setup();
    renderWidgetAt("/ballot", apiError(401, "unauthorized", "Authentication is required"));
    await user.click(await screen.findByRole("button", { name: "Open Ask" }));
    await user.type(screen.getByLabelText("Your question"), "Who is Jon Ossoff?");
    await user.click(screen.getByRole("button", { name: "Ask" }));
    expect(await screen.findByRole("link", { name: "Sign up" })).toBeInTheDocument();
  });
});
