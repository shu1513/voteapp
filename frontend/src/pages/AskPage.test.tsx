import { afterEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import AskPage from "./AskPage";
import { renderRoutes } from "../test/render";
import type { ChatbotAskResponse } from "@voteapp/api-client";

function stubAskResponse(response: ChatbotAskResponse) {
  const fetchMock = vi.fn(
    async () =>
      new Response(JSON.stringify(response), {
        status: 200,
        headers: { "content-type": "application/json" },
      })
  );
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("AskPage", () => {
  it("shows the privacy note and disables Ask until a question is typed", () => {
    renderRoutes([{ path: "/ask", element: <AskPage /> }], "/ask");
    expect(screen.getByText(/don't include personal information/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Ask" })).toBeDisabled();
  });

  it("submits a question and renders the answer with result cards and the data date", async () => {
    const fetchMock = stubAskResponse({
      outcome: "retrieval",
      answer: "Here's what our data has on that.",
      results: [
        {
          title: "US Senate — Georgia (November 3, 2026)",
          url: "/elections/11111111-1111-1111-1111-111111111111",
          snippet: "Candidates: Jon Ossoff…",
          source_type: "election",
        },
      ],
      data_current_as_of: "2026-08-11T00:00:00Z",
    });
    const user = userEvent.setup();
    renderRoutes([{ path: "/ask", element: <AskPage /> }], "/ask");

    await user.type(screen.getByLabelText("Your question"), "Who is running for US Senate in Georgia?");
    await user.click(screen.getByRole("button", { name: "Ask" }));

    expect(await screen.findByText("Here's what our data has on that.")).toBeInTheDocument();
    const card = screen.getByRole("link", { name: /US Senate — Georgia/ });
    expect(card).toHaveAttribute("href", "/elections/11111111-1111-1111-1111-111111111111");
    expect(screen.getByText(/Data current as of/)).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const body = JSON.parse((fetchMock.mock.calls[0] as unknown as [string, RequestInit])[1].body as string);
    expect(body).toEqual({ question: "Who is running for US Senate in Georgia?" });
  });

  it("sends the previous question on the next ask (deterministic follow-up)", async () => {
    const fetchMock = stubAskResponse({
      outcome: "retrieval",
      answer: "Answer",
      results: [],
      data_current_as_of: null,
    });
    const user = userEvent.setup();
    renderRoutes([{ path: "/ask", element: <AskPage /> }], "/ask");

    const input = screen.getByLabelText("Your question");
    await user.type(input, "Tell me about Jesse Petrea.");
    await user.click(screen.getByRole("button", { name: "Ask" }));
    await screen.findByText("You asked");

    await user.type(input, "What about their voting record?");
    await user.click(screen.getByRole("button", { name: "Ask" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));

    const secondBody = JSON.parse((fetchMock.mock.calls[1] as unknown as [string, RequestInit])[1].body as string);
    expect(secondBody).toEqual({
      question: "What about their voting record?",
      previous_question: "Tell me about Jesse Petrea.",
    });
  });

  it("renders official state resource links as external anchors", async () => {
    stubAskResponse({
      outcome: "template",
      answer: "Register through the official state site.",
      results: [
        {
          title: "Georgia official voter registration",
          url: "https://vote.gov/register",
          snippet: "Official state resource.",
          source_type: "official_state_resource",
        },
      ],
      data_current_as_of: null,
    });
    const user = userEvent.setup();
    renderRoutes([{ path: "/ask", element: <AskPage /> }], "/ask");

    await user.type(screen.getByLabelText("Your question"), "How do I register to vote in Georgia?");
    await user.click(screen.getByRole("button", { name: "Ask" }));

    const external = await screen.findByRole("link", { name: /Georgia official voter registration/ });
    expect(external).toHaveAttribute("href", "https://vote.gov/register");
    expect(external).toHaveAttribute("rel", "noopener noreferrer");
  });
});
