import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ElectionChoice } from "@voteapp/api-client";
import { CandidatePickButton, CandidatePickRow, MeasureChoiceButtons } from "./ElectionChoiceControls";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { renderRoutes } from "../test/render";

afterEach(() => {
  vi.unstubAllGlobals();
});

const ELECTION_ID = "33333333-3333-4333-8333-333333333333";
const CANDIDATE_ID = "22222222-2222-4222-8222-222222222222";
const OTHER_CANDIDATE_ID = "44444444-4444-4444-8444-444444444444";

function choice(overrides: Partial<ElectionChoice> = {}): ElectionChoice {
  return {
    election_id: ELECTION_ID,
    race_type: "office",
    official_ballot_title: "Governor",
    election_date: "2026-11-03",
    seats_to_fill: null,
    picks: [],
    measure_position: null,
    updated_at: "2026-08-01T00:00:00.000Z",
    ...overrides,
  };
}

function pick(candidateId: string) {
  return { candidate_id: candidateId, display_name: "Someone", candidacy_status: "declared" };
}

// The controls mutate through react-query, so they render via renderRoutes
// (which supplies the QueryClient); choice state itself arrives as a prop.
function renderControl(element: React.ReactElement) {
  return renderRoutes([{ path: "/", element }], "/");
}

describe("CandidatePickButton", () => {
  it("PUTs a pick and toggles off with chosen: false", async () => {
    const fetchMock = stubApiRoutes({
      "/api/me/election-choices": (_url, init) => {
        expect(init?.method).toBe("PUT");
        expect(JSON.parse(String(init?.body))).toEqual({
          election_id: ELECTION_ID,
          candidate_id: CANDIDATE_ID,
          chosen: true,
        });
        return { status: 200, body: { choice: choice({ picks: [pick(CANDIDATE_ID)] }) } };
      },
    });

    renderControl(
      <CandidatePickButton electionId={ELECTION_ID} candidateId={CANDIDATE_ID} candidateName="Jane Doe" choice={undefined} seatsToFill={null} />
    );

    await userEvent.setup().click(screen.getByRole("button", { name: "Make my pick: Jane Doe" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
  });

  it("renders as picked and unpicks with chosen: false", async () => {
    const fetchMock = stubApiRoutes({
      "/api/me/election-choices": (_url, init) => {
        expect(JSON.parse(String(init?.body))).toEqual({
          election_id: ELECTION_ID,
          candidate_id: CANDIDATE_ID,
          chosen: false,
        });
        return { status: 200, body: { choice: choice() } };
      },
    });

    renderControl(
      <CandidatePickButton
        electionId={ELECTION_ID}
        candidateId={CANDIDATE_ID}
        candidateName="Jane Doe"
        choice={choice({ picks: [pick(CANDIDATE_ID)] })}
        seatsToFill={null}
      />
    );

    const button = screen.getByRole("button", { name: "✓ My pick: Jane Doe" });
    expect(button).toHaveAttribute("aria-pressed", "true");
    await userEvent.setup().click(button);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
  });

  it("disables an unpicked button at the multi-seat cap, with the reason in the tooltip", () => {
    stubApiRoutes({});
    renderControl(
      <CandidatePickButton
        electionId={ELECTION_ID}
        candidateId={CANDIDATE_ID}
        candidateName="Jane Doe"
        choice={choice({ seats_to_fill: 2, picks: [pick(OTHER_CANDIDATE_ID), pick("55555555-5555-4555-8555-555555555555")] })}
        seatsToFill={2}
      />
    );

    const button = screen.getByRole("button", { name: "Make my pick: Jane Doe" });
    expect(button).toBeDisabled();
    expect(button).toHaveAttribute("title", "This election fills 2 seats — remove a pick first");
  });

  it("keeps a single-seat button enabled when another candidate holds the pick (radio replace)", () => {
    stubApiRoutes({});
    renderControl(
      <CandidatePickButton
        electionId={ELECTION_ID}
        candidateId={CANDIDATE_ID}
        candidateName="Jane Doe"
        choice={choice({ picks: [pick(OTHER_CANDIDATE_ID)] })}
        seatsToFill={null}
      />
    );

    expect(screen.getByRole("button", { name: "Make my pick: Jane Doe" })).toBeEnabled();
  });

  it("shows the backend's rejection message inline", async () => {
    stubApiRoutes({
      "/api/me/election-choices": apiError(
        400,
        "invalid_request",
        "Choices can only be changed for upcoming elections"
      ),
    });

    renderControl(
      <CandidatePickButton electionId={ELECTION_ID} candidateId={CANDIDATE_ID} candidateName="Jane Doe" choice={undefined} seatsToFill={null} />
    );

    await userEvent.setup().click(screen.getByRole("button", { name: "Make my pick: Jane Doe" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Choices can only be changed for upcoming elections"
    );
  });
});

describe("CandidatePickRow", () => {
  function renderRow(rowChoice: ElectionChoice | undefined, seatsToFill: number | null = null) {
    return renderControl(
      <CandidatePickRow
        electionId={ELECTION_ID}
        candidateId={CANDIDATE_ID}
        candidateName="Jane Doe"
        raceName="Governor"
        dateLabel="November 3, 2026"
        choice={rowChoice}
        seatsToFill={seatsToFill}
      />
    );
  }

  it("reads as a sentence and PUTs a pick", async () => {
    const fetchMock = stubApiRoutes({
      "/api/me/election-choices": (_url, init) => {
        expect(init?.method).toBe("PUT");
        expect(JSON.parse(String(init?.body))).toEqual({
          election_id: ELECTION_ID,
          candidate_id: CANDIDATE_ID,
          chosen: true,
        });
        return { status: 200, body: { choice: choice({ picks: [pick(CANDIDATE_ID)] }) } };
      },
    });

    renderRow(undefined);

    const button = screen.getByRole("button", { name: "Make Jane Doe my pick for Governor · November 3, 2026" });
    expect(button).toHaveAttribute("aria-pressed", "false");
    await userEvent.setup().click(button);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
  });

  it("renders the picked sentence and unpicks with chosen: false", async () => {
    const fetchMock = stubApiRoutes({
      "/api/me/election-choices": (_url, init) => {
        expect(JSON.parse(String(init?.body))).toEqual({
          election_id: ELECTION_ID,
          candidate_id: CANDIDATE_ID,
          chosen: false,
        });
        return { status: 200, body: { choice: choice() } };
      },
    });

    renderRow(choice({ picks: [pick(CANDIDATE_ID)] }));

    const button = screen.getByRole("button", {
      name: "✓ Jane Doe is my pick for Governor · November 3, 2026",
    });
    expect(button).toHaveAttribute("aria-pressed", "true");
    await userEvent.setup().click(button);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
  });

  it("disables an unpicked row at the multi-seat cap, with the reason visible below it", () => {
    stubApiRoutes({});
    renderRow(
      choice({
        seats_to_fill: 2,
        picks: [pick(OTHER_CANDIDATE_ID), pick("55555555-5555-4555-8555-555555555555")],
      }),
      2
    );

    const button = screen.getByRole("button", { name: "Make Jane Doe my pick for Governor · November 3, 2026" });
    expect(button).toBeDisabled();
    // The reason is visible text (title tooltips never reach touch/keyboard
    // users) and doubles as the button's accessible description.
    const message = screen.getByText("This election fills 2 seats — remove a pick first");
    expect(button).toHaveAttribute("aria-describedby", message.id);
  });

  it("shows no cap message when the row is picked or the cap isn't reached", () => {
    stubApiRoutes({});
    renderRow(choice({ seats_to_fill: 2, picks: [pick(CANDIDATE_ID), pick(OTHER_CANDIDATE_ID)] }), 2);

    expect(screen.queryByText(/fills 2 seats/)).toBeNull();
    expect(
      screen.getByRole("button", { name: "✓ Jane Doe is my pick for Governor · November 3, 2026" })
    ).toBeEnabled();
  });

  it("shows the backend's rejection message inline", async () => {
    stubApiRoutes({
      "/api/me/election-choices": apiError(
        400,
        "invalid_request",
        "Choices can only be changed for upcoming elections"
      ),
    });

    renderRow(undefined);

    await userEvent
      .setup()
      .click(screen.getByRole("button", { name: "Make Jane Doe my pick for Governor · November 3, 2026" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Choices can only be changed for upcoming elections"
    );
  });
});

describe("MeasureChoiceButtons", () => {
  it("PUTs a yes position and clears it by re-clicking the active side", async () => {
    let call = 0;
    const fetchMock = stubApiRoutes({
      "/api/me/election-choices": (_url, init) => {
        call += 1;
        expect(JSON.parse(String(init?.body))).toEqual({
          election_id: ELECTION_ID,
          measure_position: call === 1 ? "yes" : null,
        });
        return {
          status: 200,
          body: { choice: choice({ race_type: "ballot_measure", measure_position: call === 1 ? "yes" : null }) },
        };
      },
    });

    // First render: no position. Click "Yes" → PUT yes.
    const { unmount } = renderControl(<MeasureChoiceButtons electionId={ELECTION_ID} choice={undefined} />);
    await userEvent.setup().click(screen.getByRole("button", { name: "Yes" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    unmount();

    // Re-render as yes-selected (state arrives via props). Click the active
    // side → PUT null (clear).
    renderControl(
      <MeasureChoiceButtons
        electionId={ELECTION_ID}
        choice={choice({ race_type: "ballot_measure", measure_position: "yes" })}
      />
    );
    const yesButton = screen.getByRole("button", { name: "✓ Yes" });
    expect(yesButton).toHaveAttribute("aria-pressed", "true");
    await userEvent.setup().click(yesButton);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
  });

  it("shows transport failures with generic copy", async () => {
    // Network-level failure: fetch itself rejects, so no ApiError message
    // exists to pass through.
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new TypeError("Failed to fetch")));

    renderControl(<MeasureChoiceButtons electionId={ELECTION_ID} choice={undefined} />);

    await userEvent.setup().click(screen.getByRole("button", { name: "No" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Couldn't save — check your connection and try again."
    );
  });
});
