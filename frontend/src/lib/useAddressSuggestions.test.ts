import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { ApiError } from "../api/client";

const apiRequestMock = vi.fn();
vi.mock("../api/client", async (importOriginal) => {
  const original = await importOriginal<typeof import("../api/client")>();
  return {
    ...original,
    apiRequest: (...args: unknown[]) => apiRequestMock(...args),
  };
});

const { useAddressSuggestions, SUGGEST_DEBOUNCE_MS } = await import("./useAddressSuggestions");

const SUGGESTION = {
  place_id: "place-1",
  description: "200 N Spring St, Los Angeles, CA 90012, USA",
  main_text: "200 N Spring St",
  secondary_text: "Los Angeles, CA 90012, USA",
};

beforeEach(() => {
  vi.useFakeTimers();
  apiRequestMock.mockReset();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("useAddressSuggestions", () => {
  it("debounces, requires 3 chars, and reuses one session token per entry", async () => {
    apiRequestMock.mockResolvedValue({ suggestions: [SUGGESTION] });
    const { result } = renderHook(() => useAddressSuggestions());

    act(() => {
      result.current.onInputChanged("20");
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(SUGGEST_DEBOUNCE_MS + 10);
    });
    expect(apiRequestMock).not.toHaveBeenCalled();

    act(() => {
      result.current.onInputChanged("200 N");
    });
    act(() => {
      result.current.onInputChanged("200 N Spring");
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(SUGGEST_DEBOUNCE_MS + 10);
    });
    // Two keystrokes, one debounced request.
    expect(apiRequestMock).toHaveBeenCalledTimes(1);
    const firstToken = (apiRequestMock.mock.calls[0][1] as { body: { session_token: string } }).body
      .session_token;
    expect(firstToken.length).toBeGreaterThan(8);
    expect(result.current.suggestions).toEqual([SUGGESTION]);

    act(() => {
      result.current.onInputChanged("200 N Spring St");
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(SUGGEST_DEBOUNCE_MS + 10);
    });
    const secondToken = (apiRequestMock.mock.calls[1][1] as { body: { session_token: string } }).body
      .session_token;
    // Same entry session: token must not change between keystrokes.
    expect(secondToken).toBe(firstToken);
  });

  it("sends the same token on retrieve, then kills the session (billing rule)", async () => {
    apiRequestMock.mockResolvedValueOnce({ suggestions: [SUGGESTION] });
    const { result } = renderHook(() => useAddressSuggestions());

    act(() => {
      result.current.onInputChanged("200 N Spring");
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(SUGGEST_DEBOUNCE_MS + 10);
    });
    const suggestToken = (apiRequestMock.mock.calls[0][1] as { body: { session_token: string } }).body
      .session_token;

    apiRequestMock.mockResolvedValueOnce({ address: "200 N Spring St, Los Angeles, CA 90012, USA" });
    let retrieved: string | null = null;
    await act(async () => {
      retrieved = await result.current.selectSuggestion(SUGGESTION);
    });
    expect(retrieved).toBe("200 N Spring St, Los Angeles, CA 90012, USA");
    const retrieveCall = apiRequestMock.mock.calls[1];
    expect(retrieveCall[0]).toBe("/api/address/autocomplete/retrieve");
    expect((retrieveCall[1] as { body: { session_token: string } }).body.session_token).toBe(suggestToken);

    // Next entry must start a NEW session token.
    apiRequestMock.mockResolvedValueOnce({ suggestions: [] });
    act(() => {
      result.current.onInputChanged("500 Main St");
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(SUGGEST_DEBOUNCE_MS + 10);
    });
    const nextToken = (apiRequestMock.mock.calls[2][1] as { body: { session_token: string } }).body
      .session_token;
    expect(nextToken).not.toBe(suggestToken);
  });

  it("clears the token before the retrieve settles: typing mid-retrieve gets a fresh session", async () => {
    apiRequestMock.mockResolvedValueOnce({ suggestions: [SUGGESTION] });
    const { result } = renderHook(() => useAddressSuggestions());

    act(() => {
      result.current.onInputChanged("200 N Spring");
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(SUGGEST_DEBOUNCE_MS + 10);
    });
    const firstToken = (apiRequestMock.mock.calls[0][1] as { body: { session_token: string } }).body
      .session_token;

    // Retrieve hangs; user keeps typing while it is in flight.
    let resolveRetrieve!: (value: unknown) => void;
    apiRequestMock.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveRetrieve = resolve;
        })
    );
    let selectPromise!: Promise<string | null>;
    act(() => {
      selectPromise = result.current.selectSuggestion(SUGGESTION);
    });

    apiRequestMock.mockResolvedValueOnce({ suggestions: [] });
    act(() => {
      result.current.onInputChanged("200 N Spring St Apt 4");
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(SUGGEST_DEBOUNCE_MS + 10);
    });
    const midRetrieveToken = (apiRequestMock.mock.calls[2][1] as { body: { session_token: string } }).body
      .session_token;
    expect(midRetrieveToken).not.toBe(firstToken);

    await act(async () => {
      resolveRetrieve({ address: "200 N Spring St, Los Angeles, CA 90012, USA" });
      await selectPromise;
    });
  });

  it("kills the session token when retrieve fails (indeterminate session state)", async () => {
    apiRequestMock.mockResolvedValueOnce({ suggestions: [SUGGESTION] });
    const { result } = renderHook(() => useAddressSuggestions());

    act(() => {
      result.current.onInputChanged("200 N Spring");
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(SUGGEST_DEBOUNCE_MS + 10);
    });
    const firstToken = (apiRequestMock.mock.calls[0][1] as { body: { session_token: string } }).body
      .session_token;

    apiRequestMock.mockRejectedValueOnce(new ApiError(503, "upstream_unavailable", "Google hiccup"));
    let retrieved: string | null = "sentinel";
    await act(async () => {
      retrieved = await result.current.selectSuggestion(SUGGESTION);
    });
    expect(retrieved).toBeNull();

    // Next entry must NOT reuse the token from the failed-retrieve session.
    apiRequestMock.mockResolvedValueOnce({ suggestions: [] });
    act(() => {
      result.current.onInputChanged("500 Main St");
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(SUGGEST_DEBOUNCE_MS + 10);
    });
    const nextToken = (apiRequestMock.mock.calls[2][1] as { body: { session_token: string } }).body
      .session_token;
    expect(nextToken).not.toBe(firstToken);
  });

  it("disables autocomplete for the session on the not-configured 500", async () => {
    apiRequestMock.mockRejectedValueOnce(
      new ApiError(500, "internal_error", "Address autocomplete is not configured")
    );
    const { result } = renderHook(() => useAddressSuggestions());

    act(() => {
      result.current.onInputChanged("200 N Spring");
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(SUGGEST_DEBOUNCE_MS + 10);
    });
    expect(result.current.enabled).toBe(false);

    act(() => {
      result.current.onInputChanged("200 N Spring St");
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(SUGGEST_DEBOUNCE_MS + 10);
    });
    // No further requests once disabled.
    expect(apiRequestMock).toHaveBeenCalledTimes(1);
  });

  it("swallows transient errors without disabling (429, network)", async () => {
    apiRequestMock.mockRejectedValueOnce(new ApiError(429, "rate_limited", "Too many requests"));
    const { result } = renderHook(() => useAddressSuggestions());

    act(() => {
      result.current.onInputChanged("200 N Spring");
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(SUGGEST_DEBOUNCE_MS + 10);
    });
    expect(result.current.enabled).toBe(true);
    expect(result.current.suggestions).toEqual([]);
  });
});
