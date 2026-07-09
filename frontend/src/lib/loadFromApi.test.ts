import { afterEach, describe, expect, it, vi } from "vitest";
import { loadFromApi } from "./loadFromApi";

function stubFetch(impl: () => Promise<unknown>) {
  vi.stubGlobal("fetch", vi.fn(impl));
}

async function thrownBy(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error("expected promise to reject");
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("loadFromApi", () => {
  it("returns the parsed body on success", async () => {
    stubFetch(async () => ({ ok: true, status: 200, json: async () => ({ id: "e-1" }) }));
    await expect(loadFromApi("/api/elections/e-1")).resolves.toEqual({ id: "e-1" });
  });

  it.each([404, 400])("maps upstream %d to a thrown 404 Response", async (status) => {
    stubFetch(async () => ({ ok: false, status, json: async () => ({}) }));
    const error = await thrownBy(loadFromApi("/api/elections/x"));
    expect(error).toBeInstanceOf(Response);
    expect((error as Response).status).toBe(404);
  });

  it("maps other upstream failures to a thrown 502 Response", async () => {
    stubFetch(async () => ({ ok: false, status: 500, json: async () => ({}) }));
    const error = await thrownBy(loadFromApi("/api/elections/x"));
    expect(error).toBeInstanceOf(Response);
    expect((error as Response).status).toBe(502);
  });

  it("maps a fetch timeout to a thrown 504 Response", async () => {
    stubFetch(async () => {
      throw new DOMException("The operation timed out.", "TimeoutError");
    });
    const error = await thrownBy(loadFromApi("/api/elections/x"));
    expect(error).toBeInstanceOf(Response);
    expect((error as Response).status).toBe(504);
  });

  it("maps a body-read timeout to a thrown 504 Response", async () => {
    stubFetch(async () => ({
      ok: true,
      status: 200,
      json: async () => {
        throw new DOMException("The operation timed out.", "TimeoutError");
      },
    }));
    const error = await thrownBy(loadFromApi("/api/elections/x"));
    expect(error).toBeInstanceOf(Response);
    expect((error as Response).status).toBe(504);
  });

  it("rethrows non-timeout fetch failures untouched", async () => {
    const refused = new TypeError("fetch failed");
    stubFetch(async () => {
      throw refused;
    });
    await expect(loadFromApi("/api/elections/x")).rejects.toBe(refused);
  });
});
