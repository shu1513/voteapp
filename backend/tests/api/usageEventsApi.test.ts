import { type IncomingMessage, ServerResponse } from "node:http";
import { Readable, Writable } from "node:stream";
import type { Express } from "express";
import { describe, expect, it, vi } from "vitest";

import { createApiApp } from "../../src/api/apiServer.js";
import type { AddressApiServerOptions } from "../../src/api/addressApiTypes.js";

// Same in-process invoker as apiServer.test.ts / membershipApi.test.ts: the
// real express app driven through node streams, so the middleware chain
// (path allowlist, JSON parser gating, limiter, dispatch) is exercised end
// to end — the JSON-parser allowlist is exactly the seam a unit test of the
// handler alone would miss.
async function invokeExpressApp(
  app: Express,
  input: { method: string; path: string; body?: string; headers?: Record<string, string> }
): Promise<{ statusCode: number; body: unknown }> {
  const requestBody = input.body ?? "";
  const headers = {
    ...(input.headers ?? {}),
    ...(requestBody.length > 0 ? { "content-length": Buffer.byteLength(requestBody).toString() } : {}),
  };
  const request = Readable.from(requestBody.length > 0 ? [Buffer.from(requestBody)] : []) as IncomingMessage;
  Object.assign(request, { method: input.method, url: input.path, headers, socket: { remoteAddress: "127.0.0.1" } });

  const response = new ServerResponse(request);
  const chunks: Buffer[] = [];
  const socket = new Writable({
    write(chunk, _encoding, callback) {
      chunks.push(Buffer.from(chunk));
      callback();
    },
  });
  response.assignSocket(socket as never);

  return await new Promise((resolve, reject) => {
    response.on("finish", () => {
      const [, rawBody = ""] = Buffer.concat(chunks).toString("utf8").split("\r\n\r\n");
      const body =
        rawBody.length > 0 && String(response.getHeader("content-type") ?? "").includes("application/json")
          ? JSON.parse(rawBody)
          : rawBody;
      resolve({ statusCode: response.statusCode, body });
    });
    response.on("error", reject);
    app(request, response);
  });
}

const JSON_HEADERS = { "content-type": "application/json" };
const PATH = "/api/usage/events";

function options(overrides: Partial<AddressApiServerOptions> = {}): AddressApiServerOptions {
  return { resolveAddress: vi.fn(), ...overrides };
}

function validEvent(name = "address_input", props: Record<string, unknown> = {}) {
  return {
    event_id: crypto.randomUUID(),
    session_id: "1c2d3e4f-5a6b-4c7d-8e9f-a0b1c2d3e4f5",
    page_view_id: null,
    name,
    route: "home",
    client_offset_ms: 10,
    props,
  };
}

describe("POST /api/usage/events", () => {
  it("404s when the intake is not wired (flag off hides the feature)", async () => {
    const response = await invokeExpressApp(createApiApp(options()), {
      method: "POST",
      path: PATH,
      headers: JSON_HEADERS,
      body: JSON.stringify({ v: 1, events: [validEvent()] }),
    });
    expect(response.statusCode).toBe(404);
  });

  it("parses the JSON body, stores the valid rows, and answers 204", async () => {
    const recordUsageEvents = vi.fn().mockResolvedValue(undefined);
    const response = await invokeExpressApp(createApiApp(options({ recordUsageEvents })), {
      method: "POST",
      path: PATH,
      headers: JSON_HEADERS,
      body: JSON.stringify({ v: 1, events: [validEvent(), validEvent("not_in_catalog")] }),
    });
    expect(response.statusCode).toBe(204);
    expect(recordUsageEvents).toHaveBeenCalledOnce();
    const [rows, dropped] = recordUsageEvents.mock.calls[0] as [{ name: string }[], number];
    expect(rows.map((row) => row.name)).toEqual(["address_input"]);
    expect(dropped).toBe(1);
  });

  it("requires the JSON content type (blocks plain form POSTs)", async () => {
    const recordUsageEvents = vi.fn().mockResolvedValue(undefined);
    const response = await invokeExpressApp(createApiApp(options({ recordUsageEvents })), {
      method: "POST",
      path: PATH,
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: "v=1",
    });
    expect(response.statusCode).toBe(415);
    expect(recordUsageEvents).not.toHaveBeenCalled();
  });

  it("400s an unusable envelope without touching storage", async () => {
    const recordUsageEvents = vi.fn().mockResolvedValue(undefined);
    const response = await invokeExpressApp(createApiApp(options({ recordUsageEvents })), {
      method: "POST",
      path: PATH,
      headers: JSON_HEADERS,
      body: JSON.stringify({ v: 2, events: [validEvent()] }),
    });
    expect(response.statusCode).toBe(400);
    expect(response.body).toMatchObject({ error: { code: "invalid_request" } });
    expect(recordUsageEvents).not.toHaveBeenCalled();
  });

  it("rejects other methods with 405", async () => {
    const response = await invokeExpressApp(createApiApp(options({ recordUsageEvents: vi.fn() })), {
      method: "GET",
      path: PATH,
    });
    expect(response.statusCode).toBe(405);
  });
});
