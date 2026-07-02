import { type IncomingMessage, ServerResponse } from "node:http";
import { Readable, Writable } from "node:stream";
import type { Express } from "express";
import { describe, expect, it, vi } from "vitest";

import { createApiApp } from "../../src/api/apiServer.js";
import { AUTH_SESSION_COOKIE_NAME } from "../../src/auth/authCookies.js";
import type { AuthService } from "../../src/auth/authService.js";

async function invokeExpressApp(
  app: Express,
  input: {
    method: string;
    path: string;
    body?: string;
    headers?: Record<string, string>;
    remoteAddress?: string;
  }
): Promise<{ statusCode: number; headers: Record<string, string>; body: unknown; rawBody: string }> {
  const requestBody = input.body ?? "";
  const headers = {
    ...(input.headers ?? {}),
    ...(requestBody.length > 0 && !input.headers?.["content-length"]
      ? { "content-length": Buffer.byteLength(requestBody).toString() }
      : {}),
  };
  const request = Readable.from(requestBody.length > 0 ? [requestBody] : []) as IncomingMessage;
  Object.assign(request, {
    method: input.method,
    url: input.path,
    headers,
    socket: {
      remoteAddress: input.remoteAddress ?? "127.0.0.1",
    },
  });

  const response = new ServerResponse(request);
  const responseChunks: Buffer[] = [];
  const socket = new Writable({
    write(chunk, _encoding, callback) {
      responseChunks.push(Buffer.from(chunk));
      callback();
    },
  });
  response.assignSocket(socket as never);

  return await new Promise((resolve, reject) => {
    response.on("finish", () => {
      const rawResponse = Buffer.concat(responseChunks).toString("utf8");
      const [, rawBody = ""] = rawResponse.split("\r\n\r\n");
      const body =
        rawBody.length > 0 && String(response.getHeader("content-type") ?? "").includes("application/json")
          ? JSON.parse(rawBody)
          : rawBody;
      const headers = Object.fromEntries(
        Object.entries(response.getHeaders()).map(([key, value]) => [key, String(value)])
      );
      resolve({
        statusCode: response.statusCode,
        headers,
        body,
        rawBody,
      });
    });
    response.on("error", reject);
    app(request, response);
  });
}

function createAuthServiceMock(overrides: Partial<AuthService> = {}): AuthService {
  return {
    register: vi.fn().mockResolvedValue(undefined),
    verifyEmail: vi.fn().mockResolvedValue(undefined),
    login: vi.fn().mockResolvedValue({ sessionId: "session-abc" }),
    logout: vi.fn().mockResolvedValue(undefined),
    forgotPassword: vi.fn().mockResolvedValue(undefined),
    resendVerification: vi.fn().mockResolvedValue(undefined),
    resetPassword: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

describe("public auth API endpoints", () => {
  it("registers users through the auth service", async () => {
    const resolveAddress = vi.fn();
    const authService = createAuthServiceMock();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, authService }), {
      method: "POST",
      path: "/api/auth/register",
      body: JSON.stringify({
        email: "user@example.com",
        password: "correct horse battery staple",
        first_name: "Alice",
      }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({ status: "ok" });
    expect(authService.register).toHaveBeenCalledWith({
      email: "user@example.com",
      password: "correct horse battery staple",
      firstName: "Alice",
    });
  });

  it("verifies emails through the auth service", async () => {
    const resolveAddress = vi.fn();
    const authService = createAuthServiceMock();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, authService }), {
      method: "POST",
      path: "/api/auth/verify-email",
      body: JSON.stringify({ token: "verify-token" }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({ status: "ok" });
    expect(authService.verifyEmail).toHaveBeenCalledWith({ token: "verify-token" });
  });

  it("logs users in and sets the auth session cookie", async () => {
    const resolveAddress = vi.fn();
    const authService = createAuthServiceMock({
      login: vi.fn().mockResolvedValue({ sessionId: "session-abc" }),
    });

    const response = await invokeExpressApp(createApiApp({ resolveAddress, authService }), {
      method: "POST",
      path: "/api/auth/login",
      body: JSON.stringify({ email: "user@example.com", password: "correct horse battery staple" }),
      headers: { "content-type": "application/json", cookie: `${AUTH_SESSION_COOKIE_NAME}=old-session` },
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({ status: "ok" });
    expect(authService.login).toHaveBeenCalledWith({
      email: "user@example.com",
      password: "correct horse battery staple",
      currentSessionId: "old-session",
    });
    expect(response.headers["set-cookie"]).toContain(`${AUTH_SESSION_COOKIE_NAME}=session-abc`);
    expect(response.headers["set-cookie"]).toContain("HttpOnly");
  });

  it("supports cross-origin cookie auth when an explicit origin is allowed", async () => {
    const resolveAddress = vi.fn();
    const authService = createAuthServiceMock({
      login: vi.fn().mockResolvedValue({ sessionId: "session-abc" }),
    });
    const origin = "https://frontend.example";

    const response = await invokeExpressApp(
      createApiApp({
        resolveAddress,
        authService,
        allowedOrigins: [origin],
        authSessionCookieOptions: { sameSite: "none", secure: true },
      }),
      {
        method: "POST",
        path: "/api/auth/login",
        body: JSON.stringify({ email: "user@example.com", password: "correct horse battery staple" }),
        headers: { "content-type": "application/json", origin },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.headers).toMatchObject({
      "access-control-allow-origin": origin,
      "access-control-allow-credentials": "true",
    });
    expect(response.headers["set-cookie"]).toContain("SameSite=None");
    expect(response.headers["set-cookie"]).toContain("Secure");
  });

  it("logs users out and clears the auth session cookie", async () => {
    const resolveAddress = vi.fn();
    const authService = createAuthServiceMock();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, authService }), {
      method: "POST",
      path: "/api/auth/logout",
      headers: {
        cookie: `${AUTH_SESSION_COOKIE_NAME}=old-session`,
        "content-type": "application/json",
      },
      body: "{}",
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({ status: "ok" });
    expect(authService.logout).toHaveBeenCalledWith({
      currentSessionId: "old-session",
    });
    expect(response.headers["set-cookie"]).toContain(`${AUTH_SESSION_COOKIE_NAME}=;`);
    expect(response.headers["set-cookie"]).toContain("Max-Age=0");
  });

  it("rejects logout without a JSON content type so cross-site form POSTs cannot log users out", async () => {
    const resolveAddress = vi.fn();
    const authService = createAuthServiceMock();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, authService }), {
      method: "POST",
      path: "/api/auth/logout",
      headers: {
        cookie: `${AUTH_SESSION_COOKIE_NAME}=old-session`,
        "content-type": "application/x-www-form-urlencoded",
      },
    });

    expect(response.statusCode).toBe(415);
    expect(authService.logout).not.toHaveBeenCalled();
  });

  it("forgets passwords through the auth service", async () => {
    const resolveAddress = vi.fn();
    const authService = createAuthServiceMock();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, authService }), {
      method: "POST",
      path: "/api/auth/forgot-password",
      body: JSON.stringify({ email: "user@example.com" }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({ status: "ok" });
    expect(authService.forgotPassword).toHaveBeenCalledWith({ email: "user@example.com" });
  });

  it("resends verification emails through the auth service", async () => {
    const resolveAddress = vi.fn();
    const authService = createAuthServiceMock();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, authService }), {
      method: "POST",
      path: "/api/auth/resend-verification",
      body: JSON.stringify({ email: "user@example.com" }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({ status: "ok" });
    expect(authService.resendVerification).toHaveBeenCalledWith({ email: "user@example.com" });
  });

  it("resets passwords through the auth service", async () => {
    const resolveAddress = vi.fn();
    const authService = createAuthServiceMock();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, authService }), {
      method: "POST",
      path: "/api/auth/reset-password",
      body: JSON.stringify({ token: "reset-token", password: "new password 123" }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({ status: "ok" });
    expect(authService.resetPassword).toHaveBeenCalledWith({
      token: "reset-token",
      password: "new password 123",
    });
  });

  it("returns 500 when auth is not configured", async () => {
    const resolveAddress = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress }), {
      method: "POST",
      path: "/api/auth/register",
      body: JSON.stringify({ email: "user@example.com", password: "correct horse battery staple" }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: {
        code: "internal_error",
        message: "Authentication is not configured",
      },
    });
  });

  it("rate limits register requests using the auth limiter", async () => {
    const resolveAddress = vi.fn();
    const authService = createAuthServiceMock();
    const authRateLimit = vi.fn().mockReturnValue({ allowed: false, retryAfterSeconds: 17 });

    const response = await invokeExpressApp(createApiApp({ resolveAddress, authService, authRateLimit }), {
      method: "POST",
      path: "/api/auth/register",
      body: JSON.stringify({
        email: "user@example.com",
        password: "correct horse battery staple",
      }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(429);
    expect(response.headers["retry-after"]).toBe("17");
    expect(response.body).toEqual({
      error: {
        code: "rate_limited",
        message: "Too many requests. Try again later.",
      },
    });
    expect(authRateLimit).toHaveBeenCalledWith({
      clientIp: expect.any(String),
      email: "user@example.com",
      method: "POST",
      pathname: "/api/auth/register",
    });
    expect(authService.register).not.toHaveBeenCalled();
  });
});
