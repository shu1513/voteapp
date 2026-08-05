// Minimal Chrome DevTools Protocol client for the Ohio SoS acquisition
// script (ohio_plan.md decision 9 + the PR 4 acquisition-script decision).
//
// Why a real browser at all: the portal sits behind Cloudflare. Plain HTTP is
// 403 even with a descriptive user-agent, and the 2026-08-04 probes showed
// headless Chrome and a fresh-profile automated Chrome are also refused —
// only the user's own long-lived Chrome profile passes. So this attaches to a
// Chrome the user has already started rather than launching its own, and no
// fingerprint spoofing or challenge-solving is attempted.
//
// Uses Node's built-in WebSocket, so it adds no dependency.

export const DEFAULT_OHIO_SOS_CHROME_DEBUG_URL = "http://127.0.0.1:9222";

type CdpMessage = {
  id?: number;
  method?: string;
  params?: Record<string, unknown>;
  sessionId?: string;
  result?: Record<string, unknown>;
  error?: { message?: string };
};

export type OhioSosChromeEventListener = (event: {
  method: string;
  params: Record<string, unknown>;
  sessionId?: string;
}) => void;

export class OhioSosChromeSession {
  private readonly socket: WebSocket;
  private readonly pending = new Map<
    number,
    { resolve: (value: Record<string, unknown>) => void; reject: (error: Error) => void }
  >();
  private readonly listeners = new Set<OhioSosChromeEventListener>();
  private nextId = 1;
  private closed = false;

  constructor(socket: WebSocket) {
    this.socket = socket;
    this.socket.addEventListener("message", (event) => {
      let message: CdpMessage;
      try {
        message = JSON.parse(String((event as MessageEvent).data)) as CdpMessage;
      } catch {
        // A malformed frame must not throw from the listener; commands it
        // would have answered are covered by the per-command deadline.
        return;
      }
      if (typeof message.id === "number") {
        const pending = this.pending.get(message.id);
        if (!pending) {
          return;
        }
        this.pending.delete(message.id);
        if (message.error) {
          pending.reject(new Error(`Chrome DevTools error: ${message.error.message ?? "unknown"}`));
        } else {
          pending.resolve(message.result ?? {});
        }
        return;
      }
      if (message.method) {
        for (const listener of this.listeners) {
          listener({ method: message.method, params: message.params ?? {}, sessionId: message.sessionId });
        }
      }
    });
    this.socket.addEventListener("close", () => {
      this.closed = true;
      for (const [, pending] of this.pending) {
        pending.reject(new Error("Chrome DevTools connection closed"));
      }
      this.pending.clear();
    });
  }

  // Every command gets a deadline: a Chrome that accepts a command and never
  // answers while the socket stays open would otherwise hang the promise (and
  // the whole attended run) forever.
  send(
    method: string,
    params: Record<string, unknown> = {},
    sessionId?: string,
    timeoutMs = 60_000
  ): Promise<Record<string, unknown>> {
    if (this.closed) {
      throw new Error("Chrome DevTools connection is closed");
    }
    const id = this.nextId;
    this.nextId += 1;
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Chrome DevTools command ${method} timed out after ${timeoutMs} ms`));
      }, timeoutMs);
      this.pending.set(id, {
        resolve: (value) => {
          clearTimeout(timeout);
          resolve(value);
        },
        reject: (error) => {
          clearTimeout(timeout);
          reject(error);
        },
      });
      this.socket.send(JSON.stringify({ id, method, params, ...(sessionId ? { sessionId } : {}) }));
    });
  }

  on(listener: OhioSosChromeEventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  close(): void {
    this.closed = true;
    this.socket.close();
  }
}

export async function connectOhioSosChrome(input: { debugUrl?: string; timeoutMs?: number } = {}): Promise<
  OhioSosChromeSession
> {
  if (typeof WebSocket === "undefined") {
    throw new Error(
      "This Node build has no global WebSocket; run the Ohio acquisition script on Node 22 or newer."
    );
  }
  const debugUrl = (input.debugUrl ?? DEFAULT_OHIO_SOS_CHROME_DEBUG_URL).replace(/\/+$/, "");
  let webSocketDebuggerUrl: string;
  try {
    const response = await fetch(`${debugUrl}/json/version`, {
      signal: AbortSignal.timeout(input.timeoutMs ?? 15_000),
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const version = (await response.json()) as { webSocketDebuggerUrl?: string };
    if (!version.webSocketDebuggerUrl) {
      throw new Error("response has no webSocketDebuggerUrl");
    }
    webSocketDebuggerUrl = version.webSocketDebuggerUrl;
  } catch (error) {
    throw new Error(
      `Could not reach Chrome's DevTools endpoint at ${debugUrl} (${(error as Error).message}). ` +
        "Quit Chrome, then start it with --remote-debugging-port=9222 using your normal profile."
    );
  }

  const socket = new WebSocket(webSocketDebuggerUrl);
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("Timed out connecting to Chrome DevTools")), input.timeoutMs ?? 15_000);
    socket.addEventListener(
      "open",
      () => {
        clearTimeout(timeout);
        resolve();
      },
      { once: true }
    );
    socket.addEventListener(
      "error",
      () => {
        clearTimeout(timeout);
        reject(new Error("Failed to connect to Chrome DevTools"));
      },
      { once: true }
    );
  });
  return new OhioSosChromeSession(socket);
}

export type OhioSosChromeTab = {
  targetId: string;
  sessionId: string;
};

export async function openOhioSosChromeTab(session: OhioSosChromeSession): Promise<OhioSosChromeTab> {
  const created = await session.send("Target.createTarget", { url: "about:blank" });
  const targetId = String(created.targetId);
  const attached = await session.send("Target.attachToTarget", { targetId, flatten: true });
  const sessionId = String(attached.sessionId);
  await session.send("Page.enable", {}, sessionId);
  await session.send("Runtime.enable", {}, sessionId);
  return { targetId, sessionId };
}

export async function closeOhioSosChromeTab(session: OhioSosChromeSession, tab: OhioSosChromeTab): Promise<void> {
  await session.send("Target.closeTarget", { targetId: tab.targetId }).catch(() => {});
}

export async function navigateOhioSosChromeTab(
  session: OhioSosChromeSession,
  tab: OhioSosChromeTab,
  url: string,
  input: { timeoutMs?: number } = {}
): Promise<void> {
  const loaded = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      off();
      reject(new Error(`Timed out loading ${url}`));
    }, input.timeoutMs ?? 60_000);
    const off = session.on((event) => {
      if (event.sessionId === tab.sessionId && event.method === "Page.loadEventFired") {
        clearTimeout(timeout);
        off();
        resolve();
      }
    });
  });
  await session.send("Page.navigate", { url }, tab.sessionId);
  await loaded;
}

// Runs an expression in the page and returns its JSON value. Used only for
// reading the portal's own rendered tables — the script never injects
// behaviour beyond what a person clicking the page would produce.
export async function evaluateInOhioSosChromeTab<T>(
  session: OhioSosChromeSession,
  tab: OhioSosChromeTab,
  expression: string
): Promise<T> {
  const result = (await session.send(
    "Runtime.evaluate",
    { expression, awaitPromise: true, returnByValue: true },
    tab.sessionId
  )) as { result?: { value?: T }; exceptionDetails?: { text?: string } };
  if (result.exceptionDetails) {
    throw new Error(`Page evaluation failed: ${result.exceptionDetails.text ?? "unknown error"}`);
  }
  return result.result?.value as T;
}
