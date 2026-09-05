import { useEffect, useRef } from "react";
import { useLocation, useMatches } from "react-router";
import { ApiError, useMe } from "@voteapp/api-client";

// First-party usage analytics (docs/plans/usage-analytics.md). What leaves
// the browser: a per-tab session id, a per-page-view id, a route id (never a
// path — /picks/<token> must not reach analytics), a catalog name, a small
// props object, and foreground time. What never leaves: addresses, district
// ids, candidate/election ids, emails, chat text, user ids. The backend
// (backend/src/usage/events.ts) re-validates every prop against the same
// catalog, so this module cannot widen what is stored even by mistake.
//
// Inert unless VITE_USAGE_ANALYTICS_ENABLED is "true" at build time, and
// always inert under SSR, browser automation, the /privacy opt-out, or when
// sessionStorage is unavailable. track() never throws and never awaits.

export const USAGE_ROUTES = [
  "home",
  "ballot",
  "draft",
  "election",
  "candidate",
  "mission",
  "support",
  "support_member",
  "support_once",
  "disclaimer",
  "terms",
  "privacy",
  "register",
  "login",
  "forgot_password",
  "reset_password",
  "verify_email",
  "verify_email_change",
  "welcome",
  "saved_ballot",
  "picks",
  "follows",
  "settings",
  "pick_card",
  "not_found",
  "other",
] as const;
export type UsageRoute = (typeof USAGE_ROUTES)[number];

// React Router framework-mode route ids are the file paths from routes.ts
// without their extension. Anything unmapped (tests, future routes) is
// "other" — never the pathname.
const ROUTE_BY_MATCH_ID: Record<string, UsageRoute> = {
  "pages/HomePage": "home",
  "pages/BallotPage": "ballot",
  "pages/DraftPage": "draft",
  "pages/ElectionPage": "election",
  "pages/CandidatePage": "candidate",
  "pages/MissionPage": "mission",
  "pages/SupportPage": "support",
  "pages/SupportMemberPage": "support_member",
  "pages/SupportOncePage": "support_once",
  "routes/disclaimer": "disclaimer",
  "routes/terms": "terms",
  "routes/privacy": "privacy",
  "pages/RegisterPage": "register",
  "pages/LoginPage": "login",
  "pages/ForgotPasswordPage": "forgot_password",
  "pages/ResetPasswordPage": "reset_password",
  "routes/verify-email": "verify_email",
  "routes/verify-email-change": "verify_email_change",
  "pages/WelcomePage": "welcome",
  "pages/SavedBallotPage": "saved_ballot",
  "pages/PicksPage": "picks",
  "pages/FollowsPage": "follows",
  "pages/SettingsPage": "settings",
  "pages/PublicPickCardPage": "pick_card",
  "pages/NotFoundPage": "not_found",
};

export function routeForMatchId(matchId: string | undefined): UsageRoute {
  return (matchId && ROUTE_BY_MATCH_ID[matchId]) || "other";
}

export type ErrorCategory = "not_found" | "rate_limited" | "server" | "network" | "address" | "render" | "other";

/** Coarse category for an error the visitor saw; never the message. */
export function errorCategoryOf(error: unknown): ErrorCategory {
  if (error instanceof ApiError) {
    if (error.status === 404) return "not_found";
    if (error.status === 422) return "address";
    if (error.status === 429) return "rate_limited";
    if (error.status >= 500) return "server";
    return "other";
  }
  return "network";
}

export type CountBucket = "0" | "1-3" | "4-10" | "11-25" | "26+";

export function countBucket(count: number): CountBucket {
  if (count <= 0) return "0";
  if (count <= 3) return "1-3";
  if (count <= 10) return "4-10";
  if (count <= 25) return "11-25";
  return "26+";
}

export function positionBucket(position: number): "1-3" | "4-10" | "11+" {
  if (position <= 3) return "1-3";
  if (position <= 10) return "4-10";
  return "11+";
}

type Props = Record<string, unknown>;

type UsageEvent = {
  event_id: string;
  session_id: string;
  page_view_id: string | null;
  name: string;
  route: UsageRoute;
  client_offset_ms: number;
  props: Props;
};

type StoredSession = { id: string; started_at: number; last_active_at: number };

const SESSION_KEY = "voteapp_usage_session";
const OPTOUT_KEY = "voteapp_usage_optout";
const DRAFT_KEY = "voteapp_ballot_draft";
const ENDPOINT = "/api/usage/events";
const PAYLOAD_VERSION = 1;
const SESSION_IDLE_MS = 30 * 60_000;
const MAX_CLIENT_OFFSET_MS = 7 * 24 * 60 * 60_000;
const FLUSH_INTERVAL_MS = 15_000;
const FLUSH_AT_EVENTS = 20;
const PAGE_TIME_CHECKPOINT_MS = 30_000;
const MAX_QUEUE = 200;
const MAX_BATCH_EVENTS = 40;
// Under the API's 16 KB JSON limit with room for the envelope.
const MAX_BODY_BYTES = 12_000;

let session: StoredSession | null = null;
let queue: UsageEvent[] = [];
const retried = new WeakSet<UsageEvent>();
let inFlight = false;
// A 404 means the server has the feature off: stop sending for this tab.
let serverDisabled = false;
let currentRoute: UsageRoute = "other";
// The page the app is rendering right now, stamped by useUsageTracking
// DURING render (parents render before children, but child effects run
// before parent effects — so a child's track() on mount would otherwise
// see the previous page). track() syncs the page view from it lazily.
let desired: { route: UsageRoute; key: string } | null = null;
let view: { id: string; key: string; visibleMs: number; visibleSince: number | null } | null = null;

function isEnabled(): boolean {
  if (typeof window === "undefined" || typeof navigator === "undefined") return false;
  if (import.meta.env.VITE_USAGE_ANALYTICS_ENABLED !== "true") return false;
  if (navigator.webdriver) return false;
  if (serverDisabled) return false;
  return !isUsageOptedOut();
}

export function isUsageOptedOut(): boolean {
  try {
    return window.localStorage.getItem(OPTOUT_KEY) === "1";
  } catch {
    return false;
  }
}

/** The /privacy control. Opting out also drops anything not yet sent. */
export function setUsageOptOut(optedOut: boolean): void {
  try {
    if (optedOut) {
      window.localStorage.setItem(OPTOUT_KEY, "1");
    } else {
      window.localStorage.removeItem(OPTOUT_KEY);
    }
  } catch {
    // Storage blocked: nothing to remember, and the module stays inert anyway.
  }
  if (optedOut) {
    queue = [];
  }
}

function readStoredSession(): StoredSession | null {
  const raw = window.sessionStorage.getItem(SESSION_KEY);
  if (!raw) return null;
  try {
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null) return null;
    const { id, started_at, last_active_at } = parsed as Record<string, unknown>;
    if (typeof id !== "string" || typeof started_at !== "number" || typeof last_active_at !== "number") return null;
    return { id, started_at, last_active_at };
  } catch {
    return null;
  }
}

/** Ensures a live session, rotating after 30 idle minutes. Returns null (and
 * leaves the module inert) when sessionStorage is unavailable. */
function ensureSession(now: number): StoredSession | null {
  try {
    if (session && now - session.last_active_at <= SESSION_IDLE_MS) {
      session.last_active_at = now;
      window.sessionStorage.setItem(SESSION_KEY, JSON.stringify(session));
      return session;
    }
    const stored = session ?? readStoredSession();
    if (stored && now - stored.last_active_at <= SESSION_IDLE_MS) {
      session = { ...stored, last_active_at: now };
      window.sessionStorage.setItem(SESSION_KEY, JSON.stringify(session));
      return session;
    }
    session = { id: crypto.randomUUID(), started_at: now, last_active_at: now };
    window.sessionStorage.setItem(SESSION_KEY, JSON.stringify(session));
    queue.push(
      buildEvent(session, "session_start", sessionStartProps(), now, { route: currentRoute, pageViewId: view?.id ?? null })
    );
    return session;
  } catch {
    return null;
  }
}

function referrerBucket(): "search" | "social" | "direct" | "internal" | "other" {
  const referrer = document.referrer;
  if (!referrer) return "direct";
  let host: string;
  try {
    host = new URL(referrer).hostname.toLowerCase();
  } catch {
    return "other";
  }
  if (host === window.location.hostname.toLowerCase()) return "internal";
  if (/(^|\.)(google|bing|duckduckgo|yahoo|ecosia|brave)\.[a-z.]+$|search/.test(host)) return "search";
  if (
    /(^|\.)(facebook|fb|instagram|threads|twitter|x|t|reddit|linkedin|tiktok|youtube|bsky|nextdoor|snapchat)\.(com|net|co|app|social)$/.test(
      host
    )
  ) {
    return "social";
  }
  return "other";
}

function deviceBucket(): "phone" | "tablet" | "desktop" {
  const width = window.innerWidth;
  const coarse = window.matchMedia?.("(pointer: coarse)").matches ?? false;
  if (width < 640) return "phone";
  if (coarse && width < 1024) return "tablet";
  return "desktop";
}

function hadSavedDraft(): boolean {
  try {
    return window.localStorage.getItem(DRAFT_KEY) !== null;
  } catch {
    return false;
  }
}

function sessionStartProps(): Props {
  return {
    referrer_bucket: referrerBucket(),
    device: deviceBucket(),
    landing_route: currentRoute,
    had_saved_draft: hadSavedDraft(),
    auth: "unknown",
  };
}

function buildEvent(
  live: StoredSession,
  name: string,
  props: Props,
  now: number,
  attribution: { route: UsageRoute; pageViewId: string | null }
): UsageEvent {
  return {
    event_id: crypto.randomUUID(),
    session_id: live.id,
    page_view_id: attribution.pageViewId,
    name,
    route: attribution.route,
    client_offset_ms: Math.min(Math.max(0, Math.round(now - live.started_at)), MAX_CLIENT_OFFSET_MS),
    props,
  };
}

/** Queues one event; callers have already checked isEnabled(). */
function enqueue(name: string, props: Props, attribution = { route: currentRoute, pageViewId: view?.id ?? null }): void {
  const now = Date.now();
  const live = ensureSession(now);
  if (!live) return;
  queue.push(buildEvent(live, name, props, now, attribution));
  if (queue.length > MAX_QUEUE) {
    queue.splice(0, queue.length - MAX_QUEUE);
  }
  if (queue.length >= FLUSH_AT_EVENTS) {
    flush("fetch");
  }
}

/** Starts a page view for the rendered page when it changed: closes the old
 * view (final page_time, attributed to it) and emits page_view for the new
 * one. Idempotent, so both track() and the route effect may call it. */
function syncPageView(): void {
  if (!desired || (view && view.key === desired.key)) return;
  const previous = view;
  const previousRoute = currentRoute;
  if (previous) {
    accumulateVisible(performance.now());
  }
  const visible = typeof document !== "undefined" && document.visibilityState === "visible";
  currentRoute = desired.route;
  view = { id: crypto.randomUUID(), key: desired.key, visibleMs: 0, visibleSince: visible ? performance.now() : null };
  if (previous) {
    // The closing view's final time, attributed to the page it measured.
    enqueue("page_time", { visible_ms: Math.round(previous.visibleMs) }, { route: previousRoute, pageViewId: previous.id });
  }
  enqueue("page_view", {});
}

/** Records one catalog event. Safe to call from anywhere; never throws. */
export function track(name: string, props: Props = {}): void {
  try {
    if (!isEnabled()) return;
    syncPageView();
    enqueue(name, props);
  } catch {
    // Analytics must never surface as an app error.
  }
}

function takeBatch(): UsageEvent[] {
  const batch: UsageEvent[] = [];
  let bytes = 32;
  while (queue.length > 0 && batch.length < MAX_BATCH_EVENTS) {
    const next = queue[0]!;
    const size = JSON.stringify(next).length + 1;
    if (batch.length > 0 && bytes + size > MAX_BODY_BYTES) break;
    batch.push(queue.shift()!);
    bytes += size;
  }
  return batch;
}

/** Puts a failed batch back at the front, once per event. */
function requeueOnce(batch: UsageEvent[]): void {
  const again = batch.filter((event) => !retried.has(event));
  for (const event of again) retried.add(event);
  queue = [...again, ...queue].slice(-MAX_QUEUE);
}

function flush(mode: "fetch" | "beacon"): void {
  // Consent is re-checked at send time: a batch re-queued by a failed
  // request after an opt-out must never leave the browser.
  if (!isEnabled()) {
    queue = [];
    return;
  }
  if (queue.length === 0) return;
  if (mode === "beacon") {
    // Queued-not-confirmed by design: this is the page-hide path where a
    // fetch would be cancelled. Drain everything; no retry is possible.
    if (typeof navigator.sendBeacon !== "function") {
      mode = "fetch";
    } else {
      while (queue.length > 0) {
        const batch = takeBatch();
        const body = JSON.stringify({ v: PAYLOAD_VERSION, events: batch });
        if (!navigator.sendBeacon(ENDPOINT, new Blob([body], { type: "application/json" }))) {
          requeueOnce(batch);
          break;
        }
      }
      return;
    }
  }
  if (inFlight) return;
  const batch = takeBatch();
  const body = JSON.stringify({ v: PAYLOAD_VERSION, events: batch });
  inFlight = true;
  fetch(ENDPOINT, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
    keepalive: true,
    credentials: "same-origin",
  })
    .then((response) => {
      if (response.status === 404) {
        serverDisabled = true;
        queue = [];
      } else if (response.status === 429 || response.status >= 500) {
        requeueOnce(batch);
      }
    })
    .catch(() => requeueOnce(batch))
    .finally(() => {
      inFlight = false;
      if (queue.length >= FLUSH_AT_EVENTS) flush("fetch");
    });
}

function accumulateVisible(now: number): void {
  if (view && view.visibleSince !== null) {
    view.visibleMs += now - view.visibleSince;
    view.visibleSince = now;
  }
}

/** Cumulative foreground time for the current view. Reports keep the max per
 * page_view_id, so repeated checkpoints are harmless. */
function checkpointPageTime(): void {
  if (!view || !isEnabled()) return;
  accumulateVisible(performance.now());
  enqueue("page_time", { visible_ms: Math.round(view.visibleMs) }, { route: currentRoute, pageViewId: view.id });
}

function onVisibilityChange(): void {
  if (!view) return;
  if (document.visibilityState === "hidden") {
    checkpointPageTime();
    view.visibleSince = null;
    flush("beacon");
  } else {
    view.visibleSince = performance.now();
  }
}

function onPageHide(): void {
  if (!view) return;
  checkpointPageTime();
  view.visibleSince = null;
  flush("beacon");
}

/**
 * Mounted once in App: page views and foreground time per pathname change,
 * the session's auth_resolved transitions, periodic flushes, and the
 * hide-time beacon. Query-string changes (sort, filters) stay inside one
 * page view on purpose — they are controls on a page, not a new page.
 */
export function useUsageTracking(): void {
  const location = useLocation();
  const matches = useMatches();
  const { me } = useMe();
  // Render-phase stamp (idempotent module write, no React state): children
  // rendering under this layout, and their mount effects, already see the
  // page they belong to.
  if (typeof window !== "undefined") {
    desired = { route: routeForMatchId(matches[matches.length - 1]?.id), key: location.pathname };
  }
  const lastAuth = useRef<"guest" | "signed_in" | null>(null);

  useEffect(() => {
    if (!isEnabled()) return;
    syncPageView();
  }, [location.pathname]);

  useEffect(() => {
    if (me === undefined) return;
    const auth = me === null ? "guest" : "signed_in";
    if (lastAuth.current === auth) return;
    lastAuth.current = auth;
    track("auth_resolved", { auth });
  }, [me]);

  useEffect(() => {
    // Build flag only: the runtime checks (opt-out, storage, server 404)
    // live in track()/flush(), so opting back in on /privacy takes effect
    // without a reload while an opted-out tab pays only for idle timers.
    if (import.meta.env.VITE_USAGE_ANALYTICS_ENABLED !== "true") return;
    document.addEventListener("visibilitychange", onVisibilityChange);
    window.addEventListener("pagehide", onPageHide);
    const flushTimer = window.setInterval(() => flush("fetch"), FLUSH_INTERVAL_MS);
    const timeTimer = window.setInterval(() => {
      if (document.visibilityState === "visible") checkpointPageTime();
    }, PAGE_TIME_CHECKPOINT_MS);
    return () => {
      document.removeEventListener("visibilitychange", onVisibilityChange);
      window.removeEventListener("pagehide", onPageHide);
      window.clearInterval(flushTimer);
      window.clearInterval(timeTimer);
    };
  }, []);
}

type BallotQueryState = {
  isPending: boolean;
  isError: boolean;
  data: { districts: { state: string }[]; elections: unknown[] } | undefined;
  error: unknown;
};

/**
 * One ballot_result per settled list load (ready / empty / error), shared by
 * the anonymous and saved ballot pages. Coarse by construction: US state
 * codes, count buckets, and which banners showed — never district ids.
 */
export function useTrackBallotResult(
  ballot: BallotQueryState,
  context: { scope: "exact" | "zip" | "region" | "unknown"; partialBanner: boolean; ambiguousBanner: boolean }
): void {
  const tracked = useRef<unknown>(undefined);
  const { scope, partialBanner, ambiguousBanner } = context;
  useEffect(() => {
    if (ballot.isPending) return;
    const settled = ballot.isError ? ballot.error : ballot.data;
    if (settled === undefined || tracked.current === settled) return;
    tracked.current = settled;
    const data = ballot.isError ? undefined : ballot.data;
    const states = [...new Set((data?.districts ?? []).map((district) => district.state))]
      .filter((state) => /^[A-Z]{2}$/.test(state))
      .slice(0, 3);
    track("ballot_result", {
      outcome: ballot.isError ? "error" : data && data.elections.length > 0 ? "ready" : "empty",
      scope,
      states,
      election_count_bucket: countBucket(data?.elections.length ?? 0),
      district_count_bucket: countBucket(data?.districts.length ?? 0),
      partial_banner: partialBanner,
      ambiguous_banner: ambiguousBanner,
    });
  }, [ballot.isPending, ballot.isError, ballot.data, ballot.error, scope, partialBanner, ambiguousBanner]);
}

/** Test seam: flushes the queue now (fetch path). */
export function flushUsageEventsForTests(): void {
  flush("fetch");
}

/** Test seam: wipes module state between tests. */
export function resetUsageForTests(): void {
  session = null;
  queue = [];
  inFlight = false;
  serverDisabled = false;
  currentRoute = "other";
  desired = null;
  view = null;
  try {
    window.sessionStorage.removeItem(SESSION_KEY);
  } catch {
    // ignore
  }
}
