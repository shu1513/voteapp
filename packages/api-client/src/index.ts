// Platform-neutral VoteApp API layer shared by the web frontend and the
// mobile app: typed contracts, the fetch wrapper, TanStack Query hooks, and
// pure formatting/scoring helpers. Nothing in here may touch browser-only
// globals (window, document, *Storage) — platform seams stay in each app.
// Web-standard globals the mobile runtime must polyfill at startup:
// crypto.randomUUID (useAddressSuggestions; expo-crypto provides it).

export * from "./types";
export * from "./client";
export * from "./format";
export * from "./researchAreaScoring";
export * from "./useAddressSuggestions";
export * from "./useFollows";
export * from "./useMe";
export * from "./useMyResearchAreas";
