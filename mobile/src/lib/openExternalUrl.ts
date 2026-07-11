import * as WebBrowser from "expo-web-browser";

/**
 * Opens source links, official sites, and (for now) the legal documents in
 * the in-app browser sheet. Legal screens get native routes in a later
 * Phase 3 chunk; until then they point at the production site.
 */
export function openExternalUrl(url: string): void {
  // Fire and forget: a failed open must not crash the calling screen.
  void WebBrowser.openBrowserAsync(url).catch(() => {});
}

export const WEB_ORIGIN = "https://impactperdollar.com";
