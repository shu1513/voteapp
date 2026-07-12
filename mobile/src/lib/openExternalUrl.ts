import * as WebBrowser from "expo-web-browser";

/**
 * Opens source links and official sites in the in-app browser sheet.
 * (Legal documents render natively at legal/[doc].)
 */
export function openExternalUrl(url: string): void {
  // Fire and forget: a failed open must not crash the calling screen.
  void WebBrowser.openBrowserAsync(url).catch(() => {});
}
