import { useEffect } from "react";

/**
 * Injects a schema.org JSON-LD script for the current page and removes it on
 * unmount or data change. Pass undefined while the page's subject is still
 * loading. Only JS-rendering crawlers see this (the SPA ships empty HTML);
 * full coverage arrives with prerendering (plan.md Phase 6).
 */
export function useJsonLd(data: Record<string, unknown> | undefined) {
  const json = data ? JSON.stringify({ "@context": "https://schema.org", ...data }) : undefined;
  useEffect(() => {
    if (!json) {
      return;
    }
    const script = document.createElement("script");
    script.type = "application/ld+json";
    script.text = json;
    document.head.appendChild(script);
    return () => {
      script.remove();
    };
  }, [json]);
}
