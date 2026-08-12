import { useEffect, useRef, useState } from "react";

// "Sign in with Google" via Google Identity Services (GIS), JS-callback mode:
// the rendered button hands the callback a Google-signed ID-token JWT, which
// the page POSTs to /api/auth/google. No redirect URIs, no OAuth scopes, no
// Google API access. Renders nothing when the client ID is not configured or
// the GIS script fails to load — the password form is never affected.

const GIS_SCRIPT_SRC = "https://accounts.google.com/gsi/client";

// Read per render (not a module const) so vitest's stubEnv reaches it.
function getClientId(): string {
  return (import.meta.env.VITE_GOOGLE_OAUTH_CLIENT_ID as string | undefined)?.trim() ?? "";
}

type GoogleAccountsId = {
  initialize(config: {
    client_id: string;
    callback: (response: { credential?: string }) => void;
  }): void;
  renderButton(parent: HTMLElement, options: Record<string, unknown>): void;
};

declare global {
  interface Window {
    google?: { accounts?: { id?: GoogleAccountsId } };
  }
}

// Singleton, idempotent loader: Login → Register navigation must not inject
// the script twice. A load failure clears the cached promise so a later mount
// can retry instead of being stuck failed for the session.
let gisScriptPromise: Promise<void> | null = null;
function loadGisScript(): Promise<void> {
  if (window.google?.accounts?.id) {
    return Promise.resolve();
  }
  if (!gisScriptPromise) {
    gisScriptPromise = new Promise<void>((resolve, reject) => {
      const script = document.createElement("script");
      script.src = GIS_SCRIPT_SRC;
      script.async = true;
      script.onload = () => resolve();
      script.onerror = () => {
        gisScriptPromise = null;
        script.remove();
        reject(new Error("Failed to load the Google sign-in script"));
      };
      document.head.appendChild(script);
    });
  }
  return gisScriptPromise;
}

type GoogleSignInButtonProps = {
  /** GIS button label variant: register page vs login page. */
  text: "signup_with" | "signin_with";
  /** Blocks interaction (clickwrap gate / request in flight). The GIS button
   * is an iframe with no disabled state, so the wrapper eats pointer events. */
  disabled?: boolean;
  onCredential: (credential: string) => void;
};

export function GoogleSignInButton({ text, disabled = false, onCredential }: GoogleSignInButtonProps) {
  const clientId = getClientId();
  const containerRef = useRef<HTMLDivElement | null>(null);
  // The GIS callback is registered once per initialize(); route it through a
  // ref so it always calls the latest handler without re-initializing.
  const onCredentialRef = useRef(onCredential);
  onCredentialRef.current = onCredential;
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    if (!clientId) {
      return;
    }
    let cancelled = false;
    loadGisScript()
      .then(() => {
        const accountsId = window.google?.accounts?.id;
        const container = containerRef.current;
        if (cancelled || !accountsId || !container) {
          return;
        }
        // initialize() is global (last call wins), so remounts and page
        // changes never leave a stale callback behind.
        accountsId.initialize({
          client_id: clientId,
          callback: (response) => {
            if (response?.credential) {
              onCredentialRef.current(response.credential);
            }
          },
        });
        container.replaceChildren();
        accountsId.renderButton(container, {
          type: "standard",
          theme: "outline",
          size: "large",
          text,
          width: 320,
        });
      })
      .catch(() => {
        if (!cancelled) {
          setFailed(true);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [clientId, text]);

  if (!clientId || failed) {
    return null;
  }

  return (
    <div aria-disabled={disabled} className={disabled ? "pointer-events-none opacity-50" : undefined}>
      <div ref={containerRef} data-testid="google-signin-button" />
    </div>
  );
}
