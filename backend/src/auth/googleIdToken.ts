import { OAuth2Client } from "google-auth-library";

/** The ID-token claims Sign in with Google actually uses. All optional at
 * this layer — authService validates strictly and rejects what it needs but
 * does not get. */
export type GoogleIdTokenPayload = {
  sub?: string;
  email?: string;
  email_verified?: boolean;
  /** Hosted domain: present only for Google Workspace accounts. */
  hd?: string;
  given_name?: string;
};

/** Injected into authService so tests stub it — no network in vitest. */
export type VerifyGoogleIdToken = (idToken: string) => Promise<GoogleIdTokenPayload>;

/**
 * Real verifier: signature against Google's published keys (fetched and
 * cached by the library per Cache-Control), plus audience (our client ID),
 * issuer, and expiry. Any failure throws — authService normalizes every
 * throw into one generic invalid-credential error.
 */
export function createGoogleIdTokenVerifier(clientId: string): VerifyGoogleIdToken {
  const client = new OAuth2Client();
  return async (idToken) => {
    const ticket = await client.verifyIdToken({ idToken, audience: clientId });
    const payload = ticket.getPayload();
    if (!payload) {
      throw new Error("Google ID token verification returned no payload");
    }
    return {
      sub: payload.sub,
      email: payload.email,
      email_verified: payload.email_verified,
      hd: payload.hd,
      given_name: payload.given_name,
    };
  };
}
