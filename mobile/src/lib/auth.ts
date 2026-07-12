import { apiRequest, purgeAccountScopedQueries } from "@voteapp/api-client";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { registerForPushIfPermitted, revokeStoredPushRegistration } from "./pushNotifications";
import { clearSessionId, setSessionId } from "./sessionStore";

// Mobile session lifecycle around the shared api-client. The web relies on
// the httpOnly cookie; here login responses carry session_id in the body
// (the x-voteapp-client default header opts in) and the id lives in the
// platform keystore, read back per-request by initApi's getAuthHeader.

export function useLogin() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async (input: { email: string; password: string }) => {
      const response = await apiRequest<{ status: string; session_id?: string }>("/api/auth/login", {
        method: "POST",
        body: { email: input.email, password: input.password },
      });
      // A 200 without session_id means the backend classified this request
      // as browser-originated (its provenance gate withholds the id — the
      // Expo web dev surface). Navigating "logged in" without a stored
      // session would be a silent lie; fail the mutation instead. Validated
      // here rather than onSuccess because mutation callbacks that throw do
      // not stop the caller's own onSuccess from running.
      if (!response.session_id) {
        throw new Error(
          "Login succeeded but no mobile session was issued (browser-originated requests are cookie-only by design). Use the native app."
        );
      }
      // Storing is part of the mutation, not onSuccess: a keystore write
      // failure must fail the login (a session that exists only in memory
      // would silently die on the next app launch), and callbacks that
      // throw cannot stop the caller's own onSuccess navigation.
      await setSessionId(response.session_id);
      return { sessionId: response.session_id };
    },
    onSuccess: async () => {
      // A previous session may have ended without a clean logout; cached
      // account data (e.g. ["me","ballot"]) must not bleed into this one.
      // The Bearer id is already stored, so this refetch goes out
      // authenticated.
      purgeAccountScopedQueries(queryClient);
      // Logout revoked this device's push token; quietly re-register when
      // the OS permission is already granted (never prompts). Unverified
      // accounts 403 on the endpoint and the helper swallows it.
      void registerForPushIfPermitted();
      await queryClient.invalidateQueries({ queryKey: ["me"] });
    },
  });
}

export function useLogout() {
  const queryClient = useQueryClient();
  return useMutation({
    // The request must go out while the Bearer id is still stored — the
    // backend destroys the session it authenticates as. JSON content type is
    // the backend's CSRF guard, same as the web. The push revoke goes first
    // because it is bearer-authed too, and it is best-effort (it swallows
    // its errors): a signed-out device should stop receiving pushes, but a
    // failed revoke must never block logout.
    mutationFn: async () => {
      await revokeStoredPushRegistration();
      return apiRequest<{ status: string }>("/api/auth/logout", { method: "POST", body: {} });
    },
    onSettled: async () => {
      // Clear even when the request failed: a dead session id that the
      // backend already revoked must not keep the UI signed in.
      await clearSessionId().catch(() => {});
      queryClient.setQueryData(["me"], null);
      purgeAccountScopedQueries(queryClient);
    },
  });
}

export function useChangePassword() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async (input: { currentPassword: string; newPassword: string }) => {
      const response = await apiRequest<{ status: string; session_id?: string }>("/api/me/password", {
        method: "POST",
        body: { current_password: input.currentPassword, new_password: input.newPassword },
      });
      // changePassword rotates every session server-side, including the one
      // this request authenticated with; the mobile response carries the
      // replacement id (Phase 0 contract). Not storing it would leave a
      // revoked Bearer id in the keystore and the next request would 401.
      if (response.session_id) {
        try {
          await setSessionId(response.session_id);
          return response;
        } catch {
          // Fall through to the local sign-out below.
        }
      }
      // Keystore write failed, or the response carried no replacement id
      // (contract violation). Either way the password DID change and the
      // old session is already dead — don't fail the mutation as if nothing
      // happened. Drop the stale id AND the cached identity so the UI lands
      // signed out instead of a signed-in shell whose every request 401s;
      // the new password works at the next login.
      await clearSessionId().catch(() => {});
      queryClient.setQueryData(["me"], null);
      purgeAccountScopedQueries(queryClient);
      return response;
    },
  });
}

export function useLogoutAll() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => apiRequest<{ status: string }>("/api/auth/logout-all", { method: "POST", body: {} }),
    // Unlike useLogout, clear only on SUCCESS (web parity): if the request
    // failed the other sessions were NOT revoked, and clearing locally would
    // dress a failure up as "logged out everywhere".
    onSuccess: async () => {
      await clearSessionId().catch(() => {});
      queryClient.setQueryData(["me"], null);
      purgeAccountScopedQueries(queryClient);
    },
  });
}

export function useDeleteAccount() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: { password: string }) =>
      apiRequest<{ status: string }>("/api/me", { method: "DELETE", body: { password: input.password } }),
    onSuccess: async () => {
      await clearSessionId().catch(() => {});
      queryClient.setQueryData(["me"], null);
      purgeAccountScopedQueries(queryClient);
    },
  });
}
