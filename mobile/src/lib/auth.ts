import { apiRequest, purgeAccountScopedQueries } from "@voteapp/api-client";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { clearSessionId, setSessionId } from "./sessionStore";

// Mobile session lifecycle around the shared api-client. The web relies on
// the httpOnly cookie; here login responses carry session_id in the body
// (the x-voteapp-client default header opts in) and the id lives in the
// platform keystore, read back per-request by initApi's getAuthHeader.

export function useLogin() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: { email: string; password: string }) =>
      apiRequest<{ status: string; session_id?: string }>("/api/auth/login", {
        method: "POST",
        body: { email: input.email, password: input.password },
      }),
    onSuccess: async (response) => {
      // Store the Bearer id BEFORE any refetch so /api/me goes out
      // authenticated.
      if (response.session_id) {
        await setSessionId(response.session_id);
      }
      // A previous session may have ended without a clean logout; cached
      // account data (e.g. ["me","ballot"]) must not bleed into this one.
      purgeAccountScopedQueries(queryClient);
      await queryClient.invalidateQueries({ queryKey: ["me"] });
    },
  });
}

export function useLogout() {
  const queryClient = useQueryClient();
  return useMutation({
    // The request must go out while the Bearer id is still stored — the
    // backend destroys the session it authenticates as. JSON content type is
    // the backend's CSRF guard, same as the web.
    mutationFn: () => apiRequest<{ status: string }>("/api/auth/logout", { method: "POST", body: {} }),
    onSettled: async () => {
      // Clear even when the request failed: a dead session id that the
      // backend already revoked must not keep the UI signed in.
      await clearSessionId().catch(() => {});
      queryClient.setQueryData(["me"], null);
      purgeAccountScopedQueries(queryClient);
    },
  });
}
