import { VerifyTokenScreen } from "../components/VerifyTokenScreen";

/** Registration email confirmation (deep-link target in Phase 4). */
export default function VerifyEmailScreen() {
  return (
    <VerifyTokenScreen
      endpoint="/api/auth/verify-email"
      title="Confirm your email"
      successMessage="Your email is confirmed. For security you have been logged out everywhere — log in to reach your saved ballot, follows and preferences."
      revokesSessions
    />
  );
}
