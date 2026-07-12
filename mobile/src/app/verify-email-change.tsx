import { VerifyTokenScreen } from "../components/VerifyTokenScreen";

/** Email-change confirmation (deep-link target in Phase 4). */
export default function VerifyEmailChangeScreen() {
  return (
    <VerifyTokenScreen
      endpoint="/api/auth/verify-email-change"
      title="Confirm your new email"
      successMessage="Your email address has been updated."
      revokesSessions={false}
    />
  );
}
