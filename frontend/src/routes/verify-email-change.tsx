import { VerifyTokenPage } from "../pages/VerifyTokenPage";

export default function VerifyEmailChangeRoute() {
  return (
    <VerifyTokenPage
      endpoint="/api/auth/verify-email-change"
      title="Confirming your new email"
      successMessage="Your email address has been updated and verified."
    />
  );
}
