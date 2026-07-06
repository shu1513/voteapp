import { VerifyTokenPage } from "../pages/VerifyTokenPage";

export default function VerifyEmailRoute() {
  return (
    <VerifyTokenPage
      endpoint="/api/auth/verify-email"
      title="Verifying your email"
      successMessage="Your email is verified. Log in to see your saved ballot and turn on election alerts."
    />
  );
}
