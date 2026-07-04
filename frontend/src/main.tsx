import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "./index.css";
import { App } from "./App";
import { HomePage } from "./pages/HomePage";
import { BallotPage } from "./pages/BallotPage";
import { ElectionPage } from "./pages/ElectionPage";
import { CandidatePage } from "./pages/CandidatePage";
import { LegalDocumentPage } from "./pages/LegalDocumentPage";
import { RegisterPage } from "./pages/RegisterPage";
import { LoginPage } from "./pages/LoginPage";
import { ForgotPasswordPage } from "./pages/ForgotPasswordPage";
import { ResetPasswordPage } from "./pages/ResetPasswordPage";
import { VerifyTokenPage } from "./pages/VerifyTokenPage";
import { SavedBallotPage } from "./pages/SavedBallotPage";
import { FollowsPage } from "./pages/FollowsPage";
import { SettingsPage } from "./pages/SettingsPage";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 60_000,
    },
  },
});

const router = createBrowserRouter([
  {
    element: <App />,
    children: [
      { path: "/", element: <HomePage /> },
      { path: "/ballot", element: <BallotPage /> },
      { path: "/elections/:electionId", element: <ElectionPage /> },
      { path: "/candidates/:candidateId", element: <CandidatePage /> },
      { path: "/disclaimer", element: <LegalDocumentPage document="disclaimer" /> },
      { path: "/terms", element: <LegalDocumentPage document="terms" /> },
      { path: "/privacy", element: <LegalDocumentPage document="privacy" /> },
      { path: "/register", element: <RegisterPage /> },
      { path: "/login", element: <LoginPage /> },
      { path: "/forgot-password", element: <ForgotPasswordPage /> },
      { path: "/reset-password", element: <ResetPasswordPage /> },
      {
        path: "/verify-email",
        element: (
          <VerifyTokenPage
            endpoint="/api/auth/verify-email"
            title="Verifying your email"
            successMessage="Your email is verified. Log in to see your saved ballot and turn on election alerts."
          />
        ),
      },
      {
        path: "/verify-email-change",
        element: (
          <VerifyTokenPage
            endpoint="/api/auth/verify-email-change"
            title="Confirming your new email"
            successMessage="Your email address has been updated and verified."
          />
        ),
      },
      { path: "/me/ballot", element: <SavedBallotPage /> },
      { path: "/me/follows", element: <FollowsPage /> },
      { path: "/me/settings", element: <SettingsPage /> },
    ],
  },
]);

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </StrictMode>
);
