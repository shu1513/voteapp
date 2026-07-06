import { index, layout, route, type RouteConfig } from "@react-router/dev/routes";

export default [
  layout("App.tsx", [
    index("pages/HomePage.tsx"),
    route("ballot", "pages/BallotPage.tsx"),
    route("elections/:electionId", "pages/ElectionPage.tsx"),
    route("candidates/:candidateId", "pages/CandidatePage.tsx"),
    route("disclaimer", "routes/disclaimer.tsx"),
    route("terms", "routes/terms.tsx"),
    route("privacy", "routes/privacy.tsx"),
    route("register", "pages/RegisterPage.tsx"),
    route("login", "pages/LoginPage.tsx"),
    route("forgot-password", "pages/ForgotPasswordPage.tsx"),
    route("reset-password", "pages/ResetPasswordPage.tsx"),
    route("verify-email", "routes/verify-email.tsx"),
    route("verify-email-change", "routes/verify-email-change.tsx"),
    route("me/ballot", "pages/SavedBallotPage.tsx"),
    route("me/follows", "pages/FollowsPage.tsx"),
    route("me/settings", "pages/SettingsPage.tsx"),
    route("*", "pages/NotFoundPage.tsx"),
  ]),
] satisfies RouteConfig;
