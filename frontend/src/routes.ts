import { index, layout, route, type RouteConfig } from "@react-router/dev/routes";

export default [
  layout("App.tsx", [
    index("pages/HomePage.tsx"),
    route("ballot", "pages/BallotPage.tsx"),
    // Guest ballot draft — the logged-out counterpart of /me/picks, rendered
    // from localStorage. Deliberately NOT in the router-worker's edge-cache
    // allowlist: the SSR document is draft-free, but there's nothing worth
    // caching either.
    route("draft", "pages/DraftPage.tsx"),
    route("elections/:electionId", "pages/ElectionPage.tsx"),
    route("candidates/:candidateId", "pages/CandidatePage.tsx"),
    route("mission", "pages/MissionPage.tsx"),
    route("disclaimer", "routes/disclaimer.tsx"),
    route("terms", "routes/terms.tsx"),
    route("privacy", "routes/privacy.tsx"),
    route("register", "pages/RegisterPage.tsx"),
    route("login", "pages/LoginPage.tsx"),
    route("forgot-password", "pages/ForgotPasswordPage.tsx"),
    route("reset-password", "pages/ResetPasswordPage.tsx"),
    route("verify-email", "routes/verify-email.tsx"),
    route("verify-email-change", "routes/verify-email-change.tsx"),
    route("me/welcome", "pages/WelcomePage.tsx"),
    route("me/ballot", "pages/SavedBallotPage.tsx"),
    route("me/picks", "pages/PicksPage.tsx"),
    route("me/follows", "pages/FollowsPage.tsx"),
    route("me/settings", "pages/SettingsPage.tsx"),
    // Public tokenized share page — the token is the authorization.
    route("picks/:token", "pages/PublicPickCardPage.tsx"),
    route("*", "pages/NotFoundPage.tsx"),
  ]),
] satisfies RouteConfig;
