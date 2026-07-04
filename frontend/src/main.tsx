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
