import type { MetaFunction } from "react-router";
import { APP_NAME } from "@voteapp/api-client";
import { LegalDocumentPage } from "../pages/LegalDocumentPage";
import { pageMeta } from "../lib/pageMeta";

export const meta: MetaFunction = () =>
  pageMeta({
    title: `Disclaimer · ${APP_NAME}`,
    description:
      "Elections Simplified provides AI-assisted informational research only, and is not an official election source.",
    path: "/disclaimer",
  });

export default function DisclaimerRoute() {
  return <LegalDocumentPage document="disclaimer" />;
}
