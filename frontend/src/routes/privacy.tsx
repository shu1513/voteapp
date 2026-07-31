import type { MetaFunction } from "react-router";
import { APP_NAME } from "@voteapp/api-client";
import { LegalDocumentPage } from "../pages/LegalDocumentPage";
import { pageMeta } from "../lib/pageMeta";

export const meta: MetaFunction = () =>
  pageMeta({
    title: `Privacy Policy · ${APP_NAME}`,
    description:
      "How Elections Simplified handles the address you enter, account information, and device and usage information.",
    path: "/privacy",
  });

export default function PrivacyRoute() {
  return <LegalDocumentPage document="privacy" />;
}
