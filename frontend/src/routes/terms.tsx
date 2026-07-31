import type { MetaFunction } from "react-router";
import { APP_NAME } from "@voteapp/api-client";
import { LegalDocumentPage } from "../pages/LegalDocumentPage";
import { pageMeta } from "../lib/pageMeta";

export const meta: MetaFunction = () =>
  pageMeta({
    title: `Terms of Use · ${APP_NAME}`,
    description:
      "The Terms of Use for Elections Simplified, including the arbitration and class-action waiver terms in Section 12.",
    path: "/terms",
  });

export default function TermsRoute() {
  return <LegalDocumentPage document="terms" />;
}
