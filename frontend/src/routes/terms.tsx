import type { MetaFunction } from "react-router";
import { APP_NAME } from "@voteapp/api-client";
import { LegalDocumentPage } from "../pages/LegalDocumentPage";

export const meta: MetaFunction = () => [{ title: `Terms of Use · ${APP_NAME}` }];

export default function TermsRoute() {
  return <LegalDocumentPage document="terms" />;
}
