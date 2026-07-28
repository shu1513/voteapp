import type { MetaFunction } from "react-router";
import { APP_NAME } from "@voteapp/api-client";
import { LegalDocumentPage } from "../pages/LegalDocumentPage";

export const meta: MetaFunction = () => [{ title: `Privacy Policy · ${APP_NAME}` }];

export default function PrivacyRoute() {
  return <LegalDocumentPage document="privacy" />;
}
