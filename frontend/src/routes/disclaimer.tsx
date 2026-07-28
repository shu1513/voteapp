import type { MetaFunction } from "react-router";
import { APP_NAME } from "@voteapp/api-client";
import { LegalDocumentPage } from "../pages/LegalDocumentPage";

export const meta: MetaFunction = () => [{ title: `Disclaimer · ${APP_NAME}` }];

export default function DisclaimerRoute() {
  return <LegalDocumentPage document="disclaimer" />;
}
