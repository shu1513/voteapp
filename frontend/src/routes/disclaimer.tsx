import type { MetaFunction } from "react-router";
import { LegalDocumentPage } from "../pages/LegalDocumentPage";

export const meta: MetaFunction = () => [{ title: "Disclaimer | VoteApp" }];

export default function DisclaimerRoute() {
  return <LegalDocumentPage document="disclaimer" />;
}
