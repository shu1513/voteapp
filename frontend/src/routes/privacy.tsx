import type { MetaFunction } from "react-router";
import { LegalDocumentPage } from "../pages/LegalDocumentPage";

export const meta: MetaFunction = () => [{ title: "Privacy Policy · VoteApp" }];

export default function PrivacyRoute() {
  return <LegalDocumentPage document="privacy" />;
}
