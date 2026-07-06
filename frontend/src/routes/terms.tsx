import type { MetaFunction } from "react-router";
import { LegalDocumentPage } from "../pages/LegalDocumentPage";

export const meta: MetaFunction = () => [{ title: "Terms of Use | VoteApp" }];

export default function TermsRoute() {
  return <LegalDocumentPage document="terms" />;
}
