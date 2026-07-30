import { createHash } from "node:crypto";
import type { Pool, PoolClient } from "pg";

import {
  CURRENT_LEGAL_PRESENTATION_VERSION,
  CURRENT_TERMS_VERSION,
} from "../constants/legal.js";
import { isUuid } from "../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type LegalAcceptanceContext = "anonymous_search" | "registration" | "terms_renewal";

export const LEGAL_ACCEPTANCE_PRESENTATIONS: Record<
  LegalAcceptanceContext,
  { acceptanceText: string; actionText: string }
> = {
  anonymous_search: {
    acceptanceText:
      "I have read and agree to the Terms of Use, Privacy Policy, and AI Research and Election Information " +
      "Disclaimer. I understand that Elections Simplified provides AI-assisted informational research only; it is not an " +
      "official election source; results may be inaccurate, incomplete, outdated, or misleading; and I must " +
      "verify voting, registration, ballot, district, polling-place, deadline, and election-result information " +
      "with official election authorities before relying on it. I agree that disputes are resolved by binding " +
      "individual arbitration with a class-action waiver as described in Section 12 of the Terms of Use, " +
      "unless I opt out as described there.",
    actionText: "Agree and search",
  },
  registration: {
    acceptanceText:
      "I am at least 18 years old, and I have read and agree to the Terms of Use, Privacy Policy, and AI " +
      "Research and Election Information Disclaimer. I consent to enter this agreement electronically. I " +
      "understand that Elections Simplified is not an official election source, does not register voters or cast ballots, " +
      "and may display AI-assisted content that must be independently verified with official election " +
      "authorities. I agree that disputes are resolved by binding individual arbitration with a class-action " +
      "waiver as described in Section 12 of the Terms of Use, unless I opt out as described there.",
    actionText: "Create account",
  },
  terms_renewal: {
    acceptanceText:
      "I have read and agree to the updated Terms of Use, Privacy Policy, and AI Research and Election " +
      "Information Disclaimer, including the agreement to resolve disputes by binding individual arbitration " +
      "with a class-action waiver (Terms of Use Section 12), unless I opt out as described there.",
    actionText: "Agree and continue",
  },
};

export type LegalAcceptanceRequestEvidence = {
  eventId: string;
  anonymousSubjectId: string;
  termsVersion: string;
  presentationVersion: string;
  clientIp?: string | null;
  userAgent?: string | null;
  origin?: string | null;
};

export type RecordLegalAcceptanceInput = LegalAcceptanceRequestEvidence & {
  context: LegalAcceptanceContext;
  accountUserId?: string | null;
  accountEmail?: string | null;
};

type StoredAcceptanceRow = {
  id: string;
  account_user_id: string | null;
  account_email_sha256: string | null;
  anonymous_subject_id: string;
  context: LegalAcceptanceContext;
  document_bundle_version: string;
  presentation_version: string;
  acceptance_text: string;
  action_text: string;
  accepted_at: Date | string;
};

function requireUuid(value: string, fieldName: string): string {
  const normalized = typeof value === "string" ? value.trim() : "";
  if (!isUuid(normalized)) {
    throw new TypeError(`${fieldName} must be a UUID`);
  }
  return normalized;
}

function normalizeEvidenceText(value: string | null | undefined, maxLength: number): string | null {
  const normalized = value?.trim();
  return normalized ? normalized.slice(0, maxLength) : null;
}

export function hashLegalAcceptanceEmail(email: string): string {
  const normalized = typeof email === "string" ? email.trim().toLowerCase() : "";
  if (!normalized) {
    throw new TypeError("accountEmail must be a non-empty string");
  }
  return createHash("sha256").update(normalized).digest("hex");
}

export async function recordLegalAcceptance(
  db: Queryable,
  input: RecordLegalAcceptanceInput
): Promise<{ id: string; acceptedAt: string }> {
  const eventId = requireUuid(input.eventId, "eventId");
  const anonymousSubjectId = requireUuid(input.anonymousSubjectId, "anonymousSubjectId");
  const accountUserId = input.accountUserId ? requireUuid(input.accountUserId, "accountUserId") : null;
  const accountEmailSha256 = input.accountEmail ? hashLegalAcceptanceEmail(input.accountEmail) : null;

  if (input.termsVersion !== CURRENT_TERMS_VERSION) {
    throw new TypeError(`termsVersion must be the current terms version (${CURRENT_TERMS_VERSION})`);
  }
  if (input.presentationVersion !== CURRENT_LEGAL_PRESENTATION_VERSION) {
    throw new TypeError(
      `presentationVersion must be the current legal presentation version (${CURRENT_LEGAL_PRESENTATION_VERSION})`
    );
  }
  if (input.context !== "anonymous_search" && (!accountUserId || !accountEmailSha256)) {
    throw new TypeError("account evidence is required for registered-user acceptance");
  }

  const presentation = LEGAL_ACCEPTANCE_PRESENTATIONS[input.context];
  const params = [
    eventId,
    accountUserId,
    accountEmailSha256,
    anonymousSubjectId,
    input.context,
    CURRENT_TERMS_VERSION,
    CURRENT_LEGAL_PRESENTATION_VERSION,
    presentation.acceptanceText,
    presentation.actionText,
    normalizeEvidenceText(input.clientIp, 128),
    normalizeEvidenceText(input.userAgent, 1024),
    normalizeEvidenceText(input.origin, 512),
  ];

  const inserted = await db.query<StoredAcceptanceRow>(
    `
      INSERT INTO public.legal_acceptance_events (
        id,
        account_user_id,
        account_email_sha256,
        anonymous_subject_id,
        context,
        document_bundle_version,
        presentation_version,
        acceptance_text,
        action_text,
        client_ip,
        user_agent,
        origin
      )
      VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $5, $6, $7, $8, $9, $10, $11, $12)
      ON CONFLICT (id) DO NOTHING
      RETURNING *
    `,
    params
  );

  let row = inserted.rows[0];
  if (!row) {
    const existing = await db.query<StoredAcceptanceRow>(
      `
        SELECT
          id::text AS id,
          account_user_id::text AS account_user_id,
          account_email_sha256,
          anonymous_subject_id::text AS anonymous_subject_id,
          context,
          document_bundle_version,
          presentation_version,
          acceptance_text,
          action_text,
          accepted_at
        FROM public.legal_acceptance_events
        WHERE id = $1::uuid
      `,
      [eventId]
    );
    row = existing.rows[0];
    if (
      !row ||
      row.account_user_id !== accountUserId ||
      row.account_email_sha256 !== accountEmailSha256 ||
      row.anonymous_subject_id !== anonymousSubjectId ||
      row.context !== input.context ||
      row.document_bundle_version !== CURRENT_TERMS_VERSION ||
      row.presentation_version !== CURRENT_LEGAL_PRESENTATION_VERSION ||
      row.acceptance_text !== presentation.acceptanceText ||
      row.action_text !== presentation.actionText
    ) {
      throw new TypeError("legal acceptance event id was already used for different evidence");
    }
  }

  const acceptedAt = row.accepted_at instanceof Date ? row.accepted_at.toISOString() : String(row.accepted_at);
  return { id: row.id, acceptedAt };
}
