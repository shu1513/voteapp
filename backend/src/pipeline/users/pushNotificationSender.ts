import type { Pool, PoolClient } from "pg";
import { Expo, type ExpoPushMessage } from "expo-server-sdk";

import { listActiveUserPushTokens, revokeUserPushTokenByToken } from "./userPushTokens.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// Push delivery over the Expo Push Service, the device channel beside the
// SES mailers. Two halves, both driven by the notification send scripts:
// sendUserPushNotification fans a per-user summary out to the user's active
// device tokens, and processMaturePushReceipts follows up on earlier sends —
// Expo receipts only become reliable ~15 minutes after the ticket, so each
// run checks the receipts previous runs stored. DeviceNotRegistered at
// either stage revokes the token (Expo's documented contract for dead
// registrations).

/** The Expo SDK surface the sender uses; tests inject a mock. */
export type PushNotificationClient = Pick<
  Expo,
  | "chunkPushNotifications"
  | "sendPushNotificationsAsync"
  | "chunkPushNotificationReceiptIds"
  | "getPushNotificationReceiptsAsync"
>;

export type UserPushNotificationMessage = {
  title: string;
  body: string;
  /** In-app path the notification tap should open (mobile routes by it). */
  url: string;
};

export type SendUserPushNotificationResult = {
  /** Messages handed to Expo with an ok ticket. */
  sentCount: number;
  /** Tokens revoked because Expo rejected them (invalid or DeviceNotRegistered). */
  revokedTokenCount: number;
};

const DEVICE_NOT_REGISTERED = "DeviceNotRegistered";

export async function sendUserPushNotification(
  db: Queryable,
  client: PushNotificationClient,
  userId: string,
  message: UserPushNotificationMessage
): Promise<SendUserPushNotificationResult> {
  const tokens = await listActiveUserPushTokens(db, userId);
  const result: SendUserPushNotificationResult = { sentCount: 0, revokedTokenCount: 0 };
  if (tokens.length === 0) {
    return result;
  }

  // A stored token Expo would reject outright (corrupted registration) is as
  // dead as DeviceNotRegistered; revoke instead of burning a send on it.
  const validTokens: string[] = [];
  for (const token of tokens) {
    if (Expo.isExpoPushToken(token)) {
      validTokens.push(token);
    } else {
      await revokeUserPushTokenByToken(db, token);
      result.revokedTokenCount += 1;
    }
  }
  if (validTokens.length === 0) {
    return result;
  }

  const messages: ExpoPushMessage[] = validTokens.map((token) => ({
    to: token,
    title: message.title,
    body: message.body,
    data: { url: message.url },
    sound: "default",
  }));

  const receiptRows: Array<{ receiptId: string; expoPushToken: string }> = [];
  for (const chunk of client.chunkPushNotifications(messages)) {
    const tickets = await client.sendPushNotificationsAsync(chunk);
    for (const [index, ticket] of tickets.entries()) {
      // Tickets come back in chunk order (Expo API contract).
      const to = chunk[index]?.to;
      const token = typeof to === "string" ? to : null;
      if (ticket.status === "ok") {
        result.sentCount += 1;
        if (token) {
          receiptRows.push({ receiptId: ticket.id, expoPushToken: token });
        }
        continue;
      }
      if (ticket.details?.error === DEVICE_NOT_REGISTERED && token) {
        await revokeUserPushTokenByToken(db, token);
        result.revokedTokenCount += 1;
      }
      // Other ticket errors (rate limits, message-too-big) are transient or
      // message-shaped; the email channel already carries the content, so
      // they are dropped rather than retried.
    }
  }

  if (receiptRows.length > 0) {
    await db.query(
      `
        INSERT INTO public.user_push_notification_receipts (receipt_id, expo_push_token)
        SELECT receipt_id, expo_push_token
        FROM unnest($1::text[], $2::text[]) AS incoming(receipt_id, expo_push_token)
        ON CONFLICT (receipt_id) DO NOTHING
      `,
      [receiptRows.map((row) => row.receiptId), receiptRows.map((row) => row.expoPushToken)]
    );
  }

  return result;
}

export const DEFAULT_PUSH_RECEIPT_MATURITY_MINUTES = 15;
export const DEFAULT_PUSH_RECEIPT_BATCH_SIZE = 1000;

export type ProcessPushReceiptsResult = {
  checkedCount: number;
  revokedTokenCount: number;
};

/**
 * Checks receipts stored by earlier sends, once they are old enough for Expo
 * to have them (~15 minutes). Every fetched row is deleted afterwards:
 * receipts answered are final, and ids Expo no longer knows (expired) can
 * never be answered. DeviceNotRegistered receipts revoke their token.
 */
export async function processMaturePushReceipts(
  db: Queryable,
  client: PushNotificationClient,
  options: { maturityMinutes?: number; batchSize?: number } = {}
): Promise<ProcessPushReceiptsResult> {
  const maturityMinutes = options.maturityMinutes ?? DEFAULT_PUSH_RECEIPT_MATURITY_MINUTES;
  const batchSize = options.batchSize ?? DEFAULT_PUSH_RECEIPT_BATCH_SIZE;

  const pending = await db.query<{ receipt_id: string; expo_push_token: string }>(
    `
      SELECT receipt_id, expo_push_token
      FROM public.user_push_notification_receipts
      WHERE created_at < now() - make_interval(mins => $1::int)
      ORDER BY created_at
      LIMIT $2::int
    `,
    [maturityMinutes, batchSize]
  );

  const result: ProcessPushReceiptsResult = { checkedCount: 0, revokedTokenCount: 0 };
  if (pending.rows.length === 0) {
    return result;
  }

  const tokenByReceiptId = new Map(pending.rows.map((row) => [row.receipt_id, row.expo_push_token]));

  for (const idChunk of client.chunkPushNotificationReceiptIds([...tokenByReceiptId.keys()])) {
    const receipts = await client.getPushNotificationReceiptsAsync(idChunk);
    for (const [receiptId, receipt] of Object.entries(receipts)) {
      if (receipt.status === "error" && receipt.details?.error === DEVICE_NOT_REGISTERED) {
        const token = tokenByReceiptId.get(receiptId);
        if (token) {
          await revokeUserPushTokenByToken(db, token);
          result.revokedTokenCount += 1;
        }
      }
    }
  }
  result.checkedCount = pending.rows.length;

  await db.query(
    `
      DELETE FROM public.user_push_notification_receipts
      WHERE receipt_id = ANY($1::text[])
    `,
    [[...tokenByReceiptId.keys()]]
  );

  return result;
}

/**
 * Production client. The Expo push API needs no credentials; EXPO_ACCESS_TOKEN
 * (optional, from an Expo account with enhanced push security enabled) is
 * passed through when set.
 */
export function createExpoPushClientFromEnv(): PushNotificationClient {
  const accessToken = process.env.EXPO_ACCESS_TOKEN?.trim();
  return new Expo(accessToken ? { accessToken } : {});
}

/**
 * Operator kill switch for the push channel: NOTIFICATIONS_PUSH=off disables
 * it (email keeps flowing); unset or "expo" enables the Expo client. Null
 * means "channel off" to the send scripts.
 */
export function buildPushClientFromEnv(): PushNotificationClient | null {
  const kind = (process.env.NOTIFICATIONS_PUSH?.trim() || "expo").toLowerCase();
  if (kind === "off") {
    return null;
  }
  if (kind !== "expo") {
    throw new Error(`Unsupported notifications push channel: ${kind} (expected "expo" or "off")`);
  }
  return createExpoPushClientFromEnv();
}
