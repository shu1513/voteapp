import { describe, expect, it, vi } from "vitest";
import type { ExpoPushMessage, ExpoPushTicket } from "expo-server-sdk";

import {
  processMaturePushReceipts,
  sendUserPushNotification,
  type PushNotificationClient,
} from "../../../src/pipeline/users/pushNotificationSender.js";

const USER_ID = "11111111-1111-4111-8111-111111111111";
const TOKEN_A = "ExponentPushToken[aaa]";
const TOKEN_B = "ExponentPushToken[bbb]";
const MESSAGE = { title: "Elections Simplified", body: "2 updates", url: "/follows" };

// Routes the module's statements by their distinguishing SQL fragments.
function createDbMock(fixtures: {
  activeTokens?: string[];
  pendingReceipts?: Array<{ receipt_id: string; expo_push_token: string }>;
}) {
  const revokedTokens: string[] = [];
  const insertedReceipts: Array<{ ids: string[]; tokens: string[] }> = [];
  const deletedReceiptIds: string[][] = [];
  const query = vi.fn(async (sql: string, params?: unknown[]) => {
    if (sql.includes("SELECT expo_push_token")) {
      const rows = (fixtures.activeTokens ?? []).map((token) => ({ expo_push_token: token }));
      return { rows, rowCount: rows.length };
    }
    if (sql.includes("SET revoked_at = now()")) {
      revokedTokens.push(params?.[0] as string);
      return { rows: [], rowCount: 1 };
    }
    if (sql.includes("INSERT INTO public.user_push_notification_receipts")) {
      insertedReceipts.push({ ids: params?.[0] as string[], tokens: params?.[1] as string[] });
      return { rows: [], rowCount: (params?.[0] as string[]).length };
    }
    if (sql.includes("DELETE FROM public.user_push_notification_receipts")) {
      deletedReceiptIds.push([...(params?.[0] as string[])]);
      return { rows: [], rowCount: (params?.[0] as string[]).length };
    }
    if (sql.includes("FROM public.user_push_notification_receipts")) {
      const rows = fixtures.pendingReceipts ?? [];
      return { rows, rowCount: rows.length };
    }
    throw new Error(`Unexpected SQL in test: ${sql.slice(0, 80)}`);
  });
  return { query, revokedTokens, insertedReceipts, deletedReceiptIds };
}

function createClientMock(overrides: Partial<PushNotificationClient> = {}): PushNotificationClient {
  return {
    chunkPushNotifications: vi.fn((messages: ExpoPushMessage[]) => [messages]),
    sendPushNotificationsAsync: vi.fn(async (chunk: ExpoPushMessage[]) =>
      chunk.map((_, index): ExpoPushTicket => ({ status: "ok", id: `receipt-${index}` }))
    ),
    chunkPushNotificationReceiptIds: vi.fn((ids: string[]) => [ids]),
    getPushNotificationReceiptsAsync: vi.fn(async () => ({})),
    ...overrides,
  } as PushNotificationClient;
}

describe("sendUserPushNotification", () => {
  it("no-ops for a user without active tokens", async () => {
    const db = createDbMock({ activeTokens: [] });
    const client = createClientMock();

    const result = await sendUserPushNotification(db as never, client, USER_ID, MESSAGE);

    expect(result).toEqual({ sentCount: 0, revokedTokenCount: 0 });
    expect(client.sendPushNotificationsAsync).not.toHaveBeenCalled();
  });

  it("sends to every active token and stores receipt ids for ok tickets", async () => {
    const db = createDbMock({ activeTokens: [TOKEN_A, TOKEN_B] });
    const client = createClientMock();

    const result = await sendUserPushNotification(db as never, client, USER_ID, MESSAGE);

    expect(result).toEqual({ sentCount: 2, revokedTokenCount: 0 });
    const sentChunk = (client.sendPushNotificationsAsync as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(sentChunk).toEqual([
      { to: TOKEN_A, title: "Elections Simplified", body: "2 updates", data: { url: "/follows" }, sound: "default" },
      { to: TOKEN_B, title: "Elections Simplified", body: "2 updates", data: { url: "/follows" }, sound: "default" },
    ]);
    expect(db.insertedReceipts).toEqual([{ ids: ["receipt-0", "receipt-1"], tokens: [TOKEN_A, TOKEN_B] }]);
  });

  it("revokes a stored token Expo would reject without sending to it", async () => {
    const db = createDbMock({ activeTokens: ["not-a-push-token", TOKEN_A] });
    const client = createClientMock();

    const result = await sendUserPushNotification(db as never, client, USER_ID, MESSAGE);

    expect(result).toEqual({ sentCount: 1, revokedTokenCount: 1 });
    expect(db.revokedTokens).toEqual(["not-a-push-token"]);
    const sentChunk = (client.sendPushNotificationsAsync as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(sentChunk).toHaveLength(1);
    expect(sentChunk[0].to).toBe(TOKEN_A);
  });

  it("revokes tokens whose ticket reports DeviceNotRegistered", async () => {
    const db = createDbMock({ activeTokens: [TOKEN_A, TOKEN_B] });
    const client = createClientMock({
      sendPushNotificationsAsync: vi.fn(async (): Promise<ExpoPushTicket[]> => [
        { status: "error", message: "device gone", details: { error: "DeviceNotRegistered" } },
        { status: "ok", id: "receipt-b" },
      ]),
    });

    const result = await sendUserPushNotification(db as never, client, USER_ID, MESSAGE);

    expect(result).toEqual({ sentCount: 1, revokedTokenCount: 1 });
    expect(db.revokedTokens).toEqual([TOKEN_A]);
    expect(db.insertedReceipts).toEqual([{ ids: ["receipt-b"], tokens: [TOKEN_B] }]);
  });

  it("ignores non-DeviceNotRegistered ticket errors (email already carries the content)", async () => {
    const db = createDbMock({ activeTokens: [TOKEN_A] });
    const client = createClientMock({
      sendPushNotificationsAsync: vi.fn(async (): Promise<ExpoPushTicket[]> => [
        { status: "error", message: "rate limited", details: { error: "MessageRateExceeded" } },
      ]),
    });

    const result = await sendUserPushNotification(db as never, client, USER_ID, MESSAGE);

    expect(result).toEqual({ sentCount: 0, revokedTokenCount: 0 });
    expect(db.revokedTokens).toEqual([]);
    expect(db.insertedReceipts).toEqual([]);
  });

  it("propagates a whole-chunk send failure to the caller", async () => {
    const db = createDbMock({ activeTokens: [TOKEN_A] });
    const client = createClientMock({
      sendPushNotificationsAsync: vi.fn(async () => {
        throw new Error("expo api unreachable");
      }),
    });

    await expect(sendUserPushNotification(db as never, client, USER_ID, MESSAGE)).rejects.toThrow(
      "expo api unreachable"
    );
  });
});

describe("processMaturePushReceipts", () => {
  it("no-ops when no receipts are mature", async () => {
    const db = createDbMock({ pendingReceipts: [] });
    const client = createClientMock();

    const result = await processMaturePushReceipts(db as never, client);

    expect(result).toEqual({ checkedCount: 0, revokedTokenCount: 0 });
    expect(client.getPushNotificationReceiptsAsync).not.toHaveBeenCalled();
    expect(db.deletedReceiptIds).toEqual([]);
  });

  it("revokes tokens flagged DeviceNotRegistered and deletes every checked row", async () => {
    const db = createDbMock({
      pendingReceipts: [
        { receipt_id: "r1", expo_push_token: TOKEN_A },
        { receipt_id: "r2", expo_push_token: TOKEN_B },
        { receipt_id: "r3", expo_push_token: TOKEN_B },
      ],
    });
    const client = createClientMock({
      getPushNotificationReceiptsAsync: vi.fn(async () => ({
        r1: { status: "ok" as const },
        r2: {
          status: "error" as const,
          message: "device gone",
          details: { error: "DeviceNotRegistered" as const },
        },
        // r3 absent: Expo expired it; the row must still be deleted.
      })),
    });

    const result = await processMaturePushReceipts(db as never, client);

    expect(result).toEqual({ checkedCount: 3, revokedTokenCount: 1 });
    expect(db.revokedTokens).toEqual([TOKEN_B]);
    expect(db.deletedReceiptIds).toEqual([["r1", "r2", "r3"]]);
  });

  it("passes maturity and batch options into the select", async () => {
    const db = createDbMock({ pendingReceipts: [] });
    const client = createClientMock();

    await processMaturePushReceipts(db as never, client, { maturityMinutes: 30, batchSize: 50 });

    expect(db.query.mock.calls[0][1]).toEqual([30, 50]);
  });
});
