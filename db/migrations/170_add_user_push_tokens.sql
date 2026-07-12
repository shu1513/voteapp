BEGIN;

-- Device push tokens for the mobile app (Expo Push Service). One row per
-- device token; a token moving to another account (logout, login as someone
-- else on the same device) reassigns the existing row via upsert on
-- expo_push_token. revoked_at soft-revokes (explicit unregister, logout, or
-- Expo reporting DeviceNotRegistered); senders only read revoked_at IS NULL
-- rows, and notifications:prune hard-deletes long-revoked rows.
CREATE TABLE IF NOT EXISTS public.user_push_tokens (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL,
  expo_push_token text NOT NULL,
  native_token text,
  platform text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  revoked_at timestamptz,
  CONSTRAINT fk_user_push_tokens_user
    FOREIGN KEY (user_id)
    REFERENCES public.users (id)
    ON DELETE CASCADE,
  CONSTRAINT chk_user_push_tokens_platform
    CHECK (platform IN ('ios', 'android'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_user_push_tokens_expo_push_token
  ON public.user_push_tokens (expo_push_token);

CREATE INDEX IF NOT EXISTS idx_user_push_tokens_user_active
  ON public.user_push_tokens (user_id)
  WHERE revoked_at IS NULL;

-- Expo push receipt ids awaiting a delivery check. Receipts only become
-- available ~15 minutes after send, so each sender run stores the ids it
-- produced and processes the previous runs' mature rows; a receipt whose
-- status reports DeviceNotRegistered revokes its token. Rows are deleted
-- once checked, and notifications:prune removes any stragglers.
CREATE TABLE IF NOT EXISTS public.user_push_notification_receipts (
  receipt_id text PRIMARY KEY,
  expo_push_token text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_push_notification_receipts_created
  ON public.user_push_notification_receipts (created_at);

COMMIT;
