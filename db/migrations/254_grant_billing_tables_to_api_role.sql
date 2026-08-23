BEGIN;

-- Migration 250 created the membership billing tables without the
-- least-privilege grants docs/postgres-api-role.md requires for every table
-- the API writes. In production the API runs as voteapp_api, so the first
-- checkout failed with "permission denied for table billing_customers".
--
-- SELECT already arrives through the role's default privileges. What the
-- service needs beyond that (backend/src/api/membership/membershipService.ts):
--   billing_customers     INSERT (ON CONFLICT DO NOTHING ... RETURNING)
--   billing_subscriptions INSERT + UPDATE (ON CONFLICT DO UPDATE, status updates)
--   billing_payments      INSERT + UPDATE (refund amounts)
-- No DELETE anywhere: rows are retained after account deletion and the
-- user_id null-out is the FK's ON DELETE SET NULL, which runs as the table
-- owner. Guarded because the role does not exist in local dev.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'voteapp_api') THEN
    GRANT INSERT ON public.billing_customers TO voteapp_api;
    GRANT INSERT, UPDATE ON public.billing_subscriptions TO voteapp_api;
    GRANT INSERT, UPDATE ON public.billing_payments TO voteapp_api;
  END IF;
END $$;

COMMIT;
