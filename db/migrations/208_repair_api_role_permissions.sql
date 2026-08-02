BEGIN;

-- Migrations 201, 203, and 204 added API-written tables after the optional
-- least-privilege role was introduced. Repair existing installations without
-- making the role mandatory in local/CI databases.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'voteapp_api') THEN
        -- Re-assert the documented read policy. This also repairs installations
        -- where SELECT ON ALL TABLES was missed during the one-time role setup.
        EXECUTE 'GRANT USAGE ON SCHEMA public TO voteapp_api';
        EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA public TO voteapp_api';
        EXECUTE 'GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO voteapp_api';

        EXECUTE 'GRANT INSERT ON public.user_terms_acceptances TO voteapp_api';
        EXECUTE 'GRANT INSERT, UPDATE, DELETE ON public.user_election_choices TO voteapp_api';
        EXECUTE 'GRANT INSERT, UPDATE ON public.user_pick_card_shares TO voteapp_api';

        -- These are read-only catalog tables for the API. Earlier emergency
        -- workarounds may have granted UPDATE solely because SELECT ... FOR
        -- SHARE requires it; the API no longer issues those locking reads.
        EXECUTE 'REVOKE UPDATE ON public.elections, public.candidate_elections, public.candidates FROM voteapp_api';
    END IF;
END;
$$;

COMMIT;
