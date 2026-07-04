-- Clickwrap acceptance record: which disclaimer/terms version the user
-- accepted at registration and when. Courts require proof of who accepted
-- which version (Meyer v. Uber); the version string pairs with the exact
-- text archived in docs/legal/ under git history. Nullable because existing
-- accounts predate the requirement; the API requires it for new
-- registrations from now on.

ALTER TABLE public.users
    ADD COLUMN accepted_terms_version text,
    ADD COLUMN accepted_terms_at timestamptz;
