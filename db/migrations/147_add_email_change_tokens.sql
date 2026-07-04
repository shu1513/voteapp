-- Change-email flow: user_auth_tokens grows an 'email_change' purpose whose
-- rows carry the requested new address. The token is mailed to the NEW
-- address; consuming it proves control of that inbox, so the swap also sets
-- email_verified = true.

ALTER TABLE public.user_auth_tokens
    ADD COLUMN new_email citext;

ALTER TABLE public.user_auth_tokens
    DROP CONSTRAINT chk_user_auth_tokens_purpose;

ALTER TABLE public.user_auth_tokens
    ADD CONSTRAINT chk_user_auth_tokens_purpose
        CHECK (purpose IN ('email_verify', 'password_reset', 'email_change'));

-- new_email travels with email_change tokens and only those.
ALTER TABLE public.user_auth_tokens
    ADD CONSTRAINT chk_user_auth_tokens_new_email
        CHECK ((purpose = 'email_change') = (new_email IS NOT NULL));
