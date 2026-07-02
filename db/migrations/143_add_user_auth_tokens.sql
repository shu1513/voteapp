BEGIN;

CREATE TABLE public.user_auth_tokens (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL,
    token_hash text NOT NULL,
    purpose text NOT NULL,
    expires_at timestamptz NOT NULL,
    consumed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_user_auth_tokens_user
        FOREIGN KEY (user_id)
        REFERENCES public.users(id)
        ON DELETE CASCADE,
    CONSTRAINT chk_user_auth_tokens_purpose
        CHECK (purpose IN ('email_verify', 'password_reset')),
    CONSTRAINT chk_user_auth_tokens_consumed_at
        CHECK (consumed_at IS NULL OR consumed_at >= created_at)
);

CREATE UNIQUE INDEX uq_user_auth_tokens_token_hash ON public.user_auth_tokens (token_hash);
CREATE INDEX idx_user_auth_tokens_user_id_purpose_expires_at ON public.user_auth_tokens (user_id, purpose, expires_at);
CREATE INDEX idx_user_auth_tokens_expires_at ON public.user_auth_tokens (expires_at);

COMMIT;
