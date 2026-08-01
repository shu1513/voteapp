BEGIN;

-- Shareable "pick card" links: one per (user, election date), minted when
-- the user clicks Share on that date's card in My Picks. The public page
-- /picks/<token> shows a LIVE view of that user's picks for that date —
-- election titles, picked candidates, measure positions — to anyone holding
-- the link.
--
-- The token is a stored 256-bit random capability, NOT the HMAC pattern the
-- unsubscribe link uses: an HMAC token must embed the user id in cleartext,
-- which is fine inside the user's own inbox but not in a URL built to be
-- posted publicly. Stored plaintext, not hashed like session tokens
-- (authPrimitives): the Share button is idempotent — clicking again must
-- return the SAME link — which a one-way hash cannot do, and the capability
-- it grants is read-only access to one date's picks, not authentication.
-- Deleting the row revokes the link.
CREATE TABLE public.user_pick_card_shares (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    election_date date NOT NULL,
    token text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_user_pick_card_shares_user_date UNIQUE (user_id, election_date),
    CONSTRAINT uq_user_pick_card_shares_token UNIQUE (token)
);

CREATE INDEX idx_user_pick_card_shares_user_id
    ON public.user_pick_card_shares (user_id);

COMMIT;
