BEGIN;

-- Whether the public /picks/<token> page may show the owner's first name.
--
-- DEFAULT false is the point of the column: every share row that exists
-- before this feature was minted under a page that showed NO identity, and
-- those tokens are stable, non-expiring capability URLs with no user-facing
-- revoke — flipping them to named retroactively would rename links (and
-- their og previews) the owner posted precisely because they were
-- anonymous. Legacy links therefore stay anonymous forever.
--
-- New mints set it true, and re-clicking Share on an old card upgrades its
-- existing row (same token): clicking Share under the UI that shows the
-- named page and says the name is visible IS the consent event.
ALTER TABLE public.user_pick_card_shares
    ADD COLUMN show_owner_name boolean NOT NULL DEFAULT false;

COMMIT;
