BEGIN;

-- Day-before election reminder dedupe log: one row per (user, election date)
-- meaning "this user was sent the single reminder email covering every
-- election on that date in their districts". The reminder sender computes
-- recipients live (prefs + districts + elections dated tomorrow) — there is
-- no seeded event backlog like new-election alerts, because reminders are
-- time-driven, not insert-driven. This table exists only so a retried or
-- re-run send day cannot email the same user twice for the same election
-- date. Rows are inserted after a successful send (at-least-once: a crash
-- between send and insert may duplicate an email, never lose one).
CREATE TABLE public.user_election_reminder_sends (
    user_id uuid NOT NULL,
    election_date date NOT NULL,
    sent_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_user_election_reminder_sends
        PRIMARY KEY (user_id, election_date),
    CONSTRAINT fk_user_election_reminder_sends_user
        FOREIGN KEY (user_id)
        REFERENCES public.users (id)
        ON DELETE CASCADE
);

COMMIT;
