-- Joint-ticket support for district elections (e.g. Alaska Governor / Lieutenant Governor).
-- Mirrors the presidential model: the ticket lead's candidate_elections row points at the
-- running mate's canonical candidates row; the running mate does not get an own
-- candidate_elections row for the ticket election.

ALTER TABLE public.candidate_elections
  ADD COLUMN running_mate_candidate_id uuid NULL;

ALTER TABLE public.candidate_elections
  ADD CONSTRAINT fk_candidate_elections_running_mate
    FOREIGN KEY (running_mate_candidate_id)
    REFERENCES public.candidates(id)
    ON DELETE SET NULL;

ALTER TABLE public.candidate_elections
  ADD CONSTRAINT chk_candidate_elections_running_mate_not_self
    CHECK (running_mate_candidate_id IS NULL OR running_mate_candidate_id <> candidate_id);

CREATE INDEX idx_candidate_elections_running_mate_candidate_id
  ON public.candidate_elections (running_mate_candidate_id)
  WHERE running_mate_candidate_id IS NOT NULL;

CREATE UNIQUE INDEX uq_candidate_elections_election_running_mate_candidate_id
  ON public.candidate_elections (election_id, running_mate_candidate_id)
  WHERE running_mate_candidate_id IS NOT NULL;
