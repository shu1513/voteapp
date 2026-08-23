// Vocabulary for public.legislative_votes (migration 251), the review queue
// of the roll-call import. The migration test pins the DB CHECKs to these
// lists so the two cannot drift.

export const LEGISLATIVE_VOTE_CHAMBERS = ["house", "senate"] as const;
export type LegislativeVoteChamber = (typeof LEGISLATIVE_VOTE_CHAMBERS)[number];

export const LEGISLATIVE_VOTE_REVIEW_STATUSES = ["pending", "approved", "rejected"] as const;
export type LegislativeVoteReviewStatus = (typeof LEGISLATIVE_VOTE_REVIEW_STATUSES)[number];
