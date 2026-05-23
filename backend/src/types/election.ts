export type ElectionItemType = "election";

export type ElectionRaceType = "office" | "ballot_measure";
export type ElectionStage = "primary" | "general" | "runoff" | "special";

export type ElectionDistrictType =
  | "statewide"
  | "us_house"
  | "state_upper"
  | "state_lower"
  | "county"
  | "place"
  | "school_elementary"
  | "school_secondary"
  | "school_unified";

export type ElectionEntryPayload = {
  official_ballot_title: string;
  election_date: string; // YYYY-MM-DD
  description: string;
  race_type: ElectionRaceType;
  is_partisan?: boolean;
  election_stage?: ElectionStage;
  sources: string[];
};

export type ElectionEnrichedPayload = {
  district_id: string;
  district_name: string;
  district_type: ElectionDistrictType;
  state: string;
  entries: ElectionEntryPayload[];
  review_decision?: "approve" | "reject";
  review_reason?: string;
};

export type ElectionDraftPayload = {
  district_id: string;
  district_name: string;
  district_type: ElectionDistrictType;
  state: string;
};
