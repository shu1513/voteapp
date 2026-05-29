export type ElectionItemType = "election";

export type ElectionRaceType = "office" | "ballot_measure";
export type ElectionStage = "primary" | "general" | "runoff" | "special";
export type ElectionSenateClass = "class_i" | "class_ii" | "class_iii";

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
  race_type: ElectionRaceType;
  is_partisan?: boolean;
  election_stage?: ElectionStage;
  senate_class?: ElectionSenateClass;
  term_end_year?: string;
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
