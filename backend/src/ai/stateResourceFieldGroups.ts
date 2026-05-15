import type { StateResourcePayload, StateResourceSources } from "../types/stateResource.js";

export const STATE_RESOURCE_FIELD_GROUP_CONFIG = {
  mail: {
    fieldKeys: [
      "mail_voting_available",
      "mail_ballot_request_deadline_rule",
      "mail_ballot_return_deadline_rule",
      "mail_ballot_return_deadline_type",
    ],
    sourceKeys: [
      "mail_voting_available",
      "mail_ballot_request_deadline_rule",
      "mail_ballot_return_deadline_rule",
      "mail_ballot_return_deadline_type",
    ],
  },
  online_registration: {
    fieldKeys: ["online_registration_available", "online_registration_deadline_rule"],
    sourceKeys: ["online_registration_available", "online_registration_deadline_rule"],
  },
  early_voting: {
    fieldKeys: ["early_voting_available", "early_voting_start_date_rule", "early_voting_end_date_rule"],
    sourceKeys: ["early_voting_available", "early_voting_start_date_rule", "early_voting_end_date_rule"],
  },
  polling_hours: {
    fieldKeys: ["polling_hours"],
    sourceKeys: ["polling_hours"],
  },
  polling_place: {
    fieldKeys: ["polling_place_url"],
    sourceKeys: ["polling_place_url"],
  },
  same_day_registration: {
    fieldKeys: ["same_day_registration_available"],
    sourceKeys: ["same_day_registration_available"],
  },
  id_requirements: {
    fieldKeys: ["id_requirements"],
    sourceKeys: ["id_requirements"],
  },
  in_person_registration: {
    fieldKeys: ["in_person_registration_deadline_rule"],
    sourceKeys: ["in_person_registration_deadline_rule"],
  },
} as const satisfies Record<
  string,
  {
    fieldKeys: readonly (keyof StateResourcePayload)[];
    sourceKeys: readonly (keyof StateResourceSources)[];
  }
>;

export type StateResourceFieldGroup = keyof typeof STATE_RESOURCE_FIELD_GROUP_CONFIG;

export const STATE_RESOURCE_FIELD_GROUP_ORDER: readonly StateResourceFieldGroup[] = [
  "mail",
  "online_registration",
  "early_voting",
  "polling_hours",
  "polling_place",
  "same_day_registration",
  "id_requirements",
  "in_person_registration",
] as const;

export function getStateResourceFieldGroupConfig(group: StateResourceFieldGroup) {
  return STATE_RESOURCE_FIELD_GROUP_CONFIG[group];
}

