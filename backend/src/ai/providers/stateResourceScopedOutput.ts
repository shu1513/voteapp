import type { EnrichStateResourcesInput } from "../types.js";
import {
  getStateResourceFieldGroupConfig,
  type StateResourceFieldGroup,
} from "../stateResourceFieldGroups.js";

const SOURCES_BUCKET_SCHEMA = {
  type: "array",
  minItems: 1,
  items: { type: "string" },
} as const;

type JsonSchemaProperty =
  | { type: "string" }
  | { type: "boolean" }
  | { anyOf: ReadonlyArray<{ type: "string" } | { type: "null" }> }
  | { anyOf: ReadonlyArray<{ type: "string"; enum: readonly string[] } | { type: "null" }> };

const FIELD_SCHEMA: Record<string, JsonSchemaProperty> = {
  polling_place_url: { type: "string" },
  mail_voting_available: { type: "boolean" },
  mail_ballot_request_deadline_rule: { anyOf: [{ type: "string" }, { type: "null" }] },
  mail_ballot_return_deadline_rule: { anyOf: [{ type: "string" }, { type: "null" }] },
  mail_ballot_return_deadline_type: {
    anyOf: [{ type: "string", enum: ["postmarked_by", "received_by"] }, { type: "null" }],
  },
  early_voting_available: { type: "boolean" },
  early_voting_start_date_rule: { anyOf: [{ type: "string" }, { type: "null" }] },
  early_voting_end_date_rule: { anyOf: [{ type: "string" }, { type: "null" }] },
  polling_hours: { type: "string" },
  id_requirements: {
    type: "string",
  },
  same_day_registration_available: { type: "boolean" },
  online_registration_available: { type: "boolean" },
  online_registration_deadline_rule: { anyOf: [{ type: "string" }, { type: "null" }] },
  in_person_registration_deadline_rule: { type: "string" },
};

const GROUP_RULES: Record<StateResourceFieldGroup, string[]> = {
  mail: [
    "mail_voting_available must be boolean true or false.",
    "If mail_voting_available is false, set mail_ballot_request_deadline_rule, mail_ballot_return_deadline_rule, and mail_ballot_return_deadline_type to null.",
    "If mail_voting_available is true, set mail_ballot_return_deadline_rule and mail_ballot_return_deadline_type (postmarked_by or received_by).",
    "mail_ballot_request_deadline_rule and mail_ballot_return_deadline_rule must be short plain-language sentences (not URLs) when present.",
  ],
  online_registration: [
    "online_registration_available must be boolean true or false.",
    "If online_registration_available is false, set online_registration_deadline_rule to null.",
    "If online_registration_available is true, online_registration_deadline_rule must be a short plain-language sentence (not URL).",
  ],
  early_voting: [
    "early_voting_available must be boolean true or false.",
    "If early_voting_available is false, set early_voting_start_date_rule and early_voting_end_date_rule to null.",
    "If early_voting_available is true, set both early_voting_start_date_rule and early_voting_end_date_rule to short plain-language sentences (not URLs).",
  ],
  polling_hours: [
    "polling_hours must be plain-language text, not URL.",
    "Include statewide opening/closing times when available; otherwise explicitly state that hours vary by county/precinct.",
  ],
  polling_place: [
    "polling_place_url must be a valid URL for finding polling places in this state.",
  ],
  same_day_registration: [
    "same_day_registration_available must be boolean true or false.",
  ],
  id_requirements: [
    "id_requirements must be exactly one of: Strict photo ID, Strict non-photo ID, Non-strict photo ID, Non-strict, non-photo ID, No document required to vote.",
  ],
  in_person_registration: [
    "in_person_registration_deadline_rule must be a short plain-language sentence (not URL).",
  ],
};

const GROUP_REFERENCE_HINTS: Record<StateResourceFieldGroup, string> = {
  mail: "Start with the Vote.gov state registration reference URL in Evidence URLs.",
  online_registration: "Start with the Vote.gov state registration reference URL in Evidence URLs.",
  early_voting: "Start with the NCSL early in-person voting reference URL in Evidence URLs.",
  polling_hours: "Start with the NCSL polling places reference URL in Evidence URLs.",
  polling_place: "Start with polling-place reference URL(s) in Evidence URLs.",
  same_day_registration: "Start with the NCSL same-day registration reference URL in Evidence URLs.",
  id_requirements: "Start with the NCSL voter ID reference URL in Evidence URLs.",
  in_person_registration: "Start with the Vote.gov state registration reference URL in Evidence URLs.",
};

const GROUP_JSON_EXAMPLES: Record<StateResourceFieldGroup, Record<string, unknown>> = {
  polling_place: {
    polling_place_url: "https://www.sos.state.example.gov/elections/find-polling-place",
    sources: {
      polling_place_url: ["https://www.sos.state.example.gov/elections/find-polling-place"],
    },
  },
  polling_hours: {
    polling_hours: "Polls are open 7 a.m. to 8 p.m. statewide.",
    sources: {
      polling_hours: ["https://www.ncsl.org/elections-and-campaigns/polling-places"],
    },
  },
  id_requirements: {
    id_requirements: "Strict photo ID",
    sources: {
      id_requirements: ["https://www.ncsl.org/elections-and-campaigns/voter-id"],
    },
  },
  same_day_registration: {
    same_day_registration_available: true,
    sources: {
      same_day_registration_available: [
        "https://www.ncsl.org/elections-and-campaigns/same-day-voter-registration",
      ],
    },
  },
  online_registration: {
    online_registration_available: true,
    online_registration_deadline_rule: "Online registration closes 15 days before Election Day.",
    sources: {
      online_registration_available: ["https://vote.gov/register/example-state"],
      online_registration_deadline_rule: ["https://vote.gov/register/example-state"],
    },
  },
  mail: {
    mail_voting_available: true,
    mail_ballot_request_deadline_rule: "Request must be received by 5 p.m. 7 days before Election Day.",
    mail_ballot_return_deadline_rule: "Ballot must be received by 8 p.m. on Election Day.",
    mail_ballot_return_deadline_type: "received_by",
    sources: {
      mail_voting_available: ["https://vote.gov/register/example-state"],
      mail_ballot_request_deadline_rule: ["https://vote.gov/register/example-state"],
      mail_ballot_return_deadline_rule: ["https://vote.gov/register/example-state"],
      mail_ballot_return_deadline_type: ["https://vote.gov/register/example-state"],
    },
  },
  early_voting: {
    early_voting_available: true,
    early_voting_start_date_rule: "Early in-person voting starts 10 days before Election Day.",
    early_voting_end_date_rule: "Early in-person voting ends the day before Election Day.",
    sources: {
      early_voting_available: [
        "https://www.ncsl.org/elections-and-campaigns/early-in-person-voting",
      ],
      early_voting_start_date_rule: [
        "https://www.ncsl.org/elections-and-campaigns/early-in-person-voting",
      ],
      early_voting_end_date_rule: [
        "https://www.ncsl.org/elections-and-campaigns/early-in-person-voting",
      ],
    },
  },
  in_person_registration: {
    in_person_registration_deadline_rule: "In-person registration closes 15 days before Election Day.",
    sources: {
      in_person_registration_deadline_rule: ["https://vote.gov/register/example-state"],
    },
  },
};

export function buildScopedPrompt(input: EnrichStateResourcesInput): string | null {
  if (!input.fieldGroup) {
    return null;
  }

  const group = input.fieldGroup;
  const groupConfig = getStateResourceFieldGroupConfig(group);
  const fieldsList = groupConfig.fieldKeys.join(", ");
  const sourceKeysList = groupConfig.sourceKeys.join(", ");
  const rules = GROUP_RULES[group];
  const jsonExample = JSON.stringify(GROUP_JSON_EXAMPLES[group]);

  return [
    "Return only one JSON object with these keys exactly:",
    `${fieldsList}, sources.`,
    "Output must be raw JSON only (single object). No prose, no markdown, no code fences.",
    `sources must include keys: ${sourceKeysList}.`,
    "Each sources[key] must be an array of URL strings.",
    `Valid JSON example for this group: ${jsonExample}`,
    "Read the reference URL content in Evidence URLs first before broader search.",
    "Open/read the cited URL page content directly before using it to support any field.",
    "Do not cite any URL unless you actually used that URL's page content for the field.",
    "If reference URL(s) already contain enough information to answer the field accurately, stop there and do not browse additional URLs.",
    "Use additional public URLs only if reference URL(s) are missing required details or are inaccessible.",
    "Per-field citation rule:",
    "- For each field, research until you find URL(s) that directly support the final statement.",
    "- Write the field only from those supporting URL(s).",
    "- In sources[field_name], include only URL(s) that were actually used for that field.",
    "- Do not include attempted URLs that lacked the needed information.",
    GROUP_REFERENCE_HINTS[group],
    ...rules,
  ].join("\n");
}

export function buildScopedOpenAiJsonSchema(fieldGroup: StateResourceFieldGroup | undefined) {
  if (!fieldGroup) {
    return null;
  }

  const groupConfig = getStateResourceFieldGroupConfig(fieldGroup);
  const properties: Record<string, JsonSchemaProperty | { type: "object"; additionalProperties: false; required: readonly string[]; properties: Record<string, typeof SOURCES_BUCKET_SCHEMA> }> = {};

  for (const key of groupConfig.fieldKeys) {
    properties[key] = FIELD_SCHEMA[key];
  }

  const sourceProperties: Record<string, typeof SOURCES_BUCKET_SCHEMA> = {};
  for (const key of groupConfig.sourceKeys) {
    sourceProperties[key] = SOURCES_BUCKET_SCHEMA;
  }

  properties.sources = {
    type: "object",
    additionalProperties: false,
    required: groupConfig.sourceKeys,
    properties: sourceProperties,
  };

  return {
    name: `state_resource_payload_${fieldGroup}`,
    strict: true,
    schema: {
      type: "object",
      additionalProperties: false,
      required: [...groupConfig.fieldKeys, "sources"],
      properties,
    },
  } as const;
}
