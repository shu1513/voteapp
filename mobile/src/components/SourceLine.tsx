import { formatElectionDate, formatSourceHost } from "@voteapp/api-client";
import { Text } from "react-native";
import { openExternalUrl } from "../lib/openExternalUrl";

// Per-record provenance line required by the legal copy:
// "Source: [link] · researched [date]".

type SourceLineProps = {
  url: string;
  researchedDate?: string | null;
};

export function SourceLine({ url, researchedDate }: SourceLineProps) {
  return (
    <Text className="mt-1 text-xs text-ink-soft">
      Source:{" "}
      <Text className="underline" accessibilityRole="link" onPress={() => openExternalUrl(url)}>
        {formatSourceHost(url)}
      </Text>
      {researchedDate ? <> · researched {formatElectionDate(researchedDate)}</> : null}
    </Text>
  );
}
