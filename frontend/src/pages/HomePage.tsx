import { useEffect, useState } from "react";
import { useNavigate, useSearchParams } from "react-router";
import { useMutation } from "@tanstack/react-query";
import { apiRequest } from "@voteapp/api-client";
import type { AddressResolution } from "@voteapp/api-client";
import { AddressAutocomplete } from "../components/AddressAutocomplete";
import { LegalGate } from "../components/LegalGate";
import { ErrorNotice } from "../components/Status";
import { savePendingDistrictIds } from "../lib/pendingDistricts";
import { useMe } from "@voteapp/api-client";
import {
  PRE_SEARCH_ACCEPTANCE_STORAGE_KEY,
  PRE_SEARCH_CHECKBOX_LABEL,
  PRIVACY_NOTICE,
} from "@voteapp/api-client";
import { useDocumentTitle } from "../lib/useDocumentTitle";

function readStoredAcceptance(): boolean {
  try {
    return localStorage.getItem(PRE_SEARCH_ACCEPTANCE_STORAGE_KEY) === "true";
  } catch {
    return false;
  }
}

export function HomePage() {
  useDocumentTitle("Find what's on your ballot");
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { me } = useMe();
  const [address, setAddress] = useState("");
  const [accepted, setAccepted] = useState(readStoredAcceptance);

  // Returning verified users land on their saved ballot; ?new=1 is the
  // escape hatch for a one-off anonymous search.
  const oneOffSearch = searchParams.get("new") !== null;
  useEffect(() => {
    if (me?.email_verified && !oneOffSearch) {
      navigate("/me/ballot", { replace: true });
    }
  }, [me, oneOffSearch, navigate]);

  const resolve = useMutation({
    mutationFn: (input: string) =>
      apiRequest<AddressResolution>("/api/address/resolve", { method: "POST", body: { address: input } }),
    onSuccess: (resolution) => {
      // Stash for the anonymous-to-account handoff: if this visitor signs up,
      // these districts become their saved ballot once they verify. Save only
      // when identity is KNOWN to be logged out or unverified — while /api/me
      // is still loading (me === undefined) a verified user's one-off search
      // must not re-arm the handoff.
      if (me === null || me?.email_verified === false) {
        savePendingDistrictIds(resolution.districts.map((district) => district.id));
      }
      // Straight to the elections — the districts list is a detour nobody asked for.
      // The matched address rides along in router state (never the URL — it is
      // personal data) so the ballot page can show which address was geocoded.
      navigate(`/ballot?d=${resolution.districts.map((district) => district.id).join(",")}`, {
        state: {
          matchedAddress: resolution.matched_address,
          addressMatchCount: resolution.address_match_count,
        },
      });
    },
  });

  const canSearch = accepted && address.trim().length > 0 && !resolve.isPending;

  function onAcceptChange(checked: boolean) {
    setAccepted(checked);
    try {
      if (checked) {
        localStorage.setItem(PRE_SEARCH_ACCEPTANCE_STORAGE_KEY, "true");
      } else {
        localStorage.removeItem(PRE_SEARCH_ACCEPTANCE_STORAGE_KEY);
      }
    } catch {
      // Private-mode storage failures must not block searching.
    }
  }

  function onSubmit(event: React.FormEvent) {
    event.preventDefault();
    if (!canSearch) {
      return;
    }
    resolve.mutate(address.trim());
  }

  return (
    <>
      <div className="border-b border-line bg-surface">
        <div className="mx-auto max-w-2xl px-4 py-10">
          <h1 className="text-3xl font-bold">Find what's on your ballot</h1>
          <p className="mt-2 text-ink-soft">
            Enter your home address to see the elections coming up on your ballot.
          </p>
        </div>
      </div>
      <div className="mx-auto max-w-2xl px-4 py-8">
        <form onSubmit={onSubmit} className="space-y-4">
          <div>
            <label htmlFor="address" className="block text-sm font-medium text-ink">
              Home address
            </label>
            <AddressAutocomplete
              inputId="address"
              value={address}
              onChange={setAddress}
              placeholder="1600 Pennsylvania Avenue NW, Washington, DC 20500"
            />
            <p className="mt-1 text-xs text-ink-soft">{PRIVACY_NOTICE}</p>
          </div>

          <LegalGate
            inputId="pre-search-terms"
            label={PRE_SEARCH_CHECKBOX_LABEL}
            checked={accepted}
            onChange={onAcceptChange}
          />

          <button
            type="submit"
            disabled={!canSearch}
            // Disabled keeps the brand color at reduced opacity: the old
            // gray-out read as broken rather than "accept the terms first".
            className="w-full rounded-md bg-rausch px-4 py-3 font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:bg-rausch"
          >
            {resolve.isPending ? "Searching…" : "Search"}
          </button>
        </form>

        {resolve.isError ? (
          <div className="mt-4">
            <ErrorNotice error={resolve.error} />
          </div>
        ) : null}
      </div>
    </>
  );
}

export default HomePage;
