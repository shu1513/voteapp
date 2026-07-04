import { useEffect, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useMutation } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import type { AddressResolution } from "../api/types";
import { AddressAutocomplete } from "../components/AddressAutocomplete";
import { LegalGate } from "../components/LegalGate";
import { ErrorNotice } from "../components/Status";
import { formatDistrictType } from "../lib/format";
import { savePendingDistrictIds } from "../lib/pendingDistricts";
import { useMe } from "../lib/useMe";
import {
  PRE_SEARCH_ACCEPTANCE_STORAGE_KEY,
  PRE_SEARCH_CHECKBOX_LABEL,
  PRIVACY_NOTICE,
} from "../legal/copy";

function readStoredAcceptance(): boolean {
  try {
    return localStorage.getItem(PRE_SEARCH_ACCEPTANCE_STORAGE_KEY) === "true";
  } catch {
    return false;
  }
}

export function HomePage() {
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
      // these districts become their saved ballot once they verify. Verified
      // users running a one-off search must not re-arm the handoff.
      if (!me?.email_verified) {
        savePendingDistrictIds(resolution.districts.map((district) => district.id));
      }
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

  const districts = resolve.data?.districts ?? [];

  return (
    <div className="mx-auto max-w-2xl px-4 py-10">
      <h1 className="text-3xl font-bold">Find what's on your ballot</h1>
      <p className="mt-2 text-ink-soft">
        Enter your home address to see your districts and the elections coming up in them.
      </p>

      <form onSubmit={onSubmit} className="mt-6 space-y-4">
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
          className="w-full rounded-md bg-rausch px-4 py-3 font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:bg-line"
        >
          {resolve.isPending ? "Searching…" : "Search"}
        </button>
      </form>

      {resolve.isError ? (
        <div className="mt-4">
          <ErrorNotice error={resolve.error} />
        </div>
      ) : null}

      {resolve.isSuccess ? (
        <section className="mt-8">
          <h2 className="text-lg font-semibold">Matched: {resolve.data.matched_address}</h2>
          <p className="mt-1 text-sm text-ink-soft">Your districts:</p>
          <ul className="mt-2 divide-y divide-line rounded-xl border border-line">
            {districts.map((district) => (
              <li key={district.id} className="flex items-center justify-between px-3 py-2 text-sm">
                <span>{district.name}</span>
                <span className="text-ink-soft">{formatDistrictType(district.district_type)}</span>
              </li>
            ))}
          </ul>
          <button
            type="button"
            onClick={() => navigate(`/ballot?d=${districts.map((district) => district.id).join(",")}`)}
            className="mt-4 w-full rounded-md bg-rausch px-4 py-3 font-semibold text-white transition hover:bg-rausch-dark"
          >
            See your ballot
          </button>
        </section>
      ) : null}
    </div>
  );
}
