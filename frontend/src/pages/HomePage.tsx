import { useEffect, useState } from "react";
import { useNavigate, useSearchParams } from "react-router";
import { useMutation } from "@tanstack/react-query";
import { apiRequest } from "@voteapp/api-client";
import type { AddressLocation, AddressResolution } from "@voteapp/api-client";
import { AddressAutocomplete } from "../components/AddressAutocomplete";
import { FullAddressExplanation } from "../components/FullAddressExplanation";
import { PreSearchTermsDialog } from "../components/PreSearchTermsDialog";
import { ErrorNotice } from "../components/Status";
import { clearPendingDistrictIds, savePendingDistrictIds } from "../lib/pendingDistricts";
import { useMe } from "@voteapp/api-client";
import { TERMS_VERSION } from "@voteapp/api-client";
import { hasCurrentTermsAcceptance, rememberTermsAcceptance } from "../lib/termsAcceptance";
import { useDocumentTitle } from "../lib/useDocumentTitle";

export function HomePage() {
  useDocumentTitle("Find what's on your ballot");
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { me } = useMe();
  const [address, setAddress] = useState("");
  // Coordinates for the CURRENT address value, present only right after a
  // completed autocomplete selection. Any manual edit passes no location and
  // clears them, so stale coordinates can never ride along with a different
  // address string.
  const [addressLocation, setAddressLocation] = useState<AddressLocation | null>(null);
  // Set right after the autocomplete selection was an area with a known
  // state (city, neighborhood, county): the search runs the region
  // partial-ballot path. Any edit clears it, like coordinates.
  const [regionSelection, setRegionSelection] = useState<{ state: string; locality: string | null } | null>(null);
  // True right after an area selection the server could not place in a state
  // (a country pick, a territory) — nothing to search, so the form shows
  // guidance instead of letting the submit die in the geocoder. Any edit
  // clears it.
  const [regionUnsupported, setRegionUnsupported] = useState(false);
  // True while a picked suggestion's retrieve is in flight: the input
  // already shows the description, but its classification (coordinates /
  // ZIP / region) has not landed, so a quick Enter would send a bare area
  // string to the geocoder and 422.
  const [retrievePending, setRetrievePending] = useState(false);
  const [termsOpen, setTermsOpen] = useState(false);
  // The dialog's checkbox. Reset to false every time the dialog opens, never
  // seeded from storage: remembering may decide whether the dialog opens, and
  // nothing more. A box that arrives pre-ticked shows assent nobody gave.
  const [accepted, setAccepted] = useState(false);

  // Returning verified users land on their saved ballot; ?new=1 is the
  // escape hatch for a one-off anonymous search.
  const oneOffSearch = searchParams.get("new") !== null;
  useEffect(() => {
    if (me?.email_verified && !oneOffSearch) {
      navigate("/me/ballot", { replace: true });
    }
  }, [me, oneOffSearch, navigate]);

  const resolve = useMutation({
    mutationFn: (input: {
      address: string;
      coordinates: AddressLocation | null;
      region: { state: string; locality: string | null } | null;
    }) =>
      // The accepted version rides along because the endpoint enforces the
      // clickwrap too, refusing a search that carries no current acceptance.
      // Nothing is stored server-side; the disabled button is a courtesy, and
      // the endpoint is the actual gate. Coordinates (from the autocomplete
      // selection, when present) let the backend resolve venue addresses the
      // Census street data lacks.
      apiRequest<AddressResolution>("/api/address/resolve", {
        method: "POST",
        body: {
          address: input.address,
          accepted_terms_version: TERMS_VERSION,
          // Opt in to the ZIP/region partial-ballot paths: this page renders
          // the partial banner and scope-aware errors, so a bare ZIP or a
          // picked city gets a partial ballot here instead of a dead-end 422.
          allow_partial: true,
          ...(input.coordinates ? { coordinates: input.coordinates } : {}),
          ...(input.region
            ? {
                region_state: input.region.state,
                ...(input.region.locality ? { region_locality: input.region.locality } : {}),
              }
            : {}),
        },
      }),
    onSuccess: (resolution) => {
      // Stash for the anonymous-to-account handoff: if this visitor signs up,
      // these districts become their saved ballot once they verify. Save only
      // when identity is KNOWN to be logged out or unverified — while /api/me
      // is still loading (me === undefined) a verified user's one-off search
      // must not re-arm the handoff.
      if (resolution.scope === "exact") {
        if (me === null || me?.email_verified === false) {
          savePendingDistrictIds(resolution.districts.map((district) => district.id));
        }
      } else {
        // A partial (ZIP) result must not become a signed-up account's
        // saved ballot — the account would be permanently incomplete with
        // nothing recording why. Clearing instead of skipping keeps "last
        // search wins": a stale exact set from an earlier search must not
        // initialize an account the visitor thinks reflects this one.
        // Unconditional, unlike the save: the identity guard exists so a
        // verified user's one-off search cannot ARM the handoff — clearing
        // is harmless in every identity state, including still-loading.
        clearPendingDistrictIds();
      }
      // Straight to the elections — the districts list is a detour nobody asked for.
      // The matched address rides along in router state (never the URL — it is
      // personal data) so the ballot page can show which address was geocoded.
      // partial=1 IS in the URL (it carries no location) so a refresh or a
      // shared link still labels the ballot as partial.
      const query = `d=${resolution.districts.map((district) => district.id).join(",")}`;
      navigate(`/ballot?${query}${resolution.scope !== "exact" ? "&partial=1" : ""}`, {
        state: {
          matchedAddress: resolution.matched_address,
          addressMatchCount: resolution.address_match_count,
          // Lets the partial banner name the search ("ZIP code 91706" vs
          // "Los Angeles, CA, USA"); a bare link renders generic wording.
          scope: resolution.scope,
        },
      });
    },
    onError: () => {
      // Surface the failure on the page rather than inside the dialog. The
      // acceptance is already recorded, so the retry goes straight through
      // instead of asking the visitor to agree a second time.
      setTermsOpen(false);
    },
  });

  // A stateless region selection can only fail (no coordinates, no state,
  // and the string is an area the geocoder can't match), so Search disables
  // while the guidance below the field explains what to do; any edit
  // re-enables.
  const canSearch = address.trim().length > 0 && !resolve.isPending && !regionUnsupported && !retrievePending;

  function onSubmit(event: React.FormEvent) {
    event.preventDefault();
    if (!canSearch) {
      return;
    }
    // Storage is read here, in the handler, and never during render: reading
    // it while rendering would diverge from the server-rendered HTML.
    if (hasCurrentTermsAcceptance()) {
      resolve.mutate({ address: address.trim(), coordinates: addressLocation, region: regionSelection });
      return;
    }
    setAccepted(false);
    setTermsOpen(true);
  }

  function agreeAndSearch() {
    if (!accepted || resolve.isPending) {
      return;
    }
    // Recorded before the request, so a failed search does not re-ask for an
    // agreement the visitor already gave.
    rememberTermsAcceptance();
    resolve.mutate({ address: address.trim(), coordinates: addressLocation, region: regionSelection });
  }

  function cancelTerms() {
    if (resolve.isPending) {
      return;
    }
    setTermsOpen(false);
    setAccepted(false);
    // The typed address is deliberately left alone — cancelling the terms is
    // not a request to retype an address.
  }

  return (
    <>
      <div className="border-b border-line bg-surface">
        <div className="mx-auto max-w-2xl px-4 py-10">
          {/* One sentence, no sub-line: the promise is the whole pitch, and a
              second paragraph under it only pushed the address field down. Set
              full 3xl only from sm up: this headline is a whole sentence, and
              at 3xl on a phone it ran six lines and pushed the address field
              off the fold. */}
          <h1 className="text-xl font-bold sm:text-3xl">
            Find out which elections you can vote in — and who the candidates really are by their
            track records instead of slogans.
          </h1>
          {/* What the service is, where a first-time visitor actually looks.
              It used to sit in the footer, under the fold, where it repeated
              the disclaimer link beside it and told nobody anything. */}
          {/* Centred while the headline stays left: the headline is a long
              sentence that would be hard to read ragged on both sides, and the
              contrast makes this read as a standalone claim about the service
              rather than a third line of the headline. */}
          <p className="mt-3 text-center text-sm font-medium text-ink-soft sm:text-base">
            Independent, nonpartisan, AI-assisted election research with linked sources.
          </p>
        </div>
      </div>
      <div className="mx-auto max-w-2xl px-4 py-8">
        <form onSubmit={onSubmit} className="space-y-4">
          <div>
            {/* The anonymous label is an instruction, not a field name: it
                tells a first-time visitor what typing here gets them. Signed
                -in surfaces (settings, saved ballot) keep the plain "Your
                address" — those users already know. "Home address" was
                rejected as a demand for where you sleep, "Voting address"
                read as the place you go to vote. */}
            {/* The label promises the outcome; the parenthetical below the
                field carries both reassurances — privacy, and that a ZIP or
                city is enough — so the visitor who won't type where they
                live learns the escape hatch BEFORE giving up. */}
            <label htmlFor="address" className="block text-sm font-medium text-ink">
              Enter address to see which elections you can vote in:
            </label>
            <AddressAutocomplete
              inputId="address"
              value={address}
              onChange={(value, location, granularity, region) => {
                setAddress(value);
                setAddressLocation(location ?? null);
                setRegionSelection(granularity === "region" && region ? region : null);
                setRegionUnsupported(granularity === "region" && !region);
              }}
              onRetrievePendingChange={setRetrievePending}
              placeholder="1600 Pennsylvania Avenue NW, Washington, DC 20500"
            />
            {regionUnsupported ? (
              <p role="alert" className="mt-1 text-xs text-rausch-dark">
                We can’t place that selection in a state. Pick a street address, city, or ZIP
                code from the suggestions.
              </p>
            ) : null}
            {/* Notice belongs here, not only in the dialog: the autocomplete
                forwards what is typed after three characters, so collection
                starts while the visitor types and long before Search. */}
            {/* One link, not two. The Privacy Policy is still reachable at the
                point of collection — the footer carries it on every page, and
                the explainer below links it directly — so the inline copy of
                it was noise beside the question people actually ask. */}
            {/* Compressed variant of ADDRESS_FIELD_PRIVACY_NOTE plus the
                coarse-input hint: same two promises (district lookup only,
                never saved), short enough to be read. Other surfaces keep
                the full constant. */}
            <p className="mt-1 text-xs text-ink-soft">
              The address is only used to find voting districts. You can also search by ZIP or
              city, with fewer local races. <FullAddressExplanation />
            </p>
          </div>

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

      <PreSearchTermsDialog
        open={termsOpen}
        checked={accepted}
        onCheckedChange={setAccepted}
        onAgree={agreeAndSearch}
        onCancel={cancelTerms}
        pending={resolve.isPending}
      />
    </>
  );
}

export default HomePage;
