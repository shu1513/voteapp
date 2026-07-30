import { useEffect, useRef, useState } from "react";
import { useNavigate, useSearchParams } from "react-router";
import { useMutation } from "@tanstack/react-query";
import { apiRequest, LEGAL_PRESENTATION_VERSION, TERMS_VERSION } from "@voteapp/api-client";
import type { AddressResolution } from "@voteapp/api-client";
import { AddressAutocomplete } from "../components/AddressAutocomplete";
import { PreSearchLegalGate } from "../components/PreSearchLegalGate";
import { ErrorNotice } from "../components/Status";
import { savePendingDistrictIds } from "../lib/pendingDistricts";
import { useMe } from "@voteapp/api-client";
import { useDocumentTitle } from "../lib/useDocumentTitle";
import { createLegalAcceptanceId, getOrCreateLegalSubjectId } from "../lib/legalAcceptance";

export function HomePage() {
  useDocumentTitle("Find what's on your ballot");
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { me } = useMe();
  const [address, setAddress] = useState("");
  // Always starts false, never restored from storage: agreeing to the terms
  // has to be an affirmative act on this visit. Carrying a previous visit's
  // acceptance forward would hand the visitor a pre-ticked box, which is the
  // one thing a clickwrap gate must never do.
  const [legalOpen, setLegalOpen] = useState(false);
  const [accepted, setAccepted] = useState(false);
  const acceptanceId = useRef<string | null>(null);

  // Returning verified users land on their saved ballot; ?new=1 is the
  // escape hatch for a one-off anonymous search.
  const oneOffSearch = searchParams.get("new") !== null;
  useEffect(() => {
    if (me?.email_verified && !oneOffSearch) {
      navigate("/me/ballot", { replace: true });
    }
  }, [me, oneOffSearch, navigate]);

  const resolve = useMutation({
    mutationFn: (input: { address: string; acceptanceId: string }) =>
      apiRequest<AddressResolution>("/api/address/resolve", {
        method: "POST",
        body: {
          address: input.address,
          accepted_terms_version: TERMS_VERSION,
          legal_presentation_version: LEGAL_PRESENTATION_VERSION,
          legal_acceptance_id: input.acceptanceId,
          legal_subject_id: getOrCreateLegalSubjectId(),
        },
      }),
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

  const canSearch = address.trim().length > 0 && !resolve.isPending;

  function onSubmit(event: React.FormEvent) {
    event.preventDefault();
    if (!canSearch) {
      return;
    }
    setAccepted(false);
    acceptanceId.current = null;
    setLegalOpen(true);
  }

  function agreeAndSearch() {
    if (!accepted || resolve.isPending) return;
    acceptanceId.current ??= createLegalAcceptanceId();
    resolve.mutate({ address: address.trim(), acceptanceId: acceptanceId.current });
  }

  function cancelLegalGate() {
    if (resolve.isPending) return;
    setLegalOpen(false);
    setAccepted(false);
    acceptanceId.current = null;
    resolve.reset();
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
            Find out exactly what elections you can vote on, and who these candidates really are
            by their records instead of their slogans.
          </h1>
          <p className="mt-4 text-base font-medium text-ink-soft">
            Independent, nonpartisan, AI-assisted election research with sources linked.
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
            <label htmlFor="address" className="block text-sm font-medium text-ink">
              Enter your address to see the elections you can vote in:
            </label>
            <AddressAutocomplete
              inputId="address"
              value={address}
              onChange={setAddress}
              placeholder="1600 Pennsylvania Avenue NW, Washington, DC 20500"
            />
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
      <PreSearchLegalGate
        open={legalOpen}
        checked={accepted}
        onChange={setAccepted}
        onCancel={cancelLegalGate}
        onAgree={agreeAndSearch}
        pending={resolve.isPending}
        error={resolve.isError}
      />
    </>
  );
}

export default HomePage;
