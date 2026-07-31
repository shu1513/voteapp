import { useEffect, useState } from "react";
import { useNavigate, useSearchParams } from "react-router";
import { useMutation } from "@tanstack/react-query";
import { apiRequest } from "@voteapp/api-client";
import type { AddressResolution } from "@voteapp/api-client";
import { AddressAutocomplete } from "../components/AddressAutocomplete";
import { FullAddressExplanation } from "../components/FullAddressExplanation";
import { PreSearchTermsDialog } from "../components/PreSearchTermsDialog";
import { ErrorNotice } from "../components/Status";
import { savePendingDistrictIds } from "../lib/pendingDistricts";
import { useMe } from "@voteapp/api-client";
import { ADDRESS_FIELD_PRIVACY_NOTE, TERMS_VERSION } from "@voteapp/api-client";
import { hasCurrentTermsAcceptance, rememberTermsAcceptance } from "../lib/termsAcceptance";
import { useDocumentTitle } from "../lib/useDocumentTitle";

export function HomePage() {
  useDocumentTitle("Find what's on your ballot");
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { me } = useMe();
  const [address, setAddress] = useState("");
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
    mutationFn: (input: string) =>
      // The accepted version rides along because the endpoint enforces the
      // clickwrap too, refusing a search that carries no current acceptance.
      // Nothing is stored server-side; the disabled button is a courtesy, and
      // the endpoint is the actual gate.
      apiRequest<AddressResolution>("/api/address/resolve", {
        method: "POST",
        body: { address: input, accepted_terms_version: TERMS_VERSION },
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
    onError: () => {
      // Surface the failure on the page rather than inside the dialog. The
      // acceptance is already recorded, so the retry goes straight through
      // instead of asking the visitor to agree a second time.
      setTermsOpen(false);
    },
  });

  const canSearch = address.trim().length > 0 && !resolve.isPending;

  function onSubmit(event: React.FormEvent) {
    event.preventDefault();
    if (!canSearch) {
      return;
    }
    // Storage is read here, in the handler, and never during render: reading
    // it while rendering would diverge from the server-rendered HTML.
    if (hasCurrentTermsAcceptance()) {
      resolve.mutate(address.trim());
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
    resolve.mutate(address.trim());
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
            Find out exactly which elections you can vote in and who the candidates really are by
            their track records instead of their slogans.
          </h1>
          {/* What the service is, where a first-time visitor actually looks.
              It used to sit in the footer, under the fold, where it repeated
              the disclaimer link beside it and told nobody anything. */}
          <p className="mt-3 text-sm font-medium text-ink-soft sm:text-base">
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
            <label htmlFor="address" className="block text-sm font-medium text-ink">
              Enter your address to see which elections you can vote in:
            </label>
            <AddressAutocomplete
              inputId="address"
              value={address}
              onChange={setAddress}
              placeholder="1600 Pennsylvania Avenue NW, Washington, DC 20500"
            />
            {/* Notice belongs here, not only in the dialog: the autocomplete
                forwards what is typed after three characters, so collection
                starts while the visitor types and long before Search. */}
            <p className="mt-1 text-xs text-ink-soft">
              {ADDRESS_FIELD_PRIVACY_NOTE}{" "}
              {/* New tab, like the dialog's links: the address lives in this
                  page's state, so navigating away to read the policy and
                  coming back would hand the visitor an empty field. Reading
                  what you are told to read must not cost you your work. */}
              <a
                href="/privacy"
                target="_blank"
                rel="noreferrer"
                className="underline hover:text-rausch"
              >
                Privacy notice
              </a>
              {" · "}
              <FullAddressExplanation />
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
