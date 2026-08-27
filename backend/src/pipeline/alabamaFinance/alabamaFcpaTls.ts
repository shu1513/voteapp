// TLS support for fcpa.alabamavotes.gov, which serves its leaf certificate
// without the intermediate. We complete the chain ourselves instead of
// disabling verification (plan-alabama-finance.md, gotcha 11).

import { rootCertificates } from "node:tls";
import { Agent } from "undici";

// GlobalSign Atlas R3 OV TLS CA 2025 Q3 (issuer of the fcpa.alabamavotes.gov
// leaf), fetched 2026-08-26 from the leaf's CA Issuers URL
// http://secure.globalsign.com/cacert/gsatlasr3ovtlsca2025q3.crt and verified
// against the system GlobalSign Root CA - R3. Expires 2027-04-16.
export const ALABAMA_FCPA_INTERMEDIATE_CA_PEM = `-----BEGIN CERTIFICATE-----
MIIEkDCCA3igAwIBAgIRAINDHIv6TvHHkwd6mdGHshQwDQYJKoZIhvcNAQELBQAw
TDEgMB4GA1UECxMXR2xvYmFsU2lnbiBSb290IENBIC0gUjMxEzARBgNVBAoTCkds
b2JhbFNpZ24xEzARBgNVBAMTCkdsb2JhbFNpZ24wHhcNMjUwNDE2MDMxNTAzWhcN
MjcwNDE2MDAwMDAwWjBYMQswCQYDVQQGEwJCRTEZMBcGA1UEChMQR2xvYmFsU2ln
biBudi1zYTEuMCwGA1UEAxMlR2xvYmFsU2lnbiBBdGxhcyBSMyBPViBUTFMgQ0Eg
MjAyNSBRMzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKSgnHRJRj1k
YZ6Z9+SeNA90pLnX6FZ8PNejh5wi+0FbmJu1tSos1P4WBAZkeO2m5RsowlCgDNLR
vDlHtfLJazmlzUQd3qDYHJnBGQXkTKmoc7A1B1DXQh7rrONxIVY+yosjZLB57KLr
QknDKyzN2a64KyqdVitSojrmrZUtsubeU2E2MVRTBs+5bo5NBsLKQUBUrtOI6S8V
dZAQm/kpTTkQIOtYsPC6yfTOZx7s4jJXrzJoik42+NDW66P+U8Wr4BpVG8Q5QkRV
B7uQgLP96DzHoM4yuy43IvH2TfRhkd+f/2DOpYzjmJNu6EsNaqhohtW9tqY4YbDn
VNjxa8Vi8jkCAwEAAaOCAV8wggFbMA4GA1UdDwEB/wQEAwIBhjAdBgNVHSUEFjAU
BggrBgEFBQcDAQYIKwYBBQUHAwIwEgYDVR0TAQH/BAgwBgEB/wIBADAdBgNVHQ4E
FgQUXEjtqfv6es9uoq2VL8ZqnR+zGBYwHwYDVR0jBBgwFoAUj/BLf6guRSSuTVD6
Y5qL3uLdG7wwewYIKwYBBQUHAQEEbzBtMC4GCCsGAQUFBzABhiJodHRwOi8vb2Nz
cDIuZ2xvYmFsc2lnbi5jb20vcm9vdHIzMDsGCCsGAQUFBzAChi9odHRwOi8vc2Vj
dXJlLmdsb2JhbHNpZ24uY29tL2NhY2VydC9yb290LXIzLmNydDA2BgNVHR8ELzAt
MCugKaAnhiVodHRwOi8vY3JsLmdsb2JhbHNpZ24uY29tL3Jvb3QtcjMuY3JsMCEG
A1UdIAQaMBgwCAYGZ4EMAQICMAwGCisGAQQBoDIKAQIwDQYJKoZIhvcNAQELBQAD
ggEBAFY1EA5KwC1jogCx3zN7c2AMAe6XDTYBRRsR59XHSylYuN6YLEREWo07fxQ2
zFiU/otJSQgANW0gKDZojZpOZehaCdqshHUlFmLAwV4hIVAJ/F6/YS8KIFjuJdqb
6I1w5TaZ5G2qUy09x4oNDCThm5sZPNvG/9qznPXKxxeuDnz+XMJDrpQN+12ArhKS
XBWh/J4T2CJtHaES3PNIhuhxS6hwkpmg+GfEKAEp6x6ctqZTGFL1TurSme9AqFkC
jKWKc6wvWtefDecrILveOPHbYzvix0tih8A6vYGIxU04v5Jys0mli2K5TNtXFIcn
fwHqPtKzIkdI3ZePcb7vHAz2uI4=
-----END CERTIFICATE-----
`;

/**
 * Undici dispatcher trusting the system roots plus the missing intermediate.
 * `connect.ca` replaces the default store, so the system roots must be
 * re-included explicitly.
 */
export function createAlabamaFcpaDispatcher(): Agent {
  return new Agent({
    connect: { ca: [...rootCertificates, ALABAMA_FCPA_INTERMEDIATE_CA_PEM] },
  });
}
