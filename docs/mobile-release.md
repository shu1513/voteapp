# Mobile release runbook (iOS + Android)

How to take `mobile/` (Expo SDK 57) from Expo Go to the App Store and Google
Play. Steps marked **[account]** need the owner's paid accounts or credentials
and cannot be done from the repo. Everything else is code and is already in
place unless marked **[todo]**.

## 0. One-time accounts [account]

- Apple Developer Program (USD 99/year). Note the **Team ID** from
  <https://developer.apple.com/account> → Membership.
- Google Play Console (USD 25 once).
- Expo account for EAS Build / EAS Submit (<https://expo.dev>). Free tier is
  enough to start; builds queue longer.
- Sentry project for mobile, separate from the web one (optional; error
  monitoring stays off when `EXPO_PUBLIC_SENTRY_DSN` is unset).

## 1. Link the repo to EAS [account, once]

```bash
npx eas-cli login
npx eas-cli init --id <project id>   # run inside mobile/; writes extra.eas.projectId to app.json
```

Commit the `app.json` change. Push notifications depend on that `projectId`
(`mobile/src/lib/pushNotifications.ts` bails without it), so this step is
required before push works on any real device.

`mobile/eas.json` already defines three build profiles:

| profile       | purpose                                  | API origin                        |
| ------------- | ---------------------------------------- | --------------------------------- |
| `development` | dev client on a device against local API | `http://127.0.0.1:3001`           |
| `preview`     | internal test build (TestFlight / APK)   | `https://electionssimplified.com` |
| `production`  | store build, build number auto-increment | `https://electionssimplified.com` |

Version numbers: `cli.appVersionSource` is `remote`, so EAS stores
`buildNumber` / `versionCode` and bumps them on every production build. Bump
the user-visible `expo.version` in `app.json` by hand per release.

Environment values that must not live in the repo go into EAS environment
variables (Expo dashboard → project → Environment variables), scoped to the
`preview` and `production` environments:

- `EXPO_PUBLIC_SENTRY_DSN` — mobile Sentry DSN.
- `SENTRY_AUTH_TOKEN` — lets the `@sentry/react-native` config plugin upload
  source maps during the build (secret).

## 2. Push notifications [account]

- **iOS:** `npx eas-cli credentials -p ios` → let EAS create the APNs key.
- **Android:** create a Firebase project, download `google-services.json`
  into `mobile/` (add it to `.gitignore` — it is not a secret but should not
  be committed), set `"android": { "googleServicesFile": "./google-services.json" }`
  in `app.json`, then upload the FCM V1 service-account JSON with
  `npx eas-cli credentials -p android`. Without this, Android builds get no
  push token. **[todo]**
- Backend side: `backend/src/pipeline/users/pushNotificationSender.ts` sends
  through Expo's push service; nothing to change.

## 3. Deep links (universal links / app links)

Both files are served by the web frontend and need real values before launch:

- `frontend/src/routes/apple-app-site-association.ts` — replace `TEAMID` with
  the Apple Team ID. Served as `application/json` at
  `https://electionssimplified.com/.well-known/apple-app-site-association`.
  **[todo: Team ID]**
- `frontend/public/.well-known/assetlinks.json` — replace the placeholder
  fingerprint with the SHA-256 of the Play signing certificate. After the
  first production build, `npx eas-cli credentials -p android` prints it (or
  Play Console → App integrity → App signing). **[todo: fingerprint]**

Deploy the web frontend after editing either file. Verify with:

```bash
curl -sI https://electionssimplified.com/.well-known/apple-app-site-association | grep -i content-type
```

Apple caches the file through its CDN, so allow up to a day after deploy
before universal links start opening the app on a fresh install.

## 4. Store listing prerequisites [account]

Both stores:

- Privacy policy URL: `https://electionssimplified.com/privacy` (exists).
- Account deletion in-app (exists: Settings → Security → Delete account).
- Screenshots. iPhone 6.7" is mandatory; the app is phone-only
  (`ios.supportsTablet` is unset, so no iPad screenshots needed).
- Description, short description, category (News / Reference), keywords.

Apple only:

- App Privacy questionnaire. Data collected: email (account), user content
  (ballot picks, follows), device token (push), crash data (Sentry, no PII —
  `errorMonitoring.ts` scrubs it).
- Review notes with a demo login. Create a dedicated test account in prod.
- Sign in with Apple is **not** required today because the app offers only
  email/password. It becomes mandatory the day Google Sign-In is added to
  mobile.

Google only:

- Data safety form (same data as the Apple questionnaire).
- Content rating questionnaire.
- New personal developer accounts must run a closed test with at least 12
  testers for 14 days before production access is granted. Start that early.

## 5. Build and test

```bash
cd mobile
npx eas-cli build -p all --profile preview
```

Install the iOS build via TestFlight (internal testers, no review) and the
Android APK directly. Test on real devices, not Expo Go: native modules
(gesture handler, reanimated, secure store, notifications) behave differently
in a store build. Check:

- Login, register, forgot password, email verification link opens the app.
- Push token registers (Settings → notifications) and a test push arrives.
- Share links, deep links from Safari/Chrome, cold start with no network.
- Sentry receives a test error.

## 6. Submit

```bash
npx eas-cli build -p all --profile production
npx eas-cli submit -p ios       # prompts for Apple ID / App Store Connect app
npx eas-cli submit -p android   # prompts for the Play service-account JSON; first upload must be manual in Play Console
```

The first Android upload has to be done by hand in Play Console (create the
app, upload the AAB to a testing track). `eas submit` works from the second
build on.

## 7. After launch

- Over-the-air JS updates (`expo-updates` + `eas update`) are not installed.
  Add them only if store-review turnaround becomes a problem; they need a
  `runtimeVersion` policy in `app.json` and a `channel` per build profile.
- CI (`.github/workflows/mobile.yml`) runs typecheck, lint, and a bundle
  check. Store builds stay manual until there is a reason to automate them.
- Bump `expo.version` for every user-visible release; EAS handles the build
  numbers.
