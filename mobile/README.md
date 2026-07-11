# VoteApp mobile

Expo (SDK 57) app for iOS and Android. Reuses the shared
[`@voteapp/api-client`](../packages/api-client) package (typed contracts,
fetch wrapper, TanStack Query hooks) and talks to the same backend as the
web frontend over the Bearer session transport.

## Get started

1. Install dependencies from the **repo root** (npm workspaces):

   ```bash
   npm install
   ```

2. Configure the environment:

   ```bash
   cp .env.example .env   # points the app at http://127.0.0.1:3001
   ```

   and start the backend API on that port (`npm run address:api` in
   `../backend`).

3. Start the app (from `mobile/`):

   ```bash
   npm run ios       # or: npm run android / npm run web / npm start
   ```

The app runs in [Expo Go](https://expo.dev/go); no dev build is required
yet. Screens live in **src/app** ([file-based routing](https://docs.expo.dev/router/introduction));
styling is [NativeWind v4](https://www.nativewind.dev/) with the web theme
tokens mirrored in `tailwind.config.js`.

## Checks

```bash
npm run typecheck   # tsc --noEmit
npm run lint        # expo lint
npx expo install --check   # dependency/SDK compatibility
```

`react` / `react-dom` are excluded from `expo install --check` on purpose:
they track the workspace's single hoisted copy (kept in lockstep with the
web frontend) rather than Expo's exact pin. React Native's own peer range
accepts it, and one shared copy is what keeps the `@voteapp/api-client`
hooks safe to share.

## Project docs

The phased mobile plan lives in the repo root (`plan-mobile-expo.md`).
