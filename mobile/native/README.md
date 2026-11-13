# Platoon Manager, native iOS (SwiftUI)

The SwiftUI app that will replace the Capacitor shell in `../ios/`. The plan,
phases and API are in [`../NATIVE_PLAN.md`](../NATIVE_PLAN.md). This is Phase 0:
Clerk sign-in (email and Google), `POST /api/auth/sync`, `GET /api/me`, and the
roster of the user's unit as a plain list.

Same bundle ID (`com.platoonmanager.app`) and team (`687CJND9M7`) as the
Capacitor app, so its builds replace that one on TestFlight. Build numbers keep
counting up from the Capacitor app's build 1.

## Project

`project.yml` is the source of truth; `PlatoonManager.xcodeproj` is generated
from it with [XcodeGen](https://github.com/yonaskolb/XcodeGen) and committed, so
Xcode alone can build. After editing `project.yml` (a new build number, a new
package), run `xcodegen` here. Swift files added under `PlatoonManager/` also
need a `xcodegen` run to join the target.

Clerk's iOS SDK (`clerk-ios`, products `ClerkKit` and `ClerkKitUI`) comes in
through Swift Package Manager.

| File | What |
| --- | --- |
| `Config.swift` | The server URL and Clerk's publishable key (both public). |
| `APIClient.swift` | Calls `/api/...` with `Authorization: Bearer <Clerk session token>`; maps 401/402/403 inactive/503 to errors. On a 401 it logs the token's `azp` claim. |
| `AppModel.swift` | Sign-in → sync → `/api/me` → roster. |
| `RootView.swift` | Signed out (Clerk's `AuthView`), loading, locked, inactive, needs-unit screens. |
| `RosterView.swift` | The roster list. |

The app icon is a copy of `../ios/App/App/Assets.xcassets/AppIcon.appiconset`,
which `scripts/make-icons.py` generates.

## Clerk setup it needs

Done on 2026-11-13. In the Clerk dashboard (the **Platoon Manager** workspace,
production instance, Developers → Native applications):

- **Native applications**: enable the Native API, and add the iOS app with
  team ID `687CJND9M7` and bundle ID `com.platoonmanager.app`. Without the
  Native API, Clerk answers every request with `native_api_disabled` and the
  app never gets past loading.
- Google sign-in returns to `com.platoonmanager.app://callback` (the SDK's
  default; the scheme is registered in `Info.plist`). Clerk put it on the
  mobile SSO redirect allowlist by itself when the iOS app was added.

The server needed no change: the native session token passed
`_verify_clerk_session_token()`'s `azp` check as it is.

## Build and upload to TestFlight

Bump `CURRENT_PROJECT_VERSION` in `project.yml` and run `xcodegen`, then:

```bash
xcodebuild -project PlatoonManager.xcodeproj -scheme PlatoonManager \
  -configuration Release -destination 'generic/platform=iOS' \
  -archivePath build/PlatoonManager.xcarchive -allowProvisioningUpdates archive
xcodebuild -exportArchive -archivePath build/PlatoonManager.xcarchive \
  -exportOptionsPlist ExportOptions.plist -exportPath build/export \
  -allowProvisioningUpdates
```

`ExportOptions.plist` uploads straight to App Store Connect (`destination:
upload`). `build/` is gitignored.
