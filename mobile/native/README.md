# Platoon Manager, native iOS (SwiftUI)

The SwiftUI app that will replace the Capacitor shell in `../ios/`. The plan,
phases and API are in [`../NATIVE_PLAN.md`](../NATIVE_PLAN.md). Phases 0–3 are
in: Clerk sign-in (email and Google), the accountability roster (swipe or tick
to mark present, undo, the status sheet with TDY lists and repeats), the soldier
page (absences, history, profile), formation mode, reports with history, the
unit tree with today's counts, the directory (call / text), availability, the
duty roster with rotation, email alerts and quick add. The rarer settings
(TDY lists, sub-units, audit log, logo, import, accounts, time zone, backup)
open the website in an in-app Safari sheet. Billing is never shown in the app.

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
| `AppModel.swift` | Sign-in → sync → `/api/me` → units; the duty day in the organization's zone. |
| `RootView.swift` | Signed out (Clerk's `AuthView`), loading, locked, inactive, needs-unit screens. |
| `MainTabs.swift` | Today, Units, People, Plan, More; one `RosterStore` per selected unit. |
| `RosterStore.swift` | The roster and every write to it (present, absences, people, TDY lists, report history). |
| `Rules/` | The web app's pure rules ported from `index.html`: accountability, ranks, formation queue, unit tree, report text, field validation. |
| `PlatoonManagerTests/` | XCTests for `Rules/`, with expected values produced by running the JS from `index.html` under node. If the JS changes, regenerate them from it. |

Run the tests with `xcodebuild -scheme PlatoonManager -destination 'platform=iOS Simulator,name=iPhone 17 Pro' test`.

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
