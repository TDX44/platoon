import ClerkKit
import SwiftUI

@main
struct PlatoonManagerApp: App {
    @State private var model = AppModel()
    @State private var lock = AppLock()

    init() {
        Clerk.configure(publishableKey: Config.clerkPublishableKey)
    }

    var body: some Scene {
        WindowGroup {
            RootView()
                .environment(Clerk.shared)
                .environment(model)
                .environment(lock)
        }
    }
}
