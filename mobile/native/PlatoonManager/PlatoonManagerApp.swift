import ClerkKit
import SwiftUI

@main
struct PlatoonManagerApp: App {
    @State private var model = AppModel()

    init() {
        Clerk.configure(publishableKey: Config.clerkPublishableKey)
    }

    var body: some Scene {
        WindowGroup {
            RootView()
                .environment(Clerk.shared)
                .environment(model)
        }
    }
}
