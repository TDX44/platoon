import ClerkKit
import ClerkKitUI
import SwiftUI

struct RootView: View {
    @Environment(Clerk.self) private var clerk
    @Environment(AppModel.self) private var model
    @State private var showAuth = false

    var body: some View {
        Group {
            if !clerk.isLoaded {
                ClerkLoadingView()
            } else if clerk.user == nil {
                SignedOutView(showAuth: $showAuth)
            } else {
                switch model.phase {
                case .signedOut, .loading:
                    ProgressView("Signing in…")
                case .ready:
                    MainTabs()
                case .needsUnit:
                    MessageView(title: "No unit yet",
                                message: "This account isn't attached to a unit. Create one or accept an invite on app.platoonmanager.com.")
                case .locked:
                    MessageView(title: "Subscription isn't active",
                                message: "This account's subscription isn't active. It is managed on the website.")
                case .inactive:
                    MessageView(title: "Account inactive",
                                message: "This account has been deactivated. Ask your unit's owner.")
                case .failed(let message):
                    MessageView(title: "Couldn't load", message: message, showRetry: true)
                }
            }
        }
        .task(id: clerk.user?.id) {
            await model.clerkUserChanged(clerk.user)
        }
        .onChange(of: clerk.user?.id) { _, id in
            if id != nil { showAuth = false }
        }
        .onOpenURL { url in
            Task { try? await clerk.handle(url) }
        }
        .sheet(isPresented: $showAuth) {
            AuthView()
                .environment(clerk)
        }
    }
}

/// Clerk loads its environment and client before anything can sign in. If that
/// has not happened after a while (no network, or Clerk refusing the app), say
/// so rather than spin for ever. Clerk keeps retrying on foreground.
struct ClerkLoadingView: View {
    @State private var slow = false

    var body: some View {
        VStack(spacing: 16) {
            ProgressView()
            if slow {
                Text("Can't reach the sign-in service. Check your connection; it will keep trying.")
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
                Button("Try again") {
                    Task {
                        _ = try? await Clerk.shared.refreshEnvironment()
                        _ = try? await Clerk.shared.refreshClient()
                    }
                }
            }
        }
        .padding(24)
        .task {
            try? await Task.sleep(for: .seconds(10))
            slow = true
        }
    }
}

struct SignedOutView: View {
    @Binding var showAuth: Bool

    var body: some View {
        VStack(spacing: 24) {
            Spacer()
            Image(systemName: "person.3.sequence.fill")
                .font(.system(size: 56))
                .foregroundStyle(.tint)
            Text("Platoon Manager")
                .font(.largeTitle.bold())
            Text("Personnel accountability for your formation.")
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
            Spacer()
            Button {
                showAuth = true
            } label: {
                Text("Sign in").frame(maxWidth: .infinity)
            }
            .buttonStyle(.borderedProminent)
            .controlSize(.large)
        }
        .padding(24)
    }
}

struct MessageView: View {
    @Environment(AppModel.self) private var model
    let title: String
    let message: String
    var showRetry = false

    var body: some View {
        VStack(spacing: 16) {
            Text(title).font(.title2.bold())
            Text(message)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
                .textSelection(.enabled)
            if showRetry {
                Button("Try again") { Task { await model.retry() } }
                    .buttonStyle(.borderedProminent)
            }
            Button("Sign out") { Task { await model.signOut() } }
        }
        .padding(24)
    }
}
