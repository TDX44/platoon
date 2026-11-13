import ClerkKit
import Foundation
import Observation

/// What the app knows about the signed-in account, and where it is in the
/// sign-in → sync → /api/me sequence.
@MainActor
@Observable
final class AppModel {
    enum Phase {
        case signedOut
        case loading
        case ready(Me)
        case needsUnit(Me)
        case locked
        case inactive
        case failed(String)
    }

    var phase: Phase = .signedOut
    var roster: [Person] = []
    var rosterError: String?

    private let api = APIClient()
    private var syncedUserId: String?

    /// Called whenever Clerk's signed-in user changes.
    func clerkUserChanged(_ user: User?) async {
        guard let user else {
            syncedUserId = nil
            roster = []
            phase = .signedOut
            return
        }
        guard user.id != syncedUserId else { return }
        syncedUserId = user.id
        await sync(user)
    }

    func retry() async {
        guard let user = Clerk.shared.user else { phase = .signedOut; return }
        await sync(user)
    }

    /// `POST /api/auth/sync` with what `syncClerkUser()` in index.html sends,
    /// then `GET /api/me`, then the roster.
    private func sync(_ user: User) async {
        phase = .loading
        let email = user.primaryEmailAddress?.emailAddress ?? ""
        let username = user.username ?? (email.isEmpty ? user.id : email)
        let fullName = [user.firstName, user.lastName].compactMap { $0 }.filter { !$0.isEmpty }
            .joined(separator: " ")
        do {
            let _: Me = try await api.post("api/auth/sync", json: [
                "username": username, "email": email, "full_name": fullName, "invite_token": NSNull(),
            ])
            let me: Me = try await api.get("api/me")
            if me.needsUnit || me.unitId == nil {
                phase = .needsUnit(me)
                return
            }
            phase = .ready(me)
            await loadRoster()
        } catch {
            handle(error)
        }
    }

    func loadRoster() async {
        guard case .ready(let me) = phase, let unitId = me.unitId else { return }
        do {
            let people: [Person] = try await api.get(
                "api/personnel", query: [URLQueryItem(name: "unit", value: String(unitId))])
            roster = people
            rosterError = nil
        } catch {
            if case APIError.http = error { rosterError = error.localizedDescription } else { handle(error) }
        }
    }

    func signOut() async {
        try? await Clerk.shared.auth.signOut()
        syncedUserId = nil
        roster = []
        phase = .signedOut
    }

    private func handle(_ error: Error) {
        switch error {
        case APIError.subscriptionRequired: phase = .locked
        case APIError.accountInactive: phase = .inactive
        default: phase = .failed(error.localizedDescription)
        }
    }
}
