import ClerkKit
import Foundation
import Observation

/// The signed-in account, the unit tree, which unit is being looked at, and
/// where the app is in the sign-in → sync → /api/me sequence.
@MainActor
@Observable
final class AppModel {
    enum Phase: Equatable {
        case signedOut
        case loading
        case ready
        case needsUnit
        case locked
        case inactive
        case failed(String)
    }

    var phase: Phase = .signedOut
    var me: Me?
    var units: [UnitNode] = []
    var unitsError: String?
    /// The unit whose roster, directory, reports and duty are on screen.
    var selectedUnitId: Int?
    /// The organization's IANA zone; "today" is always computed in it.
    var timeZoneId = "America/Chicago"

    let api = APIClient()
    private var syncedUserId: String?

    init() {
        APIClient.onSessionError = { [weak self] error in
            self?.sessionFailed(error)
        }
    }

    var tree: UnitTree { UnitTree(units: units) }
    var selectedUnit: UnitNode? { units.first { $0.id == selectedUnitId } }
    var timeZone: TimeZone { TimeZone(identifier: timeZoneId) ?? TimeZone(identifier: "America/Chicago")! }

    /// The duty day, "YYYY-MM-DD", on the organization's clock (`getTodayStr()`).
    var today: String { DayMath.string(Date(), in: timeZone) }

    // MARK: Session

    func clerkUserChanged(_ user: User?) async {
        guard let user else {
            reset()
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
    /// then `GET /api/me` and `GET /api/units`.
    private func sync(_ user: User) async {
        phase = .loading
        let email = user.primaryEmailAddress?.emailAddress ?? ""
        let username = user.username ?? (email.isEmpty ? user.id : email)
        let fullName = [user.firstName, user.lastName].compactMap { $0 }.filter { !$0.isEmpty }
            .joined(separator: " ")
        do {
            let _: Me = try await api.post("auth/sync", [
                "username": username, "email": email, "full_name": fullName, "invite_token": NSNull(),
            ])
            let me: Me = try await api.get("me")
            adopt(me)
            if me.needsUnit || me.unitId == nil {
                phase = .needsUnit
                return
            }
            try await loadUnits()
            if selectedUnitId == nil || selectedUnit == nil { selectedUnitId = me.unitId }
            phase = .ready
        } catch let error as APIError {
            sessionFailed(error)
        } catch {
            phase = .failed(error.localizedDescription)
        }
    }

    private func adopt(_ me: Me) {
        self.me = me
        if let tz = me.timezone, TimeZone(identifier: tz) != nil { timeZoneId = tz }
    }

    /// Adopt a timezone the server sent with `GET /api/settings`.
    func adoptTimeZone(_ tz: String?) {
        if let tz, TimeZone(identifier: tz) != nil, tz != timeZoneId { timeZoneId = tz }
    }

    func loadUnits() async throws {
        units = try await api.get("units")
        unitsError = nil
    }

    func refreshUnits() async {
        do { try await loadUnits() } catch { unitsError = error.localizedDescription }
    }

    func signOut() async {
        try? await Clerk.shared.auth.signOut()
        reset()
        phase = .signedOut
    }

    private func reset() {
        syncedUserId = nil
        me = nil
        units = []
        selectedUnitId = nil
    }

    private func sessionFailed(_ error: APIError) {
        switch error {
        case .subscriptionRequired: phase = .locked
        case .accountInactive: phase = .inactive
        case .unauthorized, .noSession:
            Task { await signOut() }
        default:
            if phase == .loading { phase = .failed(error.localizedDescription) }
        }
    }
}

/// Calendar-day arithmetic on "YYYY-MM-DD" strings, the format the whole app
/// stores dates in. Every conversion names the time zone it happens in.
enum DayMath {
    private static func formatter(_ tz: TimeZone) -> DateFormatter {
        let f = DateFormatter()
        f.calendar = Calendar(identifier: .gregorian)
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = tz
        f.dateFormat = "yyyy-MM-dd"
        return f
    }

    static func string(_ date: Date, in tz: TimeZone) -> String { formatter(tz).string(from: date) }

    /// Noon on that day in the given zone: safe to hand a DatePicker.
    static func date(_ day: String, in tz: TimeZone) -> Date? {
        guard let d = formatter(tz).date(from: day) else { return nil }
        return d.addingTimeInterval(12 * 3600)
    }

    static func adding(_ days: Int, to day: String) -> String {
        let utc = TimeZone(identifier: "UTC")!
        guard let d = formatter(utc).date(from: day) else { return day }
        return string(d.addingTimeInterval(Double(days) * 86400), in: utc)
    }

    /// "Thu 13 Nov"
    static func pretty(_ day: String) -> String {
        let utc = TimeZone(identifier: "UTC")!
        guard let d = formatter(utc).date(from: day) else { return day }
        let f = DateFormatter()
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = utc
        f.dateFormat = "EEE d MMM"
        return f.string(from: d)
    }

    /// "13 Nov 2026"
    static func long(_ day: String) -> String {
        let utc = TimeZone(identifier: "UTC")!
        guard let d = formatter(utc).date(from: day) else { return day }
        let f = DateFormatter()
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = utc
        f.dateFormat = "d MMM yyyy"
        return f.string(from: d)
    }
}
