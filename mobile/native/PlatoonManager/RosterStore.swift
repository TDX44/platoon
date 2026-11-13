import Foundation
import Observation

/// One unit's roster (its whole subtree) and the TDY lists, plus every write
/// the accountability screens make. Mirrors `load()`, `apiUpdate()` and the
/// absence calls in index.html, so the server's rules stay the only rules.
@MainActor
@Observable
final class RosterStore {
    let unitId: Int
    private let app: AppModel
    private var api: APIClient { app.api }

    var soldiers: [Soldier] = []
    var schools: [String] = []
    var locations: [String] = []
    var loaded = false
    var loading = false
    var error: String?
    /// A transient message for a failed write ("toast").
    var notice: String?

    init(unitId: Int, app: AppModel) {
        self.unitId = unitId
        self.app = app
    }

    var today: String { app.today }

    func soldier(_ id: Int) -> Soldier? { soldiers.first { $0.id == id } }

    // MARK: Load

    func load() async {
        loading = true
        defer { loading = false }
        do {
            async let rows: [PersonnelRow] = api.get("personnel", query: ["unit": String(unitId)])
            async let settings: UnitSettings = api.get("settings", query: ["unit": String(unitId)])
            let (r, s) = try await (rows, settings)
            soldiers = r.map(\.soldier)
            schools = s.tdySchools ?? []
            locations = s.tdyLocations ?? []
            app.adoptTimeZone(s.timezone)
            loaded = true
            error = nil
        } catch {
            if !loaded { self.error = error.localizedDescription } else { notice = error.localizedDescription }
        }
    }

    // MARK: Present / unaccounted (PUT /api/personnel/<id>)

    /// `apiUpdate()`: an absence is owned by the server, so only a 'present'
    /// mark is ours to send; an edit never sends status.
    private func body(for p: Soldier, markPresent: Bool) -> [String: Any] {
        var body: [String: Any] = ["rank": p.rank, "last": p.last, "first": p.first, "unit_id": p.unitId,
                                   "present_date": p.presentDate, "no_formation": p.noFormation]
        if p.status == "present" {
            if markPresent {
                body["status"] = "present"; body["notes"] = p.notes
                body["from_date"] = p.from; body["to_date"] = p.to
            } else {
                body["notes"] = p.notes
            }
        }
        return body
    }

    /// Optimistic, like `savePersonChange()`: apply, save, put it back if the save fails.
    @discardableResult
    private func save(_ id: Int, _ change: (inout Soldier) -> Void) async -> Bool {
        guard let i = soldiers.firstIndex(where: { $0.id == id }) else { return false }
        let before = soldiers[i]
        change(&soldiers[i])
        do {
            let row: PersonnelRow = try await api.put("personnel/\(id)", body(for: soldiers[i], markPresent: true))
            if let j = soldiers.firstIndex(where: { $0.id == id }) {
                var updated = row.soldier
                updated.scheduledEvents = soldiers[j].scheduledEvents
                soldiers[j] = updated
            }
            return true
        } catch {
            if let j = soldiers.firstIndex(where: { $0.id == id }) { soldiers[j] = before }
            notice = error.localizedDescription
            await load()
            return false
        }
    }

    /// The tick on an unaccounted row (`markPresentToday`).
    @discardableResult
    func markPresentToday(_ id: Int) async -> Bool {
        let day = today
        return await save(id) { $0.presentDate = day }
    }

    /// `unmarkPresent`: back to unaccounted.
    @discardableResult
    func unmarkPresent(_ id: Int) async -> Bool {
        await save(id) { $0.presentDate = "" }
    }

    /// `markPresent`: ends a running absence (the server closes it).
    @discardableResult
    func markPresent(_ id: Int, reload: Bool = true) async -> Bool {
        let day = today
        let ok = await save(id) {
            $0.status = "present"; $0.notes = ""; $0.from = ""; $0.to = ""; $0.presentDate = day
        }
        if ok && reload { await load() }
        return ok
    }

    /// `guardedPresent()`: present only if still unaccounted (Everyone else present).
    func guardedPresent(_ id: Int) async -> Bool {
        do {
            let row: PersonnelRow = try await api.put("personnel/\(id)", [
                "status": "present", "present_date": today, "only_if_unaccounted": true,
            ])
            if let j = soldiers.firstIndex(where: { $0.id == id }) {
                var updated = row.soldier
                updated.scheduledEvents = soldiers[j].scheduledEvents
                soldiers[j] = updated
            }
            return true
        } catch {
            return false
        }
    }

    // MARK: Absences

    /// `POST /api/personnel/<id>/schedule`. Idempotent on the server.
    func book(_ id: Int, status: String, from: String, to: String, notes: String,
              recurrence: [String: Any]? = nil) async throws {
        var body: [String: Any] = ["status": status, "from_date": from, "to_date": to, "notes": notes]
        if let recurrence { body["recurrence"] = recurrence }
        try await api.send("POST", "personnel/\(id)/schedule", body)
    }

    /// Formation's one-tap absence (today only); mirrors the row the server wrote.
    func bookToday(_ id: Int, status: String, notes: String) async -> Bool {
        do {
            let ev: ScheduledEvent = try await api.post("personnel/\(id)/schedule", [
                "status": status, "from_date": today, "to_date": today, "notes": notes,
            ])
            if let j = soldiers.firstIndex(where: { $0.id == id }) {
                soldiers[j].status = ev.status
                soldiers[j].from = ev.from
                soldiers[j].to = ev.to
                soldiers[j].notes = ev.notes ?? ""
            }
            return true
        } catch {
            notice = error.localizedDescription
            return false
        }
    }

    /// `PUT /api/schedules/<id>`.
    func editAbsence(_ eventId: Int, status: String, from: String, to: String, notes: String) async throws {
        try await api.send("PUT", "schedules/\(eventId)",
                           ["status": status, "from_date": from, "to_date": to, "notes": notes, "location": ""])
    }

    func deleteAbsence(_ eventId: Int) async throws {
        try await api.delete("schedules/\(eventId)")
    }

    func deleteSeries(_ seriesId: String) async throws {
        let escaped = seriesId.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? seriesId
        try await api.delete("schedules/series/\(escaped)")
    }

    func absences(of id: Int) async throws -> [ScheduledEvent] {
        let list: AbsenceList = try await api.get("personnel/\(id)/absences")
        return list.absences
    }

    // MARK: People

    func addSoldier(rank: String, last: String, first: String, unitId: Int, noFormation: Bool) async throws {
        try await api.send("POST", "personnel", ["rank": rank, "last": last, "first": first,
                                                 "unit_id": unitId, "no_formation": noFormation])
        await load()
    }

    /// The Edit Person modal: name, rank, unit, no-formation. Never sends a status.
    func editSoldier(_ p: Soldier) async throws {
        try await api.send("PUT", "personnel/\(p.id)", body(for: p, markPresent: false))
        await load()
    }

    func removeSoldier(_ id: Int) async throws {
        try await api.delete("personnel/\(id)")
        soldiers.removeAll { $0.id == id }
    }

    /// Replace a whole TDY list (the TDY lists screen). Order is kept exactly:
    /// it is the order the absence form's pickers show.
    func saveList(kind: String, _ list: [String]) async throws {
        let s: UnitSettings = try await api.put("settings", ["tdy_\(kind)": list], query: ["unit": String(unitId)])
        schools = s.tdySchools ?? schools
        locations = s.tdyLocations ?? locations
    }

    /// Add a custom school or location to the unit's TDY list ("Save to list").
    func addToList(kind: String, value: String) async {
        let v = value.trimmingCharacters(in: .whitespaces)
        guard !v.isEmpty else { return }
        var list = kind == "schools" ? schools : locations
        guard !list.contains(where: { $0.caseInsensitiveCompare(v) == .orderedSame }) else { return }
        list.append(v)
        do {
            let s: UnitSettings = try await api.put("settings", ["tdy_\(kind)": list], query: ["unit": String(unitId)])
            schools = s.tdySchools ?? schools
            locations = s.tdyLocations ?? locations
        } catch {
            notice = error.localizedDescription
        }
    }

    // MARK: Reports

    private static var savedToday: (day: String, keys: Set<String>) = ("", [])

    /// `saveToHistory()`: one row per distinct text per unit per duty day.
    func saveToHistory(_ text: String, unitName: String) async {
        let day = today
        if Self.savedToday.day != day { Self.savedToday = (day, []) }
        let key = "\(unitId)|\(text)"
        guard !Self.savedToday.keys.contains(key) else { return }
        Self.savedToday.keys.insert(key)
        do {
            try await api.send("POST", "reports", ["unit_id": unitId, "unit_name": unitName, "text": text])
        } catch {
            Self.savedToday.keys.remove(key)
        }
    }
}
