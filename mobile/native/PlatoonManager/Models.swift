import Foundation

/// `GET /api/me` and `POST /api/auth/sync` (both answer `_user_json()` plus flags).
struct Me: Decodable, Sendable, Equatable {
    let id: Int
    let username: String
    let email: String?
    let fullName: String?
    let unitId: Int?
    let unitName: String?
    let unitSlug: String?
    let role: String?
    let rootId: Int?
    let timezone: String?
    let needsUnit: Bool
    let quickAdd: Bool?
    let platformAdmin: Bool?

    var isOwner: Bool { role == "owner" }
    var displayName: String { (fullName?.isEmpty == false ? fullName : nil) ?? username }
}

/// One row of a `scheduled_events` table, as `GET /api/personnel` and
/// `GET /api/personnel/<id>/absences` return it.
struct ScheduledEvent: Codable, Identifiable, Sendable, Hashable {
    let id: Int
    var personId: Int?
    var status: String
    var fromDate: String?
    var toDate: String?
    var notes: String?
    var state: String?
    var seriesId: String?
    var createdAt: String?

    var from: String { fromDate ?? "" }
    var to: String { toDate ?? "" }
}

/// A soldier on the roster, in the shape the web client keeps (`load()` in
/// index.html maps `from_date`/`to_date` to `from`/`to`). The pure rules in
/// `Rules/` work on this type.
struct Soldier: Identifiable, Sendable, Hashable {
    var id: Int
    var rank: String
    var last: String
    var first: String
    var unitId: Int
    var status: String
    var notes: String
    var from: String
    var to: String
    var presentDate: String
    var noFormation: Bool
    /// Upcoming (`state == "scheduled"`) events only, as the web client keeps them.
    var scheduledEvents: [ScheduledEvent]

    init(id: Int, rank: String = "", last: String = "", first: String = "", unitId: Int = 0,
         status: String = "present", notes: String = "", from: String = "", to: String = "",
         presentDate: String = "", noFormation: Bool = false, scheduledEvents: [ScheduledEvent] = []) {
        self.id = id; self.rank = rank; self.last = last; self.first = first; self.unitId = unitId
        self.status = status; self.notes = notes; self.from = from; self.to = to
        self.presentDate = presentDate; self.noFormation = noFormation; self.scheduledEvents = scheduledEvents
    }
}

/// The wire shape of a `personnel` row.
struct PersonnelRow: Decodable, Sendable {
    let id: Int
    let rank: String?
    let last: String?
    let first: String?
    let unitId: Int?
    let status: String?
    let notes: String?
    let fromDate: String?
    let toDate: String?
    let presentDate: String?
    let noFormation: Bool?
    let scheduledEvents: [ScheduledEvent]?

    var soldier: Soldier {
        Soldier(id: id, rank: rank ?? "", last: last ?? "", first: first ?? "", unitId: unitId ?? 0,
                status: status ?? "present", notes: notes ?? "", from: fromDate ?? "", to: toDate ?? "",
                presentDate: presentDate ?? "", noFormation: noFormation ?? false,
                scheduledEvents: (scheduledEvents ?? []).filter { $0.state == "scheduled" })
    }
}

/// One unit of `GET /api/units` (the caller's subtree, flat, with parent pointers).
struct UnitNode: Decodable, Identifiable, Sendable, Hashable {
    let id: Int
    var parentId: Int?
    var kind: String
    var name: String
    var slug: String
    /// Personnel directly in this unit.
    var count: Int?
    /// Today's present / unaccounted directly in this unit.
    var present: Int?
    var unaccounted: Int?
    /// The logo this unit shows: its own, or the nearest ancestor's.
    var logo: LogoRef?

    init(id: Int, parentId: Int? = nil, kind: String = "platoon", name: String = "", slug: String = "",
         count: Int? = nil, present: Int? = nil, unaccounted: Int? = nil) {
        self.id = id; self.parentId = parentId; self.kind = kind; self.name = name; self.slug = slug
        self.count = count; self.present = present; self.unaccounted = unaccounted
    }
}

/// Which unit's logo a unit resolves to (`_resolved_logos()`).
struct LogoRef: Decodable, Sendable, Hashable {
    var unitId: Int?
    var v: String?
    var name: String?
}

/// An account (`GET /api/users`, `GET /api/me`).
struct Account: Decodable, Identifiable, Sendable, Hashable {
    let id: Int
    var username: String
    var email: String?
    var fullName: String?
    var unitId: Int?
    var unitName: String?
    var role: String?
    var isActive: Bool?
    var editable: Bool?
    var canToggleActive: Bool?

    var displayName: String { (fullName?.isEmpty == false ? fullName : nil) ?? username }
    var isOwner: Bool { role == "owner" }
}

/// A row of `GET /api/invites`.
struct Invite: Decodable, Identifiable, Sendable, Hashable {
    let token: String
    var label: String?
    var unitId: Int?
    var unitName: String?
    var role: String?
    var createdBy: String?
    var expiresAt: String?
    var status: String?
    var id: String { token }
}

/// A row of `GET /api/audit`.
struct AuditRow: Decodable, Identifiable, Sendable, Hashable {
    let id: Int
    var timestamp: String?
    var username: String?
    var action: String?
    var details: String?
    var unitId: Int?
}

/// The server's error body: `{"error": "...", "field"?: "...", "code"?: "..."}`.
struct APIErrorBody: Decodable, Sendable {
    let error: String?
    let field: String?
    let code: String?
}

/// `GET /api/settings?unit=` (the parts the app uses).
struct UnitSettings: Decodable, Sendable {
    var unitName: String?
    var kind: String?
    var tdySchools: [String]?
    var tdyLocations: [String]?
    var timezone: String?
}

struct AbsenceList: Decodable, Sendable {
    let absences: [ScheduledEvent]
}

/// A row of `GET /api/reports` (no text) or `GET /api/reports/<id>` (with text).
struct ReportRecord: Decodable, Identifiable, Sendable, Hashable {
    let id: Int
    var unitId: Int?
    var unitName: String?
    var createdAt: String?
    var createdBy: String?
    var text: String?
}

/// `GET /api/directory?unit=`: a roster row joined with a few profile columns.
struct DirectoryEntry: Decodable, Identifiable, Sendable, Hashable {
    struct NextAbsence: Decodable, Sendable, Hashable {
        var status: String
        var fromDate: String?
        var toDate: String?
    }
    let id: Int
    var rank: String?
    var last: String?
    var first: String?
    var status: String?
    var fromDate: String?
    var toDate: String?
    var notes: String?
    var unitId: Int?
    var dodId: String?
    var dob: String?
    var mos: String?
    var section: String?
    var phone: String?
    var nextAbsence: NextAbsence?
}

/// `GET /api/availability?unit=&date=[&to=]`.
struct Availability: Decodable, Sendable {
    struct Who: Decodable, Identifiable, Sendable, Hashable {
        let id: Int
        var rank: String?
        var last: String?
        var first: String?
        var status: String?
        var fromDate: String?
        var toDate: String?
        var notes: String?
        var location: String?
        var days: [String]?
        var wholeRange: Bool?
    }
    var date: String
    var to: String
    var span: Int
    var available: [Who]
    var unavailable: [Who]
}

/// A duty conflict: the absence covering the duty date.
struct DutyConflict: Decodable, Sendable, Hashable {
    var status: String?
    var fromDate: String?
    var toDate: String?
    var label: String
}

/// A row of `GET /api/duty?unit=[&date=]`.
struct DutyEntry: Decodable, Identifiable, Sendable, Hashable {
    let id: Int
    var date: String
    var unitId: Int?
    var dutyType: String
    var personId: Int?
    var rank: String?
    var last: String?
    var first: String?
    var notes: String?
    var conflict: DutyConflict?
}

/// `GET`/`PUT /api/me/notifications`.
struct NotificationPrefs: Decodable, Sendable, Equatable {
    var accountabilityEnabled: Bool
    var accountabilityTime: String
    var digestEnabled: Bool
    var digestTime: String
    var emailEnabled: Bool?
    var email: String?
}

/// One proposal from `POST /api/absences/quick` (quick_add.read_proposals()).
struct QuickProposal: Decodable, Sendable, Hashable, Identifiable {
    var id: String { "\(personId ?? -1)-\(status)-\(fromDate ?? "")-\(name ?? "")" }
    var personId: Int?
    var name: String?
    var status: String
    var fromDate: String?
    var toDate: String?
    var school: String?
    var location: String?
    var reason: String?
    var question: String?
}

struct QuickProposals: Decodable, Sendable {
    var proposals: [QuickProposal]
}
