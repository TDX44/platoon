import Foundation

/// `GET /api/me` and `POST /api/auth/sync` (both answer `_user_json()` plus flags).
struct Me: Decodable, Sendable {
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
}

/// One row of `GET /api/personnel?unit=<id>`. Phase 0 reads only what it shows.
struct Person: Decodable, Identifiable, Sendable {
    let id: Int
    let rank: String
    let last: String
    let first: String
    let status: String?
    let unitId: Int?

    var displayName: String {
        let name = first.isEmpty ? last : "\(last), \(first)"
        return rank.isEmpty ? name : "\(rank) \(name)"
    }
}

/// The server's error body: `{"error": "...", "field"?: "...", "code"?: "..."}`.
struct APIErrorBody: Decodable, Sendable {
    let error: String?
    let code: String?
}
