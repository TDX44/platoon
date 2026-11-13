import ClerkKit
import Foundation

enum APIError: LocalizedError, Equatable {
    /// 401: the server did not accept the Clerk session token. Sign out.
    case unauthorized(azp: String?)
    /// 402: the account's subscription is not active.
    case subscriptionRequired
    /// 403 with `code: account_inactive`.
    case accountInactive
    /// 503: Clerk or the server is briefly unreachable. Retry; never sign out.
    case unavailable
    /// 429: wait this many seconds.
    case rateLimited(seconds: Int, message: String)
    /// Anything else the server refused, with its own reason.
    case server(status: Int, message: String, field: String?)
    case noSession
    case offline(String)

    var errorDescription: String? {
        switch self {
        case .unauthorized(let azp):
            return "The server refused the sign-in (401). Token azp: \(azp ?? "none")"
        case .subscriptionRequired: return "This account's subscription isn't active."
        case .accountInactive: return "This account has been deactivated."
        case .unavailable: return "The server is briefly unreachable. Try again."
        case .rateLimited(_, let message): return message
        case .server(_, let message, _): return message
        case .noSession: return "Not signed in."
        case .offline(let message): return message
        }
    }

    /// Errors that mean the whole session is unusable, not just this request.
    var isSessionLevel: Bool {
        switch self {
        case .unauthorized, .subscriptionRequired, .accountInactive, .noSession: return true
        default: return false
        }
    }
}

/// Calls https://app.platoonmanager.com/api/... with the Clerk session token as
/// `Authorization: Bearer`, which `_get_session_token()` reads before the cookie.
/// Main-actor isolated so request bodies (`[String: Any]`) never cross actors.
@MainActor
struct APIClient {
    var base: URL = Config.apiBase

    static let decoder: JSONDecoder = {
        let d = JSONDecoder()
        d.keyDecodingStrategy = .convertFromSnakeCase
        return d
    }()

    /// Every session-level failure (401, 402, inactive) is reported here so the
    /// app can leave whatever screen it is on. Set by `AppModel`.
    static var onSessionError: (@MainActor (APIError) -> Void)?

    func get<T: Decodable>(_ path: String, query: [String: String] = [:]) async throws -> T {
        try decode(try await send("GET", path, query: query, body: nil))
    }

    func post<T: Decodable>(_ path: String, _ json: [String: Any]) async throws -> T {
        try decode(try await send("POST", path, body: json))
    }

    func put<T: Decodable>(_ path: String, _ json: [String: Any], query: [String: String] = [:]) async throws -> T {
        try decode(try await send("PUT", path, query: query, body: json))
    }

    /// A JSON object as a dictionary of strings (the profile: every field is text).
    func getStrings(_ path: String) async throws -> [String: String] {
        try strings(try await send("GET", path, query: [:], body: nil))
    }

    func putStrings(_ path: String, _ json: [String: Any]) async throws -> [String: String] {
        try strings(try await send("PUT", path, query: [:], body: json))
    }

    private func strings(_ data: Data) throws -> [String: String] {
        guard let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw APIError.offline("The server's answer could not be read.")
        }
        return object.compactMapValues { value in
            if value is NSNull { return "" }
            return value as? String ?? "\(value)"
        }
    }

    /// A write whose answer we do not need.
    func send(_ method: String, _ path: String, _ json: [String: Any]? = nil) async throws {
        _ = try await send(method, path, query: [:], body: json)
    }

    func delete(_ path: String) async throws {
        _ = try await send("DELETE", path, query: [:], body: nil)
    }

    /// The answer's raw bytes (a logo PNG, a backup file).
    func data(_ path: String, query: [String: String] = [:]) async throws -> Data {
        try await send("GET", path, query: query, body: nil)
    }

    /// A POST whose answer is read as loose JSON (the restore summary).
    func postObject(_ path: String, _ json: [String: Any]) async throws -> [String: Any] {
        let data = try await send("POST", path, query: [:], body: json)
        return (try? JSONSerialization.jsonObject(with: data) as? [String: Any]) ?? [:]
    }

    private func decode<T: Decodable>(_ data: Data) throws -> T {
        do {
            return try Self.decoder.decode(T.self, from: data)
        } catch {
            print("[api] decode \(T.self) failed: \(error)")
            throw APIError.offline("The server's answer could not be read.")
        }
    }

    private func send(_ method: String, _ path: String, query: [String: String] = [:],
                      body: [String: Any]?) async throws -> Data {
        guard let token = try? await Clerk.shared.auth.getToken() else {
            throw report(.noSession)
        }
        var components = URLComponents(url: base.appending(path: "api/" + path), resolvingAgainstBaseURL: false)!
        if !query.isEmpty {
            components.queryItems = query.sorted { $0.key < $1.key }.map { URLQueryItem(name: $0.key, value: $0.value) }
        }
        var request = URLRequest(url: components.url!)
        request.httpMethod = method
        request.timeoutInterval = 30
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        if let body {
            request.httpBody = try JSONSerialization.data(withJSONObject: body)
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        }

        var data = Data()
        var response: URLResponse?
        // One retry on 503, as the web client does for /api/auth/sync.
        for attempt in 0..<2 {
            do {
                (data, response) = try await URLSession.shared.data(for: request)
            } catch {
                throw APIError.offline("Can't reach Platoon Manager. Check your connection.")
            }
            if (response as? HTTPURLResponse)?.statusCode == 503 && attempt == 0 {
                try await Task.sleep(for: .milliseconds(1500))
                continue
            }
            break
        }
        let http = response as? HTTPURLResponse
        let status = http?.statusCode ?? 0
        if (200..<300).contains(status) { return data }

        let errorBody = try? Self.decoder.decode(APIErrorBody.self, from: data)
        let message = errorBody?.error ?? HTTPURLResponse.localizedString(forStatusCode: status).capitalized
        switch status {
        case 401:
            let azp = JWT.claim("azp", in: token)
            print("[api] 401 on \(path); token azp = \(azp ?? "none")")
            throw report(.unauthorized(azp: azp))
        case 402:
            throw report(.subscriptionRequired)
        case 403 where errorBody?.code == "account_inactive":
            throw report(.accountInactive)
        case 429:
            let wait = Int(http?.value(forHTTPHeaderField: "Retry-After") ?? "") ?? 3
            throw APIError.rateLimited(seconds: wait, message: message)
        case 503:
            throw APIError.unavailable
        default:
            throw APIError.server(status: status, message: message, field: errorBody?.field)
        }
    }

    private func report(_ error: APIError) -> APIError {
        Self.onSessionError?(error)
        return error
    }
}

/// Reads one claim out of a JWT's payload without verifying it (diagnostics only).
enum JWT {
    static func claim(_ name: String, in token: String) -> String? {
        let parts = token.split(separator: ".")
        guard parts.count >= 2 else { return nil }
        var b64 = parts[1].replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
        while b64.count % 4 != 0 { b64 += "=" }
        guard let data = Data(base64Encoded: b64),
              let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let value = object[name] else { return nil }
        return "\(value)"
    }
}

/// An answer we decode and ignore.
struct Empty: Decodable, Sendable {}
