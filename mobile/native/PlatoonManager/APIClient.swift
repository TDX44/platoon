import ClerkKit
import Foundation

enum APIError: LocalizedError {
    /// 401: the server did not accept the Clerk session token.
    case unauthorized(azp: String?)
    /// 402: the account's subscription is not active.
    case subscriptionRequired
    /// 403 with `code: account_inactive`.
    case accountInactive
    /// 503: Clerk or the server is briefly unreachable. Retry; never sign out.
    case unavailable
    case http(Int, String)
    case noSession

    var errorDescription: String? {
        switch self {
        case .unauthorized(let azp):
            return "The server refused the sign-in (401). Token azp: \(azp ?? "none")"
        case .subscriptionRequired: return "This account's subscription isn't active."
        case .accountInactive: return "This account has been deactivated."
        case .unavailable: return "The server is briefly unreachable. Try again."
        case .http(let code, let message): return "\(message) (\(code))"
        case .noSession: return "Not signed in."
        }
    }
}

/// Calls https://app.platoonmanager.com/api/... with the Clerk session token as
/// `Authorization: Bearer`, which `_get_session_token()` reads before the cookie.
struct APIClient {
    var base: URL = Config.apiBase

    private static let decoder: JSONDecoder = {
        let d = JSONDecoder()
        d.keyDecodingStrategy = .convertFromSnakeCase
        return d
    }()

    func get<T: Decodable>(_ path: String, query: [URLQueryItem] = []) async throws -> T {
        try await send(path, method: "GET", query: query, body: Optional<Data>.none)
    }

    func post<T: Decodable>(_ path: String, json: [String: Any]) async throws -> T {
        let body = try JSONSerialization.data(withJSONObject: json)
        return try await send(path, method: "POST", query: [], body: body)
    }

    private func send<T: Decodable>(_ path: String, method: String, query: [URLQueryItem],
                                    body: Data?) async throws -> T {
        guard let token = try await Clerk.shared.auth.getToken() else { throw APIError.noSession }
        var components = URLComponents(url: base.appending(path: path), resolvingAgainstBaseURL: false)!
        if !query.isEmpty { components.queryItems = query }
        var request = URLRequest(url: components.url!)
        request.httpMethod = method
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        if let body {
            request.httpBody = body
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        }

        // One retry on 503, as the web client does for /api/auth/sync.
        var (data, response) = try await URLSession.shared.data(for: request)
        if (response as? HTTPURLResponse)?.statusCode == 503 {
            try await Task.sleep(for: .milliseconds(1500))
            (data, response) = try await URLSession.shared.data(for: request)
        }
        let status = (response as? HTTPURLResponse)?.statusCode ?? 0
        let errorBody = try? Self.decoder.decode(APIErrorBody.self, from: data)

        switch status {
        case 200..<300:
            return try Self.decoder.decode(T.self, from: data)
        case 401:
            let azp = JWT.claim("azp", in: token)
            print("[api] 401 on \(path); token azp = \(azp ?? "none"); body = \(String(decoding: data, as: UTF8.self))")
            throw APIError.unauthorized(azp: azp)
        case 402:
            throw APIError.subscriptionRequired
        case 403 where errorBody?.code == "account_inactive":
            throw APIError.accountInactive
        case 503:
            throw APIError.unavailable
        default:
            throw APIError.http(status, errorBody?.error ?? HTTPURLResponse.localizedString(forStatusCode: status))
        }
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
