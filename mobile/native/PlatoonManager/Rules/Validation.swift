import Foundation

// Ported from index.html: validateField(), normalizeField() and everything they
// use: CLEARANCES, WEAPONS, CLEARANCE_ALIASES, US_STATES, PHONE_COUNTRIES /
// DIAL_CODES, the phone helpers (phoneSplit, phoneJoin, maxNationalDigits,
// nationalDigits, formatPhone, phoneE164, phoneMask, phoneCountryIndex),
// isRealDate() and parseWeapons(). Spec: tests/test_validation.py (which also
// pins validation.py, the server's copy) and tests/test_phone_js.py.
// The regexes are written out by hand: JS `\d` and `[A-Za-z]` are ASCII-only,
// which Swift's character classes are not by default.

struct PhoneCountry: Sendable, Equatable, Hashable {
    let name: String
    let iso: String
    let dial: String

    /// Regional-indicator flag ("🇺🇸"), as `flagOf(iso)`.
    var flag: String {
        String(String.UnicodeScalarView(iso.unicodeScalars.compactMap { Unicode.Scalar(0x1F1E6 + $0.value - 65) }))
    }
}

struct WeaponQual: Sendable, Equatable, Hashable, Codable {
    var weapon: String
    /// "YYYY-MM-DD" or "".
    var date: String
}

enum FieldRules {
    static let clearances: [String] = ["", "None", "Confidential", "Interim Secret", "Secret",
                                       "Interim Top Secret", "Top Secret", "TS/SCI"]

    static let weapons: [String] = ["M4", "M4A1", "M16A2", "M16A4", "M17", "M18", "M9", "M11",
                                    "M249", "M240B", "M240L", "M2", "M2A1", "MK19",
                                    "M320", "M203", "M136/AT4", "M141", "M72 LAW",
                                    "M107", "M110", "M24", "M2010", "M500", "M590"]

    /// Spellings people actually write, keyed lowercase with single spaces.
    static let clearanceAliases: [String: String] = [
        "ts": "Top Secret", "tssci": "TS/SCI", "ts-sci": "TS/SCI", "ts sci": "TS/SCI",
        "ts/sci": "TS/SCI", "top secret/sci": "TS/SCI", "top secret sci": "TS/SCI",
        "sci": "TS/SCI", "s": "Secret", "sec": "Secret", "c": "Confidential",
        "n/a": "None", "na": "None", "none": "None",
    ]

    /// States, territories and the three military "states" (AA/AE/AP). "" first.
    static let usStates: [String] = ["", "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI",
        "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN",
        "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH",
        "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
        "WV", "WI", "WY",
        "AS", "GU", "MP", "PR", "VI",
        "AA", "AE", "AP"]

    static let addressFields: [String] = ["address_street", "address_street2", "address_city",
                                          "address_state", "address_zip"]
    static let nameFields: [String] = ["last", "first", "emergency_name", "spouse_dependents", "next_of_kin"]
    static let phoneFields: [String] = ["phone", "emergency_phone"]
    static let dateFields: [String] = ["dob", "date_of_rank", "ets_date", "medical_date", "dental_date"]

    static let maxLen = 200
    static let maxLenLong = 2000
    static let nanpDigits = 10
    static let e164Max = 15

    /// Curated, not exhaustive; only ever extended by hand. +1 reads back as
    /// the United States (index 0).
    static let phoneCountries: [PhoneCountry] = [
        PhoneCountry(name: "United States", iso: "US", dial: "1"), PhoneCountry(name: "Canada", iso: "CA", dial: "1"),
        PhoneCountry(name: "Mexico", iso: "MX", dial: "52"), PhoneCountry(name: "United Kingdom", iso: "GB", dial: "44"),
        PhoneCountry(name: "Germany", iso: "DE", dial: "49"), PhoneCountry(name: "Italy", iso: "IT", dial: "39"),
        PhoneCountry(name: "Spain", iso: "ES", dial: "34"), PhoneCountry(name: "France", iso: "FR", dial: "33"),
        PhoneCountry(name: "Poland", iso: "PL", dial: "48"), PhoneCountry(name: "Netherlands", iso: "NL", dial: "31"),
        PhoneCountry(name: "Belgium", iso: "BE", dial: "32"), PhoneCountry(name: "Portugal", iso: "PT", dial: "351"),
        PhoneCountry(name: "Greece", iso: "GR", dial: "30"), PhoneCountry(name: "Turkey", iso: "TR", dial: "90"),
        PhoneCountry(name: "Romania", iso: "RO", dial: "40"), PhoneCountry(name: "Bulgaria", iso: "BG", dial: "359"),
        PhoneCountry(name: "Hungary", iso: "HU", dial: "36"), PhoneCountry(name: "Czechia", iso: "CZ", dial: "420"),
        PhoneCountry(name: "Slovakia", iso: "SK", dial: "421"), PhoneCountry(name: "Austria", iso: "AT", dial: "43"),
        PhoneCountry(name: "Switzerland", iso: "CH", dial: "41"), PhoneCountry(name: "Sweden", iso: "SE", dial: "46"),
        PhoneCountry(name: "Norway", iso: "NO", dial: "47"), PhoneCountry(name: "Denmark", iso: "DK", dial: "45"),
        PhoneCountry(name: "Finland", iso: "FI", dial: "358"), PhoneCountry(name: "Iceland", iso: "IS", dial: "354"),
        PhoneCountry(name: "Ireland", iso: "IE", dial: "353"), PhoneCountry(name: "Lithuania", iso: "LT", dial: "370"),
        PhoneCountry(name: "Latvia", iso: "LV", dial: "371"), PhoneCountry(name: "Estonia", iso: "EE", dial: "372"),
        PhoneCountry(name: "Ukraine", iso: "UA", dial: "380"), PhoneCountry(name: "Croatia", iso: "HR", dial: "385"),
        PhoneCountry(name: "Slovenia", iso: "SI", dial: "386"), PhoneCountry(name: "Serbia", iso: "RS", dial: "381"),
        PhoneCountry(name: "Albania", iso: "AL", dial: "355"), PhoneCountry(name: "North Macedonia", iso: "MK", dial: "389"),
        PhoneCountry(name: "Montenegro", iso: "ME", dial: "382"), PhoneCountry(name: "Bosnia and Herzegovina", iso: "BA", dial: "387"),
        PhoneCountry(name: "Luxembourg", iso: "LU", dial: "352"), PhoneCountry(name: "Malta", iso: "MT", dial: "356"),
        PhoneCountry(name: "Cyprus", iso: "CY", dial: "357"), PhoneCountry(name: "Moldova", iso: "MD", dial: "373"),
        PhoneCountry(name: "Georgia", iso: "GE", dial: "995"), PhoneCountry(name: "Armenia", iso: "AM", dial: "374"),
        PhoneCountry(name: "Azerbaijan", iso: "AZ", dial: "994"), PhoneCountry(name: "South Korea", iso: "KR", dial: "82"),
        PhoneCountry(name: "Japan", iso: "JP", dial: "81"), PhoneCountry(name: "China", iso: "CN", dial: "86"),
        PhoneCountry(name: "Taiwan", iso: "TW", dial: "886"), PhoneCountry(name: "Philippines", iso: "PH", dial: "63"),
        PhoneCountry(name: "Thailand", iso: "TH", dial: "66"), PhoneCountry(name: "Vietnam", iso: "VN", dial: "84"),
        PhoneCountry(name: "Singapore", iso: "SG", dial: "65"), PhoneCountry(name: "Malaysia", iso: "MY", dial: "60"),
        PhoneCountry(name: "Indonesia", iso: "ID", dial: "62"), PhoneCountry(name: "India", iso: "IN", dial: "91"),
        PhoneCountry(name: "Pakistan", iso: "PK", dial: "92"), PhoneCountry(name: "Bangladesh", iso: "BD", dial: "880"),
        PhoneCountry(name: "Sri Lanka", iso: "LK", dial: "94"), PhoneCountry(name: "Nepal", iso: "NP", dial: "977"),
        PhoneCountry(name: "Australia", iso: "AU", dial: "61"), PhoneCountry(name: "New Zealand", iso: "NZ", dial: "64"),
        PhoneCountry(name: "Kuwait", iso: "KW", dial: "965"), PhoneCountry(name: "Qatar", iso: "QA", dial: "974"),
        PhoneCountry(name: "Bahrain", iso: "BH", dial: "973"), PhoneCountry(name: "United Arab Emirates", iso: "AE", dial: "971"),
        PhoneCountry(name: "Saudi Arabia", iso: "SA", dial: "966"), PhoneCountry(name: "Oman", iso: "OM", dial: "968"),
        PhoneCountry(name: "Jordan", iso: "JO", dial: "962"), PhoneCountry(name: "Israel", iso: "IL", dial: "972"),
        PhoneCountry(name: "Iraq", iso: "IQ", dial: "964"), PhoneCountry(name: "Lebanon", iso: "LB", dial: "961"),
        PhoneCountry(name: "Egypt", iso: "EG", dial: "20"), PhoneCountry(name: "Djibouti", iso: "DJ", dial: "253"),
        PhoneCountry(name: "South Africa", iso: "ZA", dial: "27"), PhoneCountry(name: "Nigeria", iso: "NG", dial: "234"),
        PhoneCountry(name: "Kenya", iso: "KE", dial: "254"), PhoneCountry(name: "Ghana", iso: "GH", dial: "233"),
        PhoneCountry(name: "Ethiopia", iso: "ET", dial: "251"), PhoneCountry(name: "Morocco", iso: "MA", dial: "212"),
        PhoneCountry(name: "Tunisia", iso: "TN", dial: "216"), PhoneCountry(name: "Brazil", iso: "BR", dial: "55"),
        PhoneCountry(name: "Argentina", iso: "AR", dial: "54"), PhoneCountry(name: "Chile", iso: "CL", dial: "56"),
        PhoneCountry(name: "Colombia", iso: "CO", dial: "57"), PhoneCountry(name: "Peru", iso: "PE", dial: "51"),
        PhoneCountry(name: "Ecuador", iso: "EC", dial: "593"), PhoneCountry(name: "Venezuela", iso: "VE", dial: "58"),
        PhoneCountry(name: "Panama", iso: "PA", dial: "507"), PhoneCountry(name: "Costa Rica", iso: "CR", dial: "506"),
        PhoneCountry(name: "Guatemala", iso: "GT", dial: "502"), PhoneCountry(name: "Honduras", iso: "HN", dial: "504"),
        PhoneCountry(name: "El Salvador", iso: "SV", dial: "503"), PhoneCountry(name: "Nicaragua", iso: "NI", dial: "505"),    ]

    /// Distinct dial codes, longest first, so +35 never shadows +351.
    static let dialCodes: [String] = {
        var seen = Set<String>(), out: [String] = []
        for c in phoneCountries where seen.insert(c.dial).inserted { out.append(c.dial) }
        return out.sorted { $0.count != $1.count ? $0.count > $1.count : $0 < $1 }
    }()
}

// MARK: - Small JS equivalents

private func asciiDigits(_ s: some StringProtocol) -> String {
    String(s.filter(\.isASCIIDigit))      // s.replace(/\D/g, '')
}

/// `/[A-Za-z]/.test(value)`.
func hasLetters(_ value: String) -> Bool {
    value.contains(where: \.isASCIILetter)
}

/// `/^[\d\s-]+$/`
private func isDigitsSpacesHyphens(_ s: String) -> Bool {
    !s.isEmpty && s.allSatisfy { $0.isASCIIDigit || $0.isWhitespace || $0 == "-" }
}

/// NAME_RE `/^[A-Za-z][A-Za-z .,'\-]*$/` and CITY_RE (the same without the comma).
private func matchesNameLike(_ s: String, allowComma: Bool) -> Bool {
    guard let first = s.first, first.isASCIILetter else { return false }
    return s.dropFirst().allSatisfy {
        $0.isASCIILetter || $0 == " " || $0 == "." || $0 == "'" || $0 == "-" || (allowComma && $0 == ",")
    }
}

/// EMAIL_RE `/^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$/`: one @, no whitespace, and a
/// domain whose last dot has something before it and 2+ letters after.
private func isEmail(_ s: String) -> Bool {
    let parts = s.split(separator: "@", omittingEmptySubsequences: false)
    guard parts.count == 2, !parts[0].isEmpty, !s.contains(where: \.isWhitespace) else { return false }
    let domain = parts[1]
    guard let dot = domain.lastIndex(of: "."), dot > domain.startIndex else { return false }
    let tld = domain[domain.index(after: dot)...]
    return tld.count >= 2 && tld.allSatisfy(\.isASCIILetter)
}

/// MOS_RE `/^\d{2,3}[A-Za-z]$/`: 11B, 35F, 155E.
private func isMOS(_ s: String) -> Bool {
    guard let last = s.last, last.isASCIILetter else { return false }
    let head = s.dropLast()
    return (2...3).contains(head.count) && head.allSatisfy(\.isASCIIDigit)
}

/// ZIP_RE `/^\d{5}(-\d{4})?$/`.
private func isZip(_ s: String) -> Bool {
    let parts = s.split(separator: "-", omittingEmptySubsequences: false)
    guard parts.count == 1 || parts.count == 2 else { return false }
    guard parts[0].count == 5, parts[0].allSatisfy(\.isASCIIDigit) else { return false }
    return parts.count == 1 || (parts[1].count == 4 && parts[1].allSatisfy(\.isASCIIDigit))
}

/// A real calendar date "YYYY-MM-DD" between 1900 and 2200.
func isRealDate(_ s: String) -> Bool {
    let parts = s.split(separator: "-", omittingEmptySubsequences: false)
    guard parts.count == 3, parts[0].count == 4, parts[1].count == 2, parts[2].count == 2,
          parts.allSatisfy({ $0.allSatisfy(\.isASCIIDigit) }),
          let y = Int(parts[0]), let m = Int(parts[1]), let d = Int(parts[2]) else { return false }
    if m < 1 || m > 12 || d < 1 || y < 1900 || y > 2200 { return false }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
    return d <= [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]
}

// MARK: - Phones

/// (dial, national). Only a leading '+' declares a country; a bare value is
/// the NANP ("1"). dial "" means a '+' with a country code not on the list,
/// which is refused, never guessed at.
func phoneSplit(_ value: String) -> (dial: String, national: String) {
    let s = jsTrim(value)
    guard s.hasPrefix("+") else { return ("1", s) }
    let rest = s.dropFirst()
    let digits = asciiDigits(rest)
    guard let dial = FieldRules.dialCodes.first(where: { digits.hasPrefix($0) }) else { return ("", s) }
    var taken = 0
    var i = rest.startIndex
    while i < rest.endIndex && taken < dial.count {
        if rest[i].isASCIIDigit { taken += 1 }
        i = rest.index(after: i)
    }
    return (dial, jsTrim(String(rest[i...])))
}

/// The stored spelling. +1 carries no prefix: it is the default and implied.
func phoneJoin(_ dial: String, _ national: String) -> String {
    let n = jsTrim(national)
    if n.isEmpty || dial.isEmpty { return n }
    return dial == "1" ? n : "+\(dial) \(n)"
}

func maxNationalDigits(_ dial: String) -> Int {
    dial == "1" ? FieldRules.nanpDigits : FieldRules.e164Max - dial.count
}

func nationalDigits(_ dial: String, _ national: String) -> String {
    var d = asciiDigits(national)
    if dial == "1" && d.count == 11 && d.first == "1" { d.removeFirst() }
    return d
}

/// The stored spelling of a phone number: "(212) 555-0143" for a whole +1
/// number, "+49 30 12345678" elsewhere; anything else back as typed (trimmed).
func formatPhone(_ value: String) -> String {
    let s = jsTrim(value)
    if s.isEmpty || hasLetters(s) { return s }
    let (dial, national) = phoneSplit(s)
    if dial.isEmpty { return s }
    let d = Array(nationalDigits(dial, national))
    if dial == "1" {
        guard d.count == FieldRules.nanpDigits else { return s }
        return phoneJoin("1", "(\(String(d[0..<3]))) \(String(d[3..<6]))-\(String(d[6...]))")
    }
    return phoneJoin(dial, national.split(whereSeparator: \.isWhitespace).joined(separator: " "))
}

/// "+<dial><digits>" for a tel:/sms: URL, or "" when the value cannot be dialled.
func phoneE164(_ value: String) -> String {
    let s = jsTrim(value)
    if s.isEmpty || hasLetters(s) { return "" }
    let (dial, national) = phoneSplit(s)
    if dial.isEmpty { return "" }
    let d = nationalDigits(dial, national)
    if dial == "1" { return d.count == FieldRules.nanpDigits ? "+1\(d)" : "" }
    return (d.count >= 4 && d.count <= maxNationalDigits(dial)) ? "+\(dial)\(d)" : ""
}

/// The national part as it should read while it is being typed, capped at the
/// country's maximum (the +1 hard stop at ten digits).
func phoneMask(_ national: String, dial: String = "1") -> String {
    if hasLetters(national) { return national }        // DSN or an extension
    if national.drop(while: \.isWhitespace).hasPrefix("+") { return national }
    var d = asciiDigits(national)
    if dial == "1" && d.count == 11 && d.first == "1" { d.removeFirst() }
    let a = Array(d.prefix(max(0, maxNationalDigits(dial))))
    if dial != "1" { return String(a) }
    if a.count <= 3 { return String(a) }
    if a.count <= 6 { return "(\(String(a[0..<3]))) \(String(a[3...]))" }
    return "(\(String(a[0..<3]))) \(String(a[3..<6]))-\(String(a[6...]))"
}

/// The first country with this dial code (0, the United States, if none).
func phoneCountryIndex(_ dial: String) -> Int {
    FieldRules.phoneCountries.firstIndex { $0.dial == dial } ?? 0
}

// MARK: - Weapons

/// The stored weapons_qual as rows, [] for "", or nil when it is the old
/// single-line prose ("M4 qual 14MAR26") or any other non-list. nil is not an
/// error and the value must never be thrown away.
func parseWeapons(_ raw: String) -> [WeaponQual]? {
    let s = jsTrim(raw)
    if s.isEmpty { return [] }
    guard let data = s.data(using: .utf8),
          let parsed = try? JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed]),
          let list = parsed as? [Any] else { return nil }
    var out: [WeaponQual] = []
    for row in list {
        guard let obj = row as? [String: Any] else { return nil }
        out.append(WeaponQual(weapon: jsStringOrEmpty(obj["weapon"]), date: jsStringOrEmpty(obj["date"])))
    }
    return out
}

/// `String(x || '')` for a value out of JSONSerialization.
private func jsStringOrEmpty(_ v: Any?) -> String {
    guard let v, !(v is NSNull) else { return "" }
    if let s = v as? String { return s }
    if let n = v as? NSNumber {
        if CFGetTypeID(n) == CFBooleanGetTypeID() { return n.boolValue ? "true" : "" }
        let d = n.doubleValue
        if d == 0 || d.isNaN { return "" }
        if d == d.rounded(), abs(d) < 1e21 { return String(Int64(d)) }
        return String(d)
    }
    if v is [String: Any] { return "[object Object]" }
    if let a = v as? [Any] { return a.map { ($0 is NSNull) ? "" : jsStringOrEmpty($0) }.joined(separator: ",") }
    return "\(v)"
}

// MARK: - The two entry points

/// The error to show under the field, or nil when the value is allowed
/// (JS returns ''). Every field accepts "": a half-finished profile still saves.
/// Run `normalizeField` first; e.g. "TS-SCI" is only legal once normalised.
func validateField(_ name: String, _ value: String) -> String? {
    let s = jsTrim(value)
    if s.isEmpty { return nil }

    let limit = (name == "address" || name == "profile_notes") ? FieldRules.maxLenLong : FieldRules.maxLen
    if s.utf16.count > limit { return "Too long (max \(limit) characters)." }

    if FieldRules.phoneFields.contains(name) {
        if hasLetters(s) { return nil }                 // a DSN or an extension, as written
        let (dial, national) = phoneSplit(s)
        if dial.isEmpty { return "Pick the country code from the list." }
        let d = nationalDigits(dial, national)
        if dial == "1" { return d.count == FieldRules.nanpDigits ? nil : "A +1 number is exactly 10 digits." }
        let max = maxNationalDigits(dial)
        if d.count < 4 { return "Enter at least 4 digits after +\(dial)." }
        return d.count <= max ? nil : "At most \(max) digits after +\(dial)."
    }
    switch name {
    case "email": return isEmail(s) ? nil : "Enter a valid email address."
    case "dod_id": return (s.count == 10 && s.allSatisfy(\.isASCIIDigit)) ? nil : "DOD ID is exactly 10 digits."
    case "mos": return isMOS(s) ? nil : "MOS is 2-3 numbers then a letter, e.g. 11B or 155E."
    default: break
    }
    if FieldRules.nameFields.contains(name) {
        return matchesNameLike(s, allowComma: true) ? nil : "Letters, spaces, hyphens and apostrophes only."
    }
    switch name {
    case "address_city":
        return matchesNameLike(s, allowComma: false) ? nil : "Letters, spaces, hyphens and apostrophes only."
    case "address_state": return FieldRules.usStates.contains(s) ? nil : "Choose a state from the list."
    case "address_zip": return isZip(s) ? nil : "ZIP is 5 digits, or 5+4 as 12345-6789."
    case "clearance": return FieldRules.clearances.contains(s) ? nil : "Choose a clearance from the list."
    default: break
    }
    if FieldRules.dateFields.contains(name) { return isRealDate(s) ? nil : "Enter a valid date." }
    if name == "weapons_qual", let rows = parseWeapons(s) {   // nil: legacy prose, left alone
        for r in rows {
            if !FieldRules.weapons.contains(r.weapon) { return "\(r.weapon.isEmpty ? "(blank)" : r.weapon) is not on the weapons list." }
            if !r.date.isEmpty && !isRealDate(r.date) { return "Enter a valid qualification date." }
        }
    }
    return nil                                          // address, section, flags, notes: free text
}

/// The one spelling that gets stored. Runs before `validateField`.
func normalizeField(_ name: String, _ value: String) -> String {
    let s = jsTrim(value)
    if s.isEmpty { return "" }
    if FieldRules.phoneFields.contains(name) { return formatPhone(s) }
    switch name {
    case "mos":
        return s.uppercased()
    case "dod_id":
        return isDigitsSpacesHyphens(s) ? asciiDigits(s) : s
    case "address_state":
        return s.utf16.count == 2 ? s.uppercased() : s
    case "address_zip":
        // A pasted ZIP+4 arrives as nine straight digits about as often as not.
        guard isDigitsSpacesHyphens(s) else { return s }
        let d = asciiDigits(s)
        if d.count == 9 { return "\(d.prefix(5))-\(d.dropFirst(5))" }
        return d.count == 5 ? d : s
    case "clearance":
        let key = s.lowercased().split(whereSeparator: \.isWhitespace).joined(separator: " ")
        if let alias = FieldRules.clearanceAliases[key] { return alias }
        return FieldRules.clearances.first { !$0.isEmpty && $0.lowercased() == key } ?? s
    default:
        return s
    }
}

/// Global spellings of the vocabularies, for pickers.
let clearances: [String] = FieldRules.clearances
let weaponOptions: [String] = FieldRules.weapons
let usStates: [String] = FieldRules.usStates
let phoneCountries: [PhoneCountry] = FieldRules.phoneCountries
