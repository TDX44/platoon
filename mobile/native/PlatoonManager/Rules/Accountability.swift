import Foundation

// Ported from index.html: isPresentToday(), isUnaccounted(), isSameDayState(),
// STATUS_LABELS / statusLabel(), REASON_FIELDS, FORMATION_REASONS,
// SAME_DAY_STATUSES, absenceFormProblem(), lastFirst(), formatDateShort().
// `today` is always the organization's duty day ("YYYY-MM-DD"), passed in:
// nothing here reads the clock or a time zone.

/// Marked present today.
func isPresentToday(_ p: Soldier, today: String) -> Bool {
    p.status == "present" && p.presentDate == today
}

/// Present status but not marked present today.
func isUnaccounted(_ p: Soldier, today: String) -> Bool {
    p.status == "present" && p.presentDate != today
}

/// Late and excused are same-day by nature: a today-to-today window says
/// nothing on those rows, so the reason stands alone.
func isSameDayState(_ p: Soldier) -> Bool {
    (p.status == "late" || p.status == "excused") && !p.from.isEmpty && p.from == p.to
}

enum Statuses {
    /// `STATUS_LABELS`.
    static let labels: [String: String] = [
        "present": "Present", "leave": "Leave", "pass": "Pass", "tdy": "TDY", "other": "Other",
        "ftr": "FTR", "late": "Late", "excused": "Excused",
    ]

    /// `FORMATION_REASONS`: ordered by what is actually said at formation,
    /// late and excused first, FTR last.
    static let formationReasons: [String] = ["late", "excused", "leave", "pass", "tdy", "other", "ftr"]

    /// `SAME_DAY_STATUSES`: the dates are filled in (today to today) the moment
    /// one is picked.
    static let sameDay: [String] = ["late", "excused"]
}

func isKnownStatus(_ status: String) -> Bool {
    Statuses.labels[status] != nil
}

/// A known status's label; anything else is shown as it came from the server.
/// (The web client HTML-escapes the unknown case; SwiftUI `Text` needs no escaping.)
func statusLabel(_ status: String) -> String {
    Statuses.labels[status] ?? status
}

/// The wording of the free-text reason box for one status.
struct ReasonField: Sendable, Equatable {
    let label: String
    let placeholder: String
    let options: [String]
}

enum ReasonFields {
    /// `REASON_FIELDS`, wording copied exactly. Only these statuses get a
    /// reason box (late deliberately has none).
    static let byStatus: [String: ReasonField] = [
        "other": ReasonField(
            label: "Category",
            placeholder: "e.g. Quarters, Staff Duty Recovery",
            options: ["Quarters", "Staff Duty Recovery", "CQ Recovery", "Appointment", "Detail"]),
        "excused": ReasonField(
            label: "Excused for",
            placeholder: "e.g. Sick call, appointment",
            options: ["Sick call", "Medical appointment", "Dental appointment",
                      "Family emergency", "Religious service", "Excused by CoC"]),
    ]
}

func reasonField(for status: String) -> ReasonField? {
    ReasonFields.byStatus[status]
}

/// What stops the status sheet from saving, or nil when nothing does (the JS
/// returns ''). Leave and Pass are always bounded.
func absenceFormProblem(status: String, from: String, to: String) -> String? {
    if status.isEmpty { return "Pick a status first." }
    if (status == "leave" || status == "pass") && to.isEmpty {
        return "\(status == "leave" ? "Leave" : "A pass") needs an end date."
    }
    return nil
}

/// "Last, First", or just "Last" when no first name is on file.
func lastFirst(_ p: Soldier) -> String {
    p.first.isEmpty ? p.last : "\(p.last), \(p.first)"
}

enum Months {
    static let short: [String] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    static let upper: [String] = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
}

/// The (year, month, day) of a "YYYY-MM-DD" string as `new Date(s + 'T00:00:00')`
/// reads it: a day past the end of the month rolls into the next one
/// ("2026-02-30" is 2 March), exactly as the browser does. nil where the
/// browser gets an Invalid Date.
func calendarDay(_ dateStr: String) -> (year: Int, month: Int, day: Int)? {
    let parts = dateStr.split(separator: "-", omittingEmptySubsequences: false)
    guard parts.count == 3, parts[0].count == 4, parts[1].count == 2, parts[2].count == 2,
          parts.allSatisfy({ $0.allSatisfy(\.isASCIIDigit) }),
          let y = Int(parts[0]), let m = Int(parts[1]), let d = Int(parts[2]),
          (1...12).contains(m), (1...31).contains(d) else { return nil }
    var cal = Calendar(identifier: .gregorian)
    cal.timeZone = TimeZone(identifier: "UTC")!
    guard let date = cal.date(from: DateComponents(year: y, month: m, day: d)) else { return nil }
    let c = cal.dateComponents([.year, .month, .day], from: date)
    guard let yy = c.year, let mm = c.month, let dd = c.day else { return nil }
    return (yy, mm, dd)
}

/// "17MAR" from "2026-03-17"; "" for "". An unparseable value is "" here
/// (the browser would print "NaNundefined").
func formatDateShort(_ dateStr: String) -> String {
    guard !dateStr.isEmpty, let d = calendarDay(dateStr) else { return "" }
    return "\(d.day)\(Months.upper[d.month - 1])"
}

extension Character {
    /// JS `\d`: ASCII 0-9 only (Swift's `isNumber` also takes other scripts).
    var isASCIIDigit: Bool { isASCII && ("0"..."9").contains(self) }
    /// JS `[A-Za-z]`.
    var isASCIILetter: Bool { isASCII && (("a"..."z").contains(self) || ("A"..."Z").contains(self)) }
}

/// Global spellings of the tables above, under the web client's names.
let reasonFields: [String: ReasonField] = ReasonFields.byStatus
let formationReasons: [String] = Statuses.formationReasons
let sameDayStatuses: [String] = Statuses.sameDay
