import Foundation

// Ported from index.html: RANK_ORDER, rankSort(), endDateSort().
// Spec: tests/test_formation_order.py, tests/test_report_js.py, and
// PlatoonManagerTests/RanksTests.swift, which carries their cases.

enum Ranks {
    /// Senior first. `RANK_ORDER` in index.html, in the same order.
    static let order: [String] = [
        "COL", "LTC", "MAJ", "CPT", "1LT", "2LT",
        "CW5", "CW4", "CW3", "CW2", "WO1",
        "CSM", "SGM", "1SG", "MSG", "SFC", "SSG", "SGT", "CPL",
        "SPC", "PFC", "PV2", "PVT",
    ]

    /// `RANK_ORDER.indexOf(rank)`: -1 for a rank not on the list, which (as in
    /// the web client) sorts it ahead of every known rank.
    static func index(of rank: String) -> Int {
        order.firstIndex(of: rank) ?? -1
    }
}

/// The collation `String.prototype.localeCompare` uses in the browser (ICU,
/// default strength): case and accents only break ties ("a" < "A" < "b"), and
/// punctuation and spaces are not ignored. `compare(_:locale:)` with a fixed
/// locale is Foundation's ICU collation; a fixed `en_US` (rather than the
/// device's locale) keeps the order the same on every phone, matching how the
/// web client is used. `<` would put every capital before every lowercase
/// letter, and `localizedStandardCompare` sorts digits numerically ("Z2" <
/// "Z10"), which localeCompare does not. Pinned by `testLocaleCompareMatchesJS`.
enum Collation {
    static let locale = Locale(identifier: "en_US")

    static func compare(_ a: String, _ b: String) -> Int {
        switch a.compare(b, options: [], range: nil, locale: locale) {
        case .orderedAscending: return -1
        case .orderedSame: return 0
        case .orderedDescending: return 1
        }
    }
}

/// JS-style comparator: negative, zero or positive.
typealias JSComparator<T> = (T, T) -> Int

extension Array {
    /// `Array.prototype.sort(cmp)`, which is stable. Swift's `sorted(by:)` is
    /// not documented as stable, and the report and formation rules rely on
    /// ties keeping roster order.
    func stableSorted(byComparator cmp: JSComparator<Element>) -> [Element] {
        enumerated()
            .sorted { l, r in
                let c = cmp(l.element, r.element)
                return c != 0 ? c < 0 : l.offset < r.offset
            }
            .map(\.element)
    }
}

/// `rankSort(a, b)` as a JS comparator: rank (senior first), then last name.
func rankCompare(_ a: Soldier, _ b: Soldier) -> Int {
    let ai = Ranks.index(of: a.rank), bi = Ranks.index(of: b.rank)
    if ai != bi { return ai - bi }
    return Collation.compare(a.last, b.last)
}

/// `rankSort` as `areInIncreasingOrder`, for `sorted(by:)`.
func rankSort(_ a: Soldier, _ b: Soldier) -> Bool {
    rankCompare(a, b) < 0
}

/// `endDateSort(a, b)`: soonest end first (an open end last), then `rankSort`.
func endDateCompare(_ a: Soldier, _ b: Soldier) -> Int {
    let aDate = a.to.isEmpty ? "9999-12-31" : a.to
    let bDate = b.to.isEmpty ? "9999-12-31" : b.to
    // The JS uses localeCompare here too, but on YYYY-MM-DD strings that is
    // plain code-point order, so a plain comparison is the same thing.
    if aDate != bDate { return aDate < bDate ? -1 : 1 }
    return rankCompare(a, b)
}

func endDateSort(_ a: Soldier, _ b: Soldier) -> Bool {
    endDateCompare(a, b) < 0
}

/// Aliases under the names the views use.
func rankLess(_ a: Soldier, _ b: Soldier) -> Bool { rankSort(a, b) }
func endDateLess(_ a: Soldier, _ b: Soldier) -> Bool { endDateSort(a, b) }
