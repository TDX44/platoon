import XCTest
@testable import PlatoonManager

/// RANK_ORDER, rankSort(), endDateSort(), and the localeCompare stand-in.
final class RanksTests: XCTestCase {
    func testRankOrderIsTheWebList() {
        XCTAssertEqual(Ranks.order.count, 23)
        XCTAssertEqual(Ranks.order.first, "COL")
        XCTAssertEqual(Ranks.order.last, "PVT")
        XCTAssertEqual(Ranks.index(of: "1SG"), 13)
        XCTAssertEqual(Ranks.index(of: "XYZ"), -1)
    }

    /// The expected order is `[...words].sort((a, b) => a.localeCompare(b))`
    /// as node (ICU) printed it.
    func testLocaleCompareMatchesJS() {
        let words = ["b", "A", "a", "B", "Z00", "z1", "Z10", "Z2", "É", "e", "E", "O'Brien", "Obrien", "OBrien",
                     "Smith-Jones", "Smith Jones", "Smithjones", "Alpha", "alpha", "1st Platoon", "2nd Platoon",
                     "10th Platoon", "HQ", "Hq", "St. John", "Stjohn", "Ash", "Alvarez", "Boone", "Crane",
                     "Mc Coy", "McCoy", "", "Ångström", "Zulu", "zebra"]
        let expected = ["", "10th Platoon", "1st Platoon", "2nd Platoon", "a", "A", "alpha", "Alpha", "Alvarez",
                        "Ångström", "Ash", "b", "B", "Boone", "Crane", "e", "E", "É", "Hq", "HQ", "Mc Coy", "McCoy",
                        "O'Brien", "Obrien", "OBrien", "Smith Jones", "Smith-Jones", "Smithjones", "St. John",
                        "Stjohn", "Z00", "z1", "Z10", "Z2", "zebra", "Zulu"]
        XCTAssertEqual(words.stableSorted(byComparator: Collation.compare), expected)
    }

    func testRankSortSeniorFirstThenLastName() {
        let people = [Soldier(id: 1, rank: "SPC", last: "Alvarez"), Soldier(id: 2, rank: "SFC", last: "Boone"),
                      Soldier(id: 3, rank: "SGT", last: "Crane"), Soldier(id: 4, rank: "SGT", last: "Ash"),
                      Soldier(id: 5, rank: "", last: "Nobody")]
        // An unknown rank is indexOf -1, so it sorts ahead of COL, as in the web client.
        XCTAssertEqual(people.sorted(by: rankSort).map(\.id), [5, 2, 4, 3, 1])
    }

    func testEndDateSortOpenEndLastThenRank() {
        let people = [Soldier(id: 1, rank: "SPC", last: "A", to: ""),
                      Soldier(id: 2, rank: "SPC", last: "B", to: "2026-03-20"),
                      Soldier(id: 3, rank: "SFC", last: "C", to: "2026-03-20"),
                      Soldier(id: 4, rank: "PVT", last: "D", to: "2026-03-18")]
        XCTAssertEqual(people.sorted(by: endDateSort).map(\.id), [4, 3, 2, 1])
    }

    func testStableSortKeepsTiesInRosterOrder() {
        let people = (1...30).map { Soldier(id: $0, rank: "SPC", last: "Same") }
        XCTAssertEqual(people.stableSorted(byComparator: rankCompare).map(\.id), Array(1...30))
    }
}
