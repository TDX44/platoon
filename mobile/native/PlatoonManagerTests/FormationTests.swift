import XCTest
@testable import PlatoonManager

/// formationQueue(), formationRemaining(), formationTally().
/// Cases are tests/test_formation_order.py's, numbered the same.
final class FormationTests: XCTestCase {
    let today = "2026-03-17"

    private func soldier(_ id: Int, _ rank: String = "SPC", _ last: String = "Zulu", status: String = "present",
                         presentDate: String = "", noFormation: Bool = false, unitId: Int = 0) -> Soldier {
        Soldier(id: id, rank: rank, last: last, first: "A", unitId: unitId, status: status,
                presentDate: presentDate, noFormation: noFormation)
    }

    private func run(_ people: [Soldier], groups: [Int: Int]? = nil) -> (queue: [Int], known: [Int]) {
        let out = groups.map { g in formationQueue(people, today: today, groupOf: { g[$0.id]! }) }
            ?? formationQueue(people, today: today)
        return (out.queue.map(\.id), out.known.map(\.id))
    }

    // 1. Everyone unaccounted gets asked, senior first, then alphabetically.
    func test1SeniorFirstThenAlphabetical() {
        let out = run([soldier(1, "SPC", "Alvarez"), soldier(2, "SFC", "Boone"),
                       soldier(3, "SGT", "Crane"), soldier(4, "SGT", "Ash")])
        XCTAssertEqual(out.queue, [2, 4, 3, 1])
        XCTAssertEqual(out.known, [])
    }

    // 2. Someone already marked present today is not asked again; yesterday's mark is stale.
    func test2PresentTodayNotAskedAgain() {
        var out = run([soldier(1), soldier(2, presentDate: today)])
        XCTAssertEqual(out.queue, [1])
        XCTAssertEqual(out.known, [])
        out = run([soldier(1, presentDate: "2026-03-16")])
        XCTAssertEqual(out.queue, [1])
    }

    // 3. Away on a current absence: not asked, handed back as known.
    func test3AwayIsKnownNotAsked() {
        let out = run([soldier(1), soldier(2, status: "tdy"), soldier(3, status: "leave")])
        XCTAssertEqual(out.queue, [1])
        XCTAssertEqual(out.known, [2, 3])
    }

    // 5. Every away status lands in known, never in the queue.
    func test5EveryAwayStatusIsKnown() {
        let away = ["tdy", "leave", "pass", "other", "ftr"].enumerated().map { soldier($0.offset + 2, status: $0.element) }
        let out = run([soldier(1)] + away)
        XCTAssertEqual(out.queue, [1])
        XCTAssertEqual(out.known, [2, 3, 4, 5, 6])
    }

    // 6. A fully accounted-for platoon produces an empty queue.
    func test6FullyAccountedIsEmpty() {
        XCTAssertEqual(run([soldier(1, presentDate: today), soldier(2, status: "tdy")]).queue, [])
    }

    // 6b. Never in formation: neither asked nor listed, whatever their status.
    func test6bNoFormationIsSkipped() {
        let out = run([soldier(1), soldier(2, noFormation: true), soldier(3, status: "leave", noFormation: true)])
        XCTAssertEqual(out.queue, [1])
        XCTAssertEqual(out.known, [])
    }

    // 7. An empty roster is not a crash.
    func test7EmptyRoster() {
        let out = run([])
        XCTAssertEqual(out.queue, [])
        XCTAssertEqual(out.known, [])
    }

    // 7b. Sub-units walked in tree order (the group number), senior first in each.
    func test7bGroupsInTreeOrder() {
        let roster = [soldier(1, "SSG", "Able"), soldier(2, "PVT", "Baker"), soldier(3, "1SG", "Cole"),
                      soldier(4, "SFC", "Dunn"), soldier(5, "SPC", "Eads"), soldier(6, "SGT", "Fry", status: "tdy")]
        let out = run(roster, groups: [1: 2, 2: 1, 3: 0, 4: 1, 5: 2, 6: 1])
        XCTAssertEqual(out.queue, [3, 4, 2, 1, 5])
        XCTAssertEqual(out.known, [6])
    }

    /// 7b again, the way startFormation() builds groupOf: from the unit tree.
    func testGroupOfFromTreeOrder() {
        let tree = UnitTree(units: [UnitNode(id: 1, name: "HHC"), UnitNode(id: 2, parentId: 1, name: "1st PLT"),
                                    UnitNode(id: 3, parentId: 1, name: "2nd PLT")])
        let roster = [soldier(1, "SSG", "Able", unitId: 3), soldier(2, "PVT", "Baker", unitId: 2),
                      soldier(3, "1SG", "Cole", unitId: 1), soldier(4, "SFC", "Dunn", unitId: 2),
                      soldier(5, "SPC", "Eads", unitId: 3), soldier(7, "COL", "Elsewhere", unitId: 99)]
        let out = formationQueue(roster, today: today, groupOf: formationGroupOf(order: tree.treeOrder(root: 1)))
        XCTAssertEqual(out.queue.map(\.id), [3, 4, 2, 1, 5, 7])   // a unit not in the tree goes last
    }

    // 9. "Everyone else present": the rest of the queue still unaccounted, never anyone away.
    func test9RemainingIsRestOfQueueStillUnaccounted() {
        let people = [soldier(1), soldier(2), soldier(3, presentDate: today), soldier(4, status: "late"), soldier(5)]
        XCTAssertEqual(formationRemaining(ids: [1, 2, 3, 4, 5], from: 1, people: people, today: today).map(\.id), [2, 5])
        XCTAssertEqual(formationRemaining(ids: [1, 2, 99], from: 0, people: people, today: today).map(\.id), [1, 2])
        XCTAssertEqual(formationRemaining(ids: [1, 2], from: 2, people: people, today: today).map(\.id), [])
        XCTAssertEqual(formationRemaining(ids: [1, 2], from: 9, people: people, today: today).map(\.id), [])
    }

    func testTallyLeavesOutNoFormation() {
        let people = [soldier(1, presentDate: today), soldier(2), soldier(3, status: "tdy"),
                      soldier(4, presentDate: today, noFormation: true), soldier(5, status: "leave", noFormation: true)]
        XCTAssertEqual(formationTally(people, today: today), FormationTally(total: 3, present: 1, away: 1, left: 1))
    }
}
