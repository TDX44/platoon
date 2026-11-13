import XCTest
@testable import PlatoonManager

/// unitById/unitBySlug/unitChildren/unitSubtree/unitHeadcount (tests/test_unit_tree_js.py),
/// plus unitTodayCounts() and unitTreeOrder().
final class UnitTreeTests: XCTestCase {
    let tree = UnitTree(units: [
        UnitNode(id: 1, parentId: nil, kind: "company", name: "HHC", slug: "hhc", count: 1),
        UnitNode(id: 2, parentId: 1, kind: "platoon", name: "2nd Platoon", slug: "2ndplatoon", count: 20),
        UnitNode(id: 3, parentId: 2, kind: "squad", name: "Alpha", slug: "alpha", count: 4),
        UnitNode(id: 4, parentId: 1, kind: "platoon", name: "1st Platoon", slug: "1stplatoon", count: 30),
    ])

    func testTheJSCases() {
        XCTAssertEqual(tree.subtree(of: 2).sorted(), [2, 3])
        XCTAssertEqual(tree.headcount(of: 1), 55)
        XCTAssertEqual(tree.unit(slug: "1stplatoon")?.id, 4)
        // Name-sorted, not id-sorted: "1st Platoon" (4) precedes "2nd Platoon" (2).
        XCTAssertEqual(tree.children(of: 1).map(\.id), [4, 2])
        XCTAssertNil(tree.unit(slug: "nope"))
    }

    func testLookupsAndOrder() {
        XCTAssertEqual(tree.unit(id: 3)?.name, "Alpha")
        XCTAssertNil(tree.unit(id: 99))
        XCTAssertEqual(tree.subtree(of: 1), [1, 4, 2, 3])        // breadth-first, children by name
        XCTAssertEqual(tree.subtree(of: 99), [99])               // the id itself, as in the JS
        XCTAssertEqual(tree.headcount(of: 99), 0)
        XCTAssertEqual(tree.treeOrder(root: 1), [1: 0, 4: 1, 2: 2, 3: 3])   // depth-first
    }

    func testTodayCountsRollUpOrNil() {
        XCTAssertNil(tree.todayCounts(of: 1), "no counts in the listing")
        let counted = UnitTree(units: [
            UnitNode(id: 1, name: "HHC", present: 1, unaccounted: 0),
            UnitNode(id: 2, parentId: 1, name: "2nd", present: 10, unaccounted: 3),
            UnitNode(id: 3, parentId: 2, name: "A", present: 2, unaccounted: nil),
        ])
        XCTAssertEqual(counted.todayCounts(of: 1), UnitTodayCounts(present: 13, unaccounted: 3))
        XCTAssertEqual(counted.todayCounts(of: 2), UnitTodayCounts(present: 12, unaccounted: 3))
        XCTAssertNil(counted.todayCounts(of: 99))
        let partial = UnitTree(units: [UnitNode(id: 1, name: "HHC", present: 1), UnitNode(id: 2, parentId: 1, name: "x")])
        XCTAssertNil(partial.todayCounts(of: 1))
        XCTAssertEqual(partial.todayCounts(of: 2), nil)
    }
}
