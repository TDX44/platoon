import XCTest
@testable import PlatoonManager

/// buildReport() and its helpers. The assertions are tests/test_report_js.py's;
/// `JSReports` holds whole reports printed by the JS buildReport() under node
/// for the same rosters, so the Swift text is pinned byte for byte.
final class ReportTests: XCTestCase {
    static let today = "2026-03-17"

    static func soldier(_ id: Int, _ rank: String, _ last: String, _ status: String = "present", unit: Int = 2,
                        present: Bool = true, first: String = "A", notes: String = "", from: String = "",
                        to: String = "", noFormation: Bool = false) -> Soldier {
        Soldier(id: id, rank: rank, last: last, first: first, unitId: unit, status: status, notes: notes,
                from: from, to: to, presentDate: (status == "present" && present) ? today : "", noFormation: noFormation)
    }

    static let roster: [Soldier] = [
        soldier(1, "SFC", "Boone", unit: 2),
        soldier(2, "SGT", "Crane", unit: 3),
        soldier(3, "SPC", "Ash", "leave", unit: 2, from: "2026-03-10", to: "2026-03-20"),
        soldier(4, "PFC", "Dale", "pass", unit: 3, from: "2026-03-17", to: "2026-03-18"),
        soldier(5, "SPC", "Eve", "tdy", unit: 3, notes: "Air Assault - Fort Example", from: "2026-03-01", to: "2026-03-30"),
        soldier(6, "PV2", "Fox", "late", unit: 2, notes: "Traffic", from: today, to: today),
        soldier(7, "SPC", "Gray", unit: 3, present: false),     // unaccounted
        soldier(8, "1SG", "Hale", unit: 1),                      // directly in the company: HQ
        soldier(9, "SPC", "Ivy", "ftr", unit: 2),
    ]
    // Company 1 with 1st PLT (2) and 2nd PLT (3, which has squad 4).
    static let groups = [ReportGroup(name: "1st PLT", ids: [2]), ReportGroup(name: "2nd PLT", ids: [3, 4])]

    let detailed = ReportOptions(format: .detailed, splitLeavePass: false)
    let split = ReportOptions(format: .detailed, splitLeavePass: true)
    let strength = ReportOptions(format: .strength, splitLeavePass: false)

    func build(_ opts: ReportOptions, people: [Soldier] = roster, groups: [ReportGroup] = []) -> String {
        buildReport(people, opts,
                    ReportContext(unitName: "HHC", dateLabel: "17 Mar 2026", today: Self.today, groups: groups))
    }

    func testFlatDetailed() {
        let flat = build(detailed)
        let lines = flat.components(separatedBy: "\n")
        XCTAssertEqual(Array(lines[..<3]), ["HHC", "17 Mar 2026", "9 Assigned"])
        let pdy = lines.firstIndex(of: "PDY 3")!
        XCTAssertEqual(lines[(pdy + 1)...(pdy + 3)].map { String($0.split(separator: " ")[0]) }, ["1SG", "SFC", "SGT"])
        let un = lines.firstIndex(of: "UNACCOUNTED 1")!
        XCTAssertTrue(lines[un...].contains { $0.contains("Gray") })
        XCTAssertTrue(lines.contains("Leave & Pass 2") && lines.contains("TDY 1") && lines.contains("FTR 1"))
        XCTAssertTrue(lines.contains { $0.contains("Air Assault | Fort Example | 1MAR-30MAR") })
        let late = lines.first { $0.contains("Fox") }!
        XCTAssertTrue(late.hasSuffix("Traffic") && !late.contains("MAR"), late)
        XCTAssertFalse(flat.contains("BY UNIT"), "a leaf unit got a breakdown")
        XCTAssertEqual(flat, JSReports.flat)
    }

    func testTreeDetailedBreakdown() {
        let text = build(detailed, groups: Self.groups)
        let tree = text.components(separatedBy: "\n")
        let i = tree.firstIndex(of: "BY UNIT")!
        XCTAssertEqual(tree[i + 1], "1st PLT: 4 Assigned, 1 PDY, 1 Leave & Pass, 1 Late, 1 FTR")
        XCTAssertEqual(tree[i + 2], "2nd PLT: 4 Assigned, 1 PDY, 1 TDY, 1 Leave & Pass, 1 Unaccounted")
        XCTAssertEqual(tree[i + 3], "HQ: 1 Assigned, 1 PDY")
        XCTAssertTrue(tree[i + 4].hasPrefix("TOTAL: 9 Assigned, 3 PDY"))
        XCTAssertTrue(tree[i + 4].hasSuffix("1 Unaccounted"))
        XCTAssertEqual(text, JSReports.tree)
    }

    func testSplitLeavePass() {
        let text = build(split)
        let lines = text.components(separatedBy: "\n")
        XCTAssertTrue(lines.contains("Leave 1") && lines.contains("Pass 1") && !lines.contains("Leave & Pass 2"))
        XCTAssertEqual(text, JSReports.split)
        XCTAssertEqual(build(split, groups: Self.groups), JSReports.splitTree)
    }

    func testStrength() {
        XCTAssertEqual(build(strength).components(separatedBy: "\n"), [
            "HHC", "17 Mar 2026", "",
            "9 Assigned, 3 PDY, 1 TDY, 2 Leave & Pass, 1 Late, 1 FTR, 1 Unaccounted"])
        let st = build(strength, groups: Self.groups).components(separatedBy: "\n")
        XCTAssertTrue(st[3].hasPrefix("1st PLT: "))
        XCTAssertEqual(st[5], "HQ: 1 Assigned, 1 PDY")
        XCTAssertTrue(st[6].hasPrefix("TOTAL: 9"))
        XCTAssertEqual(build(strength), JSReports.strengthFlat)
        XCTAssertEqual(build(strength, groups: Self.groups), JSReports.strengthTree)
    }

    // PDY names are always listed, however big the unit.
    func testBigUnitListsEveryPDY() {
        let big = (0..<20).map { Self.soldier(100 + $0, "SPC", String(format: "Z%02d", $0)) }
        let text = build(detailed, people: big)
        XCTAssertTrue(text.contains("PDY 20") && text.contains("Z00") && text.contains("Z19"))
        XCTAssertEqual(text, JSReports.big)
    }

    // An unknown status is counted as Other so the categories still add up.
    func testUnknownStatusIsOther() {
        let text = build(detailed, people: [Self.soldier(1, "SPC", "Odd", "quarters")])
        XCTAssertTrue(text.components(separatedBy: "\n").contains("OTHER 1"))
        XCTAssertEqual(text, JSReports.odd)
    }

    // Never at formation: PDY on Detailed, counted apart on Strength.
    func testNoFormation() {
        let nf = [Self.soldier(1, "SFC", "Boone"), Self.soldier(2, "SPC", "Clerk", noFormation: true),
                  Self.soldier(3, "SPC", "Late", present: false, noFormation: true)]
        let d = build(detailed, people: nf), s = build(strength, people: nf)
        XCTAssertTrue(d.components(separatedBy: "\n").contains("PDY 2") && !d.contains("Not in formation"))
        XCTAssertEqual(s.components(separatedBy: "\n").last, "3 Assigned, 1 PDY, 1 Not in formation, 1 Unaccounted")
        XCTAssertEqual(d, JSReports.nfDetailed)
        XCTAssertEqual(s, JSReports.nfStrength)
    }

    /// Odd shapes (unknown rank, no first name, open-ended windows, TDY notes
    /// with no " - ", padded excused reason, a rolled-over date), against the JS.
    func testOddShapesMatchJS() {
        let people = [
            Self.soldier(1, "XYZ", "Nobody", first: ""),
            Self.soldier(2, "SPC", "Other", "other", notes: "Quarters", from: "2026-03-16", to: ""),
            Self.soldier(3, "SGT", "Open", "leave", from: "", to: "2026-03-20"),
            Self.soldier(4, "CPT", "School", "tdy", notes: "BOLC", from: "2026-02-30", to: "2026-12-01"),
            Self.soldier(5, "SPC", "Exc", "excused", notes: "  Sick call  ", from: "2026-03-17", to: "2026-03-17"),
            Self.soldier(6, "SPC", "Exc2", "excused"),
            Self.soldier(7, "SPC", "Ftr", "ftr", from: "2026-03-15", to: ""),
            Self.soldier(8, "SPC", "Pass", "pass", from: "2026-03-17", to: "2026-03-19"),
            Self.soldier(9, "SFC", "Pass", "pass", from: "2026-03-17", to: "2026-03-18"),
        ]
        XCTAssertEqual(build(detailed, people: people), JSReports.odd2Detailed)
        XCTAssertEqual(build(split, people: people), JSReports.odd2Split)
    }

    func testEmptyRoster() {
        XCTAssertEqual(build(detailed, people: []), "HHC\n17 Mar 2026\n0 Assigned\n\nPDY 0")
        XCTAssertEqual(build(strength, people: []), "HHC\n17 Mar 2026\n\n0 Assigned, 0 PDY")
    }

    func testReportNamePadsRankToFive() {
        XCTAssertEqual(reportName(Soldier(id: 1, rank: "SPC", last: "Ash", first: "B")), "SPC   Ash, B")
        XCTAssertEqual(reportName(Soldier(id: 1, rank: "", last: "Ash")), "      Ash")
        XCTAssertEqual(reportName(Soldier(id: 1, rank: "LONGRANK", last: "Ash")), "LONGRANK Ash")
    }

    func testContextFromTree() {
        let tree = UnitTree(units: [UnitNode(id: 1, name: "HHC"), UnitNode(id: 3, parentId: 1, name: "2nd PLT"),
                                    UnitNode(id: 4, parentId: 3, name: "Sq"), UnitNode(id: 2, parentId: 1, name: "1st PLT")])
        let ctx = ReportContext(unitId: 1, unitName: "HHC", dateLabel: "x", today: Self.today, tree: tree)
        XCTAssertEqual(ctx.groups, Self.groups)
    }

    func testFormatReportDateUsesTheGivenZone() {
        // 2026-11-14 03:00 UTC is still 13 Nov in Chicago.
        let date = Date(timeIntervalSince1970: 1_794_625_200)
        XCTAssertEqual(formatReportDate(date, timeZone: TimeZone(identifier: "UTC")!), "14 Nov 2026")
        XCTAssertEqual(formatReportDate(date, timeZone: TimeZone(identifier: "America/Chicago")!), "13 Nov 2026")
    }
}

/// Printed by index.html's buildReport() under node (the harness of
/// tests/test_report_js.py) for the rosters above. Regenerate them from the JS,
/// never by hand, if the report text changes on purpose.
enum JSReports {
    static let flat = """
    HHC
    17 Mar 2026
    9 Assigned

    PDY 3
    1SG   Hale, A
    SFC   Boone, A
    SGT   Crane, A

    Leave & Pass 2
    PFC   Dale, A  17MAR-18MAR
    SPC   Ash, A  10MAR-20MAR

    TDY 1
    SPC   Eve, A  Air Assault | Fort Example | 1MAR-30MAR

    LATE 1
    PV2   Fox, A  Traffic

    FTR 1
    SPC   Ivy, A

    UNACCOUNTED 1
    SPC   Gray, A
    """

    static let tree = """
    HHC
    17 Mar 2026
    9 Assigned

    BY UNIT
    1st PLT: 4 Assigned, 1 PDY, 1 Leave & Pass, 1 Late, 1 FTR
    2nd PLT: 4 Assigned, 1 PDY, 1 TDY, 1 Leave & Pass, 1 Unaccounted
    HQ: 1 Assigned, 1 PDY
    TOTAL: 9 Assigned, 3 PDY, 1 TDY, 2 Leave & Pass, 1 Late, 1 FTR, 1 Unaccounted

    PDY 3
    1SG   Hale, A
    SFC   Boone, A
    SGT   Crane, A

    Leave & Pass 2
    PFC   Dale, A  17MAR-18MAR
    SPC   Ash, A  10MAR-20MAR

    TDY 1
    SPC   Eve, A  Air Assault | Fort Example | 1MAR-30MAR

    LATE 1
    PV2   Fox, A  Traffic

    FTR 1
    SPC   Ivy, A

    UNACCOUNTED 1
    SPC   Gray, A
    """

    static let split = """
    HHC
    17 Mar 2026
    9 Assigned

    PDY 3
    1SG   Hale, A
    SFC   Boone, A
    SGT   Crane, A

    Leave 1
    SPC   Ash, A  10MAR-20MAR

    Pass 1
    PFC   Dale, A  17MAR-18MAR

    TDY 1
    SPC   Eve, A  Air Assault | Fort Example | 1MAR-30MAR

    LATE 1
    PV2   Fox, A  Traffic

    FTR 1
    SPC   Ivy, A

    UNACCOUNTED 1
    SPC   Gray, A
    """

    static let splitTree = """
    HHC
    17 Mar 2026
    9 Assigned

    BY UNIT
    1st PLT: 4 Assigned, 1 PDY, 1 Leave, 1 Late, 1 FTR
    2nd PLT: 4 Assigned, 1 PDY, 1 TDY, 1 Pass, 1 Unaccounted
    HQ: 1 Assigned, 1 PDY
    TOTAL: 9 Assigned, 3 PDY, 1 TDY, 1 Leave, 1 Pass, 1 Late, 1 FTR, 1 Unaccounted

    PDY 3
    1SG   Hale, A
    SFC   Boone, A
    SGT   Crane, A

    Leave 1
    SPC   Ash, A  10MAR-20MAR

    Pass 1
    PFC   Dale, A  17MAR-18MAR

    TDY 1
    SPC   Eve, A  Air Assault | Fort Example | 1MAR-30MAR

    LATE 1
    PV2   Fox, A  Traffic

    FTR 1
    SPC   Ivy, A

    UNACCOUNTED 1
    SPC   Gray, A
    """

    static let strengthFlat = """
    HHC
    17 Mar 2026

    9 Assigned, 3 PDY, 1 TDY, 2 Leave & Pass, 1 Late, 1 FTR, 1 Unaccounted
    """

    static let strengthTree = """
    HHC
    17 Mar 2026

    1st PLT: 4 Assigned, 1 PDY, 1 Leave & Pass, 1 Late, 1 FTR
    2nd PLT: 4 Assigned, 1 PDY, 1 TDY, 1 Leave & Pass, 1 Unaccounted
    HQ: 1 Assigned, 1 PDY
    TOTAL: 9 Assigned, 3 PDY, 1 TDY, 2 Leave & Pass, 1 Late, 1 FTR, 1 Unaccounted
    """

    static let big = """
    HHC
    17 Mar 2026
    20 Assigned

    PDY 20
    SPC   Z00, A
    SPC   Z01, A
    SPC   Z02, A
    SPC   Z03, A
    SPC   Z04, A
    SPC   Z05, A
    SPC   Z06, A
    SPC   Z07, A
    SPC   Z08, A
    SPC   Z09, A
    SPC   Z10, A
    SPC   Z11, A
    SPC   Z12, A
    SPC   Z13, A
    SPC   Z14, A
    SPC   Z15, A
    SPC   Z16, A
    SPC   Z17, A
    SPC   Z18, A
    SPC   Z19, A
    """

    static let odd = """
    HHC
    17 Mar 2026
    1 Assigned

    PDY 0

    OTHER 1
    SPC   Odd, A  Other
    """

    static let nfDetailed = """
    HHC
    17 Mar 2026
    3 Assigned

    PDY 2
    SFC   Boone, A
    SPC   Clerk, A

    UNACCOUNTED 1
    SPC   Late, A
    """

    static let nfStrength = """
    HHC
    17 Mar 2026

    3 Assigned, 1 PDY, 1 Not in formation, 1 Unaccounted
    """

    static let odd2Detailed = """
    HHC
    17 Mar 2026
    9 Assigned

    PDY 1
    XYZ   Nobody

    Leave & Pass 3
    SFC   Pass, A  17MAR-18MAR
    SPC   Pass, A  17MAR-19MAR
    SGT   Open, A  -20MAR

    TDY 1
    CPT   School, A  BOLC | 2MAR-1DEC

    EXCUSED 2
    SPC   Exc, A  Sick call
    SPC   Exc2, A

    OTHER 1
    SPC   Other, A  Other: Quarters | 16MAR-

    FTR 1
    SPC   Ftr, A  15MAR-
    """

    static let odd2Split = """
    HHC
    17 Mar 2026
    9 Assigned

    PDY 1
    XYZ   Nobody

    Leave 1
    SGT   Open, A  -20MAR

    Pass 2
    SFC   Pass, A  17MAR-18MAR
    SPC   Pass, A  17MAR-19MAR

    TDY 1
    CPT   School, A  BOLC | 2MAR-1DEC

    EXCUSED 2
    SPC   Exc, A  Sick call
    SPC   Exc2, A

    OTHER 1
    SPC   Other, A  Other: Quarters | 16MAR-

    FTR 1
    SPC   Ftr, A  15MAR-
    """
}
