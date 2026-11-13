import XCTest
@testable import PlatoonManager

/// `ImportParse` against index.html's parseDelimited / guessImportColumn /
/// importTableMode / mapImportRows. Expected values come from running the JS
/// under node on the same inputs.
final class ImportParseTests: XCTestCase {
    private func check(_ text: String, table: [[String]], mapping: [String], spreadsheet: Bool,
                       rows: [[String: String]], file: StaticString = #filePath, line: UInt = #line) {
        let t = ImportParse.parseDelimited(text)
        XCTAssertEqual(t, table, file: file, line: line)
        let m = (t.first ?? []).map(ImportParse.guess)
        XCTAssertEqual(m, mapping, file: file, line: line)
        XCTAssertEqual(ImportParse.isSpreadsheet(t), spreadsheet, file: file, line: line)
        XCTAssertEqual(spreadsheet ? ImportParse.mapRows(t, m) : [], rows, file: file, line: line)
    }

    func testCommaWithQuotedComma() {
        check("Last,First,Rank,Grade\nSmith,John,CPT,O3\n\"Doe, Jr\",Jane,SGT,E5\n",
              table: [["Last", "First", "Rank", "Grade"], ["Smith", "John", "CPT", "O3"], ["Doe, Jr", "Jane", "SGT", "E5"]],
              mapping: ["last", "first", "rank", "rank"], spreadsheet: true,
              rows: [["last": "Smith", "first": "John", "rank": "O3"], ["last": "Doe, Jr", "first": "Jane", "rank": "E5"]])
    }

    func testTabsBOMCRLFAndNameColumn() {
        check("\u{FEFF}Name\tSquad\tCell\r\n\"SMITH JOHN A\"\t1st\t555-1212\r\n\r\nJones, Mary\t2nd\t\n",
              table: [["Name", "Squad", "Cell"], ["SMITH JOHN A", "1st", "555-1212"], ["Jones, Mary", "2nd", ""]],
              mapping: ["name", "unit", "phone"], spreadsheet: true,
              rows: [["last": "SMITH", "first": "JOHN A", "unit": "1st", "phone": "555-1212"],
                     ["last": "Jones", "first": "Mary", "unit": "2nd"]])
    }

    func testEscapedQuotesAndNotASpreadsheet() {
        check("a,\"b \"\"q\"\" c\",d\n", table: [["a", "b \"q\" c", "d"]], mapping: ["", "", ""],
              spreadsheet: false, rows: [])
    }

    func testOneLinePaste() {
        XCTAssertEqual(ImportParse.line("CPT Smith, John"), ["rank": "CPT", "last": "Smith", "first": "John"])
        XCTAssertEqual(ImportParse.line("SGT Jones Mary Ann"), ["rank": "SGT", "last": "Jones", "first": "Mary Ann"])
        XCTAssertNil(ImportParse.line("   "))
        XCTAssertNil(ImportParse.line("Smith"))
    }
}
