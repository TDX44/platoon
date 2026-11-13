import XCTest
@testable import PlatoonManager

/// isUnaccounted(), isPresentToday(), isSameDayState(), statusLabel(),
/// REASON_FIELDS, absenceFormProblem(), lastFirst(), formatDateShort().
final class AccountabilityTests: XCTestCase {
    let today = "2026-03-17"

    func testUnaccountedIsPresentStatusNotMarkedToday() {
        XCTAssertTrue(isUnaccounted(Soldier(id: 1), today: today))
        XCTAssertTrue(isUnaccounted(Soldier(id: 1, presentDate: "2026-03-16"), today: today))   // yesterday's mark is stale
        XCTAssertFalse(isUnaccounted(Soldier(id: 1, presentDate: today), today: today))
        XCTAssertFalse(isUnaccounted(Soldier(id: 1, status: "tdy"), today: today))
        XCTAssertTrue(isPresentToday(Soldier(id: 1, presentDate: today), today: today))
        XCTAssertFalse(isPresentToday(Soldier(id: 1, status: "leave", presentDate: today), today: today))
    }

    func testSameDayState() {
        XCTAssertTrue(isSameDayState(Soldier(id: 1, status: "late", from: today, to: today)))
        XCTAssertTrue(isSameDayState(Soldier(id: 1, status: "excused", from: today, to: today)))
        XCTAssertFalse(isSameDayState(Soldier(id: 1, status: "late", from: "", to: "")))
        XCTAssertFalse(isSameDayState(Soldier(id: 1, status: "excused", from: today, to: "2026-03-18")))
        XCTAssertFalse(isSameDayState(Soldier(id: 1, status: "pass", from: today, to: today)))
    }

    func testStatusLabels() {
        XCTAssertEqual(statusLabel("tdy"), "TDY")
        XCTAssertEqual(statusLabel("ftr"), "FTR")
        XCTAssertEqual(statusLabel("excused"), "Excused")
        XCTAssertEqual(statusLabel("quarters"), "quarters")
        XCTAssertTrue(isKnownStatus("present"))
        XCTAssertFalse(isKnownStatus("quarters"))
        XCTAssertEqual(Statuses.formationReasons, ["late", "excused", "leave", "pass", "tdy", "other", "ftr"])
        XCTAssertEqual(Statuses.sameDay, ["late", "excused"])
    }

    func testReasonFieldsWording() {
        XCTAssertEqual(Set(ReasonFields.byStatus.keys), ["other", "excused"])   // late deliberately has none
        XCTAssertNil(reasonField(for: "late"))
        XCTAssertEqual(reasonField(for: "other"), ReasonField(
            label: "Category", placeholder: "e.g. Quarters, Staff Duty Recovery",
            options: ["Quarters", "Staff Duty Recovery", "CQ Recovery", "Appointment", "Detail"]))
        XCTAssertEqual(reasonField(for: "excused"), ReasonField(
            label: "Excused for", placeholder: "e.g. Sick call, appointment",
            options: ["Sick call", "Medical appointment", "Dental appointment",
                      "Family emergency", "Religious service", "Excused by CoC"]))
    }

    func testAbsenceFormProblem() {
        XCTAssertEqual(absenceFormProblem(status: "", from: "", to: ""), "Pick a status first.")
        XCTAssertEqual(absenceFormProblem(status: "leave", from: today, to: ""), "Leave needs an end date.")
        XCTAssertEqual(absenceFormProblem(status: "pass", from: today, to: ""), "A pass needs an end date.")
        XCTAssertNil(absenceFormProblem(status: "pass", from: today, to: today))
        XCTAssertNil(absenceFormProblem(status: "tdy", from: today, to: ""))
    }

    func testLastFirst() {
        XCTAssertEqual(lastFirst(Soldier(id: 1, last: "Boone", first: "Al")), "Boone, Al")
        XCTAssertEqual(lastFirst(Soldier(id: 1, last: "Boone", first: "")), "Boone")
    }

    func testFormatDateShort() {
        XCTAssertEqual(formatDateShort("2026-03-01"), "1MAR")
        XCTAssertEqual(formatDateShort("2026-12-25"), "25DEC")
        XCTAssertEqual(formatDateShort(""), "")
        // new Date('2026-02-30T00:00:00') rolls over to 2 March in the browser.
        XCTAssertEqual(formatDateShort("2026-02-30"), "2MAR")
        XCTAssertEqual(formatDateShort("garbage"), "")
    }
}
