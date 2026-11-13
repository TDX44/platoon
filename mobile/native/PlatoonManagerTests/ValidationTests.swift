import XCTest
@testable import PlatoonManager

/// validateField() / normalizeField() and the phone helpers. CASES is
/// tests/test_validation.py's table, one for one, with the verdict and the
/// normalised value index.html's JS printed for each under node ("" = valid).
/// The phone cases are tests/test_phone_js.py's.
final class ValidationTests: XCTestCase {
    /// (field, value, JS validateField, JS normalizeField)
    static let cases: [(String, String, String, String)] = [
        ("phone", "", "", ""),
        ("phone", "2125550143", "", "(212) 555-0143"),
        ("phone", "212-555-0143", "", "(212) 555-0143"),
        ("phone", "+1 (212) 555-0143", "", "(212) 555-0143"),
        ("phone", "1-212-555-0143", "", "(212) 555-0143"),
        ("phone", "555-1234", "A +1 number is exactly 10 digits.", "555-1234"),
        ("phone", "212-555-0143 x204", "", "212-555-0143 x204"),
        ("phone", "DSN 312-555-0100", "", "DSN 312-555-0100"),
        ("phone", "21255501431", "A +1 number is exactly 10 digits.", "21255501431"),
        ("emergency_phone", "3105550147", "", "(310) 555-0147"),
        ("emergency_phone", "310555014", "A +1 number is exactly 10 digits.", "310555014"),
        ("phone", "+49 30 12345678", "", "+49 30 12345678"),
        ("phone", "+49 3012345678", "", "+49 3012345678"),
        ("phone", "+44 20 7946 0958", "", "+44 20 7946 0958"),
        ("phone", "+1 212 555 0143", "", "(212) 555-0143"),
        ("phone", "+49 12", "Enter at least 4 digits after +49.", "+49 12"),
        ("phone", "+351 912 345 678", "", "+351 912 345 678"),
        ("phone", "+61 2 9374 4000", "", "+61 2 9374 4000"),
        ("phone", "+999 123 4567", "Pick the country code from the list.", "+999 123 4567"),
        ("phone", "+49 1234567890123456", "At most 13 digits after +49.", "+49 1234567890123456"),
        ("emergency_phone", "+82 2 7914 4000", "", "+82 2 7914 4000"),
        ("email", "", "", ""),
        ("email", "a@b.co", "", "a@b.co"),
        ("email", "first.last@army.mil", "", "first.last@army.mil"),
        ("email", "nope", "Enter a valid email address.", "nope"),
        ("email", "a@b", "Enter a valid email address.", "a@b"),
        ("email", "a b@c.com", "Enter a valid email address.", "a b@c.com"),
        ("email", "a@@b.com", "Enter a valid email address.", "a@@b.com"),
        ("dod_id", "", "", ""),
        ("dod_id", "1234567890", "", "1234567890"),
        ("dod_id", "123456789", "DOD ID is exactly 10 digits.", "123456789"),
        ("dod_id", "12345678901", "DOD ID is exactly 10 digits.", "12345678901"),
        ("dod_id", "12345678ab", "DOD ID is exactly 10 digits.", "12345678ab"),
        ("mos", "", "", ""),
        ("mos", "11B", "", "11B"),
        ("mos", "35F", "", "35F"),
        ("mos", "155E", "", "155E"),
        ("mos", "153A", "", "153A"),
        ("mos", "11b", "", "11B"),
        ("mos", "1B", "MOS is 2-3 numbers then a letter, e.g. 11B or 155E.", "1B"),
        ("mos", "1155E", "MOS is 2-3 numbers then a letter, e.g. 11B or 155E.", "1155E"),
        ("mos", "11", "MOS is 2-3 numbers then a letter, e.g. 11B or 155E.", "11"),
        ("mos", "11BB", "MOS is 2-3 numbers then a letter, e.g. 11B or 155E.", "11BB"),
        ("last", "Smith", "", "Smith"),
        ("last", "O'Brien", "", "O'Brien"),
        ("last", "Walker-Leahy", "", "Walker-Leahy"),
        ("last", "Williams-White", "", "Williams-White"),
        ("last", "St. John", "", "St. John"),
        ("last", "Smith2", "Letters, spaces, hyphens and apostrophes only.", "Smith2"),
        ("last", "2Pac", "Letters, spaces, hyphens and apostrophes only.", "2Pac"),
        ("last", "", "", ""),
        ("first", "Jean-Luc", "", "Jean-Luc"),
        ("emergency_name", "Doe, Jane", "", "Doe, Jane"),
        ("spouse_dependents", "Jane Doe", "", "Jane Doe"),
        ("spouse_dependents", "2 kids", "Letters, spaces, hyphens and apostrophes only.", "2 kids"),
        ("next_of_kin", "Mary-Anne OBrien", "", "Mary-Anne OBrien"),
        ("clearance", "", "", ""),
        ("clearance", "Secret", "", "Secret"),
        ("clearance", "TS/SCI", "", "TS/SCI"),
        ("clearance", "Top Secret", "", "Top Secret"),
        ("clearance", "secret", "Choose a clearance from the list.", "Secret"),
        ("clearance", "Cosmic", "Choose a clearance from the list.", "Cosmic"),
        ("clearance", "TS-SCI", "Choose a clearance from the list.", "TS/SCI"),
        ("clearance", "ts", "Choose a clearance from the list.", "Top Secret"),
        ("clearance", "  TOP   SECRET  ", "Choose a clearance from the list.", "Top Secret"),
        ("dob", "", "", ""),
        ("dob", "1990-02-28", "", "1990-02-28"),
        ("dob", "1992-02-29", "", "1992-02-29"),
        ("dob", "1990-02-30", "Enter a valid date.", "1990-02-30"),
        ("dob", "1990-13-01", "Enter a valid date.", "1990-13-01"),
        ("dob", "90-01-01", "Enter a valid date.", "90-01-01"),
        ("dob", "1800-01-01", "Enter a valid date.", "1800-01-01"),
        ("ets_date", "2027-06-30", "", "2027-06-30"),
        ("medical_date", "2026-11-31", "Enter a valid date.", "2026-11-31"),
        ("weapons_qual", "", "", ""),
        ("weapons_qual", "[{\"weapon\":\"M4\",\"date\":\"2026-03-14\"}]", "", "[{\"weapon\":\"M4\",\"date\":\"2026-03-14\"}]"),
        ("weapons_qual", "[{\"weapon\":\"M4\",\"date\":\"\"},{\"weapon\":\"M9\",\"date\":\"2026-01-02\"}]", "", "[{\"weapon\":\"M4\",\"date\":\"\"},{\"weapon\":\"M9\",\"date\":\"2026-01-02\"}]"),
        ("weapons_qual", "[{\"weapon\":\"Trebuchet\",\"date\":\"\"}]", "Trebuchet is not on the weapons list.", "[{\"weapon\":\"Trebuchet\",\"date\":\"\"}]"),
        ("weapons_qual", "[{\"weapon\":\"M4\",\"date\":\"2026-02-30\"}]", "Enter a valid qualification date.", "[{\"weapon\":\"M4\",\"date\":\"2026-02-30\"}]"),
        ("weapons_qual", "[{\"weapon\":\"\",\"date\":\"\"}]", "(blank) is not on the weapons list.", "[{\"weapon\":\"\",\"date\":\"\"}]"),
        ("weapons_qual", "M4 qual 14MAR26", "", "M4 qual 14MAR26"),
        ("weapons_qual", "{\"weapon\":\"M4\"}", "", "{\"weapon\":\"M4\"}"),
        ("address", "123 Main St, Apt 4", "", "123 Main St, Apt 4"),
        ("section", "S2", "", "S2"),
        ("flags", "None", "", "None"),
        ("address_street", "123 Main St", "", "123 Main St"),
        ("address_street2", "Apt 4B", "", "Apt 4B"),
        ("address_city", "", "", ""),
        ("address_city", "Fort Walton Beach", "", "Fort Walton Beach"),
        ("address_city", "O'Fallon", "", "O'Fallon"),
        ("address_city", "Winston-Salem", "", "Winston-Salem"),
        ("address_city", "Apt 4", "Letters, spaces, hyphens and apostrophes only.", "Apt 4"),
        ("address_city", "90210", "Letters, spaces, hyphens and apostrophes only.", "90210"),
        ("address_state", "", "", ""),
        ("address_state", "FL", "", "FL"),
        ("address_state", "fl", "Choose a state from the list.", "FL"),
        ("address_state", "AE", "", "AE"),
        ("address_state", "XX", "Choose a state from the list.", "XX"),
        ("address_state", "Florida", "Choose a state from the list.", "Florida"),
        ("address_zip", "", "", ""),
        ("address_zip", "32547", "", "32547"),
        ("address_zip", "32547-1234", "", "32547-1234"),
        ("address_zip", "325471234", "ZIP is 5 digits, or 5+4 as 12345-6789.", "32547-1234"),
        ("address_zip", "3254", "ZIP is 5 digits, or 5+4 as 12345-6789.", "3254"),
        ("address_zip", "ABCDE", "ZIP is 5 digits, or 5+4 as 12345-6789.", "ABCDE"),
        ("profile_notes", String(repeating: "x", count: 300), "", String(repeating: "x", count: 300)),
        ("section", String(repeating: "x", count: 201), "Too long (max 200 characters).", String(repeating: "x", count: 201)),
    ]

    func testEveryCaseMatchesTheJS() {
        XCTAssertEqual(Self.cases.count, 107)
        for (field, value, err, norm) in Self.cases {
            XCTAssertEqual(validateField(field, value) ?? "", err, "validate \(field)=\(value)")
            XCTAssertEqual(normalizeField(field, value), norm, "normalize \(field)=\(value)")
        }
    }

    func testExpectedOK() {
        let ok: [(String, String)] = [
            ("phone", "212-555-0143"), ("phone", "1-212-555-0143"),
            ("phone", "DSN 312-555-0100"), ("phone", "212-555-0143 x204"),
            ("email", "first.last@army.mil"), ("dod_id", "1234567890"), ("mos", "155E"),
            ("mos", "11b"), ("last", "O'Brien"), ("last", "Walker-Leahy"), ("last", "St. John"),
            ("clearance", "TS/SCI"), ("dob", "1992-02-29"), ("ets_date", "2027-06-30"),
            ("address_street", "123 Main St"), ("address_city", "Fort Walton Beach"),
            ("address_city", "O'Fallon"), ("address_state", "FL"), ("address_state", "AE"),
            ("address_zip", "32547"), ("address_zip", "32547-1234"),
            ("weapons_qual", "M4 qual 14MAR26"), ("profile_notes", String(repeating: "x", count: 300)),
            ("phone", "+49 30 12345678"), ("phone", "+44 20 7946 0958"),
            ("phone", "+1 212 555 0143"), ("phone", "+351 912 345 678"),
            ("emergency_phone", "+82 2 7914 4000"),
        ]
        for (f, v) in ok { XCTAssertNil(validateField(f, v), "\(f)=\(v) should be allowed") }
    }

    func testExpectedBad() {
        let bad: [(String, String)] = [
            ("phone", "555-1234"), ("phone", "21255501431"),
            ("phone", "+49 12"), ("phone", "+999 123 4567"), ("phone", "+49 1234567890123456"),
            ("emergency_phone", "310555014"),
            ("email", "nope"), ("email", "a@b"), ("email", "a b@c.com"),
            ("dod_id", "123456789"), ("dod_id", "12345678901"), ("dod_id", "12345678ab"),
            ("mos", "1B"), ("mos", "1155E"), ("mos", "11"), ("mos", "11BB"),
            ("last", "Smith2"), ("last", "2Pac"), ("spouse_dependents", "2 kids"),
            ("clearance", "secret"), ("clearance", "Cosmic"),
            ("dob", "1990-02-30"), ("dob", "1990-13-01"), ("dob", "90-01-01"), ("dob", "1800-01-01"),
            ("medical_date", "2026-11-31"),
            ("address_city", "Apt 4"), ("address_city", "90210"),
            ("address_state", "XX"), ("address_state", "Florida"),
            ("address_zip", "3254"), ("address_zip", "ABCDE"),
            ("weapons_qual", #"[{"weapon":"Trebuchet","date":""}]"#),
            ("weapons_qual", #"[{"weapon":"M4","date":"2026-02-30"}]"#),
            ("weapons_qual", #"[{"weapon":"","date":""}]"#),
            ("section", String(repeating: "x", count: 201)),
        ]
        for (f, v) in bad { XCTAssertNotNil(validateField(f, v), "\(f)=\(v) should be rejected") }
    }

    func testNothingIsRequired() {
        for f in ["phone", "email", "dod_id", "mos", "last", "clearance", "dob", "weapons_qual"] {
            XCTAssertNil(validateField(f, ""), f)
        }
    }

    func testNormalisation() {
        XCTAssertEqual(normalizeField("phone", "212-555-0143"), "(212) 555-0143")
        XCTAssertEqual(normalizeField("mos", "11b"), "11B")
        XCTAssertEqual(normalizeField("dod_id", "123-456-7890"), "1234567890")
        XCTAssertEqual(normalizeField("address_state", "fl"), "FL")
        XCTAssertEqual(normalizeField("address_zip", "325471234"), "32547-1234")
        XCTAssertEqual(normalizeField("clearance", "TS-SCI"), "TS/SCI")
        XCTAssertEqual(normalizeField("clearance", "secret"), "Secret")
        XCTAssertEqual(normalizeField("clearance", "  TOP   SECRET  "), "Top Secret")
        for raw in ["TS-SCI", "secret", "ts"] {
            XCTAssertNil(validateField("clearance", normalizeField("clearance", raw)), raw)
        }
        XCTAssertEqual(normalizeField("clearance", "Cosmic"), "Cosmic")
        XCTAssertNotNil(validateField("clearance", "Cosmic"))
    }

    /// Idempotent, or a round trip through the form would keep changing the row.
    func testNormaliseIsIdempotent() {
        for (field, value, _, _) in Self.cases {
            let once = normalizeField(field, value)
            XCTAssertEqual(normalizeField(field, once), once, "\(field)=\(value)")
        }
    }

    func testVocabularies() {
        XCTAssertEqual(FieldRules.clearances, ["", "None", "Confidential", "Interim Secret", "Secret",
                                               "Interim Top Secret", "Top Secret", "TS/SCI"])
        XCTAssertEqual(FieldRules.weapons.count, 25)
        XCTAssertEqual(FieldRules.usStates.count, 60)
        XCTAssertEqual(FieldRules.usStates.first, "")
        XCTAssertEqual(FieldRules.phoneCountries.count, 94)
        XCTAssertEqual(FieldRules.phoneCountries[0], PhoneCountry(name: "United States", iso: "US", dial: "1"))
        XCTAssertEqual(FieldRules.phoneCountries.last, PhoneCountry(name: "Nicaragua", iso: "NI", dial: "505"))
        for c in FieldRules.phoneCountries {
            XCTAssertTrue((1...3).contains(c.dial.count) && c.dial.allSatisfy(\.isASCIIDigit), c.name)
            XCTAssertTrue(c.iso.count == 2 && c.iso == c.iso.uppercased(), c.name)
        }
        XCTAssertEqual(FieldRules.phoneCountries[0].flag, "\u{1F1FA}\u{1F1F8}")
        // DIAL_CODES as the JS computes it.
        XCTAssertEqual(FieldRules.dialCodes, ["212", "216", "233", "234", "251", "253", "254", "351", "352", "353",
            "354", "355", "356", "357", "358", "359", "370", "371", "372", "373", "374", "380", "381", "382", "385",
            "386", "387", "389", "420", "421", "502", "503", "504", "505", "506", "507", "593", "880", "886", "961",
            "962", "964", "965", "966", "968", "971", "972", "973", "974", "977", "994", "995", "20", "27", "30",
            "31", "32", "33", "34", "36", "39", "40", "41", "43", "44", "45", "46", "47", "48", "49", "51", "52",
            "54", "55", "56", "57", "58", "60", "61", "62", "63", "64", "65", "66", "81", "82", "84", "86", "90",
            "91", "92", "94", "1"])
    }

    // MARK: phones (test_validation.py's tail and test_phone_js.py)

    func testPhoneSplitJoinIsAFixedPoint() {
        for raw in ["(212) 555-0143", "+49 30 12345678", "+44 20 7946 0958", "", "DSN 1"] {
            let (dial, national) = phoneSplit(raw)
            let again = phoneSplit(phoneJoin(dial, national))
            XCTAssertEqual(again.dial, dial, raw)
            XCTAssertEqual(again.national, national, raw)
        }
        let expected: [(String, String, String)] = [
            ("(212) 555-0143", "1", "(212) 555-0143"), ("+49 30 12345678", "49", "30 12345678"),
            ("+44 20 7946 0958", "44", "20 7946 0958"), ("", "1", ""), ("DSN 1", "1", "DSN 1"),
            ("+999 123 4567", "", "+999 123 4567"), ("+351912345678", "351", "912345678"),
        ]
        for (raw, dial, national) in expected {
            XCTAssertEqual(phoneSplit(raw).dial, dial, raw)
            XCTAssertEqual(phoneSplit(raw).national, national, raw)
        }
    }

    func testMaxDigitsAndE164() {
        XCTAssertEqual(maxNationalDigits("1"), 10)
        XCTAssertEqual(maxNationalDigits("49"), 13)
        XCTAssertEqual(maxNationalDigits("351"), 12)
        XCTAssertEqual(phoneE164("+49 30 12345678"), "+493012345678")
        XCTAssertEqual(phoneE164("(212) 555-0143"), "+12125550143")
        XCTAssertEqual(phoneE164("21255501431"), "", "an 11-digit +1 number is not dialable")
        XCTAssertEqual(phoneE164("+999 123 4567"), "")
        XCTAssertEqual(phoneCountryIndex("49"), 4)
        XCTAssertEqual(phoneCountryIndex("1"), 0)
        XCTAssertEqual(phoneCountryIndex(""), 0)
    }

    static let raw = ["2125550143", "310-555-0147", "(310) 555-0147", "+1 310 555 0147", "1-310-555-0147",
                      "  3105550147  ", "310.555.0147", "555-1234", "310-555-0147 x204", "DSN 312-555-0100", ""]

    func testFormatPhoneAndE164MatchJS() {
        XCTAssertEqual(Self.raw.map(formatPhone), [
            "(212) 555-0143", "(310) 555-0147", "(310) 555-0147", "(310) 555-0147", "(310) 555-0147",
            "(310) 555-0147", "(310) 555-0147", "555-1234", "310-555-0147 x204", "DSN 312-555-0100", ""])
        XCTAssertEqual(Self.raw.map(phoneE164), [
            "+12125550143", "+13105550147", "+13105550147", "+13105550147", "+13105550147",
            "+13105550147", "+13105550147", "", "", "", ""])
        XCTAssertEqual(formatPhone(formatPhone("+1 310 555 0147")), "(310) 555-0147")
        XCTAssertEqual(formatPhone("+49   30  12345678"), "+49 30 12345678")
    }

    func testPhoneMaskWhileTyping() {
        let typed = ["", "2", "21", "212", "2125", "21255", "212555", "2125550",
                     "21255501", "212555014", "2125550143",
                     "+1 212 555 0143", "1-212-555-0143", "(212) 555-014", "(212) 555-01",
                     "(212) ", "(212", "21255501431", "DSN 312-555-0100", "212-555-0143 x2",
                     "+44 20 7946 0958"]
        let expected = ["", "2", "21", "212", "(212) 5", "(212) 55", "(212) 555",
                        "(212) 555-0", "(212) 555-01", "(212) 555-014", "(212) 555-0143",
                        "+1 212 555 0143", "(212) 555-0143", "(212) 555-014", "(212) 555-01",
                        "212", "212", "(212) 555-0143",
                        "DSN 312-555-0100", "212-555-0143 x2", "+44 20 7946 0958"]
        let mask = typed.map { phoneMask($0) }
        XCTAssertEqual(mask, expected)
        XCTAssertEqual(mask.map { phoneMask($0) }, mask, "re-masking must not shuffle the value")
        XCTAssertEqual(phoneMask("21255501439999", dial: "1"), "(212) 555-0143")
        XCTAssertEqual(phoneMask("30123456789999999", dial: "49"), "3012345678999")
        XCTAssertEqual(phoneMask("30 1234 5678", dial: "49"), "3012345678")
    }

    func testParseWeapons() {
        XCTAssertEqual(parseWeapons(""), [])
        XCTAssertEqual(parseWeapons(#"[{"weapon":"M4","date":"2026-03-14"}]"#),
                       [WeaponQual(weapon: "M4", date: "2026-03-14")])
        XCTAssertEqual(parseWeapons(#"[{"weapon":"M9"}]"#), [WeaponQual(weapon: "M9", date: "")])
        XCTAssertEqual(parseWeapons(#"[{"weapon":5,"date":null}]"#), [WeaponQual(weapon: "5", date: "")])
        XCTAssertNil(parseWeapons("M4 qual 14MAR26"))
        XCTAssertNil(parseWeapons(#"{"weapon":"M4"}"#))
        XCTAssertNil(parseWeapons(#"["M4"]"#))
        XCTAssertNil(parseWeapons(#"[null]"#))
    }

    func testIsRealDate() {
        XCTAssertTrue(isRealDate("2000-02-29"))
        XCTAssertFalse(isRealDate("1900-02-29"))
        XCTAssertTrue(isRealDate("2200-12-31"))
        XCTAssertFalse(isRealDate("2201-01-01"))
        XCTAssertFalse(isRealDate("2026-1-01"))
    }
}
