import SwiftUI

/// The soldier profile's fields, grouped as the web's soldier page groups them.
enum ProfileLayout {
    enum Kind { case text, phone, email, date, clearance, state, zip, notes, weapons }

    struct Field: Hashable {
        let key: String
        let label: String
        let kind: Kind
    }

    struct Group {
        let title: String
        let fields: [Field]
    }

    static let groups: [Group] = [
        Group(title: "Contact", fields: [
            Field(key: "phone", label: "Phone", kind: .phone),
            Field(key: "email", label: "Email", kind: .email),
            Field(key: "address_street", label: "Street", kind: .text),
            Field(key: "address_street2", label: "Apt / unit", kind: .text),
            Field(key: "address_city", label: "City", kind: .text),
            Field(key: "address_state", label: "State", kind: .state),
            Field(key: "address_zip", label: "ZIP", kind: .zip),
            Field(key: "address", label: "Old address (re-enter above)", kind: .notes),
        ]),
        Group(title: "Emergency & Family", fields: [
            Field(key: "emergency_name", label: "Emergency contact", kind: .text),
            Field(key: "emergency_phone", label: "Emergency phone", kind: .phone),
            Field(key: "spouse_dependents", label: "Spouse / dependents", kind: .text),
            Field(key: "next_of_kin", label: "Next of kin", kind: .text),
        ]),
        Group(title: "Administrative", fields: [
            Field(key: "dod_id", label: "DOD ID", kind: .text),
            Field(key: "dob", label: "Date of birth", kind: .date),
            Field(key: "date_of_rank", label: "Date of rank", kind: .date),
            Field(key: "mos", label: "MOS", kind: .text),
            Field(key: "clearance", label: "Security clearance", kind: .clearance),
            Field(key: "ets_date", label: "ETS date", kind: .date),
            Field(key: "section", label: "Section / team", kind: .text),
        ]),
        Group(title: "Readiness", fields: [
            Field(key: "flags", label: "Flags", kind: .text),
            Field(key: "medical_date", label: "Medical readiness", kind: .date),
            Field(key: "dental_date", label: "Dental readiness", kind: .date),
            Field(key: "weapons_qual", label: "Weapons qualification", kind: .weapons),
        ]),
        Group(title: "Notes", fields: [
            Field(key: "profile_notes", label: "Notes", kind: .notes),
        ]),
    ]

    /// A weapons qual row; the column holds a JSON list of these.
    struct WeaponQual: Codable, Hashable {
        var weapon: String
        var date: String
    }

    /// nil means the old free-text shape, which is never an error and never thrown away.
    static func parseWeapons(_ raw: String) -> [WeaponQual]? {
        let t = raw.trimmingCharacters(in: .whitespaces)
        if t.isEmpty { return [] }
        guard t.hasPrefix("["), let data = t.data(using: .utf8),
              let rows = try? JSONDecoder().decode([WeaponQual].self, from: data) else { return nil }
        return rows
    }

    static func encodeWeapons(_ rows: [WeaponQual]) -> String {
        let kept = rows.filter { !$0.weapon.isEmpty }
        guard !kept.isEmpty, let data = try? JSONEncoder().encode(kept) else { return "" }
        return String(decoding: data, as: UTF8.self)
    }

    /// The value as the soldier page shows it.
    static func display(_ key: String, _ raw: String) -> String {
        guard !raw.isEmpty else { return "" }
        let field = groups.flatMap(\.fields).first { $0.key == key }
        switch field?.kind {
        case .date: return DayMath.long(raw)
        case .phone: return formatPhone(raw)
        case .weapons:
            guard let rows = parseWeapons(raw) else { return raw }
            return rows.map { $0.date.isEmpty ? $0.weapon : "\($0.weapon) — \(DayMath.long($0.date))" }
                .joined(separator: "\n")
        default: return raw
        }
    }
}

/// Edit every profile field; normalised and checked with the same rules the
/// server uses (`validateField` / `normalizeField`), then one PUT.
struct ProfileEditView: View {
    @Environment(AppModel.self) private var app
    @Environment(\.dismiss) private var dismiss
    let soldier: Soldier
    let profile: [String: String]

    @State private var values: [String: String] = [:]
    @State private var weapons: [ProfileLayout.WeaponQual] = []
    @State private var weaponsLegacy: String?
    @State private var discardOldAddress = false
    @State private var errors: [String: String] = [:]
    @State private var error: String?
    @State private var saving = false
    @State private var started = false

    var body: some View {
        NavigationStack {
            Form {
                ForEach(ProfileLayout.groups, id: \.title) { group in
                    Section(group.title) {
                        ForEach(group.fields, id: \.key) { field in
                            fieldEditor(field)
                            if let msg = errors[field.key] {
                                Text(msg).font(.caption).foregroundStyle(.red)
                            }
                        }
                    }
                }
                if let error { Section { Text(error).foregroundStyle(.red) } }
            }
            .navigationTitle("\(soldier.rank) \(soldier.last)")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("Cancel") { dismiss() } }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") { Task { await save() } }.disabled(saving)
                }
            }
            .onAppear {
                guard !started else { return }
                started = true
                values = profile
                if let rows = ProfileLayout.parseWeapons(profile["weapons_qual"] ?? "") {
                    weapons = rows
                } else {
                    weaponsLegacy = profile["weapons_qual"]
                }
            }
        }
    }

    private func binding(_ key: String) -> Binding<String> {
        Binding(get: { values[key] ?? "" }, set: { values[key] = $0; errors[key] = nil })
    }

    @ViewBuilder
    private func fieldEditor(_ field: ProfileLayout.Field) -> some View {
        switch field.kind {
        case .text, .zip:
            TextField(field.label, text: binding(field.key))
                .keyboardType(field.kind == .zip || field.key == "dod_id" ? .numberPad : .default)
        case .phone:
            TextField(field.label, text: binding(field.key)).keyboardType(.phonePad).textContentType(.telephoneNumber)
        case .email:
            TextField(field.label, text: binding(field.key))
                .keyboardType(.emailAddress).textInputAutocapitalization(.never).autocorrectionDisabled()
        case .date:
            OptionalDayPicker(title: field.label, day: binding(field.key), tz: app.timeZone)
        case .clearance:
            Picker(field.label, selection: binding(field.key)) {
                Text("—").tag("")
                ForEach(clearances, id: \.self) { Text($0).tag($0) }
                let cur = values[field.key] ?? ""
                if !cur.isEmpty && !clearances.contains(cur) { Text(cur).tag(cur) }
            }
        case .state:
            Picker(field.label, selection: binding(field.key)) {
                Text("—").tag("")
                ForEach(usStates, id: \.self) { Text($0).tag($0) }
                let cur = values[field.key] ?? ""
                if !cur.isEmpty && !usStates.contains(cur) { Text(cur).tag(cur) }
            }
        case .notes:
            if field.key == "address" {
                let old = values["address"] ?? ""
                if !old.isEmpty && !discardOldAddress {
                    VStack(alignment: .leading, spacing: 6) {
                        Text(field.label).font(.caption).foregroundStyle(.secondary)
                        Text(old)
                        Button("Discard old address", role: .destructive) { discardOldAddress = true }
                            .buttonStyle(.borderless)
                    }
                }
            } else {
                TextField(field.label, text: binding(field.key), axis: .vertical).lineLimit(2...6)
            }
        case .weapons:
            if let legacy = weaponsLegacy {
                VStack(alignment: .leading, spacing: 6) {
                    Text("Old entry: \(legacy)").font(.callout)
                    Button("Replace with qualification rows") { weaponsLegacy = nil; weapons = [] }
                        .buttonStyle(.borderless)
                }
            } else {
                ForEach(weapons.indices, id: \.self) { i in
                    VStack {
                        Picker("Weapon", selection: $weapons[i].weapon) {
                            Text("—").tag("")
                            ForEach(weaponOptions, id: \.self) { Text($0).tag($0) }
                            if !weapons[i].weapon.isEmpty && !weaponOptions.contains(weapons[i].weapon) {
                                Text(weapons[i].weapon).tag(weapons[i].weapon)
                            }
                        }
                        OptionalDayPicker(title: "Qualified", day: $weapons[i].date, tz: app.timeZone)
                    }
                    .swipeActions { Button("Delete", role: .destructive) { weapons.remove(at: i) } }
                }
                Button("Add qualification", systemImage: "plus") {
                    weapons.append(.init(weapon: "", date: ""))
                }
            }
        }
    }

    private func save() async {
        var body: [String: Any] = [:]
        var bad: [String: String] = [:]
        for field in ProfileLayout.groups.flatMap(\.fields) {
            switch field.kind {
            case .weapons:
                let v = weaponsLegacy ?? ProfileLayout.encodeWeapons(weapons)
                body[field.key] = v
                if weaponsLegacy == nil, let msg = validateField(field.key, v) { bad[field.key] = msg }
            case .notes where field.key == "address":
                if discardOldAddress { body["address"] = "" }
            default:
                let v = normalizeField(field.key, values[field.key] ?? "")
                values[field.key] = v
                body[field.key] = v
                if let msg = validateField(field.key, v) { bad[field.key] = msg }
            }
        }
        errors = bad
        if !bad.isEmpty {
            error = "Fix the fields marked in red."
            return
        }
        saving = true
        defer { saving = false }
        do {
            _ = try await app.api.putStrings("personnel/\(soldier.id)/profile", body)
            Haptics.success()
            dismiss()
        } catch let APIError.server(_, message, field) {
            if let field { errors[field] = message }
            error = message
        } catch {
            self.error = error.localizedDescription
        }
    }
}

/// A date that may be blank: a toggle plus a picker.
struct OptionalDayPicker: View {
    let title: String
    @Binding var day: String
    let tz: TimeZone

    var body: some View {
        if day.isEmpty {
            HStack {
                Text(title)
                Spacer()
                Button("Add date") { day = DayMath.string(Date(), in: tz) }.buttonStyle(.borderless)
            }
        } else {
            HStack {
                DayPicker(title: title, day: $day, tz: tz)
                Button {
                    day = ""
                } label: {
                    Image(systemName: "xmark.circle.fill").foregroundStyle(.secondary)
                }
                .buttonStyle(.borderless)
                .accessibilityLabel("Clear \(title)")
            }
        }
    }
}
