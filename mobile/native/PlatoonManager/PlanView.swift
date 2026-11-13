import SwiftUI

/// Planning: who is free on a day (or a range), and the duty roster.
struct PlanView: View {
    @State private var mode = 0

    var body: some View {
        NavigationStack {
            VStack(spacing: 0) {
                Picker("View", selection: $mode) {
                    Text("Availability").tag(0)
                    Text("Duty roster").tag(1)
                }
                .pickerStyle(.segmented)
                .padding(.horizontal)
                .padding(.bottom, 6)
                if mode == 0 { AvailabilityView() } else { DutyView() }
            }
            .navigationTitle(mode == 0 ? "Availability" : "Duty roster")
            .navigationDestination(for: SoldierRoute.self) { SoldierView(soldierId: $0.id) }
        }
    }
}

// MARK: Availability

/// `GET /api/availability`: read from scheduled_events, never from today's cache.
struct AvailabilityView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @State private var from = ""
    @State private var to = ""
    @State private var range = false
    @State private var result: Availability?
    @State private var error: String?

    var body: some View {
        List {
            Section {
                DayPicker(title: range ? "From" : "Date", day: $from, tz: app.timeZone)
                Toggle("Date range", isOn: $range.animation())
                if range { DayPicker(title: "To", day: $to, tz: app.timeZone, minimum: from) }
            }
            if let error { Text(error).foregroundStyle(.red) }
            if let r = result {
                Section {
                    if r.unavailable.isEmpty {
                        Text("Nobody is away.").foregroundStyle(.secondary)
                    }
                    ForEach(r.unavailable) { w in
                        NavigationLink(value: SoldierRoute(id: w.id)) {
                            VStack(alignment: .leading, spacing: 3) {
                                HStack {
                                    Text("\(w.rank ?? "") \(lastFirst(soldier(w)))")
                                    Spacer()
                                    StatusBadge(status: w.status ?? "other")
                                }
                                let detail = [w.notes ?? "", dates(w)].filter { !$0.isEmpty }.joined(separator: " · ")
                                if !detail.isEmpty { Text(detail).font(.caption).foregroundStyle(.secondary) }
                                if r.span > 1, let days = w.days, w.wholeRange != true {
                                    Text("Out \(days.count) of \(r.span) days").font(.caption2).foregroundStyle(.orange)
                                }
                            }
                        }
                    }
                } header: {
                    SectionHeader(title: "Unavailable", count: r.unavailable.count, color: .orange)
                }
                Section {
                    ForEach(r.available) { w in
                        NavigationLink(value: SoldierRoute(id: w.id)) {
                            Text("\(w.rank ?? "") \(lastFirst(soldier(w)))")
                        }
                    }
                } header: {
                    SectionHeader(title: "Available", count: r.available.count, color: .green)
                }
            }
        }
        .task(id: "\(from)|\(to)|\(range)") { await load() }
        .refreshable { await load() }
        .onAppear { if from.isEmpty { from = store.today } }
    }

    private func soldier(_ w: Availability.Who) -> Soldier {
        Soldier(id: w.id, rank: w.rank ?? "", last: w.last ?? "", first: w.first ?? "")
    }

    private func dates(_ w: Availability.Who) -> String {
        let a = w.fromDate ?? "", b = w.toDate ?? ""
        if a.isEmpty && b.isEmpty { return "" }
        if b.isEmpty { return "from \(formatDateShort(a))" }
        if a == b { return formatDateShort(a) }
        return "\(formatDateShort(a)) – \(formatDateShort(b))"
    }

    private func load() async {
        guard !from.isEmpty else { return }
        var q = ["unit": String(store.unitId), "date": from]
        if range && !to.isEmpty { q["to"] = to }
        do {
            var r: Availability = try await app.api.get("availability", query: q)
            r.available.sort { rankLess(soldier($0), soldier($1)) }
            r.unavailable.sort { rankLess(soldier($0), soldier($1)) }
            result = r
            error = nil
        } catch {
            self.error = error.localizedDescription
        }
    }
}

// MARK: Duty roster

struct DutyView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @State private var day = ""
    @State private var entries: [DutyEntry] = []
    @State private var error: String?
    @State private var adding = false
    @State private var rotation = false

    static let dutyTypes = ["CQ", "Staff Duty", "Runner", "Driver"]

    var body: some View {
        List {
            Section {
                DayPicker(title: "Date", day: $day, tz: app.timeZone)
                HStack {
                    Button("Previous day", systemImage: "chevron.left") { day = DayMath.adding(-1, to: day) }
                    Spacer()
                    Button("Today") { day = store.today }
                    Spacer()
                    Button("Next day", systemImage: "chevron.right") { day = DayMath.adding(1, to: day) }
                }
                .labelStyle(.iconOnly)
                .buttonStyle(.borderless)
            }
            if let error { Text(error).foregroundStyle(.red) }
            Section(DayMath.pretty(day)) {
                if entries.isEmpty { Text("No duty assigned.").foregroundStyle(.secondary) }
                ForEach(entries) { e in
                    VStack(alignment: .leading, spacing: 3) {
                        HStack {
                            Text(e.dutyType).font(.subheadline.weight(.semibold))
                            Spacer()
                            Text("\(e.rank ?? "") \(e.last ?? "")\((e.first ?? "").isEmpty ? "" : ", \(e.first!)")")
                        }
                        if let c = e.conflict {
                            Label("Conflict: \(c.label)", systemImage: "exclamationmark.triangle.fill")
                                .font(.caption).foregroundStyle(.red)
                        }
                        if let n = e.notes, !n.isEmpty { Text(n).font(.caption).foregroundStyle(.secondary) }
                    }
                    .swipeActions {
                        Button("Remove", role: .destructive) { Task { await remove(e) } }
                    }
                }
            }
            Section {
                Button("Assign duty", systemImage: "plus") { adding = true }
                Button("Generate rotation…", systemImage: "arrow.triangle.2.circlepath") { rotation = true }
            }
        }
        .task(id: day) { await load() }
        .refreshable { await load() }
        .onAppear { if day.isEmpty { day = store.today } }
        .sheet(isPresented: $adding, onDismiss: { Task { await load() } }) {
            AddDutyView(day: day)
        }
        .sheet(isPresented: $rotation, onDismiss: { Task { await load() } }) {
            RotationView(start: day)
        }
    }

    private func load() async {
        guard !day.isEmpty else { return }
        do {
            entries = try await app.api.get("duty", query: ["unit": String(store.unitId), "date": day])
            error = nil
        } catch {
            self.error = error.localizedDescription
        }
    }

    private func remove(_ e: DutyEntry) async {
        do {
            try await app.api.delete("duty/\(e.id)")
            entries.removeAll { $0.id == e.id }
        } catch { self.error = error.localizedDescription }
    }
}

/// Assign one duty: the picker shows who is away that day before you pick them.
struct AddDutyView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @Environment(\.dismiss) private var dismiss
    @State var day: String
    @State private var dutyType = "CQ"
    @State private var customType = ""
    @State private var personId: Int?
    @State private var notes = ""
    @State private var conflicts: [String: DutyConflict] = [:]
    @State private var error: String?
    @State private var saving = false

    var body: some View {
        NavigationStack {
            Form {
                DayPicker(title: "Date", day: $day, tz: app.timeZone)
                Picker("Duty", selection: $dutyType) {
                    ForEach(DutyView.dutyTypes, id: \.self) { Text($0).tag($0) }
                    Text("Other…").tag("__other__")
                }
                if dutyType == "__other__" { TextField("Duty name", text: $customType) }
                Picker("Soldier", selection: $personId) {
                    Text("Choose…").tag(Int?.none)
                    ForEach(store.soldiers.sorted(by: rankLess)) { p in
                        if let c = conflicts[String(p.id)] {
                            Text("\(p.rank) \(lastFirst(p)) — \(c.label)").tag(Int?.some(p.id))
                        } else {
                            Text("\(p.rank) \(lastFirst(p))").tag(Int?.some(p.id))
                        }
                    }
                }
                if let id = personId, let c = conflicts[String(id)] {
                    Label("Away that day: \(c.label)", systemImage: "exclamationmark.triangle.fill").foregroundStyle(.orange)
                }
                TextField("Notes", text: $notes)
                if let error { Text(error).foregroundStyle(.red) }
            }
            .navigationTitle("Assign duty")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("Cancel") { dismiss() } }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") { Task { await save() } }.disabled(personId == nil || saving)
                }
            }
            .task(id: day) {
                conflicts = (try? await app.api.get("duty/conflicts", query: ["unit": String(store.unitId), "date": day])) ?? [:]
            }
        }
    }

    private func save() async {
        guard let personId else { return }
        let type = dutyType == "__other__" ? customType.trimmingCharacters(in: .whitespaces) : dutyType
        guard !type.isEmpty else { error = "Name the duty."; return }
        saving = true
        defer { saving = false }
        do {
            try await app.api.send("POST", "duty", ["unit_id": store.unitId, "date": day, "duty_type": type,
                                                    "person_id": personId, "notes": notes])
            Haptics.success()
            dismiss()
        } catch { self.error = error.localizedDescription }
    }
}

/// "Generate rotation…": the server proposes (fewest turns first, never two
/// days running, nobody away), the leader adjusts, then one bulk save.
struct RotationView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @Environment(\.dismiss) private var dismiss
    @State var start: String
    @State private var end = ""
    @State private var dutyType = "CQ"
    @State private var separateWeekends = true
    @State private var pool: Set<Int> = []
    @State private var proposal: [Pick] = []
    @State private var away: [String: [String: String]] = [:]
    @State private var error: String?
    @State private var busy = false
    @State private var started = false

    struct Pick: Identifiable, Hashable {
        var id: String { date }
        var date: String
        var personId: Int?
        var reason: String
        var existing: String?
    }

    private struct Response: Decodable {
        struct Entry: Decodable { var date: String; var personId: Int?; var reason: String?; var existing: String? }
        var proposal: [Entry]
        var away: [String: [String: String]]?
    }

    var body: some View {
        NavigationStack {
            Form {
                Section {
                    Picker("Duty", selection: $dutyType) {
                        ForEach(DutyView.dutyTypes, id: \.self) { Text($0).tag($0) }
                    }
                    DayPicker(title: "From", day: $start, tz: app.timeZone)
                    DayPicker(title: "To", day: $end, tz: app.timeZone, minimum: start)
                    Toggle("Count weekends separately", isOn: $separateWeekends)
                }
                Section {
                    let people = store.soldiers.sorted(by: rankLess)
                    Button(pool.count == people.count ? "Clear all" : "Select all") {
                        pool = pool.count == people.count ? [] : Set(people.map(\.id))
                    }
                    ForEach(people) { p in
                        Button {
                            if pool.contains(p.id) { pool.remove(p.id) } else { pool.insert(p.id) }
                        } label: {
                            HStack {
                                Text("\(p.rank) \(lastFirst(p))").foregroundStyle(.primary)
                                Spacer()
                                if pool.contains(p.id) { Image(systemName: "checkmark").foregroundStyle(.tint) }
                            }
                        }
                    }
                } header: {
                    Text("Pool (\(pool.count))")
                }
                Section {
                    Button(busy ? "Working…" : "Propose") { Task { await propose() } }
                        .disabled(busy || pool.isEmpty || end.isEmpty)
                }
                if !proposal.isEmpty {
                    Section("Proposal") {
                        ForEach($proposal) { $pick in
                            VStack(alignment: .leading) {
                                if let existing = pick.existing {
                                    LabeledContent(DayMath.pretty(pick.date), value: "Already: \(existing)")
                                } else {
                                    Picker(DayMath.pretty(pick.date), selection: $pick.personId) {
                                        Text("Nobody").tag(Int?.none)
                                        ForEach(store.soldiers.filter { pool.contains($0.id) }.sorted(by: rankLess)) { p in
                                            let note = away[pick.date]?[String(p.id)]
                                            Text("\(p.rank) \(p.last)\(note.map { " — \($0)" } ?? "")").tag(Int?.some(p.id))
                                        }
                                    }
                                    if let id = pick.personId, let note = away[pick.date]?[String(id)] {
                                        Text("Away: \(note)").font(.caption).foregroundStyle(.orange)
                                    } else if pick.personId == nil, !pick.reason.isEmpty {
                                        Text(pick.reason).font(.caption).foregroundStyle(.secondary)
                                    }
                                }
                            }
                        }
                    }
                }
                if let error { Section { Text(error).foregroundStyle(.red) } }
            }
            .navigationTitle("Generate rotation")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("Cancel") { dismiss() } }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") { Task { await save() } }
                        .disabled(busy || !proposal.contains { $0.personId != nil && $0.existing == nil })
                }
            }
            .onAppear {
                guard !started else { return }
                started = true
                if end.isEmpty { end = DayMath.adding(13, to: start) }
                pool = Set(store.soldiers.filter { $0.status == "present" }.map(\.id))
            }
        }
    }

    private func propose() async {
        busy = true
        defer { busy = false }
        let ids = store.soldiers.sorted(by: rankLess).map(\.id).filter { pool.contains($0) }
        do {
            let r: Response = try await app.api.post("duty/rotation", [
                "unit_id": store.unitId, "duty_type": dutyType, "from": start, "to": end,
                "person_ids": ids, "separate_weekends": separateWeekends,
            ])
            proposal = r.proposal.map { Pick(date: $0.date, personId: $0.personId, reason: $0.reason ?? "", existing: $0.existing) }
            away = r.away ?? [:]
            error = nil
        } catch { self.error = error.localizedDescription }
    }

    private func save() async {
        let entries: [[String: Any]] = proposal.compactMap { pick in
            guard pick.existing == nil, let id = pick.personId else { return nil }
            return ["date": pick.date, "duty_type": dutyType, "person_id": id, "notes": ""]
        }
        busy = true
        defer { busy = false }
        do {
            try await app.api.send("POST", "duty/bulk", ["unit_id": store.unitId, "entries": entries])
            Haptics.success()
            dismiss()
        } catch { self.error = error.localizedDescription }
    }
}
