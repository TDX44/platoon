import SwiftUI

/// The Set Status modal (`openTdyLeave` / `saveTdyLeave` in index.html): book an
/// absence, edit one, or take a quick-add proposal. The server owns the
/// lifecycle; this only ever POSTs or PUTs a window.
struct StatusSheet: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @Environment(\.dismiss) private var dismiss

    let target: StatusTarget

    @State private var status = ""
    @State private var from = ""
    @State private var to = ""
    @State private var hasEnd = false
    @State private var school = ""
    @State private var location = ""
    @State private var saveSchool = false
    @State private var saveLocation = false
    @State private var reason = ""
    @State private var repeatKind = ""          // "", "weekly", "interval"
    @State private var weekdays: Set<Int> = []  // Monday = 0
    @State private var everyN = 7
    @State private var until = ""
    @State private var saving = false
    @State private var error: String?
    @State private var started = false

    private static let statuses = ["late", "excused", "leave", "pass", "tdy", "other", "ftr"]
    private static let weekdayNames = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    private var soldier: Soldier? { store.soldier(target.soldierId) }
    private var editing: ScheduledEvent? { target.editing }
    private var tz: TimeZone { app.timeZone }

    var body: some View {
        NavigationStack {
            Form {
                if let p = soldier {
                    Section {
                        Text("\(p.rank) \(lastFirst(p))").font(.headline)
                        if let q = target.proposal?.question, !q.isEmpty {
                            Label(q, systemImage: "questionmark.bubble").foregroundStyle(.orange)
                        }
                    }
                }

                Section("Status") {
                    LazyVGrid(columns: [GridItem(.adaptive(minimum: 92), spacing: 8)], spacing: 8) {
                        ForEach(Self.statuses, id: \.self) { s in
                            Button {
                                pick(s)
                            } label: {
                                Label(statusLabel(s), systemImage: StatusStyle.symbol(s))
                                    .font(.subheadline.weight(.medium))
                                    .frame(maxWidth: .infinity, minHeight: 40)
                                    .foregroundStyle(status == s ? .white : StatusStyle.color(s))
                                    .background(status == s ? StatusStyle.color(s) : StatusStyle.color(s).opacity(0.12),
                                                in: RoundedRectangle(cornerRadius: 10))
                            }
                            .buttonStyle(.plain)
                            .accessibilityAddTraits(status == s ? .isSelected : [])
                        }
                    }
                    .padding(.vertical, 4)
                }

                if !status.isEmpty {
                    datesSection
                    if status == "tdy" { tdySection }
                    if let cfg = reasonFields[status] { reasonSection(cfg) }
                    if editing == nil { repeatSection }
                    Section {
                        Text(preview).font(.callout).foregroundStyle(.secondary)
                        if !from.isEmpty && from > store.today {
                            Label("Starts in the future: it will be scheduled and activate on \(DayMath.pretty(from)).",
                                  systemImage: "calendar.badge.clock")
                                .font(.footnote).foregroundStyle(.indigo)
                        }
                    } header: { Text("Shows as") }
                }

                if editing == nil, let p = soldier, !p.scheduledEvents.isEmpty {
                    Section("Already scheduled") {
                        ForEach(p.scheduledEvents) { e in
                            VStack(alignment: .leading) {
                                Text("\(statusLabel(e.status)) · \(rangeText(e.from, e.to))")
                                Text((e.notes ?? "").isEmpty ? "No notes" : e.notes!)
                                    .font(.caption).foregroundStyle(.secondary)
                            }
                            .swipeActions {
                                Button("Remove", role: .destructive) {
                                    Task {
                                        try? await store.deleteAbsence(e.id)
                                        await store.load()
                                    }
                                }
                            }
                        }
                    }
                }

                if let error {
                    Section { Text(error).foregroundStyle(.red) }
                }
            }
            .navigationTitle(editing != nil ? "Edit absence" : (target.proposal != nil ? "Quick add" : "Set status"))
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("Cancel") { finish() } }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") { Task { await save() } }
                        .disabled(status.isEmpty || saving)
                }
            }
            .onAppear(perform: start)
        }
    }

    // MARK: Sections

    private var datesSection: some View {
        Section {
            DayPicker(title: "From", day: $from, tz: tz)
                .onChange(of: from) { _, new in
                    if hasEnd, !to.isEmpty, to < new { to = new }
                }
            if status == "leave" || status == "pass" {
                DayPicker(title: "To (required)", day: $to, tz: tz, minimum: from)
            } else {
                Toggle("End date", isOn: $hasEnd.animation())
                    .onChange(of: hasEnd) { _, on in
                        if on && to.isEmpty { to = from.isEmpty ? store.today : from }
                        if !on { to = "" }
                    }
                if hasEnd {
                    DayPicker(title: "To", day: $to, tz: tz, minimum: from)
                }
            }
        } header: {
            Text("Dates")
        } footer: {
            if status == "late" || status == "excused" {
                Text("Same-day: with no end date it ends the day it starts.")
            } else if !(status == "leave" || status == "pass") && !hasEnd {
                Text("No end date: until marked present.")
            }
        }
    }

    private var tdySection: some View {
        Section("TDY") {
            ListPicker(title: "School / course", options: store.schools, value: $school, saveToList: $saveSchool)
            ListPicker(title: "Location", options: store.locations, value: $location, saveToList: $saveLocation)
        }
    }

    private func reasonSection(_ cfg: ReasonField) -> some View {
        Section(cfg.label) {
            TextField(cfg.placeholder, text: $reason)
            ScrollView(.horizontal, showsIndicators: false) {
                HStack {
                    ForEach(cfg.options, id: \.self) { o in
                        Button(o) { reason = o }
                            .buttonStyle(.bordered)
                            .controlSize(.small)
                    }
                }
            }
        }
    }

    private var repeatSection: some View {
        Section {
            Picker("Repeat", selection: $repeatKind.animation()) {
                Text("Doesn't repeat").tag("")
                Text("Weekly").tag("weekly")
                Text("Every N days").tag("interval")
            }
            if repeatKind == "weekly" {
                HStack {
                    ForEach(0..<7, id: \.self) { d in
                        let on = weekdays.contains(d)
                        Button(String(Self.weekdayNames[d].prefix(2))) {
                            if on { weekdays.remove(d) } else { weekdays.insert(d) }
                        }
                        .buttonStyle(.plain)
                        .font(.caption.weight(.semibold))
                        .frame(maxWidth: .infinity, minHeight: 32)
                        .foregroundStyle(on ? .white : .primary)
                        .background(on ? Color.accentColor : Color.secondary.opacity(0.12), in: Circle())
                    }
                }
            }
            if repeatKind == "interval" {
                Stepper("Every \(everyN) days", value: $everyN, in: 1...90)
            }
            if !repeatKind.isEmpty {
                DayPicker(title: "Until", day: $until, tz: tz, minimum: from)
            }
        } header: {
            Text("Repeat")
        } footer: {
            if !repeatKind.isEmpty {
                Text("Each occurrence is as long as this one. Up to 60 occurrences, at most six months ahead.")
            }
        }
    }

    // MARK: Logic

    private func start() {
        guard !started else { return }
        started = true
        if let e = editing {
            status = e.status
            from = e.from
            to = e.to
            hasEnd = !e.to.isEmpty
            if e.status == "tdy" { splitTDY(e.notes ?? "") } else if reasonFields[e.status] != nil { reason = e.notes ?? "" }
        } else if let pr = target.proposal {
            status = pr.status
            from = pr.fromDate ?? ""
            to = pr.toDate ?? ""
            hasEnd = !to.isEmpty
            school = pr.school ?? ""
            location = pr.location ?? ""
            reason = pr.reason ?? ""
            if from.isEmpty { from = store.today }
        } else if let p = soldier, p.status != "present" {
            // Opening a soldier who is already away shows today's absence.
            status = p.status
            from = p.from.isEmpty ? store.today : p.from
            to = p.to
            hasEnd = !p.to.isEmpty
            if p.status == "tdy" { splitTDY(p.notes) } else if reasonFields[p.status] != nil { reason = p.notes }
        } else {
            from = store.today
        }
    }

    private func splitTDY(_ notes: String) {
        if let r = notes.range(of: " - ") {
            school = String(notes[..<r.lowerBound])
            location = String(notes[r.upperBound...])
        } else {
            school = notes
        }
    }

    /// `setAwayStatus`: late always means today.
    private func pick(_ s: String) {
        Haptics.tap()
        status = s
        if s == "late" {
            from = store.today; to = store.today; hasEnd = true
        }
        if sameDayStatuses.contains(s) {
            if from.isEmpty { from = store.today }
            if to.isEmpty { to = from; hasEnd = true }
        }
        if (s == "leave" || s == "pass") && to.isEmpty { to = from.isEmpty ? store.today : from }
        error = nil
    }

    private var notes: String {
        if status == "tdy" {
            return [school, location].map { $0.trimmingCharacters(in: .whitespaces) }.filter { !$0.isEmpty }
                .joined(separator: " - ")
        }
        if reasonFields[status] != nil { return reason.trimmingCharacters(in: .whitespaces) }
        return ""
    }

    private var preview: String {
        var parts = [statusLabel(status)]
        if !notes.isEmpty { parts.append(notes) }
        let r = rangeText(from, (hasEnd || status == "leave" || status == "pass") ? to : "")
        if !r.isEmpty { parts.append(r) }
        return parts.joined(separator: " · ")
    }

    private func rangeText(_ a: String, _ b: String) -> String {
        if a.isEmpty && b.isEmpty { return "" }
        if b.isEmpty { return "from \(formatDateShort(a))" }
        if a == b { return formatDateShort(a) }
        return "\(formatDateShort(a)) – \(formatDateShort(b))"
    }

    private func save() async {
        let end = (hasEnd || status == "leave" || status == "pass") ? to : ""
        if let problem = absenceFormProblem(status: status, from: from, to: end) {
            error = problem
            return
        }
        saving = true
        defer { saving = false }
        if status == "tdy" {
            if saveSchool { await store.addToList(kind: "schools", value: school) }
            if saveLocation { await store.addToList(kind: "locations", value: location) }
        }
        do {
            if let e = editing {
                // Leave/Pass/FTR have no notes field, so keep whatever the row had.
                try await store.editAbsence(e.id, status: status, from: from, to: end,
                                            notes: notes.isEmpty ? (e.notes ?? "") : notes)
            } else {
                var rule: [String: Any]?
                if !repeatKind.isEmpty {
                    rule = ["type": repeatKind, "until": until]
                    if repeatKind == "weekly" { rule?["weekdays"] = weekdays.sorted() } else { rule?["every"] = everyN }
                }
                try await store.book(target.soldierId, status: status, from: from, to: end, notes: notes,
                                     recurrence: rule)
            }
            Haptics.success()
            await store.load()
            finish()
        } catch {
            Haptics.warning()
            self.error = error.localizedDescription
        }
    }

    private func finish() {
        target.onDone?()
        dismiss()
    }
}

/// A "YYYY-MM-DD" bound to a native date picker on the organization's clock.
struct DayPicker: View {
    let title: String
    @Binding var day: String
    let tz: TimeZone
    var minimum: String = ""

    var body: some View {
        let binding = Binding<Date>(
            get: { DayMath.date(day, in: tz) ?? Date() },
            set: { day = DayMath.string($0, in: tz) })
        let lower = DayMath.date(minimum, in: tz)
        Group {
            if let lower {
                DatePicker(title, selection: binding, in: lower..., displayedComponents: .date)
            } else {
                DatePicker(title, selection: binding, displayedComponents: .date)
            }
        }
        .environment(\.timeZone, tz)
        .onAppear { if day.isEmpty { day = DayMath.string(Date(), in: tz) } }
    }
}

/// A unit picklist (schools, locations) with "Other…" free text and Save to list.
struct ListPicker: View {
    let title: String
    let options: [String]
    @Binding var value: String
    @Binding var saveToList: Bool
    @State private var other = false

    var body: some View {
        let known = options.contains(value)
        Picker(title, selection: Binding(
            get: { other || (!value.isEmpty && !known) ? "__other__" : value },
            set: { new in
                if new == "__other__" { other = true; if known { value = "" } } else { other = false; value = new }
            })) {
            Text("— None —").tag("")
            ForEach(options, id: \.self) { Text($0).tag($0) }
            Text("Other…").tag("__other__")
        }
        if other || (!value.isEmpty && !known) {
            TextField("\(title) (type it)", text: $value)
            Toggle("Save to the unit's list", isOn: $saveToList)
        }
    }
}
