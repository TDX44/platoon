import SwiftUI

/// The soldier page: today's status, every absence (current, upcoming,
/// history) with edit / cancel, the profile, and Edit Person.
struct SoldierView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @Environment(\.dismiss) private var dismiss
    let soldierId: Int

    @State private var absences: [ScheduledEvent] = []
    @State private var profile: [String: String] = [:]
    @State private var loadError: String?
    @State private var statusFor: StatusTarget?
    @State private var editPerson = false
    @State private var editProfile = false
    @State private var confirmRemove = false
    @State private var seriesToCancel: String?
    @State private var message: String?

    var body: some View {
        Group {
            if let p = store.soldier(soldierId) {
                content(p)
            } else {
                ContentUnavailableView("Not on this roster", systemImage: "person.crop.circle.badge.questionmark")
            }
        }
        .navigationBarTitleDisplayMode(.inline)
        .task { await reload() }
        .refreshable {
            await store.load()
            await reload()
        }
        .sheet(item: $statusFor) { StatusSheet(target: $0) }
        .sheet(isPresented: $editPerson) {
            if let p = store.soldier(soldierId) { EditSoldierView(soldier: p) }
        }
        .sheet(isPresented: $editProfile, onDismiss: { Task { await reload() } }) {
            if let p = store.soldier(soldierId) { ProfileEditView(soldier: p, profile: profile) }
        }
        .confirmationDialog("Remove the rest of this repeating absence?", isPresented: Binding(
            get: { seriesToCancel != nil }, set: { if !$0 { seriesToCancel = nil } }), titleVisibility: .visible) {
            Button("Remove", role: .destructive) {
                if let s = seriesToCancel { Task { await cancelSeries(s) } }
            }
        } message: {
            Text("Every occurrence that has not started yet is removed. One already under way, and past ones, stay.")
        }
        .toast($message)
    }

    private func content(_ p: Soldier) -> some View {
        let today = store.today
        let live = absences.filter { $0.state != "completed" }
        let history = absences.filter { $0.state == "completed" }
        let unit = app.tree.unit(p.unitId)
        return List {
            Section {
                VStack(alignment: .leading, spacing: 6) {
                    Text("\(p.rank) \(p.last)").font(.largeTitle.bold())
                    if !p.first.isEmpty { Text(p.first).font(.title3).foregroundStyle(.secondary) }
                    if let unit { Label(unit.name, systemImage: "flag").font(.subheadline).foregroundStyle(.secondary) }
                    HStack {
                        if isUnaccounted(p, today: today) {
                            StatusBadge(status: "unaccounted", label: "Unaccounted")
                        } else {
                            StatusBadge(status: p.status)
                        }
                        if p.noFormation { StatusBadge(status: "other", label: "Doesn't attend formation") }
                    }
                    if p.status != "present" {
                        let detail = [p.notes, isSameDayState(p) ? "" : rangeText(p.from, p.to)]
                            .filter { !$0.isEmpty }.joined(separator: " · ")
                        if !detail.isEmpty { Text(detail).font(.callout).foregroundStyle(.secondary) }
                    }
                }
                .padding(.vertical, 4)
                actions(p, today: today)
            }

            Section("Absences") {
                if live.isEmpty {
                    Text("Nothing current or scheduled.").foregroundStyle(.secondary)
                }
                ForEach(live) { e in absenceRow(e, editable: true) }
                Button {
                    statusFor = StatusTarget(soldierId: p.id)
                } label: {
                    Label("Book an absence", systemImage: "calendar.badge.plus")
                }
            }

            if !history.isEmpty {
                Section("History") {
                    ForEach(history) { e in absenceRow(e, editable: false) }
                }
            }

            profileSections

            Section {
                Button("Edit person", systemImage: "pencil") { editPerson = true }
                Button("Remove from roster", systemImage: "trash", role: .destructive) { confirmRemove = true }
            }
            .confirmationDialog("Remove \(p.rank) \(lastFirst(p))?", isPresented: $confirmRemove, titleVisibility: .visible) {
                Button("Remove", role: .destructive) {
                    Task {
                        do {
                            try await store.removeSoldier(p.id)
                            dismiss()
                        } catch { message = error.localizedDescription }
                    }
                }
            }

            if let loadError {
                Section { Text(loadError).foregroundStyle(.red) }
            }
        }
    }

    @ViewBuilder
    private func actions(_ p: Soldier, today: String) -> some View {
        if isUnaccounted(p, today: today) {
            Button { Task { if await store.markPresentToday(p.id) { Haptics.success() } } } label: {
                Label("Mark present", systemImage: "checkmark.circle.fill")
            }
            .tint(.green)
        } else if isPresentToday(p, today: today) {
            Button { Task { await store.unmarkPresent(p.id) } } label: {
                Label("Unmark present", systemImage: "arrow.uturn.backward")
            }
        } else if p.status != "present" {
            Button {
                Task {
                    if await store.markPresent(p.id) { Haptics.success(); await reload() }
                }
            } label: {
                Label("Mark present (ends this absence)", systemImage: "checkmark.circle.fill")
            }
            .tint(.green)
        }
        Button { statusFor = StatusTarget(soldierId: p.id) } label: {
            Label(p.status == "present" ? "Set status…" : "Update status…", systemImage: "calendar.badge.plus")
        }
    }

    private func absenceRow(_ e: ScheduledEvent, editable: Bool) -> some View {
        VStack(alignment: .leading, spacing: 3) {
            HStack {
                StatusBadge(status: e.status)
                Text(rangeText(e.from, e.to)).font(.subheadline)
                Spacer()
                if let state = e.state {
                    Text(state.capitalized).font(.caption).foregroundStyle(.secondary)
                }
            }
            if let notes = e.notes, !notes.isEmpty {
                Text(notes).font(.caption).foregroundStyle(.secondary)
            }
            if e.seriesId != nil {
                Label("Repeating", systemImage: "repeat").font(.caption2).foregroundStyle(.secondary)
            }
        }
        .contentShape(Rectangle())
        .onTapGesture {
            if editable { statusFor = StatusTarget(soldierId: soldierId, editing: e, onDone: { Task { await reload() } }) }
        }
        .swipeActions {
            if editable {
                Button("Remove", role: .destructive) { Task { await remove(e) } }
                Button("Edit") {
                    statusFor = StatusTarget(soldierId: soldierId, editing: e, onDone: { Task { await reload() } })
                }
                .tint(.blue)
                if let s = e.seriesId {
                    Button("Cancel series") { seriesToCancel = s }.tint(.orange)
                }
            }
        }
    }

    @ViewBuilder
    private var profileSections: some View {
        ForEach(ProfileLayout.groups, id: \.title) { group in
            let rows = group.fields.compactMap { f -> (ProfileLayout.Field, String)? in
                let v = ProfileLayout.display(f.key, profile[f.key] ?? "")
                return v.isEmpty ? nil : (f, v)
            }
            if !rows.isEmpty {
                Section(group.title) {
                    ForEach(rows, id: \.0.key) { field, value in
                        ProfileValueRow(field: field, value: value, raw: profile[field.key] ?? "")
                    }
                }
            }
        }
        Section {
            Button(profile.values.contains(where: { !$0.isEmpty }) ? "Edit profile" : "Add profile details",
                   systemImage: "person.text.rectangle") { editProfile = true }
        }
    }

    private func rangeText(_ a: String, _ b: String) -> String {
        if a.isEmpty && b.isEmpty { return "No dates" }
        if b.isEmpty { return "From \(formatDateShort(a))" }
        if a == b { return formatDateShort(a) }
        return "\(formatDateShort(a)) – \(formatDateShort(b))"
    }

    private func reload() async {
        do {
            async let a = store.absences(of: soldierId)
            async let pr = app.api.getStrings("personnel/\(soldierId)/profile")
            (absences, profile) = try await (a, pr)
            loadError = nil
        } catch {
            loadError = error.localizedDescription
        }
    }

    private func remove(_ e: ScheduledEvent) async {
        do {
            try await store.deleteAbsence(e.id)
            await store.load()
            await reload()
        } catch { message = error.localizedDescription }
    }

    private func cancelSeries(_ s: String) async {
        do {
            try await store.deleteSeries(s)
            await store.load()
            await reload()
        } catch { message = error.localizedDescription }
    }
}

struct ProfileValueRow: View {
    let field: ProfileLayout.Field
    let value: String
    let raw: String

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(field.label).font(.caption).foregroundStyle(.secondary)
            HStack {
                Text(value).textSelection(.enabled)
                Spacer()
                if field.kind == .phone {
                    if let tel = Phone.url("tel", raw) { Link(destination: tel) { Image(systemName: "phone.fill") } }
                    if let sms = Phone.url("sms", raw) { Link(destination: sms) { Image(systemName: "message.fill") } }
                } else if field.kind == .email, let url = URL(string: "mailto:\(raw)") {
                    Link(destination: url) { Image(systemName: "envelope.fill") }
                }
            }
            .buttonStyle(.borderless)
        }
    }
}

/// Add / edit a soldier (the Edit Person modal): rank, name, unit, formation.
struct EditSoldierView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @Environment(\.dismiss) private var dismiss
    let soldier: Soldier?

    @State private var rank = ""
    @State private var last = ""
    @State private var first = ""
    @State private var unitId = 0
    @State private var noFormation = false
    @State private var error: String?
    @State private var saving = false

    var body: some View {
        NavigationStack {
            Form {
                Section {
                    Picker("Rank", selection: $rank) {
                        Text("—").tag("")
                        ForEach(Ranks.order, id: \.self) { Text($0).tag($0) }
                        if !rank.isEmpty && !Ranks.order.contains(rank) { Text(rank).tag(rank) }
                    }
                    TextField("Last name", text: $last).textContentType(.familyName)
                    TextField("First name", text: $first).textContentType(.givenName)
                }
                let ids = app.tree.subtree(of: store.unitId)
                if ids.count > 1 {
                    Section("Unit") {
                        Picker("Unit", selection: $unitId) {
                            ForEach(ids, id: \.self) { id in
                                Text(app.tree.unit(id)?.name ?? "Unit \(id)").tag(id)
                            }
                        }
                    }
                }
                Section {
                    Toggle("Doesn't attend formation", isOn: $noFormation)
                } footer: {
                    Text("Skipped by formation mode; still accounted for by hand on the roster.")
                }
                if let error { Section { Text(error).foregroundStyle(.red) } }
            }
            .navigationTitle(soldier == nil ? "Add soldier" : "Edit person")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("Cancel") { dismiss() } }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") { Task { await save() } }.disabled(saving)
                }
            }
            .onAppear {
                rank = soldier?.rank ?? ""
                last = soldier?.last ?? ""
                first = soldier?.first ?? ""
                unitId = soldier?.unitId ?? store.unitId
                noFormation = soldier?.noFormation ?? false
            }
        }
    }

    private func save() async {
        let l = last.trimmingCharacters(in: .whitespaces), f = first.trimmingCharacters(in: .whitespaces)
        if l.isEmpty || f.isEmpty { error = "First and last name are required."; return }
        if let msg = validateField("last", l) ?? validateField("first", f) { error = msg; return }
        saving = true
        defer { saving = false }
        do {
            if var p = soldier {
                p.rank = rank; p.last = l; p.first = f; p.unitId = unitId; p.noFormation = noFormation
                try await store.editSoldier(p)
            } else {
                try await store.addSoldier(rank: rank, last: l, first: f, unitId: unitId, noFormation: noFormation)
            }
            dismiss()
        } catch {
            self.error = error.localizedDescription
        }
    }
}
