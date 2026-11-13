import SwiftUI

/// The accountability roster (`render()` in index.html): Needs Action, Present
/// for Duty, Away but Accounted For, FTR, Scheduled.
struct AccountabilityView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @State private var search = ""
    @State private var statusFor: StatusTarget?
    @State private var showFormation = false
    @State private var showReport = false
    @State private var showQuickAdd = false
    @State private var showAddSoldier = false
    @State private var undo: UndoMark?
    @State private var presentExpanded = false
    @State private var path = NavigationPath()

    struct UndoMark: Equatable { let id: Int; let name: String; let wasAbsence: Bool }

    var body: some View {
        @Bindable var store = store
        NavigationStack(path: $path) {
            content
                .navigationTitle(app.selectedUnit?.name ?? "Today")
                .navigationDestination(for: SoldierRoute.self) { route in
                    SoldierView(soldierId: route.id)
                }
                .searchable(text: $search, prompt: "Search name or notes")
                .refreshable {
                    await store.load()
                    await app.refreshUnits()
                }
                .toolbar { toolbar }
                .safeAreaInset(edge: .bottom) { undoBar }
                .toast($store.notice)
        }
        .sheet(item: $statusFor) { target in
            StatusSheet(target: target)
        }
        .fullScreenCover(isPresented: $showFormation) {
            FormationView()
        }
        .sheet(isPresented: $showReport) {
            ReportView()
        }
        .sheet(isPresented: $showQuickAdd) {
            QuickAddView()
        }
        .sheet(isPresented: $showAddSoldier) {
            EditSoldierView(soldier: nil)
        }
    }

    // MARK: Content

    @ViewBuilder
    private var content: some View {
        if !store.loaded {
            if let error = store.error {
                ContentUnavailableView {
                    Label("Couldn't load this roster", systemImage: "wifi.exclamationmark")
                } description: {
                    Text(error)
                } actions: {
                    Button("Try again") { Task { await store.load() } }
                }
            } else {
                ProgressView("Loading roster…")
            }
        } else {
            list
        }
    }

    private var visible: [Soldier] {
        let term = search.trimmingCharacters(in: .whitespaces).lowercased()
        guard !term.isEmpty else { return store.soldiers }
        return store.soldiers.filter {
            $0.rank.lowercased().contains(term) || $0.last.lowercased().contains(term)
                || $0.first.lowercased().contains(term) || $0.notes.lowercased().contains(term)
        }
    }

    private var list: some View {
        let today = store.today
        let people = visible
        let byRank: ([Soldier]) -> [Soldier] = { $0.sorted(by: rankLess) }
        let byEnd: ([Soldier]) -> [Soldier] = { $0.sorted(by: endDateLess) }
        let needs = byRank(people.filter { isUnaccounted($0, today: today) })
        let present = byRank(people.filter { isPresentToday($0, today: today) })
        let away = byRank(people.filter { $0.status == "late" })
            + byRank(people.filter { $0.status == "excused" })
            + byEnd(people.filter { $0.status == "leave" || $0.status == "pass" })
            + byEnd(people.filter { $0.status == "tdy" })
            + byEnd(people.filter { $0.status == "other" })
        let ftr = byEnd(people.filter { $0.status == "ftr" })
        let scheduled = byRank(people.filter { !$0.scheduledEvents.isEmpty })

        return List {
            Section {
                SummaryRow(soldiers: store.soldiers, today: today)
            }

            Section {
                if needs.isEmpty {
                    Label(search.isEmpty ? "All personnel accounted for." : "No matching personnel.",
                          systemImage: "checkmark.seal.fill")
                        .foregroundStyle(.green)
                } else {
                    ForEach(needs) { p in needsRow(p) }
                }
            } header: {
                SectionHeader(title: "Needs Action", count: needs.count, color: .orange)
            }

            if !present.isEmpty {
                Section {
                    let limit = 8
                    let shown = presentExpanded ? present : Array(present.prefix(limit))
                    ForEach(shown) { p in presentRow(p) }
                    if present.count > limit {
                        Button(presentExpanded ? "Show less" : "+ \(present.count - limit) more") {
                            withAnimation { presentExpanded.toggle() }
                        }
                    }
                } header: {
                    SectionHeader(title: "Present for Duty", count: present.count, color: .green)
                }
            }

            if !away.isEmpty {
                Section {
                    ForEach(away) { p in awayRow(p, today: today) }
                } header: {
                    SectionHeader(title: "Away but Accounted For", count: away.count, color: .blue)
                }
            }

            if !ftr.isEmpty {
                Section {
                    ForEach(ftr) { p in awayRow(p, today: today) }
                } header: {
                    SectionHeader(title: "FTR — Failure to Report", count: ftr.count, color: .red)
                }
            }

            if !scheduled.isEmpty {
                Section {
                    ForEach(scheduled) { p in scheduledRow(p) }
                } header: {
                    SectionHeader(title: "Scheduled", count: scheduled.reduce(0) { $0 + $1.scheduledEvents.count },
                                  color: .indigo)
                }
            }
        }
        .listStyle(.insetGrouped)
        .animation(.default, value: store.soldiers)
    }

    // MARK: Rows

    private func needsRow(_ p: Soldier) -> some View {
        HStack {
            NavigationLink(value: SoldierRoute(id: p.id)) {
                SoldierLine(p: p, detail: p.noFormation ? "Not at formation" : "Not yet accounted for")
            }
            Button {
                Task { await markToday(p) }
            } label: {
                Image(systemName: "checkmark.circle")
                    .font(.title2)
                    .foregroundStyle(.green)
            }
            .buttonStyle(.borderless)
            .accessibilityLabel("Mark \(p.rank) \(p.last) present")
        }
        .swipeActions(edge: .leading, allowsFullSwipe: true) {
            Button { Task { await markToday(p) } } label: { Label("Present", systemImage: "checkmark") }
                .tint(.green)
        }
        .swipeActions(edge: .trailing) {
            Button { statusFor = StatusTarget(soldierId: p.id) } label: {
                Label("Set status", systemImage: "calendar.badge.plus")
            }
            .tint(.blue)
        }
    }

    private func presentRow(_ p: Soldier) -> some View {
        NavigationLink(value: SoldierRoute(id: p.id)) {
            SoldierLine(p: p, badge: "present")
        }
        .swipeActions(edge: .trailing) {
            Button { Task { await store.unmarkPresent(p.id) } } label: {
                Label("Unmark", systemImage: "arrow.uturn.backward")
            }
            .tint(.orange)
            Button { statusFor = StatusTarget(soldierId: p.id) } label: {
                Label("Set status", systemImage: "calendar.badge.plus")
            }
            .tint(.blue)
        }
        .contextMenu {
            Button("Unmark present", systemImage: "arrow.uturn.backward") { Task { await store.unmarkPresent(p.id) } }
            Button("Set status…", systemImage: "calendar.badge.plus") { statusFor = StatusTarget(soldierId: p.id) }
        }
    }

    private func awayRow(_ p: Soldier, today: String) -> some View {
        let sameDay = isSameDayState(p)
        let range = (!sameDay && (!p.from.isEmpty || !p.to.isEmpty))
            ? "\(formatDateShort(p.from)) – \(formatDateShort(p.to))" : ""
        let returning = !sameDay && !p.to.isEmpty && p.to == today
        let detail = [p.notes, range].filter { !$0.isEmpty }.joined(separator: " · ")
        return HStack {
            NavigationLink(value: SoldierRoute(id: p.id)) {
                SoldierLine(p: p, detail: detail, badge: p.status, returning: returning)
            }
            if p.status == "late" {
                Button {
                    Task { await markPresentFromAbsence(p) }
                } label: {
                    Image(systemName: "checkmark.circle").font(.title2).foregroundStyle(.green)
                }
                .buttonStyle(.borderless)
                .accessibilityLabel("Arrived: mark \(p.rank) \(p.last) present")
            }
        }
        .swipeActions(edge: .leading, allowsFullSwipe: true) {
            Button { Task { await markPresentFromAbsence(p) } } label: {
                Label("Present", systemImage: "checkmark")
            }
            .tint(.green)
        }
        .swipeActions(edge: .trailing) {
            Button { statusFor = StatusTarget(soldierId: p.id) } label: {
                Label("Update", systemImage: "square.and.pencil")
            }
            .tint(.blue)
        }
    }

    private func scheduledRow(_ p: Soldier) -> some View {
        let lines = p.scheduledEvents.sorted { ($0.from.isEmpty ? "9999" : $0.from) < ($1.from.isEmpty ? "9999" : $1.from) }
            .map { e -> String in
                let range = (!e.from.isEmpty || !e.to.isEmpty) ? " · \(formatDateShort(e.from)) – \(formatDateShort(e.to))" : ""
                let notes = (e.notes ?? "").isEmpty ? "" : ": \(e.notes ?? "")"
                return "\(statusLabel(e.status))\(notes)\(range)"
            }
        return NavigationLink(value: SoldierRoute(id: p.id)) {
            SoldierLine(p: p, detail: lines.joined(separator: "\n"),
                        badge: "scheduled", badgeLabel: "\(p.scheduledEvents.count) Scheduled")
        }
    }

    // MARK: Actions

    private func markToday(_ p: Soldier) async {
        Haptics.success()
        if await store.markPresentToday(p.id) {
            withAnimation { undo = UndoMark(id: p.id, name: "\(p.rank) \(p.last)", wasAbsence: false) }
        }
    }

    private func markPresentFromAbsence(_ p: Soldier) async {
        Haptics.success()
        if await store.markPresent(p.id) {
            withAnimation { undo = UndoMark(id: p.id, name: "\(p.rank) \(p.last)", wasAbsence: true) }
        }
    }

    @ViewBuilder
    private var undoBar: some View {
        if let undo {
            HStack {
                Text("\(undo.name) marked present").font(.subheadline)
                Spacer()
                if !undo.wasAbsence {
                    Button("Undo") {
                        Task {
                            await store.unmarkPresent(undo.id)
                            withAnimation { self.undo = nil }
                        }
                    }
                    .bold()
                }
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 12)
            .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 14))
            .padding(.horizontal, 12)
            .padding(.bottom, 6)
            .transition(.move(edge: .bottom).combined(with: .opacity))
            .task(id: undo) {
                try? await Task.sleep(for: .seconds(5))
                withAnimation { self.undo = nil }
            }
        }
    }

    @ToolbarContentBuilder
    private var toolbar: some ToolbarContent {
        ToolbarItem(placement: .topBarLeading) {
            Button {
                showFormation = true
            } label: {
                Label("Formation", systemImage: "person.3.sequence")
            }
        }
        ToolbarItemGroup(placement: .topBarTrailing) {
            Button {
                showReport = true
            } label: {
                Label("Report", systemImage: "doc.text")
            }
            Menu {
                Button("Formation mode", systemImage: "person.3.sequence") { showFormation = true }
                Button("Generate report", systemImage: "doc.text") { showReport = true }
                if app.me?.quickAdd == true {
                    Button("Quick add absence…", systemImage: "sparkles") { showQuickAdd = true }
                }
                Button("Add soldier", systemImage: "person.badge.plus") { showAddSoldier = true }
                Divider()
                UnitSwitcher()
            } label: {
                Label("More", systemImage: "ellipsis.circle")
            }
        }
    }
}

struct SoldierRoute: Hashable { let id: Int }

/// What the status sheet is for: one soldier, optionally one existing absence,
/// optionally pre-filled by quick add.
struct StatusTarget: Identifiable {
    let soldierId: Int
    var editing: ScheduledEvent? = nil
    var proposal: QuickProposal? = nil
    var onDone: (@MainActor () -> Void)? = nil
    var id: String { "\(soldierId)-\(editing?.id ?? 0)-\(proposal?.id ?? "")" }
}

struct SectionHeader: View {
    let title: String
    let count: Int
    let color: Color

    var body: some View {
        HStack {
            Circle().fill(color).frame(width: 8, height: 8)
            Text(title)
            Spacer()
            Text("\(count)").monospacedDigit()
        }
    }
}

struct SoldierLine: View {
    let p: Soldier
    var detail: String = ""
    var badge: String? = nil
    var badgeLabel: String? = nil
    var returning = false

    var body: some View {
        VStack(alignment: .leading, spacing: 3) {
            HStack(spacing: 6) {
                Text(p.rank).font(.subheadline.weight(.semibold)).foregroundStyle(.secondary)
                    .frame(minWidth: 36, alignment: .leading)
                Text(lastFirst(p)).font(.body)
                Spacer(minLength: 4)
                if let badge {
                    StatusBadge(status: badge, label: badgeLabel)
                }
            }
            if returning {
                Text("RETURNING TODAY").font(.caption2.weight(.bold)).foregroundStyle(.green)
            }
            if !detail.isEmpty {
                Text(detail).font(.caption).foregroundStyle(.secondary).lineLimit(3)
            }
        }
        .padding(.vertical, 2)
    }
}

/// Today's tally across the roster: present / assigned and the unaccounted count.
struct SummaryRow: View {
    let soldiers: [Soldier]
    let today: String

    var body: some View {
        let present = soldiers.filter { isPresentToday($0, today: today) }.count
        let unaccounted = soldiers.filter { isUnaccounted($0, today: today) }.count
        let away = soldiers.count - present - unaccounted
        HStack(spacing: 0) {
            stat("\(present)", "Present", .green)
            stat("\(away)", "Away", .blue)
            stat("\(unaccounted)", "Unaccounted", unaccounted > 0 ? .orange : .secondary)
            stat("\(soldiers.count)", "Assigned", .primary)
        }
        .padding(.vertical, 4)
    }

    private func stat(_ n: String, _ label: String, _ color: Color) -> some View {
        VStack(spacing: 2) {
            Text(n).font(.title2.bold()).monospacedDigit().foregroundStyle(color)
            Text(label).font(.caption).foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity)
    }
}
