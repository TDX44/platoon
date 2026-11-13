import SwiftUI

/// Formation mode: one card at a time, unaccounted only, in unit-tree then rank
/// order (`formationQueue`). Writes only through the roster's own calls.
struct FormationView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @Environment(\.dismiss) private var dismiss

    @State private var ids: [Int] = []
    @State private var known: [Int] = []
    @State private var index = 0
    @State private var asking = false
    @State private var reasonFor: String?
    @State private var reason = ""
    @State private var busy = false
    @State private var ready = false
    @State private var confirmRest = false
    @State private var showReport = false
    @State private var message: String?

    var body: some View {
        NavigationStack {
            Group {
                if !ready {
                    ProgressView("Loading today's roster…")
                } else if index >= ids.count {
                    done
                } else if let p = store.soldier(ids[index]) {
                    card(p)
                } else {
                    Color.clear.onAppear { index += 1 }
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(Color(.systemGroupedBackground))
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button(index >= ids.count && ready ? "Close" : "Exit") { exit() }
                }
                ToolbarItem(placement: .principal) {
                    if ready { Text("\(min(index + 1, max(ids.count, 1))) of \(ids.count)").monospacedDigit().bold() }
                }
                ToolbarItem(placement: .primaryAction) {
                    if ready && !asking && index < ids.count && remaining.count > 1 {
                        Button("Everyone else present") { confirmRest = true }
                    }
                }
            }
            .safeAreaInset(edge: .top) {
                if ready {
                    ProgressView(value: Double(min(index, ids.count)), total: Double(max(ids.count, 1)))
                        .padding(.horizontal)
                }
            }
            .confirmationDialog("Mark \(remaining.count) present?", isPresented: $confirmRest, titleVisibility: .visible) {
                Button("Mark present") { Task { await restPresent() } }
            } message: {
                Text("Everyone still to be asked (\(remaining.count)) is marked present for today. Anyone already away stays away.")
            }
            .toast($message)
        }
        .task { await start() }
        .sheet(isPresented: $showReport) { ReportView() }
    }

    private var remaining: [Soldier] {
        formationRemaining(ids: ids, from: index, people: store.soldiers, today: store.today)
    }

    // MARK: Card

    private func card(_ p: Soldier) -> some View {
        let unit = p.unitId != store.unitId ? app.tree.unit(p.unitId) : nil
        return VStack(spacing: 24) {
            Spacer()
            VStack(spacing: 8) {
                Text("\(p.rank) \(p.last)")
                    .font(.system(size: 44, weight: .bold))
                    .multilineTextAlignment(.center)
                    .minimumScaleFactor(0.5)
                if !p.first.isEmpty { Text(p.first).font(.title2).foregroundStyle(.secondary) }
                if let unit { Text(unit.name).font(.headline).foregroundStyle(.tint) }
                if asking && reasonFor == nil {
                    Text("Why are they not here?").font(.title3).padding(.top, 8)
                }
            }
            .padding(.horizontal)
            Spacer()
            if let s = reasonFor, let cfg = reasonFields[s] {
                reasonForm(s, cfg)
            } else if asking {
                LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 12) {
                    ForEach(formationReasons, id: \.self) { s in
                        Button {
                            pickReason(s)
                        } label: {
                            Label(statusLabel(s), systemImage: StatusStyle.symbol(s))
                                .frame(maxWidth: .infinity, minHeight: 56)
                        }
                        .buttonStyle(.bordered)
                        .tint(StatusStyle.color(s))
                    }
                }
                .padding(.horizontal)
            } else {
                HStack(spacing: 16) {
                    Button {
                        Haptics.tap()
                        withAnimation { asking = true }
                    } label: {
                        Text("Not here").font(.title3.bold()).frame(maxWidth: .infinity, minHeight: 72)
                    }
                    .buttonStyle(.bordered)
                    .tint(.orange)
                    Button {
                        Task { await mark("present") }
                    } label: {
                        Text("Present").font(.title3.bold()).frame(maxWidth: .infinity, minHeight: 72)
                    }
                    .buttonStyle(.borderedProminent)
                    .tint(.green)
                }
                .padding(.horizontal)
            }
            backButton
                .padding(.bottom, 12)
        }
        .disabled(busy)
        .id(p.id)
        .transition(.asymmetric(insertion: .move(edge: .trailing), removal: .move(edge: .leading)).combined(with: .opacity))
    }

    private func reasonForm(_ s: String, _ cfg: ReasonField) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("\(cfg.label) (optional)").font(.headline)
            TextField(cfg.placeholder, text: $reason)
                .textFieldStyle(.roundedBorder)
                .submitLabel(.done)
                .onSubmit { Task { await mark(s, notes: reason) } }
            ScrollView(.horizontal, showsIndicators: false) {
                HStack {
                    ForEach(cfg.options, id: \.self) { o in
                        Button(o) { reason = o }.buttonStyle(.bordered).controlSize(.small)
                    }
                }
            }
            HStack(spacing: 12) {
                Button("Skip") { Task { await mark(s, notes: "") } }
                    .buttonStyle(.bordered).frame(maxWidth: .infinity)
                Button("Save \(statusLabel(s))") { Task { await mark(s, notes: reason) } }
                    .buttonStyle(.borderedProminent).frame(maxWidth: .infinity)
            }
        }
        .padding(.horizontal)
    }

    @ViewBuilder
    private var backButton: some View {
        if reasonFor != nil {
            Button("← Pick another reason") { reasonFor = nil }
        } else if asking {
            Button("Cancel") { withAnimation { asking = false } }
        } else if index > 0, let prev = store.soldier(ids[index - 1]) {
            Button("← Back to \(prev.rank) \(prev.last)") { withAnimation { index -= 1 } }
        }
    }

    // MARK: Done

    private var done: some View {
        let roster = store.soldiers.filter { !$0.noFormation }
        let present = roster.filter { isPresentToday($0, today: store.today) }.count
        let away = roster.filter { $0.status != "present" }.count
        let left = roster.count - present - away
        let stillKnown = known.compactMap { store.soldier($0) }.filter { $0.status != "present" }
        return List {
            Section {
                VStack(spacing: 8) {
                    Image(systemName: left == 0 ? "checkmark.seal.fill" : "exclamationmark.circle.fill")
                        .font(.system(size: 48))
                        .foregroundStyle(left == 0 ? .green : .orange)
                    Text(left == 0 ? "Accountability complete" : "\(left) still unaccounted")
                        .font(.title2.bold())
                    Text(ids.isEmpty ? "Everyone was already accounted for."
                                     : "\(ids.count) soldier\(ids.count == 1 ? "" : "s") counted just now.")
                        .foregroundStyle(.secondary)
                    HStack(spacing: 24) {
                        tally(present, "present", .green)
                        tally(away, "away", .blue)
                        tally(roster.count, "assigned", .primary)
                    }
                    .padding(.top, 4)
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 8)
            }
            if !stillKnown.isEmpty {
                Section("Already on the books — not asked") {
                    ForEach(stillKnown) { p in
                        HStack {
                            VStack(alignment: .leading) {
                                Text("\(p.rank) \(lastFirst(p))")
                                Text(statusLabel(p.status)).font(.caption).foregroundStyle(.secondary)
                            }
                            Spacer()
                            Button("Actually here") { Task { await knownPresent(p.id) } }
                                .buttonStyle(.bordered)
                                .disabled(busy)
                        }
                    }
                }
            }
            Section {
                Button {
                    showReport = true
                } label: {
                    Label("Report", systemImage: "doc.text").frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                .listRowBackground(Color.clear)
                Button("Back to roster") { exit() }.frame(maxWidth: .infinity)
            }
        }
    }

    private func tally(_ n: Int, _ label: String, _ color: Color) -> some View {
        VStack {
            Text("\(n)").font(.title.bold()).monospacedDigit().foregroundStyle(color)
            Text(label).font(.caption).foregroundStyle(.secondary)
        }
    }

    // MARK: Logic

    /// `startFormation()`: build the queue from today's roster, not a stale one.
    private func start() async {
        await store.load()
        let order = app.tree.treeOrder(root: store.unitId)
        let split = formationQueue(store.soldiers, today: store.today) { p in order[p.unitId] ?? order.count }
        ids = split.queue.map(\.id)
        known = split.known.map(\.id)
        index = 0
        ready = true
    }

    private func pickReason(_ s: String) {
        Haptics.tap()
        if reasonFields[s] == nil {
            Task { await mark(s) }
        } else {
            reason = ""
            reasonFor = s
        }
    }

    private func mark(_ status: String, notes: String = "") async {
        guard !busy, index < ids.count else { return }
        busy = true
        let id = ids[index]
        let ok = status == "present" ? await store.markPresent(id, reload: false) : await store.bookToday(id, status: status, notes: notes)
        busy = false
        if ok {
            if status == "present" { Haptics.success() } else { Haptics.tap() }
            asking = false
            reasonFor = nil
            withAnimation { index += 1 }
        } else {
            Haptics.warning()
            message = store.notice ?? "Could not save. Try again."
        }
    }

    private func restPresent() async {
        guard !busy else { return }
        let rest = remaining
        guard !rest.isEmpty else { return }
        busy = true
        var failed = 0
        await withTaskGroup(of: Bool.self) { group in
            for p in rest { group.addTask { await store.guardedPresent(p.id) } }
            for await ok in group where !ok { failed += 1 }
        }
        busy = false
        if failed > 0 {
            message = "\(failed) could not be marked present."
            if let next = ids.indices.first(where: { $0 >= index && store.soldier(ids[$0]).map { isUnaccounted($0, today: store.today) } == true }) {
                index = next
            }
        } else {
            Haptics.success()
            message = "\(rest.count) marked present."
            withAnimation { index = ids.count }
        }
        asking = false
        reasonFor = nil
    }

    private func knownPresent(_ id: Int) async {
        busy = true
        _ = await store.markPresent(id, reload: false)
        busy = false
    }

    private func exit() {
        Task { await store.load() }
        dismiss()
    }
}
