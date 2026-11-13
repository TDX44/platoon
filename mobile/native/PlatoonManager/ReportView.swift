import SwiftUI

/// Generate the accountability report (`buildReport`), copy or share it, and
/// file it in report history once per distinct text per duty day.
struct ReportView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @Environment(\.dismiss) private var dismiss

    @AppStorage("report.format") private var formatRaw = "detailed"
    @AppStorage("report.splitLeavePass") private var splitLeavePass = false
    @State private var includeUnaccounted = false
    @State private var copied = false

    private var format: ReportOptions.Format { formatRaw == "strength" ? .strength : .detailed }
    private var unaccounted: [Soldier] {
        store.soldiers.filter { isUnaccounted($0, today: store.today) }.sorted(by: rankLess)
    }

    private var text: String {
        let unit = app.selectedUnit
        let tree = app.tree
        let groups = unit.map { u in
            tree.children(of: u.id).map { ReportGroup(name: $0.name, ids: tree.subtree(of: $0.id)) }
        } ?? []
        let ctx = ReportContext(unitName: unit?.name ?? "", dateLabel: formatReportDate(Date(), timeZone: app.timeZone),
                                today: store.today, groups: groups)
        return buildReport(store.soldiers, ReportOptions(format: format, splitLeavePass: splitLeavePass), ctx)
    }

    var body: some View {
        NavigationStack {
            Group {
                if !unaccounted.isEmpty && !includeUnaccounted {
                    unaccountedGate
                } else {
                    report
                }
            }
            .navigationTitle(format == .strength ? "Strength Report" : "Detailed Report")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("Done") { dismiss() } }
            }
        }
    }

    /// `reportUnaccountedOk()`: anyone unaccounted is a choice, never a silent drop.
    private var unaccountedGate: some View {
        List {
            Section {
                Text("They will be listed on an Unaccounted line in the report.")
            } header: {
                Text("\(unaccounted.count) not yet accounted for")
            }
            Section {
                ForEach(unaccounted) { p in Text("\(p.rank) \(lastFirst(p))") }
            }
            Section {
                Button("Include as Unaccounted") { includeUnaccounted = true }
                Button("Go mark them", role: .cancel) { dismiss() }
            }
        }
    }

    private var report: some View {
        let text = self.text
        return VStack(spacing: 0) {
            // Sized by its content, never a fixed height: a fixed-height Form
            // clipped the toggle behind the report at larger text sizes.
            VStack(spacing: 12) {
                Picker("Format", selection: $formatRaw) {
                    Text("Detailed").tag("detailed")
                    Text("Strength").tag("strength")
                }
                .pickerStyle(.segmented)
                Toggle("Leave and Pass separate", isOn: $splitLeavePass)
            }
            .padding(.horizontal)
            .padding(.vertical, 12)
            .fixedSize(horizontal: false, vertical: true)
            Divider()
            ScrollView {
                Text(text)
                    .font(.system(.footnote, design: .monospaced))
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding()
            }
            .background(Color(.secondarySystemBackground))
            HStack(spacing: 12) {
                Button {
                    UIPasteboard.general.string = text
                    Haptics.success()
                    copied = true
                    Task { await save(text) }
                } label: {
                    Label(copied ? "Copied" : "Copy", systemImage: copied ? "checkmark" : "doc.on.doc")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                ShareLink(item: text) {
                    Label("Share", systemImage: "square.and.arrow.up").frame(maxWidth: .infinity)
                }
                .buttonStyle(.bordered)
                .simultaneousGesture(TapGesture().onEnded { Task { await save(text) } })
            }
            .controlSize(.large)
            .padding()
        }
        .task(id: text) {
            copied = false
            await save(text)
        }
    }

    private func save(_ text: String) async {
        await store.saveToHistory(text, unitName: app.selectedUnit?.name ?? "")
    }
}

/// Report history: every saved report for this unit's subtree.
struct ReportHistoryView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @State private var reports: [ReportRecord] = []
    @State private var error: String?
    @State private var loaded = false

    var body: some View {
        List {
            if let error { Text(error).foregroundStyle(.red) }
            if loaded && reports.isEmpty {
                ContentUnavailableView("No reports yet", systemImage: "doc.text",
                                       description: Text("Reports you generate or copy are saved here."))
            }
            ForEach(groupedByDay, id: \.day) { group in
                Section(group.day) {
                    ForEach(group.items) { r in
                        NavigationLink {
                            ReportDetailView(record: r, onDelete: { reports.removeAll { $0.id == r.id } })
                        } label: {
                            VStack(alignment: .leading) {
                                Text(r.unitName?.isEmpty == false ? r.unitName! : "Report")
                                Text([time(r.createdAt), r.createdBy ?? ""].filter { !$0.isEmpty }.joined(separator: " · "))
                                    .font(.caption).foregroundStyle(.secondary)
                            }
                        }
                    }
                }
            }
        }
        .navigationTitle("Report history")
        .task { await load() }
        .refreshable { await load() }
    }

    private var groupedByDay: [(day: String, items: [ReportRecord])] {
        var order: [String] = []
        var map: [String: [ReportRecord]] = [:]
        for r in reports {
            let day = String((r.createdAt ?? "").prefix(10))
            if map[day] == nil { order.append(day) }
            map[day, default: []].append(r)
        }
        return order.map { (DayMath.long($0), map[$0] ?? []) }
    }

    private func time(_ stamp: String?) -> String {
        guard let s = stamp, s.count >= 16 else { return "" }
        return String(s.dropFirst(11).prefix(5))
    }

    private func load() async {
        do {
            reports = try await app.api.get("reports", query: ["unit": String(store.unitId)])
            error = nil
        } catch {
            self.error = error.localizedDescription
        }
        loaded = true
    }
}

struct ReportDetailView: View {
    @Environment(AppModel.self) private var app
    @Environment(\.dismiss) private var dismiss
    let record: ReportRecord
    let onDelete: () -> Void
    @State private var text = ""
    @State private var error: String?
    @State private var confirmDelete = false

    var body: some View {
        ScrollView {
            Text(text)
                .font(.system(.footnote, design: .monospaced))
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding()
            if let error { Text(error).foregroundStyle(.red).padding() }
        }
        .navigationTitle(record.unitName ?? "Report")
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItemGroup(placement: .primaryAction) {
                Button("Copy", systemImage: "doc.on.doc") {
                    UIPasteboard.general.string = text
                    Haptics.success()
                }
                ShareLink(item: text)
                if canDelete {
                    Button("Delete", systemImage: "trash", role: .destructive) { confirmDelete = true }
                }
            }
        }
        .confirmationDialog("Delete this report?", isPresented: $confirmDelete, titleVisibility: .visible) {
            Button("Delete", role: .destructive) {
                Task {
                    do {
                        try await app.api.delete("reports/\(record.id)")
                        onDelete()
                        dismiss()
                    } catch { self.error = error.localizedDescription }
                }
            }
        }
        .task {
            do {
                let full: ReportRecord = try await app.api.get("reports/\(record.id)")
                text = full.text ?? ""
            } catch { self.error = error.localizedDescription }
        }
    }

    /// The server allows the author or an owner.
    private var canDelete: Bool {
        app.me?.isOwner == true || record.createdBy == app.me?.username
    }
}
