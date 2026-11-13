import SwiftUI

/// The directory: everyone in the unit's subtree with MOS, section and phone;
/// tap to call or text, or open the soldier.
struct DirectoryView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @State private var entries: [DirectoryEntry] = []
    @State private var search = ""
    @State private var error: String?
    @State private var loaded = false
    @State private var sort: Sort = .rank

    enum Sort: String, CaseIterable { case rank = "Rank", name = "Name", section = "Section" }

    var body: some View {
        NavigationStack {
            List {
                if let error { Text(error).foregroundStyle(.red) }
                ForEach(visible) { e in
                    NavigationLink(value: SoldierRoute(id: e.id)) {
                        row(e)
                    }
                    .swipeActions(edge: .leading) {
                        if let tel = Phone.url("tel", e.phone) {
                            Link(destination: tel) { Label("Call", systemImage: "phone.fill") }.tint(.green)
                        }
                        if let sms = Phone.url("sms", e.phone) {
                            Link(destination: sms) { Label("Text", systemImage: "message.fill") }.tint(.blue)
                        }
                    }
                }
            }
            .overlay {
                if !loaded && error == nil { ProgressView() }
                else if loaded && visible.isEmpty && error == nil {
                    ContentUnavailableView.search(text: search)
                }
            }
            .navigationTitle("People")
            .navigationDestination(for: SoldierRoute.self) { SoldierView(soldierId: $0.id) }
            .searchable(text: $search, prompt: "Name, MOS, section, phone")
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Menu {
                        Picker("Sort", selection: $sort) {
                            ForEach(Sort.allCases, id: \.self) { Text($0.rawValue).tag($0) }
                        }
                    } label: {
                        Label("Sort", systemImage: "arrow.up.arrow.down")
                    }
                }
            }
            .task { await load() }
            .refreshable { await load() }
        }
    }

    private var visible: [DirectoryEntry] {
        let term = search.trimmingCharacters(in: .whitespaces).lowercased()
        let filtered = term.isEmpty ? entries : entries.filter { e in
            [e.rank, e.last, e.first, e.mos, e.section, e.phone, e.dodId].compactMap { $0?.lowercased() }
                .contains { $0.contains(term) }
        }
        switch sort {
        case .rank:
            return filtered.sorted { rankLess(soldier($0), soldier($1)) }
        case .name:
            return filtered.sorted { lastFirst(soldier($0)).localizedCaseInsensitiveCompare(lastFirst(soldier($1))) == .orderedAscending }
        case .section:
            return filtered.sorted {
                let a = $0.section ?? "", b = $1.section ?? ""
                if a != b { return a.isEmpty ? false : (b.isEmpty ? true : a.localizedCompare(b) == .orderedAscending) }
                return rankLess(soldier($0), soldier($1))
            }
        }
    }

    private func soldier(_ e: DirectoryEntry) -> Soldier {
        Soldier(id: e.id, rank: e.rank ?? "", last: e.last ?? "", first: e.first ?? "", unitId: e.unitId ?? 0)
    }

    private func row(_ e: DirectoryEntry) -> some View {
        let p = soldier(e)
        let meta = [e.mos, e.section, app.tree.unit(e.unitId ?? 0).flatMap { $0.id == store.unitId ? nil : $0.name }]
            .compactMap { $0 }.filter { !$0.isEmpty }.joined(separator: " · ")
        return HStack {
            VStack(alignment: .leading, spacing: 2) {
                HStack(spacing: 6) {
                    Text(p.rank).font(.subheadline.weight(.semibold)).foregroundStyle(.secondary)
                    Text(lastFirst(p))
                }
                if !meta.isEmpty { Text(meta).font(.caption).foregroundStyle(.secondary) }
                if let phone = e.phone, !phone.isEmpty {
                    Text(formatPhone(phone)).font(.caption).foregroundStyle(.tint)
                }
            }
            Spacer()
            if let s = e.status, s != "present" { StatusBadge(status: s) }
            if let next = e.nextAbsence {
                Image(systemName: "calendar.badge.clock")
                    .foregroundStyle(.indigo)
                    .accessibilityLabel("Next: \(statusLabel(next.status)) from \(formatDateShort(next.fromDate ?? ""))")
            }
        }
    }

    private func load() async {
        do {
            entries = try await app.api.get("directory", query: ["unit": String(store.unitId)])
            error = nil
        } catch {
            self.error = error.localizedDescription
        }
        loaded = true
    }
}
