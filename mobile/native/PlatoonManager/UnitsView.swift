import SwiftUI

/// The unit tree (the web's org chart): each unit with today's present /
/// assigned for its whole subtree and an unaccounted badge. Tap to open it.
struct UnitsView: View {
    @Environment(AppModel.self) private var app
    var onPick: () -> Void

    var body: some View {
        NavigationStack {
            List {
                if let err = app.unitsError { Text(err).foregroundStyle(.red) }
                let tree = app.tree
                Section {
                    ForEach(roots(tree), id: \.self) { id in
                        UnitBranch(id: id, depth: 0, tree: tree, onPick: pick)
                    }
                } footer: {
                    Text("★ is your primary unit: the app opens on it. Swipe or touch and hold a unit to change it.")
                }
            }
            .navigationTitle("Units")
            .refreshable { await app.refreshUnits() }
            .task { await app.refreshUnits() }
        }
    }

    /// The user's own unit is the top of what they can see.
    private func roots(_ tree: UnitTree) -> [Int] {
        if let mine = app.me?.unitId, tree.unit(mine) != nil { return [mine] }
        let ids = Set(app.units.map(\.id))
        return app.units.filter { $0.parentId == nil || !ids.contains($0.parentId!) }.map(\.id)
    }

    private func pick(_ id: Int) {
        Haptics.tap()
        app.selectedUnitId = id
        onPick()
    }
}

private struct UnitBranch: View {
    @Environment(AppModel.self) private var app
    let id: Int
    let depth: Int
    let tree: UnitTree
    let onPick: (Int) -> Void

    var body: some View {
        if let u = tree.unit(id) {
            row(u)
            ForEach(tree.children(of: id).map(\.id), id: \.self) { child in
                UnitBranch(id: child, depth: depth + 1, tree: tree, onPick: onPick)
            }
        }
    }

    private func row(_ u: UnitNode) -> some View {
        let assigned = tree.headcount(of: u.id)
        let counts = tree.todayCounts(of: u.id)
        let selected = app.selectedUnitId == u.id
        return Button {
            onPick(u.id)
        } label: {
            HStack(spacing: 10) {
                VStack(alignment: .leading, spacing: 2) {
                    HStack(spacing: 4) {
                        Text(u.name).font(depth == 0 ? .headline : .body).foregroundStyle(.primary)
                        if app.primaryUnitId == u.id {
                            Image(systemName: "star.fill").font(.caption).foregroundStyle(.yellow)
                                .accessibilityLabel("Primary unit")
                        }
                    }
                    Text(u.kind.capitalized).font(.caption).foregroundStyle(.secondary)
                }
                Spacer()
                if let counts {
                    VStack(alignment: .trailing, spacing: 2) {
                        Text("\(counts.present) / \(assigned)").monospacedDigit().foregroundStyle(.primary)
                        if counts.unaccounted > 0 {
                            Text("\(counts.unaccounted) unaccounted")
                                .font(.caption2.weight(.semibold))
                                .foregroundStyle(.orange)
                        } else if assigned > 0 {
                            Text("all accounted").font(.caption2).foregroundStyle(.green)
                        }
                    }
                } else {
                    Text("\(assigned) personnel").font(.caption).foregroundStyle(.secondary)
                }
                if selected { Image(systemName: "checkmark").foregroundStyle(.tint) }
            }
            .padding(.leading, CGFloat(depth) * 18)
        }
    }
}
