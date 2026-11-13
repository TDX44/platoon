import SwiftUI

/// The signed-in app: one roster store for the unit being looked at, shared by
/// every tab, and recreated when the leader switches unit.
struct MainTabs: View {
    @Environment(AppModel.self) private var app
    @State private var store: RosterStore?
    @State private var tab: Tab = .today

    enum Tab: Hashable { case today, units, people, plan, more }

    var body: some View {
        Group {
            if let store {
                TabView(selection: $tab) {
                    AccountabilityView()
                        .tabItem { Label("Today", systemImage: "checklist") }
                        .tag(Tab.today)
                    UnitsView(onPick: { tab = .today })
                        .tabItem { Label("Units", systemImage: "point.3.connected.trianglepath.dotted") }
                        .tag(Tab.units)
                    DirectoryView()
                        .tabItem { Label("People", systemImage: "person.2") }
                        .tag(Tab.people)
                    PlanView()
                        .tabItem { Label("Plan", systemImage: "calendar") }
                        .tag(Tab.plan)
                    MoreView()
                        .tabItem { Label("More", systemImage: "ellipsis.circle") }
                        .tag(Tab.more)
                }
                .environment(store)
                .id(store.unitId)
            } else {
                ProgressView()
            }
        }
        .task(id: app.selectedUnitId) {
            guard let id = app.selectedUnitId else { return }
            if store?.unitId != id {
                let s = RosterStore(unitId: id, app: app)
                store = s
                await s.load()
            }
        }
    }
}

/// A unit-switcher for the navigation bar: every unit in the subtree, indented.
struct UnitSwitcher: View {
    @Environment(AppModel.self) private var app

    var body: some View {
        let tree = app.tree
        let rootId = app.me?.unitId ?? app.selectedUnitId ?? 0
        let order = tree.unit(rootId) != nil ? tree.subtree(of: rootId) : app.units.map(\.id)
        if order.count > 1 {
            Menu {
                ForEach(order, id: \.self) { id in
                    if let u = tree.unit(id) {
                        Button {
                            app.selectedUnitId = id
                        } label: {
                            if id == app.selectedUnitId {
                                Label(indent(u, tree) + u.name, systemImage: "checkmark")
                            } else {
                                Text(indent(u, tree) + u.name)
                            }
                        }
                    }
                }
            } label: {
                Label("Switch unit", systemImage: "arrow.triangle.branch")
            }
        }
    }

    private func indent(_ u: UnitNode, _ tree: UnitTree) -> String {
        var depth = 0
        var cur = u.parentId
        while let p = cur, let parent = tree.unit(p), depth < 8 {
            depth += 1
            cur = parent.parentId
        }
        return String(repeating: "   ", count: max(0, depth - baseDepth(tree)))
    }

    private func baseDepth(_ tree: UnitTree) -> Int {
        guard let root = app.me?.unitId, let u = tree.unit(root) else { return 0 }
        var depth = 0
        var cur = u.parentId
        while let p = cur, let parent = tree.unit(p), depth < 8 {
            depth += 1
            cur = parent.parentId
        }
        return depth
    }
}
