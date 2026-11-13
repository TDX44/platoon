import Foundation

// Ported from index.html: unitById(), unitBySlug(), unitChildren(),
// unitSubtree(), unitHeadcount(), unitTodayCounts(), unitTreeOrder().
// Spec: tests/test_unit_tree_js.py. The web client reads a global `units`;
// here the listing is the value the struct is built from.

struct UnitTodayCounts: Sendable, Equatable {
    var present: Int
    var unaccounted: Int
}

struct UnitTree: Sendable, Equatable {
    /// `GET /api/units`: the caller's subtree, flat, with parent pointers.
    let units: [UnitNode]

    init(units: [UnitNode]) {
        self.units = units
    }

    func unit(id: Int) -> UnitNode? {
        units.first { $0.id == id }
    }

    /// `unitById`, unlabeled.
    func unit(_ id: Int) -> UnitNode? { unit(id: id) }

    func unit(slug: String) -> UnitNode? {
        units.first { $0.slug == slug }
    }

    /// Direct children, sorted by name (`localeCompare`, stable), which is the
    /// order the home tree renders: "1st Platoon" before "2nd Platoon".
    func children(of id: Int) -> [UnitNode] {
        units.filter { $0.parentId == id }
            .stableSorted { Collation.compare($0.name, $1.name) }
    }

    /// `id` itself, then its descendants breadth-first. The id is included
    /// even when it is not in the listing, as in the JS.
    func subtree(of id: Int) -> [Int] {
        var out = [id]
        var seen: Set<Int> = [id]          // a guard the JS lacks; a parent cycle would loop it forever
        var i = 0
        while i < out.count {
            for u in children(of: out[i]) where seen.insert(u.id).inserted { out.append(u.id) }
            i += 1
        }
        return out
    }

    /// Personnel in the whole subtree (each unit's own `count`, nil as 0).
    func headcount(of id: Int) -> Int {
        subtree(of: id).reduce(0) { $0 + (unit(id: $1)?.count ?? 0) }
    }

    /// Today's present / unaccounted rolled up over the subtree, or nil when
    /// the listing carries no counts (any unit missing `present`).
    func todayCounts(of id: Int) -> UnitTodayCounts? {
        let tree = subtree(of: id).compactMap { unit(id: $0) }
        if tree.isEmpty || tree.contains(where: { $0.present == nil }) { return nil }
        return tree.reduce(UnitTodayCounts(present: 0, unaccounted: 0)) {
            UnitTodayCounts(present: $0.present + ($1.present ?? 0),
                            unaccounted: $0.unaccounted + ($1.unaccounted ?? 0))
        }
    }

    /// Depth-first position of every unit under `root` (the root itself is 0,
    /// so anyone directly in the unit, its HQ, comes first). Feed it to
    /// `formationGroupOf(order:)`.
    func treeOrder(root: Int) -> [Int: Int] {
        var order: [Int: Int] = [:]
        func walk(_ id: Int) {
            guard order[id] == nil else { return }   // cycle guard, as in subtree(of:)
            order[id] = order.count
            for k in children(of: id) { walk(k.id) }
        }
        walk(root)
        return order
    }
}
