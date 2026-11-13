import Foundation

// Ported from index.html: formationQueue(), formationRemaining(), formationTally().
// Spec: tests/test_formation_order.py.

struct FormationSplit: Sendable, Equatable {
    /// Who gets asked, in the order they are asked.
    var queue: [Soldier]
    /// Already away on a current absence: not asked, shown on the finish screen.
    var known: [Soldier]
}

/// The only rule about who gets asked at formation and in what order.
/// `groupOf` is a number: at a unit with sub-units pass the soldier's unit's
/// position from `UnitTree.treeOrder(root:)` (unknown units after every known
/// one, as `startFormation()` does), so the queue walks the tree in order,
/// senior first inside each. Omitted, it is one group and the order is
/// `rankSort` alone. `known` is always a plain rank list.
func formationQueue(_ people: [Soldier], today: String,
                    groupOf: (Soldier) -> Int = { _ in 0 }) -> FormationSplit {
    var queue: [Soldier] = [], known: [Soldier] = []
    for p in people {
        if p.noFormation { continue }                       // accounted for by hand
        if p.status != "present" { known.append(p); continue }
        if p.presentDate == today { continue }              // already accounted for
        queue.append(p)
    }
    let byGroup: JSComparator<Soldier> = { a, b in
        let g = groupOf(a) - groupOf(b)
        return g != 0 ? g : rankCompare(a, b)
    }
    return FormationSplit(queue: queue.stableSorted(byComparator: byGroup),
                          known: known.stableSorted(byComparator: rankCompare))
}

/// `groupOf` for `formationQueue` built from a tree order, the way
/// `startFormation()` builds it: a unit missing from the order goes last.
func formationGroupOf(order: [Int: Int]) -> (Soldier) -> Int {
    { p in order[p.unitId] ?? order.count }
}

/// Who "Everyone else present" would mark: the rest of the queue (from
/// `index` on) that is still unaccounted. Never anyone on a current absence.
/// Pure form of the JS, which reads `formation.ids`, `formation.i` and `personnel`.
func formationRemaining(ids: [Int], from index: Int, people: [Soldier], today: String) -> [Soldier] {
    guard index < ids.count else { return [] }
    var byId: [Int: Soldier] = [:]
    for p in people where byId[p.id] == nil { byId[p.id] = p }   // find(): first match wins
    return ids[max(0, index)...]
        .compactMap { byId[$0] }
        .filter { isUnaccounted($0, today: today) }
}

struct FormationTally: Sendable, Equatable {
    var total: Int
    var present: Int
    var away: Int
    var left: Int
}

/// The finish screen's counts, over everyone who stands in formation.
func formationTally(_ people: [Soldier], today: String) -> FormationTally {
    let roster = people.filter { !$0.noFormation }
    let present = roster.filter { isPresentToday($0, today: today) }.count
    let away = roster.filter { $0.status != "present" }.count
    return FormationTally(total: roster.count, present: present, away: away,
                          left: roster.count - present - away)
}
