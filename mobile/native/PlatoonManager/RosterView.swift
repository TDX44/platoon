import SwiftUI

/// Phase 0: the roster of the signed-in user's unit, as plain text.
struct RosterView: View {
    @Environment(AppModel.self) private var model
    let me: Me

    var body: some View {
        NavigationStack {
            List {
                if let error = model.rosterError {
                    Text(error).foregroundStyle(.red)
                }
                Section("\(model.roster.count) personnel") {
                    ForEach(model.roster) { person in
                        HStack {
                            Text(person.displayName)
                            Spacer()
                            Text(person.status ?? "")
                                .foregroundStyle(.secondary)
                        }
                    }
                }
            }
            .navigationTitle(me.unitName?.isEmpty == false ? me.unitName! : "Roster")
            .refreshable { await model.loadRoster() }
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Menu {
                        Text(me.fullName?.isEmpty == false ? me.fullName! : me.username)
                        Button("Sign out", role: .destructive) { Task { await model.signOut() } }
                    } label: {
                        Image(systemName: "person.crop.circle")
                    }
                }
            }
        }
    }
}
